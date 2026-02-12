"""bsw_competitive_index.py

BrandSafway Competitor Mapping (Client-Ready)
============================================

Goal
----
For each BrandSafway branch (lat/lon), identify nearby competitor locations within a
specified radius (default: 30 miles) using Google Places API, then enrich each
competitor with:
  - Company size bucket (Small 0–49, Medium 50–249, Large 250+ employees; Unknown when evidence is thin)
  - Estimated employee range (if available)
  - Service offerings relevant to BrandSafway (scaffolding, forming/shoring,
    insulation, painting, motorized, specialty)

Enrichment is performed via the McKinsey QuantumBlack OpenAI Gateway using the
Responses API with web_search enabled.

Cost & Speed Optimizations
--------------------------
This script is optimized to keep cost low and run quickly by:
  1) Global de-duplication: enrich each unique Google place_id once (even if it
     appears near multiple branches).
  2) Persistent caching: reuse enrichment results across re-runs.
  3) Concurrency with safe throttling: parallel branch discovery + parallel
     enrichment with global RPS and max in-flight controls.
  4) Optional batch enrichment: send multiple companies per OpenAI call.

Required Environment Variables
------------------------------
  - GOOGLE_PLACES_API_KEY
  - OPENAI_API_KEY
  - OPENAI_BASE_URL

Usage
-----
  python bsw_competitive_index.py --branches branches.csv

branches.csv must contain columns:
  - branch_name
  - latitude
  - longitude

Outputs (in --output_dir)
------------------------
  - competitors_enriched.csv
  - branch_competitor_summary.csv

Notes
-----
- Google Places cannot guarantee returning *every* business in a radius; this
  script uses a tuned set of keyword text searches (with pagination) to maximize
  recall for likely competitors.
- To control cost, consider setting --max_candidates_per_branch and/or a cheaper
  model (default is gpt-4o-mini).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import requests

# The client runtime is expected to have the OpenAI SDK installed.
import openai  # type: ignore

# ---------------------------- Defaults ---------------------------- #

METERS_PER_MILE = 1609.34

# Places API (New) endpoints and text-search pagination
PLACES_TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
PLACES_NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"
MAX_TEXT_SEARCH_PAGES = 3  # Cap pagination per query to control latency/cost
TEXT_SEARCH_PAGE_DELAY_SECONDS = 1.5  # Brief delay before next page (API token validity)

# Include editorialSummary if you decide to expand signals later.
PLACES_FIELDS = (
    "places.id,places.displayName,places.formattedAddress,"
    "places.location,places.primaryType,places.types,"
    "places.websiteUri,places.rating,places.userRatingCount"
)

DEFAULT_KEYWORDS: List[str] = [
    # Scaffolding / Access
    "industrial scaffolding contractor",
    "containment scaffold contractor",
    "swing stage scaffolding",
    "suspended access platform",
    "mast climber contractor",
    "industrial access solutions",

    # Insulation
    "industrial insulation contractor",
    "cryogenic insulation contractor",
    "LNG insulation contractor",

    # Coatings / Blasting
    "industrial coatings contractor",
    "abrasive blasting contractor",
    "protective coatings contractor",
    "fireproofing contractor",

    # Forming & Shoring (restricted)
    "forming and shoring contractor",
    "heavy civil shoring contractor",

    # Specialty (industrial multi-trade / regulated)
    "industrial fireproofing contractor",
    "UL-listed firestopping contractor",
    "industrial refractory contractor",
    "industrial heat tracing contractor",
    "cathodic protection contractor",
    "leak sealing hot tapping contractor",

    # Industrial multi-trade
    "industrial turnaround contractor",
    "industrial maintenance contractor",
]



# Filter out obvious non-competitors (fast, cheap). This is intentionally
# conservative; tune as needed.
BLOCK_TYPES = {
    "restaurant",
    "cafe",
    "bar",
    "bakery",
    "hotel",
    "lodging",
    "shopping_mall",
    "clothing_store",
    "shoe_store",
    "beauty_salon",
    "hair_care",
    "supermarket",
    "grocery_store",
    "pharmacy",
    "hospital",
    "dentist",
    "doctor",
    "school",
    "university",
    "real_estate_agency",
    "insurance_agency",
    "lawyer",
    "accounting",
    "bank",
    "travel_agency",
    "tourist_attraction",
    "gym",
    "fitness_center",
    "spa",
    "church",
    "mosque",
    "synagogue",
    "funeral_home",
    "plumber",
    "electrician",
    "hvac",
    "roofing_contractor",
    "flooring",
    "moving_company",
    "landscaping",
}

EXCLUDE_NAME_SUBSTRINGS = {
    "brandsafway",
    "brand safway",
    "safway",  # removes obvious BrandSafway locations; adjust if overly aggressive
}

DEFAULT_MODEL = "gpt-4o-mini"  # lower cost; override with --model if needed

SYSTEM_PROMPT = (
    "You are an analyst mapping competitors to BrandSafway. "
    "BrandSafway operates in complex industrial environments including nuclear, LNG, "
    "shipyards, heavy civil, semiconductor fabs, and pharmaceutical cleanrooms. "
    "Core services include: industrial scaffolding and containment systems, "
    "forming & shoring (heavy civil only), industrial insulation (including cryogenic), "
    "industrial coatings/blasting/fireproofing, motorized access (MCWP, suspended platforms), "
    "and specialty industrial services (e.g., refractory, heat tracing, cathodic protection, "
    "UL-listed firestopping, leak sealing/hot tapping, lead/asbestos abatement). "
    "Do NOT classify companies focused on residential construction, decorative painting, "
    "residential fireproofing, or generic commercial concrete as competitors. "
    "Be conservative. If industrial scope cannot be verified from credible sources, "
    "mark is_competitor=False and reduce confidence."
)


# Service offering keys; each has a boolean and a _confidence (0-1) in the schema.
SERVICE_KEYS = [
    "scaffolding",
    "forming_shoring",
    "insulation",
    "painting",
    "motorized",
    "specialty",
]

# Size buckets: Small 0-49, Medium 50-249, Large 250+ employees; Unknown when evidence is thin.
SIZE_BUCKETS = ["Small", "Medium", "Large", "Unknown"]
MIN_SERVICE_CONFIDENCE = 0.7  # Service "match" only when confidence >= this.


# ---------------------------- Utilities ---------------------------- #


class RateLimiter:
    """Thread-safe rate limiter enforcing a maximum calls-per-second rate."""

    def __init__(self, calls_per_second: float):
        if calls_per_second <= 0:
            raise ValueError("calls_per_second must be > 0")
        self._interval = 1.0 / calls_per_second
        self._lock = threading.Lock()
        self._last_call = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last_call = time.time()


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two coordinates, in miles."""

    r = 3958.8  # Earth radius in miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return r * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def ensure_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {var_name}")
    return value


def safe_json_dump(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


# ---------------------------- Data Models ---------------------------- #


@dataclass(frozen=True)
class Branch:
    branch_name: str
    latitude: float
    longitude: float
    region: str = ""


@dataclass
class PlaceCandidate:
    branch_name: str
    region: str
    branch_latitude: float
    branch_longitude: float

    place_id: str
    company_name: str
    address: str
    latitude: float
    longitude: float
    website: str
    primary_type: str
    types: str
    rating: Optional[float]
    user_rating_count: Optional[int]

    distance_miles: float


# ---------------------------- Persistent Cache ---------------------------- #


class JsonCache:
    """Enrichment cache keyed by place_id, with optional TTL. Persists OpenAI enrichment across runs to avoid re-calling the API for the same place."""

    def __init__(self, path: Path, ttl_days: Optional[int] = None):
        self._path = path
        self._ttl_seconds = ttl_days * 86400 if ttl_days else None
        self._lock = threading.Lock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            self._data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Failed to read cache file; starting with empty cache: %s", self._path)
            self._data = {}

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            if self._ttl_seconds:
                ts = float(entry.get("ts", 0.0))
                if (time.time() - ts) > self._ttl_seconds:
                    return None
            return entry.get("value")

    def set(self, key: str, value: Dict[str, Any]) -> None:
        with self._lock:
            self._data[key] = {"ts": time.time(), "value": value}
            self._dirty = True

    def save(self) -> None:
        with self._lock:
            if not self._dirty:
                return
            safe_json_dump(self._path, self._data)
            self._dirty = False


# ---------------------------- Google Places Client ---------------------------- #


class PlacesClient:
    def __init__(
        self,
        api_key: str,
        calls_per_second: float,
        radius_miles: float,
        timeout_s: int = 30,
        max_retries: int = 4,
    ):
        self._api_key = api_key
        self._limiter = RateLimiter(calls_per_second)
        self._radius_meters = int(radius_miles * METERS_PER_MILE)
        self._timeout_s = timeout_s
        self._max_retries = max_retries
        self._local = threading.local()

    def _session(self) -> requests.Session:
        if getattr(self._local, "session", None) is None:
            self._local.session = requests.Session()
        return self._local.session

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self._api_key,
            "X-Goog-FieldMask": PLACES_FIELDS,
        }

    def _post(self, url: str, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST with retry/backoff on transient errors."""

        backoff = 1.0
        for attempt in range(self._max_retries):
            self._limiter.wait()
            try:
                resp = self._session().post(url, headers=self._headers(), json=body, timeout=self._timeout_s)
                if resp.status_code == 400:
                    raise requests.HTTPError(f"HTTP 400 Bad Request: {resp.text[:500]}")
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code}: {resp.text[:200]}")
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                if attempt == self._max_retries - 1:
                    raise
                logging.warning("Places request failed (attempt %s/%s): %s", attempt + 1, self._max_retries, e)
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
        raise RuntimeError("Unreachable")

    def search_text(self, lat: float, lon: float, query: str) -> List[Dict[str, Any]]:
        # searchText only supports locationBias with circle (not locationRestriction.circle).
        # Paginate with nextPageToken (cap at MAX_TEXT_SEARCH_PAGES) and de-dupe by place_id.
        body: Dict[str, Any] = {
            "textQuery": query,
            "locationBias": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": self._radius_meters,
                }
            },
        }
        seen_ids: set = set()
        all_places: List[Dict[str, Any]] = []
        for _ in range(MAX_TEXT_SEARCH_PAGES):
            resp = self._post(PLACES_TEXT_URL, body)
            for p in resp.get("places", []):
                pid = p.get("id")
                if pid and pid not in seen_ids:
                    seen_ids.add(pid)
                    all_places.append(p)
            next_token = resp.get("nextPageToken")
            if not next_token:
                break
            time.sleep(TEXT_SEARCH_PAGE_DELAY_SECONDS)
            body = {**body, "pageToken": next_token}
        return all_places

    def search_nearby(self, lat: float, lon: float, included_types: List[str]) -> List[Dict[str, Any]]:
        """Nearby type search. Kept for possible future use; discovery currently uses text search only."""
        body = {
            "includedTypes": included_types,
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": self._radius_meters,
                }
            },
        }
        return self._post(PLACES_NEARBY_URL, body).get("places", [])


# ---------------------------- OpenAI Enrichment ---------------------------- #


def _extract_output_text(response: Any) -> str:
    """Robustly extract text output from an OpenAI Responses API object."""

    if hasattr(response, "output_text"):
        return response.output_text  # type: ignore[attr-defined]

    # Fallback: walk the response.output structure
    parts: List[str] = []
    for item in getattr(response, "output", []) or []:
        if getattr(item, "type", None) != "message":
            continue
        for content in getattr(item, "content", []) or []:
            if getattr(content, "type", None) in ("output_text", "text"):
                parts.append(getattr(content, "text", ""))
    return "".join(parts)


class OpenAIEnricher:
    """Enriches companies using Responses API + web_search, with throttling and caching."""

    def __init__(
        self,
        api_key: str,
        base_url: str,
        model: str,
        calls_per_second: float,
        max_in_flight: int,
        cache: JsonCache,
        batch_size: int = 1,
        max_retries: int = 3,
    ):
        self._api_key = api_key
        self._base_url = base_url
        self._model = model
        self._limiter = RateLimiter(calls_per_second)
        self._sema = threading.BoundedSemaphore(max_in_flight)
        self._cache = cache
        self._batch_size = max(1, batch_size)
        self._max_retries = max_retries
        self._local = threading.local()

    def _client(self) -> Any:
        if getattr(self._local, "client", None) is None:
            self._local.client = openai.OpenAI(api_key=self._api_key, base_url=self._base_url)
        return self._local.client

    @staticmethod
    def _service_offerings_schema() -> Tuple[Dict[str, Any], List[str]]:
        """Build service_offerings object schema: each service has bool, _confidence (0-1), _evidence (string)."""
        props: Dict[str, Any] = {}
        req: List[str] = []
        for k in SERVICE_KEYS:
            props[k] = {"type": "boolean"}
            props[k + "_confidence"] = {"type": "number", "minimum": 0.0, "maximum": 1.0}
            props[k + "_evidence"] = {"type": "string"}
            req.extend([k, k + "_confidence", k + "_evidence"])
        return (
            {
                "type": "object",
                "properties": props,
                "required": req,
                "additionalProperties": False,
            },
            req,
        )

    @staticmethod
    def _single_schema() -> Dict[str, Any]:
        svc_schema, _ = OpenAIEnricher._service_offerings_schema()
        return {
            "type": "object",
            "properties": {
                "is_competitor": {"type": "boolean"},
                "size_bucket": {
                    "type": "string",
                    "enum": SIZE_BUCKETS,
                },
                "employee_estimate": {"type": "string"},
                "size_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                "size_evidence": {"type": "string"},
                "service_offerings": svc_schema,
                "summary": {"type": "string"},
                "top_sources": {"type": "array", "items": {"type": "string"}},
                "citations": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            },
            "required": [
                "is_competitor",
                "size_bucket",
                "employee_estimate",
                "size_confidence",
                "size_evidence",
                "service_offerings",
                "summary",
                "top_sources",
                "citations",
                "confidence",
            ],
            "additionalProperties": False,
        }

    @staticmethod
    def _batch_schema() -> Dict[str, Any]:
        svc_schema, _ = OpenAIEnricher._service_offerings_schema()
        batch_item = {
            "type": "object",
            "properties": {
                "place_id": {"type": "string"},
                "is_competitor": {"type": "boolean"},
                "size_bucket": {
                    "type": "string",
                    "enum": SIZE_BUCKETS,
                },
                "employee_estimate": {"type": "string"},
                "size_confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                "size_evidence": {"type": "string"},
                "service_offerings": svc_schema,
                "summary": {"type": "string"},
                "top_sources": {"type": "array", "items": {"type": "string"}},
                "citations": {"type": "string"},
                "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
            },
            "required": [
                "place_id",
                "is_competitor",
                "size_bucket",
                "employee_estimate",
                "size_confidence",
                "size_evidence",
                "service_offerings",
                "summary",
                "top_sources",
                "citations",
                "confidence",
            ],
            "additionalProperties": False,
        }
        return {
            "type": "object",
            "properties": {
                "results": {"type": "array", "items": batch_item},
            },
            "required": ["results"],
            "additionalProperties": False,
        }

    def get_cached(self, place_id: str) -> Optional[Dict[str, Any]]:
        return self._cache.get(place_id)

    def enrich_missing(self, places: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Enrich a list of places (dicts with place_id/name/website/address). Uses batching if configured."""

        missing: List[Dict[str, Any]] = []
        results: Dict[str, Dict[str, Any]] = {}

        for p in places:
            pid = p["place_id"]
            cached = self.get_cached(pid)
            if cached is not None:
                results[pid] = cached
            else:
                missing.append(p)

        if not missing:
            return results

        if self._batch_size <= 1:
            for p in missing:
                results[p["place_id"]] = self._enrich_one_with_retry(p)
        else:
            for batch in chunked(missing, self._batch_size):
                batch_results = self._enrich_batch_with_retry(batch)
                for pid, val in batch_results.items():
                    results[pid] = val

        return results

    def _enrich_one_with_retry(self, place: Dict[str, Any]) -> Dict[str, Any]:
        backoff = 1.0
        for attempt in range(self._max_retries):
            try:
                return self._enrich_one(place)
            except Exception as e:
                if attempt == self._max_retries - 1:
                    logging.error("OpenAI enrichment failed for %s: %s", place.get("place_id"), e)
                    break
                logging.warning("OpenAI enrichment retry for %s (attempt %s/%s): %s", place.get("place_id"), attempt + 1, self._max_retries, e)
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

        # Fallback on hard failure
        fallback = {
            "is_competitor": False,
            "size_bucket": "Unknown",
            "employee_estimate": "",
            "size_confidence": 0.0,
            "size_evidence": "",
            "service_offerings": _default_service_offerings(),
            "summary": "",
            "top_sources": [],
            "citations": "",
            "confidence": 0.0,
        }
        self._cache.set(place["place_id"], fallback)
        return fallback

    def _enrich_batch_with_retry(self, places: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        backoff = 1.0
        for attempt in range(self._max_retries):
            try:
                return self._enrich_batch(places)
            except Exception as e:
                if attempt == self._max_retries - 1:
                    logging.error("OpenAI batch enrichment failed (size=%s): %s", len(places), e)
                    break
                logging.warning(
                    "OpenAI batch enrichment retry (attempt %s/%s, size=%s): %s",
                    attempt + 1,
                    self._max_retries,
                    len(places),
                    e,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

        # Fallback for batch
        out: Dict[str, Dict[str, Any]] = {}
        for p in places:
            out[p["place_id"]] = {
                "is_competitor": False,
                "size_bucket": "Unknown",
                "employee_estimate": "",
                "size_confidence": 0.0,
                "size_evidence": "",
                "service_offerings": _default_service_offerings(),
                "summary": "",
                "top_sources": [],
                "citations": "",
                "confidence": 0.0,
            }
            self._cache.set(p["place_id"], out[p["place_id"]])
        return out

    def _enrich_one(self, place: Dict[str, Any]) -> Dict[str, Any]:
        pid = place["place_id"]
        name = place.get("company_name", "")
        website = place.get("website", "")
        address = place.get("address", "")

        prompt = (
            f"Company: {name}\n"
            f"Website: {website or 'N/A'}\n"
            f"Address: {address or 'N/A'}\n\n"
            "Task:\n"
            "1) Determine whether this company competes with BrandSafway in industrial access or related services.\n"
            "2) For each service type, set the boolean, _confidence (0.0–1.0), and _evidence (short text with helpful keywords):\n"
            "   - scaffolding: industrial/containment/engineered access; scaffolding_evidence e.g. 'tube-and-clamp, shoring, containment'\n"
            "   - forming_shoring: heavy civil/structural only; forming_shoring_evidence e.g. 'bridge, dam, nuclear formwork'\n"
            "   - insulation: industrial/cryogenic/LNG; insulation_evidence e.g. 'pipe insulation, cryogenic, refractory'\n"
            "   - painting: industrial coatings/blasting/fireproofing; painting_evidence e.g. 'protective coatings, sandblasting, fireproofing'\n"
            "   - motorized: MCWP, mast climbers, suspended platforms; motorized_evidence e.g. 'mast climber, swing stage, BMU'\n"
            "   - specialty: refractory, heat tracing, cathodic protection, UL firestopping, leak sealing, abatement; specialty_evidence with keywords\n"
            "3) Size bucket: Small (0–49), Medium (50–249), Large (250+), or Unknown. size_confidence (0–1), size_evidence (brief). employee_estimate: free-text range when known.\n"
            "4) citations: List ALL web-searched URLs or source references used (semicolon-separated) for full traceability.\n\n"
            "Guardrails: No decorative/residential-only; Forming & Shoring only for heavy civil/nuclear; Specialty only for industrial/regulated. Confidence >= 0.7 = solid match."
        )

        with self._sema:
            self._limiter.wait()
            response = self._client().responses.create(
                model=self._model,
                tools=[{"type": "web_search"}],
                include=["web_search_call.action.sources"],
                instructions=SYSTEM_PROMPT,
                input=prompt,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "company_profile",
                        "schema": self._single_schema(),
                        "strict": True,
                    }
                },
            )

        data = json.loads(_extract_output_text(response))
        self._cache.set(pid, data)
        return data

    def _enrich_batch(self, places: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Batch enrichment: multiple companies per OpenAI call (optional)."""

        companies_payload = [
            {
                "place_id": p["place_id"],
                "company_name": p.get("company_name", ""),
                "website": p.get("website", ""),
                "address": p.get("address", ""),
            }
            for p in places
        ]

        prompt = (
            "You will be given a JSON list of companies. For EACH company, use web search to determine:\n"
            "(a) is_competitor: whether it competes with BrandSafway.\n"
            "(b) Size bucket: Small (0–49), Medium (50–249), Large (250+), or Unknown. size_confidence (0–1), size_evidence (brief). employee_estimate: free-text range when known.\n"
            "(c) For each service (scaffolding, forming_shoring, insulation, painting, motorized, specialty): set the boolean, _confidence (0–1), and _evidence (short text with helpful keywords, e.g. 'containment, tube-and-clamp' for scaffolding). Only confidence >= 0.7 counts as a solid match.\n"
            "(d) citations: List ALL web-searched URLs/sources used for this company (semicolon-separated) for traceability.\n"
            "Return one object per input company with the same place_id. Populate top_sources with up to 3 URLs and citations with all sources used.\n\n"
            f"INPUT_COMPANIES_JSON:\n{json.dumps(companies_payload, ensure_ascii=False)}"
        )

        with self._sema:
            self._limiter.wait()
            response = self._client().responses.create(
                model=self._model,
                tools=[{"type": "web_search"}],
                include=["web_search_call.action.sources"],
                instructions=SYSTEM_PROMPT,
                input=prompt,
                text={
                    "format": {
                        "type": "json_schema",
                        "name": "company_profile_batch",
                        "schema": self._batch_schema(),
                        "strict": True,
                    }
                },
            )

        payload = json.loads(_extract_output_text(response))
        out: Dict[str, Dict[str, Any]] = {}

        for item in payload.get("results", []) or []:
            pid = item.get("place_id")
            if not pid:
                continue
            # Remove place_id from cached value to keep cache compact
            value = {k: v for k, v in item.items() if k != "place_id"}
            self._cache.set(pid, value)
            out[pid] = value

        # Ensure every input has an output
        for p in places:
            pid = p["place_id"]
            if pid not in out:
                out[pid] = self._cache.get(pid) or {
                    "is_competitor": False,
                    "size_bucket": "Unknown",
                    "employee_estimate": "",
                    "size_confidence": 0.0,
                    "size_evidence": "",
                    "service_offerings": _default_service_offerings(),
                    "summary": "",
                    "top_sources": [],
                    "citations": "",
                    "confidence": 0.0,
                }
        return out


# ---------------------------- Competitor Discovery ---------------------------- #


def is_blocked(place: Dict[str, Any]) -> bool:
    """Fast filter for obvious non-competitors."""

    types = set(place.get("types") or [])
    if types & BLOCK_TYPES:
        return True

    name = normalize_text(((place.get("displayName") or {}).get("text") or ""))
    if any(sub in name for sub in EXCLUDE_NAME_SUBSTRINGS):
        return True

    return False


# ---------------------------- Prefilter (Cost Control) ---------------------------- #

# The OpenAI web_search enrichment step is the primary cost driver. This lightweight
# prefilter reduces cost by skipping enrichment for low-signal candidates.
#
# Design goals:
# - Conservative: avoid filtering out true competitors (when in doubt, do not skip).
# - Explainable: produce a simple score + reasons for auditing.
# - Cheap: pure string/type heuristics (no external calls).
# - Company-name cache: reuse prefilter outcome by normalized company name across runs.

def normalize_company_name(company_name: str) -> str:
    """Normalize company name for cache key: lowercased, stripped, single spaces."""
    return " ".join((company_name or "").strip().lower().split())


class PrefilterCache:
    """Persistent cache of prefilter results keyed by normalized company name. Separate from enrichment cache because the key is company name, not place_id, and the value is prefilter score/reasons only."""

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            self._data = json.loads(self._path.read_text(encoding="utf-8"))
        except Exception:
            logging.warning("Failed to read prefilter cache; starting empty: %s", self._path)
            self._data = {}

    def get(self, normalized_name: str) -> Optional[Dict[str, Any]]:
        """Return cached {score, reasons} or None."""
        with self._lock:
            return self._data.get(normalized_name)

    def set(self, normalized_name: str, score: int, reasons: str) -> None:
        with self._lock:
            self._data[normalized_name] = {"score": score, "reasons": reasons}
            self._dirty = True

    def save(self) -> None:
        with self._lock:
            if not self._dirty:
                return
            safe_json_dump(self._path, self._data)
            self._dirty = False

# Strong service hints that frequently indicate BrandSafway-like competitors.
PREFILTER_STRONG_TERMS = {
    # Access / Scaffold / Motorized
    "scaffold",
    "scaffolding",
    "swing stage",
    "swingstage",
    "mast climber",
    "mastclimber",
    "shoring",
    "forming",
    "formwork",
    "temporary access",
    "access solutions",
    "work platform",
    "work platforms",
    "motorized access",

    # Insulation
    "industrial insulation",
    "insulation",

    # Coatings / Fireproofing
    "fireproofing",
    "coatings",
    "coating",
    "protective coating",
    "protective coatings",
    "abrasive blasting",
    "sandblasting",
    "linings",
    "lining",
    "chemical resistant",

    # Specialty (industrial multi-trade)
    "refractory",
    "heat tracing",
    "cathodic protection",
    "cp",  # keep for recall; model should confirm context via enrichment
    "firestopping",
    "fire stopping",
    "ul-listed firestopping",
    "intumescent",
    "leak sealing",
    "leak detection",
    "hot tapping",
    "frp",
    "abatement",
    "asbestos abatement",
    "lead abatement",

    # Nuclear
    "nuclear",
    "containment",
    "reactor",
    "turbine building",
    "nqa-1",
    "alara",

    # LNG
    "lng",
    "cryogenic",
    "cold box",
    "mche",
    "compressor building",

    # Shipyard
    "dry dock",
    "shipyard",

    # Industrial coating certifications
    "ampp",
    "qp",
    "qs",

    # Pharma / cleanroom
    "cleanroom",
    "gmp",

    # Federal
    "em 385",
    "doe",
    "dod",
}


# Context terms that can support the strong terms (weak signal by themselves).
PREFILTER_CONTEXT_TERMS = {
    "industrial",
    "commercial",
    "contractor",
    "construction",
    "rental",
    "rentals",
    "equipment",
    "services",
    "plant",
    "refinery",
}

# Trade terms that usually indicate *non-competitors* (negative weight). Keep this
# list modest to avoid false negatives.
PREFILTER_NEGATIVE_TERMS = {
    "plumbing",
    "plumber",
    "electric",
    "electrical",
    "hvac",
    "landscaping",
    "lawn",
    "roofing",
    "roofer",
    "paving",
    "asphalt",
    "concrete",
    "flooring",
    "carpet",
    "fencing",
    "window",
    "windows",
    "door",
    "doors",
    "kitchen",
    "bath",
    "residential",
    "home remodeling",
    "interior painting",
    "house painting",
    "driveway",
}

# Place types that can indicate relevance. These are intentionally broad; the score
# threshold controls whether they are enough to trigger enrichment.
PREFILTER_RELEVANT_TYPES = {
    "equipment_rental_agency",
    "general_contractor",
    "construction_company",
    "industrial_equipment_supplier",
}


def _as_type_set(types_val: Any) -> set:
    """Normalize a Places `types` value into a set[str]."""

    if types_val is None:
        return set()
    if isinstance(types_val, list):
        return {str(t).strip() for t in types_val if str(t).strip()}
    if isinstance(types_val, str):
        return {t.strip() for t in types_val.split(",") if t.strip()}
    return {str(types_val).strip()} if str(types_val).strip() else set()


def prefilter_score(place_payload: Dict[str, Any]) -> Tuple[int, List[str]]:
    """Compute a cheap relevance score for a potential competitor.

    Returns:
        (score, reasons)

    Scoring (conservative):
      +3 per strong service hint (capped at 2 hits)
      +1 per context term (capped at 2 hits)
      +1 if any relevant place type is present
      -2 if any negative trade hint is present (capped at 1 hit)

    The intent is not perfect classification; it is a cost-control gate that
    filters only low-signal items.
    """

    name = normalize_text(place_payload.get("company_name") or "")
    website = normalize_text(place_payload.get("website") or "")
    address = normalize_text(place_payload.get("address") or "")
    primary_type = normalize_text(place_payload.get("primary_type") or "")

    types_set = _as_type_set(place_payload.get("types"))

    haystack = " ".join([name, website, address, primary_type, " ".join(sorted(types_set))])

    score = 0
    reasons: List[str] = []

    strong_hits = [t for t in PREFILTER_STRONG_TERMS if t in haystack]
    if strong_hits:
        # cap to avoid huge scores from repeated tokens
        hits = strong_hits[:2]
        score += 3 * len(hits)
        reasons.append(f"strong={','.join(hits)}")

    ctx_hits = [t for t in PREFILTER_CONTEXT_TERMS if t in haystack]
    if ctx_hits:
        hits = ctx_hits[:2]
        score += 1 * len(hits)
        reasons.append(f"context={','.join(hits)}")

    if types_set & PREFILTER_RELEVANT_TYPES:
        score += 1
        reasons.append(f"types={','.join(sorted(types_set & PREFILTER_RELEVANT_TYPES))}")

    neg_hits = [t for t in PREFILTER_NEGATIVE_TERMS if t in haystack]
    if neg_hits:
        score -= 2
        reasons.append(f"negative={neg_hits[0]}")

    if not reasons:
        reasons.append("no_signals")

    return score, reasons


def prefilter_fallback(score: int, reasons: List[str], min_score: int) -> Dict[str, Any]:
    """Fallback enrichment record for prefiltered-out items."""
    reason_str = ";".join(reasons[:3])
    return {
        "is_competitor": False,
        "size_bucket": "Unknown",
        "employee_estimate": "",
        "size_confidence": 0.0,
        "size_evidence": "",
        "service_offerings": _default_service_offerings(),
        "summary": f"Prefilter skipped enrichment (score={score} < {min_score}). Reasons: {reason_str}.",
        "top_sources": [],
        "citations": "",
        "confidence": 0.0,
    }


def place_to_candidate(branch: Branch, place: Dict[str, Any], radius_miles: float) -> Optional[PlaceCandidate]:
    place_id = place.get("id")
    if not place_id:
        return None

    loc = place.get("location") or {}
    plat = loc.get("latitude")
    plon = loc.get("longitude")
    if plat is None or plon is None:
        return None

    dist = haversine_miles(branch.latitude, branch.longitude, float(plat), float(plon))
    if dist > radius_miles:
        return None

    name = ((place.get("displayName") or {}).get("text") or "").strip()
    address = (place.get("formattedAddress") or "").strip()

    return PlaceCandidate(
        branch_name=branch.branch_name,
        region=getattr(branch, "region", "") or "",
        branch_latitude=branch.latitude,
        branch_longitude=branch.longitude,
        place_id=place_id,
        company_name=name,
        address=address,
        latitude=float(plat),
        longitude=float(plon),
        website=(place.get("websiteUri") or "").strip(),
        primary_type=(place.get("primaryType") or ""),
        types=",".join(place.get("types") or []),
        rating=place.get("rating"),
        user_rating_count=place.get("userRatingCount"),
        distance_miles=round(dist, 3),
    )


def discover_branch_candidates(
    branch: Branch,
    places: PlacesClient,
    keywords: List[str],
    radius_miles: float,
    max_candidates_per_branch: Optional[int],
) -> List[PlaceCandidate]:
    """Find candidates near a branch via targeted Places searches."""

    discovered: Dict[str, Dict[str, Any]] = {}

    # 1) Keyword text search (tuned for BrandSafway-like competitors)
    for kw in keywords:
        try:
            for p in places.search_text(branch.latitude, branch.longitude, kw):
                pid = p.get("id")
                if pid and pid not in discovered:
                    discovered[pid] = p
        except Exception as e:
            logging.warning("Branch %s keyword '%s' search failed: %s", branch.branch_name, kw, e)

    candidates: List[PlaceCandidate] = []
    for p in discovered.values():
        if is_blocked(p):
            continue
        cand = place_to_candidate(branch, p, radius_miles)
        if cand:
            candidates.append(cand)

    # Sort by proximity (useful for capping)
    candidates.sort(key=lambda x: x.distance_miles)

    if max_candidates_per_branch and max_candidates_per_branch > 0 and len(candidates) > max_candidates_per_branch:
        candidates = candidates[:max_candidates_per_branch]

    return candidates


# ---------------------------- Outputs & Summaries ---------------------------- #


def _default_service_offerings() -> Dict[str, Any]:
    """Default service_offerings: each service has bool False, confidence 0, evidence ''."""
    out: Dict[str, Any] = {}
    for k in SERVICE_KEYS:
        out[k] = False
        out[k + "_confidence"] = 0.0
        out[k + "_evidence"] = ""
    return out


def _citations_string(enrichment: Dict[str, Any]) -> str:
    """Build citations string: only non-empty URLs, joined by '; '. No empty semicolons."""
    urls: List[str] = []
    raw = enrichment.get("citations")
    if isinstance(raw, str) and raw.strip():
        urls = [u.strip() for u in raw.split(";") if u.strip()]
    if not urls:
        for u in enrichment.get("top_sources") or []:
            if isinstance(u, str) and u.strip():
                urls.append(u.strip())
    return "; ".join(urls) if urls else ""


def flatten_enrichment(enrichment: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested enrichment into a single-level dict for CSV.
    Outputs ChatGPT schema: for each service, {service}, {service}_confidence, {service}_evidence.
    citations is last and contains only 'url; url; url' when found (no empty semicolons).
    """
    svc = dict(enrichment.get("service_offerings") or {})
    for k in SERVICE_KEYS:
        if k + "_confidence" not in svc:
            svc[k + "_confidence"] = 0.0
        if k + "_evidence" not in svc:
            svc[k + "_evidence"] = ""

    flat: Dict[str, Any] = {
        "is_competitor": enrichment.get("is_competitor"),
        "size_bucket": enrichment.get("size_bucket"),
        "employee_estimate": enrichment.get("employee_estimate", ""),
        "size_confidence": enrichment.get("size_confidence") if enrichment.get("size_confidence") is not None else 0.0,
        "size_evidence": enrichment.get("size_evidence", ""),
        "summary": enrichment.get("summary", ""),
        "confidence": enrichment.get("confidence"),
    }
    for k in SERVICE_KEYS:
        flat[k] = bool(svc.get(k))
        flat[k + "_confidence"] = float(svc.get(k + "_confidence", 0.0))
        flat[k + "_evidence"] = svc.get(k + "_evidence", "") or ""
    flat["citations"] = _citations_string(enrichment)
    return flat


def build_branch_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Create branch-level competitor summary stats."""

    # Focus summary on competitors if the column exists; otherwise include all.
    if "is_competitor" in df.columns:
        dfc = df[df["is_competitor"] == True].copy()  # noqa: E712
    else:
        dfc = df.copy()

    if dfc.empty:
        return pd.DataFrame(columns=["branch_name", "competitor_count"])  # empty but valid

    base = (
        dfc.groupby("branch_name", as_index=False)
        .agg(
            competitor_count=("place_id", "nunique"),
            avg_distance_miles=("distance_miles", "mean"),
            median_distance_miles=("distance_miles", "median"),
        )
    )
    # Add region as second column if present in input
    if "region" in dfc.columns:
        region_df = dfc[["branch_name", "region"]].drop_duplicates("branch_name")
        base = base.merge(region_df, on="branch_name", how="left")
        cols = ["branch_name", "region"] + [c for c in base.columns if c not in ("branch_name", "region")]
        base = base[cols]

    # Size bucket counts
    if "size_bucket" in dfc.columns:
        size_counts = (
            dfc.pivot_table(
                index="branch_name",
                columns="size_bucket",
                values="place_id",
                aggfunc="nunique",
                fill_value=0,
            )
            .reset_index()
        )
        # Normalize column names
        size_counts.columns = [
            ("size_" + str(c).lower().replace(" ", "_") if c != "branch_name" else c)
            for c in size_counts.columns
        ]
        base = base.merge(size_counts, on="branch_name", how="left")

    # Service counts: high-confidence (>=0.7) matches only (schema columns: scaffolding, scaffolding_confidence, etc.)
    svc_df = dfc[["branch_name"]].copy()
    for k in SERVICE_KEYS:
        if k in dfc.columns and (k + "_confidence") in dfc.columns:
            high = (dfc[k] == True) & (dfc[k + "_confidence"] >= MIN_SERVICE_CONFIDENCE)  # noqa: E712
            svc_df[f"count_{k}_high_confidence"] = high.astype(int)
    if len(svc_df.columns) > 1:
        svc_counts = svc_df.groupby("branch_name", as_index=False).sum()
        base = base.merge(svc_counts, on="branch_name", how="left")

    # Closest competitors (top 10)
    def _closest(group: pd.DataFrame) -> str:
        g = group.sort_values("distance_miles").head(10)
        return "; ".join(f"{n} ({d:.1f}mi)" for n, d in zip(g["company_name"], g["distance_miles"]))

    closest = (
        dfc.groupby("branch_name")
        .apply(_closest)
        .reset_index(name="closest_competitors")
    )

    base = base.merge(closest, on="branch_name", how="left")
    return base


# ---------------------------- Main Orchestration ---------------------------- #


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Map BrandSafway competitors around branch locations")
    p.add_argument("--branches", required=True, help="Branch input: .xlsx (branch_name, latitude, longitude, region) or .csv (branch_name, latitude, longitude; optional region)")
    p.add_argument("--output_dir", default="out", help="Directory for output CSVs")

    p.add_argument("--radius_miles", type=float, default=30.0)
    p.add_argument("--keywords", default="", help="Optional semicolon-separated keyword overrides")

    # Cost/speed controls
    p.add_argument("--max_candidates_per_branch", type=int, default=75, help="Cap candidates per branch to control cost (0 = no cap)")

    # Prefilter (cost-control). Disable to maximize recall at higher cost.
    p.add_argument("--disable_prefilter", action="store_true", help="Disable heuristic prefilter (higher cost, potentially higher recall)")
    p.add_argument("--prefilter_min_score", type=int, default=0, help="Minimum prefilter score to run OpenAI enrichment when not cached")
    p.add_argument("--prefilter_cache_path", default="prefilter_cache.json", help="Path to persistent prefilter cache (keyed by normalized company name)")

    # Concurrency controls
    p.add_argument("--branch_workers", type=int, default=8, help="Parallelism for branch discovery")
    p.add_argument("--enrich_workers", type=int, default=12, help="Parallelism for enrichment tasks")

    # Throttling controls
    p.add_argument("--places_rps", type=float, default=5.0, help="Max Places calls/sec across all threads")
    p.add_argument("--openai_rps", type=float, default=1.5, help="Max OpenAI calls/sec across all threads")
    p.add_argument("--openai_max_in_flight", type=int, default=6, help="Max concurrent OpenAI requests")

    # OpenAI options
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--openai_batch_size", type=int, default=3, help="Companies per OpenAI call (1 = no batching)")

    # Cache
    p.add_argument("--cache_path", default="enrichment_cache.json", help="Path to persistent cache JSON")
    p.add_argument("--cache_ttl_days", type=int, default=90, help="Cache TTL in days (0 = no TTL)")

    p.add_argument("--log_level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return p.parse_args()


def load_branches(path: Path) -> List[Branch]:
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path, engine="openpyxl")
        required = {"branch_name", "latitude", "longitude", "region"}
    else:
        df = pd.read_csv(path)
        required = {"branch_name", "latitude", "longitude"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"branches file missing columns: {sorted(missing)}")

    branches: List[Branch] = []
    for _, row in df.iterrows():
        region = str(row.get("region", "")).strip() if "region" in df.columns else ""
        branches.append(
            Branch(
                branch_name=str(row["branch_name"]),
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                region=region,
            )
        )
    return branches


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Validate required env vars early
    places_key = ensure_env("GOOGLE_PLACES_API_KEY")
    openai_key = ensure_env("OPENAI_API_KEY")
    openai_base_url = ensure_env("OPENAI_BASE_URL")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    keywords = DEFAULT_KEYWORDS
    if args.keywords.strip():
        keywords = [k.strip() for k in args.keywords.split(";") if k.strip()]

    cache_ttl = None if args.cache_ttl_days == 0 else args.cache_ttl_days
    cache = JsonCache(Path(args.cache_path), ttl_days=cache_ttl)

    places = PlacesClient(
        api_key=places_key,
        calls_per_second=args.places_rps,
        radius_miles=args.radius_miles,
    )

    enricher = OpenAIEnricher(
        api_key=openai_key,
        base_url=openai_base_url,
        model=args.model,
        calls_per_second=args.openai_rps,
        max_in_flight=args.openai_max_in_flight,
        cache=cache,
        batch_size=args.openai_batch_size,
    )

    branches = load_branches(Path(args.branches))
    logging.info("Loaded %s branches", len(branches))

    # ------------------ Phase 1: Discover candidates (parallel per branch) ------------------ #

    all_candidates: List[PlaceCandidate] = []

    logging.info("Discovering candidates (branch_workers=%s)...", args.branch_workers)
    with ThreadPoolExecutor(max_workers=max(1, args.branch_workers)) as ex:
        futs = {
            ex.submit(
                discover_branch_candidates,
                b,
                places,
                keywords,
                args.radius_miles,
                None if args.max_candidates_per_branch == 0 else args.max_candidates_per_branch,
            ): b
            for b in branches
        }

        for fut in as_completed(futs):
            b = futs[fut]
            try:
                cands = fut.result()
                all_candidates.extend(cands)
                logging.info("Branch %s: %s candidates", b.branch_name, len(cands))
            except Exception as e:
                logging.error("Branch %s discovery failed: %s", b.branch_name, e)

    if not all_candidates:
        logging.warning("No candidates found. Exiting.")
        return

    logging.info("Total branch-place candidates: %s", len(all_candidates))

    # ------------------ Phase 2: Global de-dupe places and enrich (parallel) ------------------ #

    unique_places: Dict[str, Dict[str, Any]] = {}
    for c in all_candidates:
        if c.place_id not in unique_places:
            unique_places[c.place_id] = {
                "place_id": c.place_id,
                "company_name": c.company_name,
                "website": c.website,
                "address": c.address,
                "primary_type": c.primary_type,
                "types": c.types,
            }

    place_list = list(unique_places.values())
    logging.info("Unique places to enrich (before cache): %s", len(place_list))

    # Prefilter gate (cost control): only run OpenAI enrichment on places with
    # sufficient signal, unless already enrichment-cached. Uses company-name cache.
    prefilter_cache = PrefilterCache(Path(args.prefilter_cache_path))
    prefilter_meta: Dict[str, Dict[str, Any]] = {}
    prefilter_skipped: Set[str] = set()
    enrich_pool: List[Dict[str, Any]] = []

    min_score = int(args.prefilter_min_score)
    prefilter_enabled = not args.disable_prefilter

    for p in place_list:
        pid = p["place_id"]
        enrichment_cached = enricher.get_cached(pid)
        if enrichment_cached is not None:
            enrich_pool.append(p)
            continue

        if not prefilter_enabled:
            enrich_pool.append(p)
            continue

        normalized_name = normalize_company_name(p.get("company_name") or "")
        cached_result = prefilter_cache.get(normalized_name)
        if cached_result is not None:
            score = int(cached_result.get("score", 0))
            reasons_str = cached_result.get("reasons", "no_signals")
            reasons = reasons_str.split(";") if isinstance(reasons_str, str) else list(reasons_str)
        else:
            score, reasons = prefilter_score(p)
            prefilter_cache.set(normalized_name, score, ";".join(reasons))

        prefilter_meta[pid] = {
            "prefilter_score": score,
            "prefilter_reasons": ";".join(reasons) if isinstance(reasons, list) else str(reasons),
            "prefilter_skipped": score < min_score,
        }
        if score >= min_score:
            enrich_pool.append(p)
        else:
            prefilter_skipped.add(pid)

    # Determine which are cache misses among the enrichment pool
    cache_misses = [p for p in enrich_pool if enricher.get_cached(p["place_id"]) is None]
    logging.info("Cache misses requiring OpenAI calls (after prefilter): %s", len(cache_misses))
    if prefilter_enabled:
        logging.info("Prefilter skipped (not cached): %s", len(prefilter_skipped))

    enriched_by_place_id: Dict[str, Dict[str, Any]] = {}

    if cache_misses:
        # Parallel enrichment. Each worker processes one batch (or one company).
        batches = list(chunked(cache_misses, max(1, args.openai_batch_size))) if args.openai_batch_size > 1 else [[p] for p in cache_misses]

        logging.info(
            "Enriching with OpenAI (enrich_workers=%s, batch_size=%s, openai_rps=%s, max_in_flight=%s)...",
            args.enrich_workers,
            args.openai_batch_size,
            args.openai_rps,
            args.openai_max_in_flight,
        )

        with ThreadPoolExecutor(max_workers=max(1, args.enrich_workers)) as ex:
            futs2 = []
            for batch in batches:
                futs2.append(ex.submit(enricher.enrich_missing, batch))

            for fut in as_completed(futs2):
                try:
                    partial = fut.result()
                    enriched_by_place_id.update(partial)
                except Exception as e:
                    logging.error("Enrichment batch failed: %s", e)

        # Save cache after enrichment
        cache.save()

    # Track place_ids that got hard fallback (no cache, not prefilter skip) for ai_enriched column.
    hard_fallback_ids: Set[str] = set()

    # Fill enriched_by_place_id for all places (cache hits, prefilter skip fallback, hard fallback).
    for p in place_list:
        pid = p["place_id"]
        cached = enricher.get_cached(pid)
        if cached is not None:
            enriched_by_place_id[pid] = cached
            continue

        # Not cached. If prefilter skipped it, attach a deterministic fallback.
        if pid in prefilter_skipped:
            meta = prefilter_meta.get(pid, {})
            score = int(meta.get("prefilter_score", 0) or 0)
            reasons = str(meta.get("prefilter_reasons", "no_signals")).split(";")
            enriched_by_place_id[pid] = prefilter_fallback(score, reasons, min_score)
            continue

        # Hard fallback (e.g., enrichment failure)
        hard_fallback_ids.add(pid)
        enriched_by_place_id[pid] = {
            "is_competitor": False,
            "size_bucket": "Unknown",
            "employee_estimate": "",
            "size_confidence": 0.0,
            "size_evidence": "",
            "service_offerings": _default_service_offerings(),
            "summary": "",
            "top_sources": [],
            "citations": "",
            "confidence": 0.0,
        }

    # ------------------ Phase 3: Join enrichment back to branch-level candidates ------------------ #

    rows: List[Dict[str, Any]] = []
    for c in all_candidates:
        enrichment = enriched_by_place_id.get(c.place_id, {})
        row = {
            "branch_name": c.branch_name,
            "region": getattr(c, "region", "") or "",
            "branch_latitude": c.branch_latitude,
            "branch_longitude": c.branch_longitude,
            "place_id": c.place_id,
            "company_name": c.company_name,
            "address": c.address,
            "latitude": c.latitude,
            "longitude": c.longitude,
            "website": c.website,
            "primary_type": c.primary_type,
            "types": c.types,
            "rating": c.rating,
            "user_rating_count": c.user_rating_count,
            "distance_miles": c.distance_miles,
        }
        row.update(prefilter_meta.get(c.place_id, {}))
        row.update(flatten_enrichment(enrichment))
        # True when enrichment came from OpenAI or cache (originally from AI); False for prefilter or hard fallback.
        row["ai_enriched"] = c.place_id not in prefilter_skipped and c.place_id not in hard_fallback_ids
        rows.append(row)

    df_out = pd.DataFrame(rows)

    # Optional: keep only competitor rows in the main output? We'll keep all, but users can filter.
    out_path = output_dir / "competitors_enriched.xlsx"
    df_out.to_excel(out_path, index=False, engine="openpyxl")
    logging.info("Wrote: %s", out_path)

    # Branch-level summary
    df_summary = build_branch_summary(df_out)
    summary_path = output_dir / "branch_competitor_summary.csv"
    df_summary.to_csv(summary_path, index=False)
    logging.info("Wrote: %s", summary_path)

    # Final cache save (safe)
    cache.save()
    prefilter_cache.save()


if __name__ == "__main__":
    main()
