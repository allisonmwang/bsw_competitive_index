#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Brand Industrials — Competitor Intelligence (30 miles) with AI labeling + PROGRESS + ALL-HITS XLSX
OPTIMIZED VERSION with async rate limiting and performance improvements

Inputs (CSV, header required):
    msi_name,lat,lon   (or: name -> auto-renamed to msi_name; lng/longitude accepted)

Outputs:
    1) out/competitors_details.csv  — CLEANED (kept) rows with AI labels
    2) out/competitors_all_hits.csv — ALL raw hits, including dropped ones, with drop_reason & kept_final
    3) out/competitors_outputs.xlsx — Excel with 2 sheets: cleaned, all_hits
    4) out/gpt_cache.json           — cache of LLM labels keyed by place_id

Usage (PowerShell):
    $env:GOOGLE_MAPS_API_KEY = "YOUR_GOOGLE_PLACES_KEY"
    $env:OPENAI_API_KEY      = "YOUR_OPENAI_KEY"
    $env:OPENAI_GPT_MODEL    = "gpt-4o"  # Optional: defaults to gpt-4o
    pip install requests pandas openai tqdm openpyxl
    python .\brand_competitor_counts_v5.py --input .\branch_input.csv --outdir .\out --kw-mode smart --kw-target 15 --concurrency 4 --gpt-workers 20 --gpt-rps 15 --rps 6 --log-hits --log-limit 50
"""

import argparse
import json
import math
import os
import re
import sys
import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

import asyncio, random, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# progress / printing that works even if tqdm is missing or console isn't a TTY
try:
    from tqdm.auto import tqdm
    TQDM_AVAILABLE = True
    def pwrite(msg: str):
        try:
            tqdm.write(str(msg))
        except Exception:
            print(str(msg), flush=True)
except Exception:
    TQDM_AVAILABLE = False
    def tqdm(x, **kwargs):  # no-op progress bar
        return x
    def pwrite(msg: str):
        print(str(msg), flush=True)

# -----------------------
# API Keys & Endpoints
# -----------------------
API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")           # <-- env var
TEXT_URL = "https://places.googleapis.com/v1/places:searchText"
NEARBY_URL = "https://places.googleapis.com/v1/places:searchNearby"

# OpenAI (optional but recommended)
try:
    from openai import OpenAI, AsyncOpenAI
except Exception:
    OpenAI = None
    AsyncOpenAI = None

OPENAI_API_KEY  = os.environ.get("OPENAI_API_KEY")
OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL")  # McKinsey gateway URL
GPT_MODEL       = os.environ.get("OPENAI_GPT_MODEL", "gpt-4o")

def get_openai_clients():
    """Return (sync_client, async_client) or (None, None) if not configured."""
    if not OPENAI_API_KEY or OpenAI is None:
        if not OPENAI_API_KEY:
            print("WARNING: OPENAI_API_KEY environment variable not set")
        if OpenAI is None:
            print("WARNING: OpenAI library not installed or import failed")
        return None, None
    
    common = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        common["base_url"] = OPENAI_BASE_URL
        print(f"Using OpenAI base URL: {OPENAI_BASE_URL}")
    
    try:
        sync_client = OpenAI(**common)
        async_client = AsyncOpenAI(**common)
        print("OpenAI clients initialized successfully")
        return sync_client, async_client
    except Exception as e:
        print(f"ERROR: Failed to initialize OpenAI clients: {e}")
        return None, None

# -----------------------
# Async Rate Limiter
# -----------------------
class AsyncRateLimiter:
    """Token bucket rate limiter for async operations."""
    
    def __init__(self, rate: float, burst: int = None):
        """
        Args:
            rate: Requests per second
            burst: Maximum burst capacity (defaults to rate * 2)
        """
        self.rate = rate
        self.burst = burst or int(rate * 2)
        self.tokens = self.burst
        self.last_update = time.time()
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1):
        """Acquire tokens, waiting if necessary."""
        async with self._lock:
            now = time.time()
            # Add tokens based on elapsed time
            elapsed = now - self.last_update
            self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
            self.last_update = now
            
            # Wait if not enough tokens
            if self.tokens < tokens:
                wait_time = (tokens - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= tokens

# Add this class after the AsyncRateLimiter class (around line 130)
class GPTCallTracker:
    """Track GPT API calls, performance, and costs."""
    
    def __init__(self):
        self.start_time = time.time()
        self.total_calls = 0
        self.successful_calls = 0
        self.failed_calls = 0
        self.cached_calls = 0
        self.fast_rule_calls = 0
        self.batch_calls = 0
        self.total_tokens = 0
        self.total_cost = 0.0
        self.call_times = []
        self.error_counts = defaultdict(int)
        
    def log_call(self, call_type: str, success: bool, tokens: int = 0, cost: float = 0.0, 
                 call_time: float = 0.0, error: str = None):
        """Log a GPT API call."""
        self.total_calls += 1
        if success:
            self.successful_calls += 1
        else:
            self.failed_calls += 1
            if error:
                self.error_counts[error] += 1
        
        if call_type == "cached":
            self.cached_calls += 1
        elif call_type == "fast_rule":
            self.fast_rule_calls += 1
        elif call_type == "batch":
            self.batch_calls += 1
            
        self.total_tokens += tokens
        self.total_cost += cost
        if call_time > 0:
            self.call_times.append(call_time)
    
    def get_stats(self) -> dict:
        """Get current statistics."""
        elapsed = time.time() - self.start_time
        avg_call_time = sum(self.call_times) / len(self.call_times) if self.call_times else 0
        calls_per_minute = (self.total_calls / elapsed) * 60 if elapsed > 0 else 0
        
        return {
            "elapsed_time": elapsed,
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "cached_calls": self.cached_calls,
            "fast_rule_calls": self.fast_rule_calls,
            "batch_calls": self.batch_calls,
            "total_tokens": self.total_tokens,
            "total_cost": self.total_cost,
            "avg_call_time": avg_call_time,
            "calls_per_minute": calls_per_minute,
            "success_rate": (self.successful_calls / self.total_calls * 100) if self.total_calls > 0 else 0,
            "error_counts": dict(self.error_counts)
        }
    
    def print_progress(self, current: int, total: int):
        """Print progress with GPT call stats."""
        stats = self.get_stats()
        progress_pct = (current / total * 100) if total > 0 else 0
        
        print(f"\n🤖 GPT Progress: {current}/{total} ({progress_pct:.1f}%)")
        print(f"   📊 Calls: {stats['total_calls']} total, {stats['successful_calls']} success, {stats['failed_calls']} failed")
        print(f"   ⚡ Speed: {stats['calls_per_minute']:.1f} calls/min, {stats['avg_call_time']:.2f}s avg")
        print(f"   💰 Cost: ${stats['total_cost']:.4f}, {stats['total_tokens']:,} tokens")
        print(f"   🎯 Success Rate: {stats['success_rate']:.1f}%")
        
        if stats['cached_calls'] > 0:
            print(f"   💾 Cached: {stats['cached_calls']}, Fast Rules: {stats['fast_rule_calls']}")
        
        if stats['error_counts']:
            print(f"   ❌ Errors: {dict(stats['error_counts'])}")

# -----------------------
# Geometry & pacing
# -----------------------
RADIUS_M = 48280  # 30 miles in meters
MAX_RPS = 5
SLEEP_BETWEEN_PAGES = 0.6

# -----------------------
# Domain keywords & filters
# -----------------------
KEYWORDS = [
    "scaffolding", "scaffold", "scaffold rental", "scaffold services",
    "swing stage", "suspended scaffolding", "mast climber",
    "construction hoist", "industrial access", "access solutions",
    "rope access", "shoring", "forming"
]
KW_BASIC = ["scaffold", "scaffolding", "scaffold rental"]  # fast starters
KW_FULL  = KEYWORDS

# Service type keyword mappings based on industry descriptions
SERVICE_TYPE_KEYWORDS = {
    "scaffolding": [
        # Scaffolding services
        "scaffolding", "scaffold", "scaffold rental", "scaffold services", "scaffold erection",
        "swing stage", "suspended scaffolding", "mast climber", "construction hoist", 
        "industrial access", "access solutions", "rope access", "commercial scaffolding",
        "industrial scaffolding", "multi-offering project", "bridge access",
        "industrial mechanical access", "industrial rope access", "commercial mechanical access",
        "commercial rope access", "mcwp mast climber work platform", "elevator", 
        "industrial powered access"
    ],
    "forming_shoring": [
        # Forming/Shoring services
        "concrete construction", "forming", "shoring", "formwork", "concrete forming",
        "shoring systems", "concrete shoring", "forming systems", "falsework"
    ],
    "insulation": [
        # Insulation services
        "insulation", "thermal insulation", "industrial insulation", "pipe insulation",
        "equipment insulation", "mechanical insulation", "hvac insulation"
    ],
    "motorized": [
        # Motorized services
        "motorized", "powered access", "aerial lift", "boom lift", "scissor lift",
        "cherry picker", "man lift", "aerial work platform", "awp", "mewp",
        "mobile elevated work platform", "telescopic boom", "articulating boom"
    ],
    "painting": [
        # Painting & Coatings services
        "painting", "coatings", "protective coatings", "industrial painting", "commercial painting",
        "surface preparation", "sandblasting", "powder coating", "spray painting", "brush painting",
        "roller painting", "epoxy coating", "polyurethane coating", "marine coating", "automotive painting",
        "architectural coating", "anti-corrosive coating", "primer", "paint application", "coating application",
        "paint contractor", "coating contractor", "industrial coatings", "commercial coatings"
    ],
    "specialty": [
        # Specialty services
        "refractory", "heat tracing", "abrasive blasting", "fireproofing", "cp construction",
        "leak detection", "repair", "cp products", "civil construction", "asbestos abatement",
        "carpentry services", "removable covers", "grounds maintenance", 
        "corrosion under insulation", "cp technical services", "siding", "cp tanks",
        "ac mitigation", "safety services", "horizontal directional drill", "pipeline surveys",
        "mechanical", "cp products non-proprietary", "corrosion services", "lead abatement",
        "special events services", "vci vapor corr inhibitor apps", "manufacturing product sales",
        "composite wrap", "fire stopping", "iss specialty services", "scaffold surveys",
        "vac service", "pro services", "studbusters", "hydraulic bolting", "tensioning",
        "janitorial", "thermal spray aluminium", "corrosion under fireproofing",
        "thermal surveys", "non-asbestos demo", "tank maintenance services", "solar",
        "obsolete was", "field machining", "leak sealing", "heat treating", "cabins",
        "hot tapping"
    ]
}

EXCLUDE_PATTERNS = [
    "brandsafway", "brand safway", "brand industrial services",
    "brand industrial", "safway"
]

BLOCK_TYPES = {
    "department_store", "hardware_store", "home_improvement_store",
    "furniture_store", "big_box_store", "supermarket", "grocery_or_supermarket",
    "shopping_mall", "warehouse_store", "home_goods_store", "electronics_store",
    "discount_store", "convenience_store", "car_rental", "car_dealer",
    "gas_station", "pharmacy", "liquor_store", "pet_store", "store"
}

# US state abbreviations for multi-state detection
US_STATES = set("""
AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO
MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC
""".split())

# -----------------------
# CLI
# -----------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV with columns: msi_name,lat,lon")
    ap.add_argument("--outdir", default="./out", help="Output directory")
    ap.add_argument("--include-nearby", action="store_true",
                    help="Also run Nearby Search (rank by DISTANCE, name-filtered)")
    ap.add_argument("--rps", type=float, default=MAX_RPS, help="Max requests/sec (shared across workers)")
    ap.add_argument("--concurrency", type=int, default=4, help="Parallel branches")
    ap.add_argument("--gpt-workers", type=int, default=20, help="Parallel GPT calls (can be much higher with rate limiting)")
    ap.add_argument("--gpt-rps", type=float, default=15.0, help="GPT requests per second (rate limit)")
    ap.add_argument("--gpt-burst", type=int, default=30, help="GPT burst capacity")
    ap.add_argument("--kw-mode", choices=["basic","smart","full"], default="smart",
                    help="Keyword strategy: basic=3 kws, smart=escalate as needed, full=all kws")
    ap.add_argument("--kw-target", type=int, default=12,
                    help="Smart mode: escalate keywords until ~this many candidates per branch or all tried")
    ap.add_argument("--max-gpt", type=int, default=0,
                    help="Hard cap on NEW GPT classifications (0 = unlimited). Cache still used.")
    ap.add_argument("--log-hits", action="store_true",
                    help="Print a line for each kept competitor (name, distance, source)")
    ap.add_argument("--log-limit", type=int, default=0,
                    help="Max kept competitors to print per branch (0 = no limit)")
    ap.add_argument("--batch-size", type=int, default=50, help="Batch size for processing")
    return ap.parse_args()

# -----------------------
# Distance & Throttle
# -----------------------
def haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.7613
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def throttle(last_ts, rps):
    min_interval = 1.0 / max(rps, 0.1)
    now = time.time()
    wait = last_ts + min_interval - now
    if wait > 0:
        time.sleep(wait)
        now = time.time()
    return now

# -----------------------
# Places API helpers (no Atmosphere fields)
# -----------------------
def pages(session, url, body, fieldmask, rps) -> Iterable[dict]:
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": fieldmask
    }
    page_token = None
    last_ts = 0.0
    while True:
        if page_token:
            body["pageToken"] = page_token
        last_ts = throttle(last_ts, rps)
        r = session.post(url, json=body, headers=headers, timeout=30)

        if r.status_code in (429, 500, 503):
            # honor Retry-After when present
            retry_after = r.headers.get("Retry-After")
            try:
                sleep_s = float(retry_after) if retry_after else 0.8
            except Exception:
                sleep_s = 0.8
            time.sleep(sleep_s + random.random()*0.4)
            # second attempt
            r = session.post(url, json=body, headers=headers, timeout=30)
        if r.status_code == 429:
            time.sleep(1.5 + random.random()*0.5)
        r.raise_for_status()

        data = r.json()
        yield data
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(SLEEP_BETWEEN_PAGES)

def search_text(session, lat, lon, query, rps) -> Iterable[dict]:
    body = {
        "textQuery": query,
        "locationBias": {"circle": {"center": {"latitude": lat, "longitude": lon}, "radius": RADIUS_M}},
        "pageSize": 20
    }
    fieldmask = (
        "places.id,places.name,places.displayName,places.formattedAddress,"
        "places.location,places.types,places.websiteUri,places.rating,places.userRatingCount,nextPageToken"
    )
    for page in pages(session, TEXT_URL, body, fieldmask, rps):
        for p in page.get("places", []):
            yield p

def nearby_scout(session, lat, lon, rps) -> Iterable[dict]:
    body = {
        "rankPreference": "DISTANCE",
        "locationRestriction": {"circle": {"center": {"latitude": lat, "longitude": lon}, "radius": RADIUS_M}},
        "maxResultCount": 20
    }
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": (
            "places.id,places.name,places.displayName,places.formattedAddress,"
            "places.location,places.types,places.websiteUri,places.rating,places.userRatingCount"
        )
    }
    last_ts = 0.0
    last_ts = throttle(last_ts, rps)
    r = session.post(NEARBY_URL, json=body, headers=headers, timeout=30)
    if r.status_code in (429, 500, 503):
        time.sleep(1.5)
        r = session.post(NEARBY_URL, json=body, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("places", [])

# -----------------------
# Normalization & filters
# -----------------------
def types_blocked(types_list: List[str]) -> Optional[str]:
    for t in types_list or []:
        if t in BLOCK_TYPES:
            return t
    return None

def normalize_place(msi_name, mlat, mlon, p, source, matched_kw=""):
    name_resource = p.get("name", "")
    place_id = name_resource.split("/", 1)[-1] if "/" in name_resource else (p.get("id") or name_resource)
    disp = (p.get("displayName", {}) or {}).get("text", "") or ""
    addr = p.get("formattedAddress", "")
    loc = p.get("location", {}) or {}
    plat, plon = loc.get("latitude"), loc.get("longitude")
    if plat is None or plon is None:
        return None
    dist = haversine_miles(mlat, mlon, plat, plon)
    types_list = p.get("types", []) or []
    primary_type = types_list[0] if types_list else ""

    row = {
        "msi_name": msi_name,
        "msi_lat": mlat, "msi_lon": mlon,
        "place_id": place_id,
        "resource_name": name_resource,
        "name": disp,
        "address": addr,
        "lat": plat, "lon": plon,
        "distance_mi": round(dist, 2),
        "primary_type": primary_type,
        "types": ",".join(types_list),
        "website": p.get("websiteUri") or "",
        "rating": p.get("rating"),
        "userRatingCount": p.get("userRatingCount"),
        "source": source,
        "matched_keyword": matched_kw
    }
    return row

def looks_like_self(vendor_name: str) -> bool:
    s = vendor_name.lower()
    for pat in EXCLUDE_PATTERNS:
        if pat in s:
            return True
    return False

def name_hits_keyword(vendor_name: str) -> bool:
    s = vendor_name.lower()
    for kw in KEYWORDS:
        if kw.lower() in s:
            return True
    return False

# -----------------------
# Website fetching & facts extraction (fast + tiny) - OPTIMIZED
# -----------------------
HEADERS = {"User-Agent": "Mozilla/5.0 (Brand-Intel Bot)"}
SLUGS = ["", "about", "services", "careers", "locations", "company", "rental", "contact"]

EMP_RE = re.compile(r"\b(\d{1,3}(?:,\d{3})*|\d{2,4})\s+(?:employees|team members|staff)\b", re.I)
ST_ABBR_RE = re.compile(r"\b([A-Z]{2})\b")
NATION_RE = re.compile(r"\b(nationwide|coast-to-coast|across\s+\d+\s+states)\b", re.I)
HIRE_RE = re.compile(r"\b(careers|we['']?re\s+hiring|join our team)\b", re.I)

def safe_strip_html(text: str, max_chars: int = 12000) -> str:
    text = re.sub(r"(?is)<script.*?>.*?</script>", " ", text)
    text = re.sub(r"(?is)<style.*?>.*?</style>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_chars]

def same_host(url: str, candidate: str) -> bool:
    try:
        return urlparse(url).netloc == urlparse(candidate).netloc
    except Exception:
        return False

async def gather_site_facts_async(url: str, timeout: float = 6.0, per_page_bytes: int = 120_000) -> dict:
    """Async version of site facts gathering for better performance."""
    out = dict(
        homepage_text="", pages_fetched=0, employees=None, states_count=0,
        nationwide=False, hiring=False, has_linkedin=False
    )
    if not url or not url.startswith(("http://", "https://")):
        return out
    base = url if url.endswith("/") else url + "/"
    texts = []
    seen = set()
    
    # Use aiohttp for async requests if available, fallback to sync
    try:
        import aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as session:
            for slug in SLUGS:
                u = urljoin(base, slug)
                if u in seen: continue
                seen.add(u)
                try:
                    async with session.get(u, headers=HEADERS, allow_redirects=True) as r:
                        if r.status >= 400: continue
                        text = safe_strip_html((await r.text())[:per_page_bytes])
                        if not text: continue
                        texts.append(text)
                        out["pages_fetched"] += 1
                        if "linkedin.com/company" in text.lower():
                            out["has_linkedin"] = True
                        if out["pages_fetched"] >= 4:
                            break
                except Exception:
                    continue
    except ImportError:
        # Fallback to sync version
        return gather_site_facts(url, timeout, per_page_bytes)

    if not texts:
        return out

    # Aggregate
    big = " ".join(texts)

    # employees
    m = EMP_RE.search(big)
    if m:
        try:
            n = int(m.group(1).replace(",", ""))
            out["employees"] = n
        except Exception:
            pass

    # multi-state presence
    states = {abbr for abbr in ST_ABBR_RE.findall(big) if abbr in US_STATES}
    out["states_count"] = len(states)
    out["nationwide"] = bool(NATION_RE.search(big))

    # hiring signals
    out["hiring"] = bool(HIRE_RE.search(big))

    # sample homepage text (first page)
    out["homepage_text"] = texts[0][:4000]
    return out

def gather_site_facts(url: str, timeout: float = 6.0, per_page_bytes: int = 120_000) -> dict:
    """Fetch a few same-domain pages & extract signals. Very lightweight; best-effort."""
    out = dict(
        homepage_text="", pages_fetched=0, employees=None, states_count=0,
        nationwide=False, hiring=False, has_linkedin=False
    )
    if not url or not url.startswith(("http://", "https://")):
        return out
    base = url if url.endswith("/") else url + "/"
    texts = []
    seen = set()
    try:
        # fetch slugs
        for slug in SLUGS:
            u = urljoin(base, slug)
            if u in seen: continue
            seen.add(u)
            r = requests.get(u, headers=HEADERS, timeout=timeout, allow_redirects=True)
            if r.status_code >= 400: continue
            text = safe_strip_html(r.text[:per_page_bytes])
            if not text: continue
            texts.append(text)
            out["pages_fetched"] += 1
            if "linkedin.com/company" in r.text.lower():
                out["has_linkedin"] = True
            # be gentle
            if out["pages_fetched"] >= 4:
                break
    except Exception:
        pass

    if not texts:
        return out

    # Aggregate
    big = " ".join(texts)

    # employees
    m = EMP_RE.search(big)
    if m:
        try:
            n = int(m.group(1).replace(",", ""))
            out["employees"] = n
        except Exception:
            pass

    # multi-state presence
    states = {abbr for abbr in ST_ABBR_RE.findall(big) if abbr in US_STATES}
    out["states_count"] = len(states)
    out["nationwide"] = bool(NATION_RE.search(big))

    # hiring signals
    out["hiring"] = bool(HIRE_RE.search(big))

    # sample homepage text (first page)
    out["homepage_text"] = texts[0][:4000]
    return out

# -----------------------
# GPT helpers (SAS + Size with proxies) - OPTIMIZED
# -----------------------
def load_cache(cache_path: str) -> Dict[str, dict]:
    if not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_cache(cache_path: str, data: Dict[str, dict]) -> None:
    tmp = cache_path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, cache_path)

def detect_service_types(text: str, website_text: str = "") -> Dict[str, dict]:
    """Detect service types based on keywords in name, types, and website text with enhanced accuracy."""
    combined_text = f"{text} {website_text}".lower()
    results = {}
    
    for service_type, keywords in SERVICE_TYPE_KEYWORDS.items():
        matched_keywords = []
        confidence_boost = 0.0
        
        for keyword in keywords:
            keyword_lower = keyword.lower()
            if keyword_lower in combined_text:
                matched_keywords.append(keyword)
                
                # Higher confidence for company name matches
                if keyword_lower in text.lower():
                    # Check if keyword appears in company name (not just business type)
                    name_part = text.split('|')[0].lower() if '|' in text else text.lower()
                    if keyword_lower in name_part:
                        confidence_boost += 0.2  # Strong signal from company name
                    else:
                        confidence_boost += 0.1  # Signal from business type
                else:
                    confidence_boost += 0.05  # Signal from website content
        
        if matched_keywords:
            # Calculate confidence based on number and strength of matches
            base_confidence = min(0.6, 0.4 + (len(matched_keywords) * 0.05))
            final_confidence = min(0.95, base_confidence + confidence_boost)
            
            results[service_type] = {
                "service": "Yes",
                "confidence": final_confidence,
                "matched_keywords": matched_keywords[:5]  # Limit to top 5 matches
            }
        else:
            results[service_type] = {
                "service": "Unknown",
                "confidence": 0.0,
                "matched_keywords": []
            }
    
    return results

CLASSIFY_SYS_PROMPT = (
  "You are an expert construction-industry analyst. Classify companies across 6 service types: "
  "Scaffolding, Forming/Shoring, Insulation, Motorized, Painting, Specialty. "
  "Use ONLY the evidence provided in the user message, except when WELL_KNOWN_COMPANY=true; in that case, "
  "you may supplement with widely known facts about that specific company (not generic industry assumptions). "
  "Do not fabricate keywords or evidence; every keyword must be a literal substring from the provided evidence. "
  "Label semantics:\n"
  "- 'Yes' = there is affirmative evidence (direct or strongly implied) that the company provides the service.\n"
  "- 'No'  = there is affirmative evidence of absence (e.g., a statement that the company does not offer it) "
  "or clear scope that excludes the service.\n"
  "- 'Unknown' = insufficient evidence either way.\n"
  "Avoid false negatives: treat multiple weak but consistent cues across different pages as acceptable for 'Yes' "
  "when they are action-oriented (e.g., provide/offer/rent/install/erect) or equipment/service listings. "
  "Overlapping categories are allowed (e.g., 'mast climber' may support Scaffolding and Motorized if powered access is implied).\n"
  "Confidence guidance (calibrate carefully, no inflation):\n"
  "• 0.95–1.00: Service page or equipment/rental page explicitly states the service with action verbs (provide/offer/rent/install/erect), OR named product lines clearly belonging to the service.\n"
  "• 0.85–0.90: Multiple snippets across ≥2 pages or path-weighted pages (/services, /equipment, /rentals, /solutions) with action verbs or specific equipment terms (e.g., ringlock, formwork, boom lift, intumescent).\n"
  "• 0.70–0.80: One strong snippet with action verbs OR several consistent mentions including brand/technology terms strongly tied to the service.\n"
  "• 0.50–0.65: Ambiguous mentions without action context → should not produce 'Yes' unless supplemented by other clear cues.\n"
  "• 0.30–0.45: Generic or historical mentions only → typically 'Unknown'.\n"
  "Size classification (Small 0–49, Mid 50–249, Large 250+): prioritize explicit headcount, then multi-location/scale claims ('nationwide', number of states/offices), then fleet size proxies. "
  "Return strictly valid JSON only."
)


CLASSIFY_USER_TMPL = """CONSTRUCTION COMPANY ANALYSIS

Company: {name}
Business Types: {types}
Website: {website}
Rating: {rating}/5 ({userRatingCount} reviews)
WELL_KNOWN_COMPANY: {well_known}

EVIDENCE (short, tagged snippets; each line must only includetext drawn from evidence):
{evidence_block}

{web_search_results}

DECISION RULES FOR THIS TASK (apply strictly):
- Prefer action-oriented statements (provide/offer/rent/install/erect/supply/perform) and equipment/service listings.
- Mentions on /services, /equipment, /rentals, /solutions carry more weight than /about, /blog, or news pages.
- Accept overlapping services if the evidence supports them (e.g., 'material hoist' → Motorized; 'mast climber' → Scaffolding and often Motorized).
- Do NOT infer from unrelated phrases (e.g., 'insulation resistance'); ignore directories or social pages.
- Only include keywords that literally appear in the snippets above; no invented synonyms.

SERVICE DEFINITIONS (for mapping terms→services):
1. Scaffolding: scaffolding systems, swing stages, mast climbers (as access systems), rope/suspended platforms.
2. Forming/Shoring: formwork, shoring systems, falsework, concrete forms, formwork rental/installation.
3. Insulation: mechanical/thermal/industrial insulation (pipe/duct/blanket/jacketing).
4. Motorized: aerial/boom/scissor/vertical-mast lifts, telehandlers, hoists, powered access.
5. Painting: industrial/commercial painting, protective coatings, surface prep, abrasive blasting.
6. Specialty: refractory, fireproofing (incl. intumescent), heat tracing, leak detection, FRP/linings, hot tapping, lead/asbestos abatement.

OUTPUT (strict JSON):
{{
  "scaffolding": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "forming_shoring": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "insulation": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "motorized": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "painting": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "specialty": {{"service": "Yes|No|Unknown", "confidence": 0.0-1.0, "evidence": "short cited line", "keywords": ["exact_terms_from_evidence"]}},
  "size_bucket": "Small|Mid|Large|Unknown",
  "size_confidence": 0.0-1.0,
  "size_evidence": "short cited line(s) or concise reasoning tied to the evidence"
}}
"""



async def classify_one_async(async_client: AsyncOpenAI, model: str, pdata: dict, facts: dict,
                             rate_limiter: AsyncRateLimiter, max_retries: int = 6) -> dict:
    user_prompt = CLASSIFY_USER_TMPL.format(
        name=pdata.get("name",""),
        address=pdata.get("address",""),
        primary_type=pdata.get("primary_type",""),
        types=pdata.get("types",""),
        website=pdata.get("website",""),
        rating=str(pdata.get("rating","")),
        userRatingCount=str(pdata.get("userRatingCount","")),
        employees=facts.get("employees"),
        states_count=facts.get("states_count"),
        nationwide=facts.get("nationwide"),
        hiring=facts.get("hiring"),
        has_linkedin=facts.get("has_linkedin"),
        homepage_text=facts.get("homepage_text") or "(none)"
    )
    delay = 0.6
    for _ in range(max_retries):
        try:
            # Apply rate limiting
            await rate_limiter.acquire()
            
            resp = await async_client.chat.completions.create(
                model=model, temperature=0.2, response_format={"type":"json_object"},
                messages=[{"role":"system","content":CLASSIFY_SYS_PROMPT},
                          {"role":"user","content":user_prompt}]
            )
            data = json.loads(resp.choices[0].message.content)
            
            # Parse service types
            out = {}
            service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
            
            for service_type in service_types:
                service_data = data.get(service_type, {})
                service_label = str(service_data.get("service", "Unknown")).title()
                if service_label not in {"Yes", "No", "Unknown"}:
                    service_label = "Unknown"
                
                out[f"{service_type}_label"] = service_label
                out[f"{service_type}_confidence"] = max(0.0, min(1.0, float(service_data.get("confidence", 0.0))))
                out[f"{service_type}_keywords"] = ",".join(service_data.get("keywords", []))
                out[f"{service_type}_evidence"] = service_data.get("evidence", "")
            
            # Parse size information
            out["size_bucket"] = str(data.get("size_bucket", "Unknown")).title()
            if out["size_bucket"] not in {"Micro", "Small", "Mid", "Large", "Unknown"}:
                out["size_bucket"] = "Unknown"
            out["size_confidence"] = max(0.0, min(1.0, float(data.get("size_confidence", 0.0))))
            out["size_evidence"] = data.get("size_evidence", "")
            
            # Apply heuristic size if still unknown
            if out["size_bucket"] == "Unknown":
                h = heuristic_size_from_facts(facts)
                if h:
                    out["size_bucket"] = h["size_bucket"]
                    out["size_confidence"] = max(out["size_confidence"], h["size_confidence"])
                    out["size_evidence"] = out["size_evidence"] or h["size_evidence"]
            
            return out
        except Exception as e:
            msg = str(e).lower()
            error_type = type(e).__name__
            
            # Handle authorization errors specifically
            if ("401" in msg) or ("403" in msg) or ("unauthorized" in msg) or ("forbidden" in msg):
                error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":f"auth_error:{error_type}"}
                service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
                for service_type in service_types:
                    error_result[f"{service_type}_label"] = "Unknown"
                    error_result[f"{service_type}_confidence"] = 0.0
                    error_result[f"{service_type}_keywords"] = f"auth_error:{error_type}"
                    error_result[f"{service_type}_evidence"] = f"auth_error:{error_type}"
                return error_result
            
            # Handle retryable errors
            if ("rate" in msg) or ("429" in msg) or ("timeout" in msg) or ("503" in msg) or ("temporar" in msg):
                await asyncio.sleep(delay + random.random()*0.4)
                delay = min(delay*2, 8.0)
                continue
                
            # Handle other errors
            error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":f"gpt_error:{error_type}"}
            service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
            for service_type in service_types:
                error_result[f"{service_type}_label"] = "Unknown"
                error_result[f"{service_type}_confidence"] = 0.0
                error_result[f"{service_type}_keywords"] = f"gpt_error:{error_type}"
                error_result[f"{service_type}_evidence"] = f"gpt_error:{error_type}"
            return error_result
    
    # Final fallback for rate limiting
    error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":"gpt_error:rate_limited"}
    service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
    for service_type in service_types:
        error_result[f"{service_type}_label"] = "Unknown"
        error_result[f"{service_type}_confidence"] = 0.0
        error_result[f"{service_type}_keywords"] = "gpt_error:rate_limited"
        error_result[f"{service_type}_evidence"] = "gpt_error:rate_limited"
    return error_result

async def classify_batch_async(async_client: AsyncOpenAI, model: str,
                               to_classify: List[str],
                               uniq_by_place: Dict[str,dict],
                               facts_by_place: Dict[str,dict],
                               rate_limiter: AsyncRateLimiter,
                               tracker: GPTCallTracker,
                               max_concurrency: int = 100) -> Dict[str, dict]:
    """Ultra-fast batch processing with 20 places per GPT call."""
    results: Dict[str, dict] = {}
    
    # Process in batches of 20 for maximum efficiency
    batch_size = 20
    total_batches = (len(to_classify) + batch_size - 1) // batch_size
    
    for i in range(0, len(to_classify), batch_size):
        batch = to_classify[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        # Create single prompt for entire batch
        batch_prompt = "Classify these businesses for scaffolding services:\n\n"
        for j, pid in enumerate(batch):
            pdata = uniq_by_place[pid]
            batch_prompt += f"{j+1}. {pdata['name']} | {pdata['types']} | {pdata['website']}\n"
        
        batch_prompt += f"\nReturn JSON: {{'results': [{{ 'scaffolding': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'forming_shoring': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'insulation': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'motorized': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'painting': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'specialty': {{'service': 'Yes|No|Unknown', 'confidence': 0.0-1.0, 'evidence': 'evidence', 'keywords': []}}, 'size_bucket': 'Micro|Small|Mid|Large|Unknown', 'size_confidence': 0.0-1.0, 'size_evidence': '' }}, ...]}}"
        
        await rate_limiter.acquire()
        
        start_time = time.time()
        try:
            resp = await async_client.chat.completions.create(
                model=model,
                temperature=0.1,
                response_format={"type":"json_object"},
                messages=[{"role":"user","content":batch_prompt}]
            )
            
            call_time = time.time() - start_time
            
            # Estimate tokens and cost (rough estimates)
            input_tokens = len(batch_prompt) // 4  # Rough estimate
            output_tokens = len(batch) * 20  # Rough estimate for JSON response
            total_tokens = input_tokens + output_tokens
            cost = total_tokens * 0.0000015  # Rough cost estimate for gpt-3.5-turbo
            
            # Log successful batch call
            tracker.log_call("batch", True, total_tokens, cost, call_time)
            
            # Parse batch response
            data = json.loads(resp.choices[0].message.content)
            batch_results = data.get("results", [])
            
            for j, pid in enumerate(batch):
                if j < len(batch_results):
                    result = batch_results[j]
                    batch_result = {}
                    
                    # Parse service types
                    service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
                    for service_type in service_types:
                        service_data = result.get(service_type, {})
                        service_label = str(service_data.get("service", "Unknown")).title()
                        if service_label not in {"Yes", "No", "Unknown"}:
                            service_label = "Unknown"
                        
                        batch_result[f"{service_type}_label"] = service_label
                        batch_result[f"{service_type}_confidence"] = max(0.0, min(1.0, float(service_data.get("confidence", 0.8))))
                        batch_result[f"{service_type}_keywords"] = ",".join(service_data.get("keywords", []))
                        batch_result[f"{service_type}_evidence"] = service_data.get("evidence", "batch_classification")
                    
                    # Parse size
                    batch_result["size_bucket"] = str(result.get("size_bucket","Unknown")).title()
                    batch_result["size_confidence"] = max(0.0, min(1.0, float(result.get("size_confidence", 0.8))))
                    batch_result["size_evidence"] = result.get("size_evidence", "batch_classification")
                    
                    results[pid] = batch_result
                else:
                    error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":"batch_error"}
                    service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
                    for service_type in service_types:
                        error_result[f"{service_type}_label"] = "Unknown"
                        error_result[f"{service_type}_confidence"] = 0.0
                        error_result[f"{service_type}_keywords"] = "batch_error"
                        error_result[f"{service_type}_evidence"] = "batch_error"
                    results[pid] = error_result
            
            # Print progress every 3 batches or on completion
            if batch_num % 3 == 0 or batch_num == total_batches:
                elapsed = time.time() - tracker.start_time
                rate = batch_num / elapsed * 60 if elapsed > 0 else 0
                remaining = total_batches - batch_num
                eta = remaining / (batch_num / elapsed) if batch_num > 0 and elapsed > 0 else 0
                
                print(f"   📈 Batch {batch_num}/{total_batches} ({batch_num/total_batches*100:.1f}%) | "
                      f"Rate: {rate:.1f} batches/min | ETA: {eta/60:.1f} min")
                
                if batch_num == total_batches:
                    tracker.print_progress(batch_num, total_batches)
                    
        except Exception as e:
            call_time = time.time() - start_time
            error_type = type(e).__name__
            
            # Log failed batch call
            tracker.log_call("batch", False, 0, 0, call_time, error_type)
            
            # Handle errors for entire batch
            for pid in batch:
                error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":f"batch_error:{error_type}"}
                service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
                for service_type in service_types:
                    error_result[f"{service_type}_label"] = "Unknown"
                    error_result[f"{service_type}_confidence"] = 0.0
                    error_result[f"{service_type}_keywords"] = f"batch_error:{error_type}"
                    error_result[f"{service_type}_evidence"] = f"batch_error:{error_type}"
                results[pid] = error_result
    
    return results

def fast_rule_label_enhanced(place: dict) -> Optional[dict]:
    """Enhanced fast rules to skip more GPT calls - handles multiple service types."""
    name = (place.get("name") or "").lower()
    types = (place.get("types") or "").lower()
    text = f"{name} {types}"
    
    # Detect service types using keyword matching
    service_detections = detect_service_types(text)
    
    # Check if any service type was detected with high confidence
    any_service_detected = False
    for service_type, detection in service_detections.items():
        if detection["service"] == "Yes" and detection["confidence"] >= 0.8:
            any_service_detected = True
            break
    
    if any_service_detected:
        # Build result with all service types
        result = {"size_bucket": "Unknown", "size_confidence": 0.0, "size_evidence": "not evaluated"}
        
        for service_type in ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]:
            detection = service_detections[service_type]
            result[f"{service_type}_label"] = detection["service"]
            result[f"{service_type}_confidence"] = detection["confidence"]
            result[f"{service_type}_keywords"] = ",".join(detection["matched_keywords"])
        
        return result
    
    # Strong negative indicators (no services detected)
    NEG_STRONG = ["hardware store", "home depot", "lowes", "ace hardware", "retail", "store"]
    if any(k in text for k in NEG_STRONG) and "rental" not in text and "service" not in text:
        result = {"size_bucket": "Unknown", "size_confidence": 0.0, "size_evidence": "not evaluated"}
        service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
        for service_type in service_types:
            result[f"{service_type}_label"] = "No"
            result[f"{service_type}_confidence"] = 0.9
            result[f"{service_type}_keywords"] = "rule: retail store"
        return result
    
    # Block obvious non-construction types
    if place.get("primary_type") in BLOCK_TYPES and "rental" not in text and "service" not in text:
        result = {"size_bucket": "Unknown", "size_confidence": 0.0, "size_evidence": "not evaluated"}
        service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
        for service_type in service_types:
            result[f"{service_type}_label"] = "No"
            result[f"{service_type}_confidence"] = 0.85
            result[f"{service_type}_keywords"] = "rule: blocked type"
        return result
    
    return None

def heuristic_size_from_facts(facts: dict) -> Optional[dict]:
    """Fallback if GPT still returns Unknown."""
    emp = facts.get("employees")
    if isinstance(emp, int):
        if emp >= 250: return {"size_bucket":"Large","size_confidence":0.85,"size_evidence":f"employees≈{emp}"}
        if emp >= 50:  return {"size_bucket":"Mid","size_confidence":0.8,"size_evidence":f"employees≈{emp}"}
        if emp >= 10:  return {"size_bucket":"Small","size_confidence":0.7,"size_evidence":f"employees≈{emp}"}
        if emp >= 1:   return {"size_bucket":"Micro","size_confidence":0.7,"size_evidence":f"employees≈{emp}"}
    states = facts.get("states_count", 0)
    if facts.get("nationwide") or states >= 10:
        return {"size_bucket":"Large","size_confidence":0.7,"size_evidence":"nationwide/states>=10"}
    if states >= 4:
        return {"size_bucket":"Mid","size_confidence":0.6,"size_evidence":"states>=4"}
    if states in (2,3):
        return {"size_bucket":"Small","size_confidence":0.55,"size_evidence":"states=2-3"}
    return None

def classify_with_gpt(client: "OpenAI", place: dict, facts: dict) -> dict:
    user_prompt = CLASSIFY_USER_TMPL.format(
        name=place.get("name",""),
        address=place.get("address",""),
        primary_type=place.get("primary_type",""),
        types=place.get("types",""),
        website=place.get("website",""),
        rating=str(place.get("rating","")),
        userRatingCount=str(place.get("userRatingCount","")),
        employees=facts.get("employees"),
        states_count=facts.get("states_count"),
        nationwide=facts.get("nationwide"),
        hiring=facts.get("hiring"),
        has_linkedin=facts.get("has_linkedin"),
        homepage_text=facts.get("homepage_text") or "(none)"
    )
    try:
        resp = client.chat.completions.create(
            model=GPT_MODEL,
            temperature=0.2,
            response_format={"type":"json_object"},
            messages=[
                {"role":"system","content":CLASSIFY_SYS_PROMPT},
                {"role":"user","content":user_prompt},
            ],
        )
        data = json.loads(resp.choices[0].message.content)
        
        # Parse service types
        out = {}
        service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
        
        for service_type in service_types:
            service_data = data.get(service_type, {})
            service_label = str(service_data.get("service", "Unknown")).title()
            if service_label not in {"Yes", "No", "Unknown"}:
                service_label = "Unknown"
            
            out[f"{service_type}_label"] = service_label
            out[f"{service_type}_confidence"] = max(0.0, min(1.0, float(service_data.get("confidence", 0.0))))
            out[f"{service_type}_keywords"] = ",".join(service_data.get("keywords", []))
            out[f"{service_type}_evidence"] = service_data.get("evidence", "")
        
        # Parse size information
        out["size_bucket"] = str(data.get("size_bucket", "Unknown")).title()
        if out["size_bucket"] not in {"Micro", "Small", "Mid", "Large", "Unknown"}:
            out["size_bucket"] = "Unknown"
        out["size_confidence"] = max(0.0, min(1.0, float(data.get("size_confidence", 0.0))))
        out["size_evidence"] = data.get("size_evidence", "")
        
        # Apply heuristic size if still unknown
        if out["size_bucket"] == "Unknown":
            h = heuristic_size_from_facts(facts)
            if h:
                out["size_bucket"] = h["size_bucket"]
                out["size_confidence"] = max(out["size_confidence"], h["size_confidence"])
                out["size_evidence"] = out["size_evidence"] or h["size_evidence"]
        
        return out
    except Exception as e:
        msg = str(e).lower()
        error_type = type(e).__name__
        
        # Handle authorization errors specifically
        if ("401" in msg) or ("403" in msg) or ("unauthorized" in msg) or ("forbidden" in msg):
            error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":f"auth_error:{error_type}"}
            service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
            for service_type in service_types:
                error_result[f"{service_type}_label"] = "Unknown"
                error_result[f"{service_type}_confidence"] = 0.0
                error_result[f"{service_type}_keywords"] = f"auth_error:{error_type}"
                error_result[f"{service_type}_evidence"] = f"auth_error:{error_type}"
            return error_result
        
        error_result = {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":f"gpt_error:{error_type}"}
        service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
        for service_type in service_types:
            error_result[f"{service_type}_label"] = "Unknown"
            error_result[f"{service_type}_confidence"] = 0.0
            error_result[f"{service_type}_keywords"] = f"gpt_error:{error_type}"
            error_result[f"{service_type}_evidence"] = f"gpt_error:{error_type}"
        return error_result

# -----------------------
# Branch processing (to enable parallelism) - OPTIMIZED
# -----------------------
def process_branch(row, include_nearby: bool, rps: float, kw_mode: str, kw_target: int) -> Tuple[List[dict], List[dict], dict]:
    msi_name, mlat, mlon = row["msi_name"], float(row["lat"]), float(row["lon"])
    pwrite(f"→ Scanning {msi_name} (lat={mlat}, lon={mlon})")

    kept_rows: List[dict] = []
    all_hits_rows: List[dict] = []
    counters = dict(kept=0, drop_type=0, drop_self=0, drop_far=0)
    seen_ids: Dict[str, dict] = {}

    def record_hit(base_row: dict, reason: str):
        rec = dict(base_row); rec["drop_reason"] = reason; rec["kept_final"] = "No"
        all_hits_rows.append(rec)

    def kw_sets():
        yield KW_BASIC
        if kw_mode in ("smart","full"):
            yield [k for k in KW_FULL if k not in KW_BASIC]

    total_candidates = 0
    with requests.Session() as s:
        for kws in kw_sets():
            for kw in kws:
                for p in search_text(s, mlat, mlon, kw, rps=rps):
                    disp = (p.get("displayName") or {}).get("text","")
                    norm = normalize_place(msi_name, mlat, mlon, p, source="text_search", matched_kw=kw)
                    if not norm: continue
                    if looks_like_self(disp):
                        counters["drop_self"] += 1; record_hit(norm, "self_brand"); continue
                    tblock = types_blocked(p.get("types", []))
                    if tblock:
                        counters["drop_type"] += 1; record_hit(norm, f"blocked_type:{tblock}"); continue
                    if norm["distance_mi"] > 30:
                        counters["drop_far"] += 1; record_hit(norm, "beyond_30mi"); continue
                    record_hit(norm, "")
                    total_candidates += 1
                    pid = norm["place_id"]
                    if pid not in seen_ids or norm["distance_mi"] < seen_ids[pid]["distance_mi"]:
                        seen_ids[pid] = norm
            if kw_mode == "smart" and total_candidates >= kw_target:
                break

        do_nearby = include_nearby and (kw_mode != "smart" or total_candidates < kw_target)
        if do_nearby:
            for p in nearby_scout(s, mlat, mlon, rps=rps):
                disp = (p.get("displayName") or {}).get("text","")
                if not name_hits_keyword(disp):
                    continue
                norm = normalize_place(msi_name, mlat, mlon, p, source="nearby_scout", matched_kw="name_match")
                if not norm: continue
                if looks_like_self(disp):
                    counters["drop_self"] += 1; record_hit(norm, "self_brand"); continue
                tblock = types_blocked(p.get("types", []))
                if tblock:
                    counters["drop_type"] += 1; record_hit(norm, f"blocked_type:{tblock}"); continue
                if norm["distance_mi"] > 30:
                    counters["drop_far"] += 1; record_hit(norm, "beyond_30mi"); continue
                record_hit(norm, "")
                pid = norm["place_id"]
                if pid not in seen_ids or norm["distance_mi"] < seen_ids[pid]["distance_mi"]:
                    seen_ids[pid] = norm

    # mark kept vs farther duplicates
    kept_map = {pid: r["distance_mi"] for pid, r in seen_ids.items()}
    for rec in all_hits_rows:
        if rec["drop_reason"] == "":
            pid = rec["place_id"]
            if pid in kept_map and abs(rec["distance_mi"] - kept_map[pid]) < 1e-6:
                rec["kept_final"] = "Yes"
            else:
                rec["drop_reason"] = "duplicate_farther"

    kept_rows = list(seen_ids.values())
    counters["kept"] = len(kept_rows)
    return kept_rows, all_hits_rows, counters

def safe_to_csv(df: pd.DataFrame, path: str, retries: int = 5, base_sleep: float = 0.7):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    last_err = None
    for i in range(retries):
        tmp = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            df.to_csv(tmp, index=False)
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            try:
                if os.path.exists(tmp): os.remove(tmp)
            except Exception:
                pass
            time.sleep(base_sleep * (2 ** i))
    fallback = os.path.join(os.path.dirname(path),
                            f"{os.path.splitext(os.path.basename(path))[0]}_{int(time.time())}.csv")
    try:
        df.to_csv(fallback, index=False)
        print(f"! Could not overwrite locked file. Wrote fallback: {fallback}")
        return
    except Exception:
        pass
    raise last_err or PermissionError(f"Could not write {path}")

def safe_to_xlsx(cleaned_df: pd.DataFrame, all_hits_df: pd.DataFrame, path: str,
                 cleaned_cols: list, all_hits_cols: list, retries: int = 5, base_sleep: float = 0.7):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    last_err = None
    for i in range(retries):
        tmp = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            with pd.ExcelWriter(tmp) as writer:
                cleaned_df[cleaned_cols].to_excel(writer, sheet_name="cleaned", index=False)
                (all_hits_df[all_hits_cols] if not all_hits_df.empty else pd.DataFrame(columns=all_hits_cols)) \
                    .to_excel(writer, sheet_name="all_hits", index=False)
            os.replace(tmp, path)
            return
        except PermissionError as e:
            last_err = e
            try:
                if os.path.exists(tmp): os.remove(tmp)
            except Exception:
                pass
            time.sleep(base_sleep * (2 ** i))
    fallback = os.path.join(os.path.dirname(path),
                            f"{os.path.splitext(os.path.basename(path))[0]}_{int(time.time())}.xlsx")
    try:
        with pd.ExcelWriter(fallback) as writer:
            cleaned_df[cleaned_cols].to_excel(writer, sheet_name="cleaned", index=False)
            (all_hits_df[all_hits_cols] if not all_hits_df.empty else pd.DataFrame(columns=all_hits_cols)) \
                .to_excel(writer, sheet_name="all_hits", index=False)
        print(f"! Could not overwrite locked file. Wrote fallback: {fallback}")
        return
    except Exception:
        pass
    raise last_err or PermissionError(f"Could not write {path}")


# -----------------------
# Main - OPTIMIZED (COMPLETE)
# -----------------------
def run(input_csv: str, outdir: str, include_nearby: bool, rps: float,
        concurrency: int, gpt_workers: int, gpt_rps: float, gpt_burst: int,
        kw_mode: str, kw_target: int, max_gpt_new: int, log_hits: bool, 
        log_limit: int, batch_size: int):
    if not API_KEY:
        print("ERROR: Set GOOGLE_MAPS_API_KEY environment variable.", file=sys.stderr)
        sys.exit(2)
    os.makedirs(outdir, exist_ok=True)

    df = pd.read_csv(input_csv)
    cols = {c.lower(): c for c in df.columns}
    if "msi_name" not in cols:
        if "name" in cols: df.rename(columns={cols["name"]:"msi_name"}, inplace=True)
        else: raise ValueError("Input must include 'msi_name' or 'name' column")
    if "lat" not in cols: raise ValueError("Input must include 'lat' column")
    if not (("lon" in cols) or ("lng" in cols) or ("longitude" in cols)):
        raise ValueError("Input must include 'lon' or 'lng' or 'longitude' column")
    if "lon" not in df.columns:
        if "lng" in df.columns: df.rename(columns={"lng":"lon"}, inplace=True)
        elif "longitude" in df.columns: df.rename(columns={"longitude":"lon"}, inplace=True)

    results_rows: List[dict] = []
    all_hits_rows: List[dict] = []
    totals = dict(kept=0, drop_type=0, drop_self=0, drop_far=0)

    eff_rps = max(0.2, rps / max(1, int(concurrency * 1.5)))

    # Parallel branch processing with progress tracking
    print(f"\n🔍 Starting competitor search for {len(df)} locations...")
    print(f"   🔧 Using {concurrency} parallel workers at {eff_rps:.1f} RPS each")
    
    futures = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        for _, row in df.iterrows():
            futures.append(ex.submit(process_branch, row, include_nearby, eff_rps, kw_mode, kw_target))

        completed = 0
        for fut in tqdm(
            as_completed(futures),
            total=len(futures),
            desc="🔍 Searching locations",
            unit="location",
            dynamic_ncols=True,
            mininterval=1.0,
            leave=True,
            disable=False
        ):
            kept_rows, hits_rows, counters = fut.result()
            results_rows.extend(kept_rows)
            all_hits_rows.extend(hits_rows)
            for k, v in counters.items():
                totals[k] = totals.get(k, 0) + v
            
            completed += 1
            if completed % 5 == 0 or completed == len(futures):
                elapsed = time.time() - start_time
                rate = completed / elapsed * 60 if elapsed > 0 else 0
                print(f"   📊 Processed {completed}/{len(futures)} locations | "
                      f"Found {totals.get('kept', 0)} competitors | Rate: {rate:.1f}/min")
    
    search_time = time.time() - start_time
    print(f"   ✅ Location search completed in {search_time/60:.1f} minutes")

    # file paths
    details_path = os.path.join(outdir, "competitors_details.csv")
    allhits_path = os.path.join(outdir, "competitors_all_hits.csv")
    xlsx_path = os.path.join(outdir, "competitors_outputs.xlsx")
    cache_path = os.path.join(outdir, "gpt_cache.json")

    # If nothing found, write empty files
    cleaned_cols = [
        "msi_name","msi_lat","msi_lon","place_id","resource_name","name","address","lat","lon",
        "distance_mi","primary_type","types","website","rating","userRatingCount",
        # Service type classifications
        "scaffolding_label","scaffolding_confidence","scaffolding_keywords","scaffolding_evidence",
        "forming_shoring_label","forming_shoring_confidence","forming_shoring_keywords","forming_shoring_evidence",
        "insulation_label","insulation_confidence","insulation_keywords","insulation_evidence",
        "motorized_label","motorized_confidence","motorized_keywords","motorized_evidence",
        "painting_label","painting_confidence","painting_keywords","painting_evidence",
        "specialty_label","specialty_confidence","specialty_keywords","specialty_evidence",
        # Size classification
        "size_bucket","size_confidence","size_evidence"
    ]
    all_hits_cols = [
        "msi_name","msi_lat","msi_lon","place_id","resource_name","name","address","lat","lon",
        "distance_mi","primary_type","types","website","rating","userRatingCount","source","matched_keyword",
        "drop_reason","kept_final",
        # Service type classifications
        "scaffolding_label","scaffolding_confidence","scaffolding_keywords","scaffolding_evidence",
        "forming_shoring_label","forming_shoring_confidence","forming_shoring_keywords","forming_shoring_evidence",
        "insulation_label","insulation_confidence","insulation_keywords","insulation_evidence",
        "motorized_label","motorized_confidence","motorized_keywords","motorized_evidence",
        "painting_label","painting_confidence","painting_keywords","painting_evidence",
        "specialty_label","specialty_confidence","specialty_keywords","specialty_evidence",
        # Size classification
        "size_bucket","size_confidence","size_evidence"
    ]

    if not results_rows and not all_hits_rows:
        safe_to_csv(pd.DataFrame(columns=cleaned_cols), details_path)
        safe_to_csv(pd.DataFrame(columns=all_hits_cols), allhits_path)
        try:
            safe_to_xlsx(pd.DataFrame(columns=cleaned_cols),
                        pd.DataFrame(columns=all_hits_cols),
                        xlsx_path, cleaned_cols, all_hits_cols)
        except Exception:
            pass

        print(f"\nNo results. Wrote empty files:\n  - {details_path}\n  - {allhits_path}\n  - {xlsx_path}")
        return details_path

    # -----------------------
    # AI labeling (kept rows only) with website facts + GPT (async if available) - OPTIMIZED
    # -----------------------
    cache = load_cache(cache_path)
    sync_client, async_client = get_openai_clients()

    # Initialize GPT tracker
    gpt_tracker = GPTCallTracker()
    print("🤖 Initializing GPT Call Tracker...")

    # Build unique places dict (fast processing - no website scraping)
    uniq_by_place: Dict[str, dict] = {}
    facts_by_place: Dict[str, dict] = {}

    print(f"📊 Processing {len(results_rows)} results into {len(set(r['place_id'] for r in results_rows))} unique companies...")
    
    for r in results_rows:
        pid = r["place_id"]
        if pid not in uniq_by_place:
            uniq_by_place[pid] = {
                "place_id": pid,
                "name": r.get("name",""),
                "address": r.get("address",""),
                "primary_type": r.get("primary_type",""),
                "types": r.get("types",""),
                "website": r.get("website",""),
                "rating": r.get("rating"),
                "userRatingCount": r.get("userRatingCount"),
            }
            # Use empty facts for maximum speed
            facts_by_place[pid] = {
                "homepage_text": "", "pages_fetched": 0, "employees": None, 
                "states_count": 0, "nationwide": False, "hiring": False, "has_linkedin": False
            }
    
    print(f"   ✅ Prepared {len(uniq_by_place)} unique companies for classification")

    # Prelabel with fast rule (skips GPT on obvious cases)
    print(f"⚡ Running fast keyword detection...")
    fast_rule_count = 0
    for pid, pdata in uniq_by_place.items():
        if pid in cache:
            gpt_tracker.log_call("cached", True)
            continue
        fr = fast_rule_label_enhanced(pdata)
        if fr:
            gpt_tracker.log_call("fast_rule", True)
            cache[pid] = fr
            fast_rule_count += 1
    
    print(f"   ✅ Fast rules classified {fast_rule_count} companies, cached: {gpt_tracker.cached_calls}")

    to_classify = [pid for pid in uniq_by_place if pid not in cache]
    if max_gpt_new and len(to_classify) > max_gpt_new:
        to_classify = to_classify[:max_gpt_new]

    print(f"\n🚀 Starting GPT-4o processing for {len(to_classify)} places...")
    print(f"   📊 Cache hits: {gpt_tracker.cached_calls}, Fast rules: {gpt_tracker.fast_rule_calls}")
    
    if len(to_classify) > 0:
        estimated_time = (len(to_classify) / gpt_rps) * 1.2  # Add 20% buffer
        print(f"   ⏱️  Estimated completion: {estimated_time/60:.1f} minutes at {gpt_rps} RPS")

    if to_classify:
        if async_client is not None:
            # OPTIMIZATION: Use rate limiter for async batch processing
            rate_limiter = AsyncRateLimiter(gpt_rps, gpt_burst)
            print(f"   🔧 Using async GPT processing: {gpt_rps} RPS, {gpt_workers} workers, burst {gpt_burst}")
            
            start_time = time.time()
            labels = asyncio.run(
                classify_batch_async(async_client, GPT_MODEL, to_classify,
                                    uniq_by_place, facts_by_place, rate_limiter, gpt_tracker,
                                    max_concurrency=max(1, int(gpt_workers)))
            )
            actual_time = time.time() - start_time
            print(f"   ✅ GPT processing completed in {actual_time/60:.1f} minutes")
            cache.update(labels)
        elif sync_client is not None:
            # Fallback: sync, in a thread pool
            def gpt_task(pid):
                pdata = uniq_by_place[pid]
                facts = facts_by_place.get(pid, {})
                return pid, classify_with_gpt(sync_client, pdata, facts)
            with ThreadPoolExecutor(max_workers=max(1, int(gpt_workers))) as ex:
                for pid, label in ex.map(gpt_task, to_classify):
                    cache[pid] = label
        else:
            # No OpenAI configured → heuristic only for size
            for pid in to_classify:
                h = heuristic_size_from_facts(facts_by_place.get(pid, {})) \
                    or {"size_bucket":"Unknown","size_confidence":0.0,"size_evidence":"no data"}
                
                result = {
                    "size_bucket":h["size_bucket"],"size_confidence":h["size_confidence"],"size_evidence":h["size_evidence"]
                }
                
                # Add all service types as Unknown
                service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
                for service_type in service_types:
                    result[f"{service_type}_label"] = "Unknown"
                    result[f"{service_type}_confidence"] = 0.0
                    result[f"{service_type}_keywords"] = "no_openai_client"
                    result[f"{service_type}_evidence"] = "no_openai_client"
                
                cache[pid] = result

    save_cache(cache_path, cache)

    # Attach AI to kept rows
    enriched_rows: List[dict] = []
    printed = 0
    for r in results_rows:
        ai = cache.get(r["place_id"], {})
        
        # Default values for all service types
        default_ai = {
            "size_bucket":"Unknown","size_confidence":0.0,"size_evidence":"not_labeled"
        }
        service_types = ["scaffolding", "forming_shoring", "insulation", "motorized", "painting", "specialty"]
        for service_type in service_types:
            default_ai[f"{service_type}_label"] = "Unknown"
            default_ai[f"{service_type}_confidence"] = 0.0
            default_ai[f"{service_type}_keywords"] = "not_labeled"
            default_ai[f"{service_type}_evidence"] = "not_labeled"
        
        # Merge with actual AI results
        for key, default_value in default_ai.items():
            if key not in ai:
                ai[key] = default_value
        
        r2 = dict(r)
        
        # Add all service type columns
        for service_type in service_types:
            r2[f"{service_type}_label"] = ai.get(f"{service_type}_label", "Unknown")
            r2[f"{service_type}_confidence"] = ai.get(f"{service_type}_confidence", 0.0)
            r2[f"{service_type}_keywords"] = ai.get(f"{service_type}_keywords", "")
            r2[f"{service_type}_evidence"] = ai.get(f"{service_type}_evidence", "")
        
        # Add size columns
        r2.update({
            "size_bucket": ai.get("size_bucket","Unknown"),
            "size_confidence": ai.get("size_confidence",0.0),
            "size_evidence": ai.get("size_evidence",""),
        })
        
        enriched_rows.append(r2)
        if log_hits and (log_limit == 0 or printed < log_limit):
            # Show all detected service types in log
            services = []
            for service_type in service_types:
                if r2.get(f"{service_type}_label") == "Yes":
                    services.append(service_type.replace("_", "/").title())
            services_str = ",".join(services) if services else "None"
            pwrite(f"[{r['msi_name']}] + {r['name']}  ({r['distance_mi']} mi)  "
                f"Services={services_str}  Size={r2['size_bucket']}")
            printed += 1

    # CLEANED sheet
    cleaned_df = pd.DataFrame(enriched_rows).sort_values(["msi_name", "distance_mi"])
    safe_to_csv(cleaned_df, details_path)

    # ALL-HITS (AI columns only for kept rows)
    all_hits_df = pd.DataFrame(all_hits_rows)

    if not all_hits_df.empty:
        ai_cols = [
            "place_id",
            # Service type classifications
            "scaffolding_label","scaffolding_confidence","scaffolding_keywords","scaffolding_evidence",
            "forming_shoring_label","forming_shoring_confidence","forming_shoring_keywords","forming_shoring_evidence",
            "insulation_label","insulation_confidence","insulation_keywords","insulation_evidence",
            "motorized_label","motorized_confidence","motorized_keywords","motorized_evidence",
            "painting_label","painting_confidence","painting_keywords","painting_evidence",
            "specialty_label","specialty_confidence","specialty_keywords","specialty_evidence",
            # Size classification
            "size_bucket", "size_confidence", "size_evidence"
        ]

        # Ensure AI columns exist on cleaned_df (safety)
        for c in ai_cols:
            if c not in cleaned_df.columns:
                cleaned_df[c] = ""

        join_df = cleaned_df[ai_cols].drop_duplicates("place_id")
        all_hits_df = all_hits_df.merge(join_df, on="place_id", how="left")

        # Ensure required columns exist before ordering
        missing = [c for c in all_hits_cols if c not in all_hits_df.columns]
        for c in missing:
            all_hits_df[c] = ""

        all_hits_df = all_hits_df[all_hits_cols]
        all_hits_df.sort_values(
            ["msi_name", "kept_final", "distance_mi"],
            ascending=[True, False, True],
            inplace=True
        )

        safe_to_csv(all_hits_df, allhits_path)
    else:
        # make sure the variable exists with the right schema for the XLSX write
        all_hits_df = pd.DataFrame(columns=all_hits_cols)
        safe_to_csv(all_hits_df, allhits_path)

    # XLSX (2 tabs) — uses safe writer and the actual dataframes built above
    try:
        safe_to_xlsx(cleaned_df, all_hits_df, xlsx_path, cleaned_cols, all_hits_cols)
    except Exception as e:
        print(f"! Could not write Excel workbook ({e}). CSVs were written instead.")

    # Summary
    print("\nSummary")
    print("-------")
    print(f"Kept: {totals['kept']}, Dropped by type: {totals['drop_type']}, Self-brand: {totals['drop_self']}, >30mi: {totals['drop_far']}")
    print(f"Wrote:\n  - {details_path}\n  - {allhits_path}\n  - {xlsx_path}\n  - cache: {cache_path}")

    # Print final GPT statistics
    print("\n" + "="*60)
    print("🤖 FINAL GPT STATISTICS")
    print("="*60)
    final_stats = gpt_tracker.get_stats()
    print(f"⏱️  Total Time: {final_stats['elapsed_time']:.1f} seconds")
    print(f"📊 Total Calls: {final_stats['total_calls']}")
    print(f"✅ Successful: {final_stats['successful_calls']}")
    print(f"❌ Failed: {final_stats['failed_calls']}")
    print(f"💾 Cached: {final_stats['cached_calls']}")
    print(f"⚡ Fast Rules: {final_stats['fast_rule_calls']}")
    print(f"🔄 Batch Calls: {final_stats['batch_calls']}")
    print(f" Success Rate: {final_stats['success_rate']:.1f}%")
    print(f"⚡ Speed: {final_stats['calls_per_minute']:.1f} calls/minute")
    print(f"💰 Total Cost: ${final_stats['total_cost']:.4f}")
    print(f" Total Tokens: {final_stats['total_tokens']:,}")
    if final_stats['error_counts']:
        print(f"❌ Error Breakdown: {final_stats['error_counts']}")
    print("="*60)
    
    return details_path

# -----------------------
# Entrypoint
# -----------------------
if __name__ == "__main__":
    args = parse_args()
    run(
        input_csv=args.input,
        outdir=args.outdir,
        include_nearby=args.include_nearby,
        rps=args.rps,
        concurrency=int(args.concurrency),
        gpt_workers=int(args.gpt_workers),
        gpt_rps=float(args.gpt_rps),
        gpt_burst=int(args.gpt_burst),
        kw_mode=args.kw_mode,
        kw_target=int(args.kw_target),
        max_gpt_new=int(args.max_gpt) if args.max_gpt is not None else 0,
        log_hits=args.log_hits,
        log_limit=int(args.log_limit) if args.log_limit is not None else 0,
        batch_size=int(args.batch_size),
    )
