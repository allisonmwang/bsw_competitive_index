# BrandSafway Competitor Mapping

This repository contains a client-ready Python script to **map competitor locations around BrandSafway branches** using **Google Places API**, then **enrich each competitor with company size and service offerings** using the **McKinsey QuantumBlack OpenAI Gateway** (**Responses API + `web_search`**).

## What it produces

For each branch (lat/lon):

1. Finds nearby candidate companies within a configurable radius (default **30 miles**)
2. De-duplicates candidates globally (enrich each unique `place_id` once even if it appears near multiple branches)
3. Enriches candidates with:
   - `is_competitor` (vs BrandSafway)
   - `size_bucket`: **Small** (0–49 employees), **Mid** (50–249), **Large** (250+), or **Unknown** when evidence is thin
   - `employee_estimate` (free-text range when available)
   - `size_confidence`, `size_evidence` (0–1 and brief justification for size)
   - Per-service offerings with confidence (0–1): scaffolding, forming_shoring, industrial_insulation, coatings_painting, motorized_access
   - **Service matches** in summaries use only offerings with confidence ≥ 0.7
   - `confidence` (overall 0–1), `summary`, `top_sources` (up to 3 URLs)

## Files

- `bsw_competitive_index.py` — main script
- `README.md` — this guide

## Requirements

- Python **3.9+** recommended
- Packages:
  - `pandas`
  - `requests`
  - `openai` (Python SDK **v1+**; must support `client.responses.create(...)`)

Install:

```bash
pip install -U pandas requests openai
```

## Authentication (environment variables)

Set these in your terminal/session:

- `GOOGLE_PLACES_API_KEY`
- `OPENAI_API_KEY`
- `OPENAI_BASE_URL` (QuantumBlack gateway base URL)

Example (bash):

```bash
export GOOGLE_PLACES_API_KEY="..."
export OPENAI_API_KEY="..."
export OPENAI_BASE_URL="https://openai.prod.ai-gateway.quantumblack.com/<tenant-id>/v1"
```

> **Security note:** Do not hardcode or commit API keys.

## Input format

Provide a branches CSV with columns:

- `branch_name`
- `latitude`
- `longitude`

Example:

```csv
branch_name,latitude,longitude
Houston North,29.8765,-95.3621
Baton Rouge,30.4515,-91.1546
```

## Run

Basic run:

```bash
python bsw_competitive_index.py --branches branches.csv
```

Specify output directory:

```bash
python bsw_competitive_index.py --branches branches.csv --output_dir out
```

## Outputs

Written to `--output_dir` (default: current directory):

1. `competitors_enriched.csv`
   - Branch-candidate rows (one row per branch × place)
   - Includes enrichment fields and prefilter audit fields

2. `branch_competitor_summary.csv`
   - Branch-level summary stats (counts, distance stats, size distribution, service counts, closest competitors)

## Cost & speed controls (most important flags)

### 1) Persistent caching (biggest cost saver)
- `--cache_path enrichment_cache.json` (default)
- `--cache_ttl_days 90` (default)

Reruns will be **dramatically cheaper** because previously enriched `place_id`s are reused.

To force a fresh run:
- delete the cache file, or set `--cache_ttl_days 0`.

### 2) Prefilter gate (reduces web_search calls)
The script uses a lightweight heuristic prefilter **before** OpenAI enrichment. It scores each place from name, website, address, and place types (no API calls). Only places with score below the threshold are skipped; when in doubt they are sent to OpenAI to avoid false negatives.

- Default: enabled with `--prefilter_min_score 2`
- Disable: `--disable_prefilter` (higher cost, potentially higher recall)
- **Company-name cache:** Prefilter results are cached by normalized company name in `--prefilter_cache_path` (default `prefilter_cache.json`). The same business name reuses the cached score across runs and across different `place_id`s, so you avoid recomputing the heuristic and get consistent skip/don’t-skip decisions.

Prefilter audit columns in `competitors_enriched.csv`:
- `prefilter_score`
- `prefilter_reasons`
- `prefilter_skipped`

### 3) Candidate cap per branch
Reduce candidate count (and cost) with:

```bash
--max_candidates_per_branch 75
```

Set `0` to remove the cap (higher cost).

### 4) Batch multiple companies per OpenAI call
Reduces per-call overhead (often cheaper/faster), but very large batches can reduce answer quality.

- `--openai_batch_size 3` (default)
- Try `5` if you want lower cost and your results remain good

### 5) Model choice
Default is a lower-cost model:

- `--model gpt-4o-mini` (default)

Use a stronger (typically higher-cost) model if needed:

```bash
--model gpt-4o
```

### 6) Concurrency and throttling
- Branch discovery:
  - `--branch_workers 8`
  - `--places_rps 5.0`
- Enrichment:
  - `--enrich_workers 12`
  - `--openai_rps 1.5`
  - `--openai_max_in_flight 6`

**Recommended tuning approach:**
1. Start with defaults.
2. If you hit 429/Rate limit errors, reduce `--openai_rps` and/or `--openai_max_in_flight`.
3. If you need faster runs, increase `--enrich_workers` modestly while keeping throttles safe.

## Recommended presets

### Lowest cost (good quality)
```bash
python bsw_competitive_index.py \
  --branches branches.csv \
  --model gpt-4o-mini \
  --openai_batch_size 5 \
  --prefilter_min_score 3 \
  --max_candidates_per_branch 50
```

### Highest quality (higher cost)
```bash
python bsw_competitive_index.py \
  --branches branches.csv \
  --model gpt-4o \
  --openai_batch_size 1 \
  --disable_prefilter \
  --max_candidates_per_branch 0
```

## Interpreting results

- Filter to likely competitors using:
  - `is_competitor == True`
  - `confidence >= 0.6` (adjust as needed)
- Service **matches** (confidence ≥ 0.7) are in columns `offers_*_high_confidence` (e.g. `offers_scaffolding_high_confidence`).
- Raw flags and per-service confidence: `offers_scaffolding`, `scaffolding_confidence`, etc., for coverage maps per branch.

## Troubleshooting

- **Missing env var error:** ensure keys are exported in the same shell.
- **429 / rate limit errors:** reduce `--openai_rps`, reduce `--openai_max_in_flight`, or decrease `--enrich_workers`.
- **No candidates found:** broaden keywords via `--keywords` or increase `--max_candidates_per_branch`.

## Known limitations

- Google Places does not guarantee returning *every* business in a radius.
  This script maximizes recall using a tuned keyword set plus nearby-type search.
- Web search enrichment quality depends on online footprint and naming ambiguity.
  Use `top_sources` + `summary` + `confidence` for auditing.

---
