# BrandSafway Competitor Mapping

This repository contains a client-ready Python script to **map competitor locations around BrandSafway branches** using **Google Places API**, then **enrich each competitor with company size and service offerings** using the **McKinsey QuantumBlack OpenAI Gateway** (**Responses API + `web_search`**).

## What it produces

For each branch (lat/lon):

1. Finds nearby candidate companies within a configurable radius (default **30 miles**) using **keyword text search only** (with pagination; no nearby-type search)
2. De-duplicates candidates globally (enrich each unique `place_id` once even if it appears near multiple branches)
3. Enriches candidates with:
   - `is_competitor` (vs BrandSafway)
   - `size_bucket`: **Small** (0–49 employees), **Medium** (50–249), **Large** (250+), or **Unknown** when evidence is thin
   - `employee_estimate` (free-text range when available)
   - `size_confidence`, `size_evidence` (0–1 and brief justification for size)
   - Per-service offerings with confidence (0–1): `scaffolding`, `forming_shoring`, `insulation`, `painting`, `motorized`, `specialty` (each has a boolean, `_confidence`, and `_evidence` columns in the CSV)
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
  - `openpyxl` (for reading/writing .xlsx branch input and output)

Install:

```bash
pip install -U pandas requests openai openpyxl
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

**Inputs:** Branch input path via `--branches` (`.xlsx` or `.csv`); optional keyword overrides via `--keywords` (semicolon-separated).

- **.xlsx** (e.g. `branch_input.xlsx`): columns `branch_name`, `latitude`, `longitude`, `region`.
- **.csv**: columns `branch_name`, `latitude`, `longitude`; optional `region`.

Example (.xlsx): columns `branch_name`, `latitude`, `longitude`, `region`. Example (.csv):

```csv
branch_name,latitude,longitude,region
Houston North,29.8765,-95.3621,Gulf Coast
Baton Rouge,30.4515,-91.1546,Gulf Coast
```

## Run

Basic run:

```bash
python bsw_competitive_index.py --branches branch_input.xlsx
```

Specify output directory:

```bash
python bsw_competitive_index.py --branches branch_input.xlsx --output_dir out
```

## Outputs

Written to `--output_dir` (default: `out`):

1. **`competitors_enriched.xlsx`**
   - One row per branch × place (branch-candidate pairs).
   - **Branch/place:** `branch_name`, `region` (second column, from branch input), `branch_latitude`, `branch_longitude`, `place_id`, `company_name`, `address`, `latitude`, `longitude`, `website`, `primary_type`, `types`, `rating`, `user_rating_count`, `distance_miles`.
   - **Enrichment:** `is_competitor`, `size_bucket`, `employee_estimate`, `size_confidence`, `size_evidence`; per-service flags and confidences (`scaffolding`, `scaffolding_confidence`, `scaffolding_evidence`, and similarly for `forming_shoring`, `insulation`, `painting`, `motorized`, `specialty`); `summary`, `confidence`, `citations`.
   - **Prefilter audit:** `prefilter_score`, `prefilter_reasons`, `prefilter_skipped`.
   - **`ai_enriched`:** `True` when the row’s enrichment came from OpenAI or cache (originally from AI); `False` when from prefilter fallback or hard fallback.

2. **`branch_competitor_summary.csv`**
   - Branch-level summary: `branch_name`, `region` (if in input), `competitor_count`, `avg_distance_miles`, `median_distance_miles`; size-bucket counts; per-service high-confidence counts; `closest_competitors`.

## Cache files

The script uses two separate cache files (both created/updated in the working directory, or at the paths you specify):

- **Enrichment cache** (`--cache_path`, default `enrichment_cache.json`): Keyed by `place_id`. Stores full OpenAI enrichment so re-runs reuse results and avoid extra API cost. Optional TTL via `--cache_ttl_days`.
- **Prefilter cache** (`--prefilter_cache_path`, default `prefilter_cache.json`): Keyed by **normalized company name**. Stores prefilter score and reasons so the same business name reuses the heuristic across runs and across different `place_id`s; keeps skip/don’t-skip decisions consistent.

## Cost & speed controls (most important flags)

See **Cache files** above for the two cache types and their roles.

### 1) Enrichment cache (biggest cost saver)
- `--cache_path enrichment_cache.json` (default)
- `--cache_ttl_days 90` (default)

Reruns are **dramatically cheaper** because previously enriched `place_id`s are reused. To force a fresh run: delete the enrichment cache file, or set `--cache_ttl_days 0`.

### 2) Prefilter gate (reduces web_search calls)
A lightweight heuristic prefilter runs **before** OpenAI enrichment. It scores each place from name, website, address, and place types (no API calls). Only places with score below the threshold are skipped; when in doubt they are sent to OpenAI to avoid false negatives.

- Default: enabled with `--prefilter_min_score 2`
- Disable: `--disable_prefilter` (higher cost, potentially higher recall)
- **Prefilter cache:** Results are cached by normalized company name in `--prefilter_cache_path` (default `prefilter_cache.json`). See **Cache files** above.

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
  --branches branch_input.xlsx \
  --model gpt-4o-mini \
  --openai_batch_size 5 \
  --prefilter_min_score 3 \
  --max_candidates_per_branch 50
```

### Highest quality (higher cost)
```bash
python bsw_competitive_index.py \
  --branches branch_input.xlsx \
  --model gpt-4o \
  --openai_batch_size 1 \
  --disable_prefilter \
  --max_candidates_per_branch 0
```

## Interpreting results

- Filter to likely competitors using:
  - `is_competitor == True`
  - `confidence >= 0.6` (adjust as needed)
- In **branch_competitor_summary.csv**, service **matches** (confidence ≥ 0.7) are in columns `count_*_high_confidence` (e.g. `count_scaffolding_high_confidence`).
- In **competitors_enriched.csv**, raw flags and per-service confidence: `scaffolding`, `scaffolding_confidence`, `scaffolding_evidence`, etc., for coverage maps per branch.

## Troubleshooting

- **Missing env var error:** ensure keys are exported in the same shell.
- **429 / rate limit errors:** reduce `--openai_rps`, reduce `--openai_max_in_flight`, or decrease `--enrich_workers`.
- **No candidates found:** broaden keywords via `--keywords` or increase `--max_candidates_per_branch`.

## Known limitations

- Google Places does not guarantee returning *every* business in a radius. This script maximizes recall using a tuned set of keyword text searches (with pagination).
- Web search enrichment quality depends on online footprint and naming ambiguity. Use `top_sources` + `summary` + `confidence` for auditing.

---
