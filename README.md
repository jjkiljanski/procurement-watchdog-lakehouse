# Procurement Watchdog Lakehouse

A Spark-based lakehouse pipeline for public procurement data (BZP first), with deterministic Bronze -> Silver -> Gold processing and daily/as-of analytical marts.

## Overview

The repository is organized around three data layers:

- `bronze`: raw API payloads (`data/raw/bzp_YYYY-MM-DD.json`)
- `silver`: conformed notice-level datasets split into common envelope + notice-type tables
- `gold`: analytical marts and buyer daily signals

Core goals:

- deterministic, idempotent processing
- safe daily reruns (date-partition overwrite)
- stable schemas for downstream analytics/reporting

## Current Data Layout

### Bronze

- Input files: `data/raw/bzp_YYYY-MM-DD.json`
- Minimal transformation, source-preserving records

### Silver

Built by `scripts/build_silver.py`:

- `data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/`
- `data/silver/notice_type_tables/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`

Built by `scripts/build_case_derived_facts.py`:

- `data/silver/case_derived_facts/asOfDate=YYYY-MM-DD/`

Notes:

- Ingest is processed in sorted notice-type batches.
- Envelope stores shared columns used across types.
- Notice-type tables store type-specific payloads and remove mostly-null cross-type columns.
- Address fields (`ulica`, `kod_pocztowy`) are promoted to envelope.
- `procedureResult` and `procedureResultParsed` are kept only for `TenderResultNotice` specific output.

### Gold

Built by `scripts/build_gold.py`:

- `data/gold/case_mart/date=YYYY-MM-DD/`
- `data/gold/buyer_mart/date=YYYY-MM-DD/`
- `data/gold/market_mart/date=YYYY-MM-DD/`
- `data/gold/signals_buyer_daily/date=YYYY-MM-DD/`

`build_gold.py` supports:

- `--scope daily` (single-day marts)
- `--scope asof` (marts built from all Silver days up to target date)

## Key Scripts

- `scripts/fetch_raw.py` - fetch raw daily data
- `scripts/build_silver.py` - notice-ingest Silver build
- `scripts/build_case_derived_facts.py` - case-grain Silver derived layer (`full` / `incremental`)
- `scripts/build_gold.py` - Gold marts/signals
- `scripts/build_run_stats.py` - run-level reporting artifacts

## Local Execution

Recommended (Docker Spark runtime):

```bash
docker build -t procurement-pipeline .
docker run --rm -v <repo_path>:/app -w /app procurement-pipeline python scripts/build_silver.py 2025-10-01
docker run --rm -v <repo_path>:/app -w /app procurement-pipeline python scripts/build_case_derived_facts.py 2025-10-01 --mode full
docker run --rm -v <repo_path>:/app -w /app procurement-pipeline python scripts/build_gold.py 2025-10-01 --scope daily
```

Direct Python runs are also possible if local Spark/PySpark is configured.

## Testing

```bash
pytest -q
```

## Repository Structure

```text
src/procurement/
  bronze/
  silver/
  gold/
  ingest/
scripts/
tests/
docs/
examples/
data/
```

## Disclaimer

This project provides data engineering and analytical signals for transparency/research. Outputs are not legal conclusions.
