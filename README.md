# Procurement Watchdog Lakehouse

A Spark-based lakehouse pipeline for public procurement data (BZP), focused on deterministic Bronze → Silver processing. Business-facing analytical logic lives in a separate dbt repo:

- `https://github.com/jjkiljanski/procurement-watchdog-analytics`

## Overview

The repository is organized around medallion-style layers:

- `bronze_raw`: raw API payloads (`data/bronze_raw/bzp_YYYY-MM-DD.json`)
- `bronze`: validated canonical notices in Parquet, partitioned by `noticeType/publicationDateDay`
- `silver`: conformed notice-level datasets split into common envelope + notice-type tables
- `silver/case_derived_facts`: Spark-built case-grain derived layer used as input for downstream analytics

Core goals:

- deterministic, idempotent processing
- safe daily reruns (date-partition overwrite)
- stable schemas for downstream analytics/reporting
- reproducible lineage metadata (inputs, code hashes, run metadata)
- business-logic-agnostic lakehouse preparation in Spark; downstream business interpretation in dbt

## Operating Modes

- `Daily Incremental (CRON)`:
  - fetch yesterday to `bronze_raw`,
  - build Bronze for that day,
  - build Silver for that day,
  - update `case_derived_facts` incrementally.
- `Massive Backfill`:
  - first fetch large date ranges to `bronze_raw`,
  - then run Spark transforms (`bronze → silver → case_derived`) in long-lived jobs.

See `docs/runbooks/OPERATING_MODES.md` for exact sequencing and retry semantics.

## Current Data Layout

### Bronze

- `bronze_raw` input files: `data/bronze_raw/bzp_YYYY-MM-DD.json`
- `bronze` canonical Parquet: `data/bronze/notices/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`
- Bronze validation errors: `data/bronze/errors/bzp_YYYY-MM-DD_errors.json`
- Bronze lineage manifests: `data/bronze/_meta/day=YYYY-MM-DD.json`
- API fetch and Bronze Spark conversion are intentionally separated (`bronze_raw` -> `bronze`) to improve backfill throughput and failure isolation.

### Silver

Built by `scripts/pipeline/build_silver_day.py` or `scripts/pipeline/build_silver_backfill.py` (reads Bronze Parquet by default):

**Two parallel pipelines:**

**1. Section table pipeline** — profile-driven HTML → structured section columns per `data_model`:

- `data/silver/notice_type_tables/noticeType=<TYPE>/data_model=<MODEL>/publicationDateDay=YYYY-MM-DD/`

  Each notice type has a `src/procurement/silver/notice_schemas/*_profile.json` mapping section
  numbers to column names and data models. `data_model` values include `core` (one row/notice),
  `part`, `client`, `change_matter`, `criterion_procedure`, etc. Nested child models such as
  `part.criterion` are materialized as their own Silver child tables (for example `data_model=part_criterion`)
  rather than as nested arrays on the parent row.

  Silver is intended to stay structurally faithful and business-logic-agnostic. It may include
  deterministic parsing/normalization needed to make the data queryable, but downstream business
  interpretation is expected to happen in the analytics repo.

**2. Common envelope pipeline** — structured Bronze columns only, no HTML parsing:

- `data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/`

  Contains all Bronze structured fields plus small derived columns: `clientTypeName`,
  `provinceName`, `caseId` (coalesce of `tenderId`/`objectId`), `noticeStage`.

Built by `scripts/pipeline/build_case_derived_facts.py`:

- `data/silver/case_derived_facts/asOfDate=YYYY-MM-DD/`

Lineage/performance metadata: `data/silver/_meta/day=YYYY-MM-DD.json`

Notes:

- Ingest is processed in parallel notice-type batches.
- Section profiles and Pydantic models live in `src/procurement/silver/notice_schemas/`.
- See `src/procurement/silver/README.md` for full architecture details.

## Key Scripts

- `scripts/pipeline/fetch_bzp_yesterday.py` - fetch daily API payloads to `bronze_raw`
- `scripts/pipeline/build_bronze.py` - validate + write canonical Bronze Parquet
- `scripts/pipeline/build_silver_day.py` - Silver build for a single day
- `scripts/pipeline/build_silver_backfill.py` - Silver backfill over a date range with state tracking
- `scripts/pipeline/build_case_derived_facts.py` - case-grain Silver derived layer (`full` / `incremental`)
- `scripts/pipeline/build_obs.py` - Silver observability snapshot + dashboard
- `scripts/ops/run_pipeline.py` - daily orchestrator: fetch → bronze → silver
- `scripts/ops/run_transforms_for_day.py` - transform stack without fetch: bronze → silver → case-derived
- `scripts/ops/backfill_parallel.py` - bounded parallel backfill runner
- `scripts/dev/*` - exploratory one-off tools (non-prod)
- `docs/runbooks/OPERATING_MODES.md` - operational runbook (daily vs backfill + restart semantics)

## GCP Runtime Images

This repo provides three deployable runtime adapters that reuse the same core scripts as local runs:

- `downloader`: job adapter for API fetch (`apps/downloader/main.py`)
- `dispatcher`: HTTP service that picks next backfill date and triggers downloader (`apps/dispatcher/main.py`)
- `launcher`: HTTP service that triggers pipeline launch command (`apps/launcher/main.py`)

Build commands:

```bash
docker build -t procurement-downloader -f Dockerfile.downloader .
docker build -t procurement-dispatcher -f Dockerfile.dispatcher .
docker build -t procurement-launcher -f Dockerfile.launcher .
```

Runtime env contracts are documented in `docs/deployment/RUNTIME_CONTRACTS.md`.

## Local Execution

Recommended (Docker Spark runtime):

```bash
docker build -t procurement-lakehouse .
docker run --rm -v <repo_path>:/app -w /app procurement-lakehouse python scripts/pipeline/fetch_bzp_yesterday.py 2025-10-01
docker run --rm -v <repo_path>:/app -w /app procurement-lakehouse python scripts/pipeline/build_bronze.py 2025-10-01
docker run --rm -v <repo_path>:/app -w /app procurement-lakehouse python scripts/pipeline/build_silver_day.py 2025-10-01 --bronze-dir data/bronze --silver-dir data/silver
```

For Windows users, see `docs/runbooks/LOCAL_WINDOWS_DOCKER.md` for the optimal Docker invocation using WSL2 native mounts (~100 s/day with tuned flags):

```bash
# Best-performing flags on Windows/WSL2 (~100 s for a typical production day):
python scripts/pipeline/build_silver_day.py 2025-10-01 \
  --bronze-dir data/bronze \
  --silver-dir data/silver \
  --shuffle-partitions 8 \
  --max-batch-workers 6
```

Silver outputs are consumed by the dbt analytics repo; this repo does not produce business marts.

Direct Python runs are also possible if local Spark/PySpark is configured.

## Testing

```bash
pytest -q
```

## Contributing

See `CONTRIBUTING.md` for repository structure, naming conventions, artifact policy, and documentation update rules.

## Repository Structure

```text
src/procurement/
  bronze/
  silver/
scripts/
  pipeline/
  ops/
  dev/
apps/
  downloader/
  dispatcher/
  launcher/
  common/
tests/
docs/
examples/
data/
```

## Disclaimer

This project provides data engineering and analytical signals for transparency/research. Outputs are not legal conclusions.
