# Procurement Watchdog Lakehouse

A Spark-based lakehouse pipeline for Polish public procurement data (BZP/eZamówienia),
focused on deterministic Bronze → Silver processing. Business-facing analytical logic
lives in a separate dbt repo:

- `https://github.com/jjkiljanski/procurement-watchdog-analytics`

## Overview

The repository is organized around medallion-style layers:

- `bronze_raw`: raw API payloads (`data/bronze_raw/bzp_YYYY-MM-DD.json` or `gs://bucket/bronze_raw/`)
- `bronze`: validated canonical notices in Parquet, partitioned by `noticeType/publicationDateDay`
- `silver`: conformed notice-level datasets split into common envelope + notice-type tables
- `silver/notice_update_deltas`: change delta records built from `NoticeUpdateNotice` silver

Core goals:

- deterministic, idempotent processing
- safe daily reruns (date-partition overwrite)
- stable schemas for downstream analytics/reporting
- reproducible lineage metadata (inputs, code hashes, run metadata)
- business-logic-agnostic lakehouse preparation in Spark; downstream business interpretation in dbt

## Running Modes

The pipeline supports two **deployment environments**, controlled by `RUNTIME_ENV`:

| Mode | `RUNTIME_ENV` | Compute | Storage |
|---|---|---|---|
| **Local** | `local` (default) | Local PySpark | `data/` directory |
| **GCP** | `gcp` | Dataproc Serverless | GCS + BigQuery |

And two **pipeline modes**:

| Pipeline mode | Trigger | What it does |
|---|---|---|
| **Daily** | 03:00 UTC cron (Airflow `bzp_daily` DAG) | Download yesterday → bronze → silver → deltas |
| **Backfill** | Manual (Airflow `bzp_backfill` DAG) | Process a date range with per-day hash checks |

See `docs/cloud_architecture.md` for GCP setup and `docs/runbooks/OPERATING_MODES.md` for
local operation.

## Quick Start — Local

```bash
# Install dependencies
pip install -e ".[dev]"

# (Optional) set data root explicitly — defaults to data/ in CWD
export LOCAL_DATA_ROOT=data
export RUNTIME_ENV=local   # this is the default

# Fetch yesterday's data
python scripts/pipeline/fetch_bzp_yesterday.py

# Build bronze
python scripts/pipeline/build_bronze.py

# Build silver for yesterday
python scripts/pipeline/build_silver_day.py

# Build notice-change deltas for yesterday
python scripts/pipeline/build_silver_update_deltas.py $(date -d yesterday +%Y-%m-%d)

# Or use the convenience wrapper (bronze → silver → deltas):
python scripts/ops/run_transforms_for_day.py $(date -d yesterday +%Y-%m-%d)
```

Or with Docker (recommended, matches the GCP container):

```bash
docker build -t procurement-lakehouse .
docker run --rm -v $(pwd)/data:/app/data -e RUNTIME_ENV=local \
  procurement-lakehouse python scripts/pipeline/fetch_bzp_yesterday.py 2025-10-01
docker run --rm -v $(pwd)/data:/app/data -e RUNTIME_ENV=local \
  procurement-lakehouse python scripts/pipeline/build_bronze.py 2025-10-01
docker run --rm -v $(pwd)/data:/app/data -e RUNTIME_ENV=local \
  procurement-lakehouse python scripts/pipeline/build_silver_day.py 2025-10-01
```

For Windows/WSL2 performance tips see `docs/runbooks/LOCAL_WINDOWS_DOCKER.md`.

## Quick Start — GCP

```bash
# 1. Configure environment
cp config/runtime_gcp.env.example ~/.procurement-gcp.env
# Edit ~/.procurement-gcp.env with your project values
export $(grep -v '^#' ~/.procurement-gcp.env | xargs)

# 2. Build and push containers
GIT_SHA=$(git rev-parse --short HEAD)
docker build -f Dockerfile.spark --build-arg GIT_SHA=$GIT_SHA \
  -t ${DATAPROC_CONTAINER_IMAGE} .
docker push ${DATAPROC_CONTAINER_IMAGE}

# 3. Upload pipeline scripts to GCS
gsutil -m cp scripts/pipeline/*.py gs://${LAKEHOUSE_BUCKET}/jobs/

# 4. Create BigQuery external tables
python scripts/ops/setup_bq_external_tables.py

# 5. Sync DAGs to Cloud Composer (see docs/cloud_architecture.md for full setup)
```

See `docs/cloud_architecture.md` for complete GCP setup instructions.

## Data Layout

### Bronze

- Raw input: `{data_root}/bronze_raw/bzp_YYYY-MM-DD.json`
- Canonical Parquet: `{data_root}/bronze/notices/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`
- Validation errors: `{data_root}/bronze/errors/bzp_YYYY-MM-DD_errors.json`

### Silver

Built by `scripts/pipeline/build_silver_day.py`:

- Notice-type tables: `{data_root}/silver/notice_type_tables/noticeType=<TYPE>/data_model=<MODEL>/publicationDateDay=YYYY-MM-DD/`
- Common envelope: `{data_root}/silver/common_envelope/publicationDateDay=YYYY-MM-DD/`
- Change deltas: `{data_root}/silver/notice_update_deltas/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`

On GCP, `{data_root}` = `gs://{LAKEHOUSE_BUCKET}`.  Silver is also available via
BigQuery external tables (created by `scripts/ops/setup_bq_external_tables.py`).

## Key Scripts

| Script | Purpose |
|---|---|
| `scripts/pipeline/fetch_bzp_yesterday.py` | Fetch daily API payloads to bronze_raw |
| `scripts/pipeline/build_bronze.py` | Validate + write canonical Bronze Parquet |
| `scripts/pipeline/build_silver_day.py` | Silver build for a single day |
| `scripts/pipeline/build_silver_backfill.py` | Silver backfill with resumable state |
| `scripts/pipeline/build_silver_update_deltas.py` | NoticeUpdateNotice change deltas |
| `scripts/pipeline/build_obs.py` | Observability snapshot + dashboard |
| `scripts/ops/setup_bq_external_tables.py` | Create/replace BigQuery external tables |
| `scripts/ops/run_pipeline.py` | Local daily orchestrator: fetch → bronze → silver |

## Container Images

| Dockerfile | Purpose |
|---|---|
| `Dockerfile` | Local dev runner |
| `Dockerfile.spark` | Dataproc Serverless container (bronze, silver, deltas batches) |
| `Dockerfile.downloader` | Cloud Run Job container (BZP API fetch) |

Build all:
```bash
docker build -t procurement-lakehouse .
docker build -t procurement-spark -f Dockerfile.spark .
docker build -t procurement-downloader -f Dockerfile.downloader .
```

## Airflow DAGs

| DAG | Trigger | Purpose |
|---|---|---|
| `dags/daily_dag.py` | 03:00 UTC cron | Full daily pipeline for yesterday |
| `dags/backfill_dag.py` | Manual | Date-range backfill |

DAGs are synced to Cloud Composer from this repo.  See
`docs/cloud_architecture.md` for setup and `.github/workflows/deploy.yml` for
the planned CI/CD automation (not yet active).

## Architecture Docs

- `docs/cloud_architecture.md` — GCP deployment, runtime abstraction, setup
- `docs/iceberg.md` — Planned migration from Parquet to Iceberg for silver
- `docs/observability.md` — Pipeline run metadata + data quality metrics
- `docs/runbooks/OPERATING_MODES.md` — Local + GCP operating runbook
- `docs/runbooks/LOCAL_WINDOWS_DOCKER.md` — Windows/WSL2 Docker tips

## Testing

```bash
pytest -q
```

## Repository Structure

```text
src/procurement/
  bronze/           — validation models
  silver/           — HTML parsing + section pipeline
  runtime/          — provider abstraction (local / gcp / ...)
  obs.py            — observability writers
apps/
  downloader/       — Cloud Run Job: BZP API fetch adapter
  common/           — shared runtime utilities for apps
scripts/
  pipeline/         — core pipeline scripts (entry points for Dataproc batches)
  ops/              — orchestration helpers + setup scripts
  dev/              — exploratory one-off tools (non-prod)
dags/               — Airflow DAGs (synced to Cloud Composer)
config/             — environment variable templates
docs/               — architecture + runbook documentation
tests/              — pytest suite
```

## Disclaimer

This project provides data engineering and analytical signals for transparency/research.
Outputs are not legal conclusions.
