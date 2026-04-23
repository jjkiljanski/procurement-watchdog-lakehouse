# Procurement Watchdog Lakehouse

Production-grade lakehouse for Polish public procurement transparency — Spark, Apache Iceberg, GCS/BigQuery, Cloud Composer, 900+ tests.

Ingests the full Polish public procurement bulletin (BZP/eZamówienia API) daily, transforms it through a Bronze → Silver medallion pipeline, and exposes clean analytical datasets via BigQuery external tables. Business-facing logic lives in a companion dbt repo:
[procurement-watchdog-analytics](https://github.com/jjkiljanski/procurement-watchdog-analytics)

---

## Architecture

```
BZP API
   │  Cloud Run Job (Dockerfile.downloader)
   ▼
bronze_raw/          — raw JSON payloads, one file per day
   │  Dataproc Serverless (Dockerfile.spark)
   ▼
bronze/              — Pydantic-validated canonical Parquet, partitioned by noticeType/publicationDateDay
   │
   ▼
silver/              — Apache Iceberg tables on GCS; notice-type section tables + common envelope
   │
   ▼
silver/notice_update_deltas/   — change-delta records from NoticeUpdateNotice
   │
   ▼
BigQuery external Iceberg tables  ←  setup_bq_external_tables.py --format iceberg
   │
   ▼
dbt analytics (separate repo)
```

Cloud Composer (Airflow) orchestrates the full pipeline.
Both a **daily incremental** mode (cron, yesterday's data) and a **date-range backfill**
mode (manual trigger, full date window) are supported.

---

## Key Design Decisions

**Idempotency everywhere.** Every pipeline step is safe to re-run.
Bronze uses date-partition overwrite; silver uses Iceberg `overwritePartitions()` per notice-type per day.
Processed-date manifests (SHA-256 of the entry-point script) guard against re-running unchanged batches during backfill.

**Apache Iceberg for silver.** The silver layer writes to an Iceberg HadoopCatalog
warehouse (`data/iceberg/` locally, `gs://{bucket}/iceberg/` on GCP).
Iceberg ACID commits replace the old file-based day locks and enable safe concurrent
batch writes across notice types.

**Runtime abstraction.** A thin provider layer (`src/procurement/runtime/`) swaps
`local` and `gcp` via a single env var — no pipeline script changes needed.
Local runs use the filesystem and in-process Spark; GCP runs use GCS, Dataproc
Serverless, BigQuery, and Cloud Composer.

**Profile-driven HTML parsing.** BZP notices embed procurement data in structured HTML.
Section profiles (`notice_types/*_sections_profile.json`) drive a generic extractor
that maps HTML section numbers to typed output columns without per-type parsing code.
Column-level parsers are fault-tolerant: a failed field sets that column to null and
appends an error to a `parse_errors` array rather than failing the row.

**Pydantic validation at every boundary.** Bronze ingestion validates raw API payloads
against Pydantic models.  Silver section outputs are checked by a second Pydantic
contract layer that catches parser return-type bugs.

**Test coverage.** 900+ pytest tests covering unit, integration, and smoke levels.
Tests run without Spark (mock/stub where needed) for fast CI.

---

## Running Modes

| `RUNTIME_ENV` | Compute | Storage |
|---|---|---|
| `local` (default) | Local PySpark | `data/` directory |
| `gcp` | Dataproc Serverless | GCS + BigQuery |

| Pipeline mode | Trigger | What it does |
|---|---|---|
| **Daily** | 03:00 UTC cron (`bzp_daily` DAG) | Download yesterday → bronze → silver → deltas |
| **Backfill** | Manual (`bzp_backfill` DAG) | Full date range, per-day hash-based skip checks |

---

## Quick Start — Local

```bash
pip install -e ".[dev]"

# Fetch yesterday's data
python scripts/pipeline/fetch_bzp_yesterday.py

# Build bronze (Pydantic validation + canonical Parquet)
python scripts/pipeline/build_bronze.py

# Build silver (Iceberg tables)
python scripts/pipeline/build_silver_day.py

# Build notice-change deltas
python scripts/pipeline/build_silver_update_deltas.py $(date -d yesterday +%Y-%m-%d)

# Or run all three transforms at once
python scripts/ops/run_transforms_for_day.py $(date -d yesterday +%Y-%m-%d)
```

Or with Docker (matches the GCP Dataproc container exactly):

```bash
docker build -t procurement-spark -f Dockerfile.spark .
docker run --rm -v $(pwd)/data:/app/data -e RUNTIME_ENV=local \
  procurement-spark python scripts/pipeline/build_silver_day.py 2025-10-01
```

For Windows/WSL2 tips see `docs/runbooks/LOCAL_WINDOWS_DOCKER.md`.

---

## Quick Start — GCP

```bash
# 1. Configure environment
cp config/runtime_gcp.env.example ~/.procurement-gcp.env
# Edit with your project values, then:
export $(grep -v '^#' ~/.procurement-gcp.env | xargs)

# 2. Build and push containers
GIT_SHA=$(git rev-parse --short HEAD)
docker build -f Dockerfile.spark --build-arg GIT_SHA=$GIT_SHA -t ${DATAPROC_CONTAINER_IMAGE} .
docker push ${DATAPROC_CONTAINER_IMAGE}

# 3. Upload pipeline scripts to GCS
gsutil -m cp scripts/pipeline/*.py gs://${LAKEHOUSE_BUCKET}/jobs/

# 4. Create BigQuery external Iceberg tables
python scripts/ops/setup_bq_external_tables.py --format iceberg

# 5. Sync DAGs to Cloud Composer
gcloud composer environments storage dags import \
  --environment your-composer-env --location ${DATAPROC_REGION} --source dags/
```

See `docs/cloud_architecture.md` for the full setup guide.

---

## Data Layout

### Bronze

| Path | Contents |
|---|---|
| `{root}/bronze_raw/bzp_YYYY-MM-DD.json` | Raw API payload |
| `{root}/bronze/notices/noticeType=*/publicationDateDay=*/` | Canonical Parquet |
| `{root}/bronze/errors/bzp_YYYY-MM-DD_errors.json` | Validation errors |

### Silver (Iceberg)

| Iceberg table | Contents |
|---|---|
| `silver.notice_type_tables.{type}__{model}` | Per-notice-type section tables, partitioned by `publicationDateDay` |
| `silver.common.common_envelope` | Lightweight structured fields common to all notice types |
| `silver.common.quarantine` | Rows excluded from section tables due to parse errors |
| `silver.notice_update_deltas.{target_notice_type}` | Change-delta Iceberg tables, partitioned by `publicationDateDay` |

On GCP, `{root}` = `gs://{LAKEHOUSE_BUCKET}`.
The Iceberg warehouse is at `data/iceberg/` locally and `gs://{LAKEHOUSE_BUCKET}/iceberg/` on GCP.

---

## Key Scripts

| Script | Purpose |
|---|---|
| `scripts/pipeline/fetch_bzp_yesterday.py` | Fetch daily API payloads to bronze_raw |
| `scripts/pipeline/fetch_bzp_range.py` | Fetch a date range (backfill phase A) |
| `scripts/pipeline/build_bronze.py` | Validate + write canonical Bronze Parquet |
| `scripts/pipeline/build_silver_day.py` | Silver Iceberg write for a single day |
| `scripts/pipeline/build_silver_backfill.py` | Silver backfill with resumable state |
| `scripts/pipeline/build_silver_update_deltas.py` | NoticeUpdateNotice change deltas |
| `scripts/pipeline/build_obs.py` | Observability snapshot + dashboard |
| `scripts/ops/setup_bq_external_tables.py` | Create/replace BigQuery external tables |
| `scripts/ops/run_transforms_for_day.py` | Bronze → silver → deltas convenience wrapper |

---

## Container Images

| Dockerfile | Used for |
|---|---|
| `Dockerfile` | Local dev runner |
| `Dockerfile.spark` | Dataproc Serverless batches (bronze, silver, deltas) |
| `Dockerfile.downloader` | Cloud Run Job (BZP API fetch) |

---

## Airflow DAGs

| DAG | Trigger | Purpose |
|---|---|---|
| `dags/daily_dag.py` | 03:00 UTC cron | Full daily pipeline for yesterday |
| `dags/backfill_dag.py` | Manual | Date-range backfill with hash-based skip logic |

---

## Documentation

| Doc | Contents |
|---|---|
| `docs/cloud_architecture.md` | GCP deployment, runtime abstraction, Apache Iceberg section, setup steps |
| `docs/iceberg.md` | Iceberg catalog config, migration rationale, BQ integration options |
| `docs/observability.md` | Pipeline run metadata, DQ metrics, local Parquet vs BigQuery backends |
| `docs/runbooks/OPERATING_MODES.md` | Daily and backfill operating runbook |
| `docs/runbooks/LOCAL_WINDOWS_DOCKER.md` | Windows/WSL2 Docker performance tips |

---

## Testing

```bash
pytest -q        # ~900 tests, no Spark required
```

Tests cover unit (parsers, validators, helpers), integration (full pipeline steps with
fixtures), and smoke (script imports + wire-up checks).

---

## Repository Structure

```
src/procurement/
  bronze/         — Pydantic validation models + Bronze Spark transforms
  silver/         — HTML section pipeline, Iceberg writes, common envelope
  runtime/        — provider abstraction (local / gcp)
  fetch/          — shared BZP API helpers (backoff, dedup, output)
  obs.py          — observability writers (local Parquet or BigQuery)
apps/
  downloader/     — Cloud Run Job: BZP API fetch adapter
scripts/
  pipeline/       — core pipeline entry points (Dataproc batch scripts)
  ops/            — orchestration helpers, setup scripts
dags/             — Airflow DAGs (synced to Cloud Composer)
config/           — environment variable templates
docs/             — architecture + runbook documentation
tests/            — pytest suite (900+ tests)
```

---

## Disclaimer

This project provides data engineering infrastructure for public procurement transparency research.
Outputs are not legal conclusions.
