# Cloud Architecture

This document describes the Google Cloud Platform deployment of the procurement
watchdog lakehouse, the runtime abstraction that makes provider swapping
possible, and the one-time setup steps required before running the daily
pipeline.

---

## Overview

The pipeline can run in two modes, controlled by the `RUNTIME_ENV` environment
variable:

| `RUNTIME_ENV` | Compute | Storage | Orchestration |
|---|---|---|---|
| `local` (default) | Local Python / Spark in-process | Local filesystem under `data/` | Run scripts manually or via `scripts/ops/` |
| `gcp` | Dataproc Serverless (Spark batches) | GCS (bronze, silver) + BigQuery (external tables over silver) | Cloud Scheduler + Cloud Workflows |

Switching modes requires **only environment variables** — no code changes.

---

## GCP Architecture

```
   Cloud Scheduler (03:00 UTC)
         │
         ▼
   Cloud Workflows (bzp-daily / bzp-backfill)
         │
         │ step 1: Cloud Run Job
         ▼
         ┌─────────────────────┐
         │  Cloud Run Job      │  Dockerfile.downloader
         │  (bzp-downloader)   │  fetch BZP API data
         └──────────┬──────────┘
                    │ writes JSON
                    ▼
         ┌─────────────────────┐
         │  GCS bucket         │
         │  /bronze_raw/       │  bzp_YYYY-MM-DD.json
         └──────────┬──────────┘
                    │ step 2: Dataproc Serverless batch
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│  Dockerfile.spark
         │  build_bronze[_range│  → /bronze/notices/noticeType=*/publicationDateDay=*/
         │    ].py             │
         └──────────┬──────────┘
                    │ step 3
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│
         │  build_silver_[day/ │  → /iceberg/notice_type_tables/ + /iceberg/common/
         │    range].py        │    (Apache Iceberg HadoopCatalog)
         └──────────┬──────────┘
                    │ step 4
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│
         │  build_silver_      │  → /iceberg/notice_update_deltas/
         │  update_deltas.py   │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  BigQuery external  │  created by setup_bq_external_tables.py
         │  Iceberg tables     │  --format iceberg
         │  over silver        │  queryable via SQL from BigQuery + dbt
         └─────────────────────┘

   workflows/daily.yaml    — bzp-daily   (Cloud Workflows, triggered by Cloud Scheduler)
   workflows/backfill.yaml — bzp-backfill (Cloud Workflows, manual trigger)
```

---

## Runtime Abstraction Layer

All path resolution and Spark session creation goes through
`src/procurement/runtime/`.  This is the **only place** that knows about
cloud provider specifics.

```
src/procurement/runtime/
  __init__.py          # exports get_runtime(), RuntimeConfig
  base.py              # abstract: StorageProvider, SparkLauncher, StateBackend
  config.py            # factory: reads RUNTIME_ENV, returns RuntimeConfig
  providers/
    local.py           # LocalStorageProvider, LocalSparkLauncher, LocalStateBackend
    gcp.py             # GCSStorageProvider, DataprocServerlessLauncher, GCSStateBackend
```

Pipeline scripts obtain paths and Spark sessions like this:

```python
from procurement.runtime import get_runtime

rt = get_runtime()
bronze_dir  = rt.storage.resolve("bronze")   # "/abs/data/bronze" or "gs://bucket/bronze"
spark       = rt.spark.get_session("my-app") # local or Dataproc-configured session
state       = rt.state.load("backfill_2025") # dict from local JSON or GCS JSON
obs_dir     = rt.storage.obs_path()          # Path or None (GCP: None until obs.py extended)
```

### Adding a new cloud provider (e.g. AWS, Azure)

1. Create `src/procurement/runtime/providers/aws.py` (or `azure.py`)
2. Implement `StorageProvider`, `SparkLauncher`, `StateBackend`
3. Add `elif env == "aws":` in `src/procurement/runtime/config.py`
4. Add `aws` optional dependencies in `pyproject.toml`
5. Create `config/runtime_aws.env.example`

No pipeline script changes are needed.

---

## GCP Services Used

| Service | Purpose | Provisioned by |
|---|---|---|
| **GCS bucket** | bronze_raw, bronze, silver, Iceberg warehouse, job scripts | Terraform |
| **Dataproc Serverless** | Run PySpark batches without cluster management | Terraform (VPC, SA, quotas) |
| **Cloud Workflows** | Pipeline orchestration (daily + backfill) | `gcloud workflows deploy` (CI/CD) |
| **Cloud Scheduler** | Triggers `bzp-daily` workflow at 03:00 UTC | Terraform |
| **Cloud Run** | Downloader job (fetch BZP API data) | Terraform |
| **Artifact Registry** | Docker image registry for Dataproc + Cloud Run | Terraform |
| **BigQuery** | External Iceberg tables over silver | Terraform (dataset) + `setup_bq_external_tables.py --format iceberg` (tables) |
| **Cloud IAM** | Service accounts + roles | Terraform |

The Terraform repo provisions all of the above. CI/CD (`_deploy-env.yml`)
deploys Cloud Workflows and updates the Cloud Scheduler message body on each
merge to `main`. This repo owns the application code and workflow definitions.

---

## Terraform Repo Responsibilities

The separate Terraform repo must provision:

- GCS bucket (`{project}-lakehouse`) with standard storage class
- BigQuery dataset (`procurement_silver`) in the same region
- Dataproc Serverless API enabled + VPC/subnet configured
- Cloud Run service account with `roles/storage.objectAdmin` on the GCS bucket
- Dataproc service account with:
    - `roles/dataproc.worker`
    - `roles/storage.objectAdmin` (on the GCS bucket)
    - `roles/bigquery.dataEditor` (on the BigQuery dataset)
- Artifact Registry repository for Docker images
- Cloud Run Job resource for the downloader
- Workflows service account with:
    - `roles/run.invoker` (to trigger the Cloud Run downloader job)
    - `roles/dataproc.editor` (to submit Serverless batches)
    - `roles/logging.logWriter` (for `sys.log` calls in workflows)

---

## One-time Setup Steps

### 1. Provision infrastructure

```bash
cd ../procurement-watchdog-gcp-platform/envs/dev   # or envs/prod
terraform apply
```

Terraform creates all GCP resources: bucket, VPC, IAM service accounts, Artifact
Registry repos, Cloud Run job, BigQuery datasets, Cloud Workflows, Cloud Scheduler
job, and Workload Identity Federation for GitHub Actions.

### 2. Merge to `main` — CI/CD takes over

After the first `terraform apply`, a push to `main` triggers `.github/workflows/deploy.yml`,
which automatically:

- Builds and pushes `Dockerfile.spark` and `Dockerfile.downloader` to Artifact Registry
- Uploads `scripts/pipeline/*.py` to `gs://{LAKEHOUSE_BUCKET}/jobs/`
- Deploys the `bzp-daily` and `bzp-backfill` Cloud Workflows
- Updates the Cloud Scheduler message body with the new image URI
- Refreshes BigQuery external Iceberg table definitions

No manual steps 2–7 are needed.

---

## Operating Modes

### Daily (automated)

The `bzp-daily` Cloud Workflow runs at 03:00 UTC every day (triggered by Cloud
Scheduler).  It computes `yesterday = now - 86400s` and processes that date
through download → bronze → silver → deltas.  No manual intervention needed.

### Backfill (manual)

Trigger the `bzp-backfill` Cloud Workflow with `gcloud workflows run`:

```bash
gcloud workflows run bzp-backfill \
  --location ${DATAPROC_REGION} \
  --data '{
    "project": "'${GCP_PROJECT}'",
    "region": "'${DATAPROC_REGION}'",
    "bucket": "'${LAKEHOUSE_BUCKET}'",
    "container_image": "'${IMAGE}'",
    "jobs_prefix": "gs://'${LAKEHOUSE_BUCKET}'/jobs",
    "start_date": "2025-01-01",
    "end_date": "2025-12-31",
    "force": "false"
  }'
```

The backfill workflow submits exactly **3 Dataproc batches** for the entire range
(one per pipeline stage: bronze, silver, deltas), not N_days × 3 batches.
Range scripts (`build_bronze_range.py`, `build_silver_range.py`,
`build_silver_update_deltas.py --start-date … --end-date …`) process all dates
in a single Spark session per stage, eliminating per-day cold-start overhead.

```
fetch (Cloud Run Job, full range)
  ↓
build_bronze_range.py   (1 Dataproc batch, all dates)
  ↓
build_silver_range.py   (1 Dataproc batch, all dates — loops over notice types)
  ↓
build_silver_update_deltas.py --start-date … --end-date …   (1 Dataproc batch)
```

**Hash-based skipping**: the range scripts check per-(date, notice_type) manifests
at `gs://{LAKEHOUSE_BUCKET}/_processed/{layer}/{date}/{notice_type}.json` before
processing each batch.  Pass `force=true` to the workflow to reprocess all dates
regardless of manifest state (e.g. after deploying a new script version).

#### Bronze range cross-day deduplication

The bronze layer deduplicates `objectId` values across days: if the same
`objectId` was already ingested on a *different* day's partition, the record is
dropped from the current day.  In `build_bronze.py` (single-date) this costs
one full Spark scan per run.

`build_bronze_range.py` uses a **running seen-set** to avoid repeating that
scan on every date in the loop:

1. **Pre-compute** — before the date loop, one Spark scan reads every
   `objectId` from bronze for dates *before* `start_date` (`publicationDateDay
   < start_date`).  Partition pruning limits I/O to only the relevant files.
   The result is kept in a Python `set[str]` called `seen_ids`.

2. **Per-date processing** — `seen_ids` is passed directly to
   `_deduplicate_via_spark` as `prebuilt_seen_ids`, bypassing the Spark scan.
   After writing, the new date's `objectId` values are added to `seen_ids`.

3. **Skipped dates** — dates whose manifest already matches (no-op) still
   contribute `objectId` values to `seen_ids` so that subsequent dates can
   dedup against them.  A single partition-pruned scan
   (`publicationDateDay == skipped_date`) collects those IDs at minimal cost.

For a completely fresh backfill, step 1 returns an empty set and step 3 never
fires, so the only Spark action before the first write is the pre-range scan
(which is a no-op).  This reduces N full-table scans to at most 1 + skipped_count
partition-pruned scans for the entire range.

**Workflow runtime arguments**:

| Argument | Required | Default | Description |
|---|---|---|---|
| `project` | yes | — | GCP project ID |
| `region` | yes | — | Dataproc Serverless + Cloud Run region |
| `bucket` | yes | — | GCS bucket name (no `gs://` prefix) |
| `container_image` | yes | — | Dataproc container image URI |
| `jobs_prefix` | yes | — | GCS prefix for pipeline scripts |
| `start_date` | yes (backfill) | — | First date YYYY-MM-DD (inclusive) |
| `end_date` | yes (backfill) | — | Last date YYYY-MM-DD (inclusive) |
| `force` | no | `"false"` | `"true"` to reprocess all dates |
| `subnet` | no | `"default"` | VPC subnet short name |
| `downloader_job_name` | no | `"bzp-downloader"` | Cloud Run Job name |

---

## Apache Iceberg — Silver Layer

Silver writes use **Apache Iceberg** (HadoopCatalog) instead of plain Parquet.
The Iceberg warehouse lives at `data/iceberg/` locally and
`gs://{LAKEHOUSE_BUCKET}/iceberg/` on GCP.

### Catalog configuration

| Setting | Local | GCP |
|---|---|---|
| `spark.sql.catalog.silver` | `org.apache.iceberg.spark.SparkCatalog` | same |
| `spark.sql.catalog.silver.type` | `hadoop` | `hadoop` |
| `spark.sql.catalog.silver.warehouse` | `data/iceberg` | `gs://{bucket}/iceberg` |

The catalog is registered as `silver` on every SparkSession (via both
`local.py` and `gcp.py`).

### Iceberg JAR delivery

The Iceberg Spark runtime JAR
(`iceberg-spark-runtime-3.5_2.12-1.5.2.jar`) is downloaded into the
container at build time and placed at `/opt/iceberg-spark-runtime.jar`.

- **Local**: `SPARK_EXTRA_CLASSPATH=/opt/iceberg-spark-runtime.jar` makes it
  available on the driver classpath for in-container Spark sessions.
- **Dataproc Serverless**: every batch config includes
  `jar_file_uris=["file:///opt/iceberg-spark-runtime.jar"]` so executors also
  have it on their classpaths (see `gcp.py: submit_batch()`).

### Table layout

```
iceberg/
  notice_type_tables/
    contract_notice__core/          ← silver.notice_type_tables.contract_notice__core
    contract_notice__part_core/
    tender_result_notice__core/
    …                               (one table per notice-type × data-model pair)
  common/
    common_envelope/                ← silver.common.common_envelope
    quarantine/                     ← silver.common.quarantine
  notice_update_deltas/
    contract_notice/                ← silver.notice_update_deltas.contract_notice
    tender_result_notice/
    …                               (one table per original notice type that was updated)
```

Section tables are partitioned by `publicationDateDay`.
`quarantine` is partitioned by `(publicationDateDay, notice_type)`.
`common_envelope` is partitioned by `publicationDateDay`.
Delta tables are partitioned by `publicationDateDay` (= NUN publication date).

### ACID write semantics

| Table type | Write mode | Idempotency |
|---|---|---|
| Section tables | `overwritePartitions()` | Replaces only the target day's partition; safe per notice-type batch |
| Quarantine | `overwritePartitions()` | Partitioned by (day, type) — each concurrent batch owns a distinct partition |
| Common envelope | `append()` + pre-delete | Day partition is deleted before the ThreadPoolExecutor starts; each batch appends safely via Iceberg ACID commits |
| Delta tables | `overwritePartitions()` | One table per target notice type; replaces only the target NUN day's partition |

File-based day locks (`_locks/silver_day=*/`) are no longer needed.

### BigQuery access

Run `scripts/ops/setup_bq_external_tables.py --format iceberg` to create BQ
external Iceberg tables pointing at the GCS warehouse.  BigQuery resolves the
latest Iceberg snapshot automatically — no DDL updates needed when new days are
written.

```bash
python scripts/ops/setup_bq_external_tables.py --format iceberg
```

For BigLake Metastore integration (auto-discovery without re-running the
setup script after schema changes), see `docs/iceberg.md`.

---


## Observability

| Mode | Where obs data is written |
|---|---|
| Local | `data/obs/` — Parquet files, date-partitioned |
| GCP | BigQuery dataset `BQ_OBS_DATASET` (default: `procurement_obs`) |

In GCP mode, `obs_path()` returns `None`, and `obs.py` detects `RUNTIME_ENV=gcp`
to stream rows to BigQuery instead.  Tables (`pipeline_runs`, `dq_metrics`,
`quarantine_summary`) are created automatically on first write.

The dataset name is controlled by `BQ_OBS_DATASET` (see
`config/runtime_gcp.env.example`).  Cloud Logging captures structured
operational logs from Dataproc and Cloud Run automatically — no extra
configuration needed.

For Terraform: create the `procurement_obs` dataset in BigQuery.  The tables
themselves are managed by `obs.py`.
