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
| **Cloud Scheduler** | Triggers `bzp-daily` workflow at 03:00 UTC | `gcloud scheduler jobs create` (one-time) |
| **Cloud Run** | Downloader job (fetch BZP API data) | Terraform |
| **Artifact Registry** | Docker image registry for Dataproc + Cloud Run | Terraform |
| **BigQuery** | External Iceberg tables over silver | Terraform (dataset) + `setup_bq_external_tables.py --format iceberg` (tables) |
| **Cloud IAM** | Service accounts + roles | Terraform |

The Terraform repo provisions all of the above except Cloud Workflows (deployed
by CI/CD via `gcloud workflows deploy`) and Cloud Scheduler (created once
manually per the setup steps below).  This repo owns the application code and
workflow definitions.

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
cd ../your-terraform-repo
terraform apply
```

### 2. Build and push the Spark container

```bash
GIT_SHA=$(git rev-parse --short HEAD)
IMAGE=europe-west1-docker.pkg.dev/${GCP_PROJECT}/spark/procurement-spark:${GIT_SHA}

docker build -f Dockerfile.spark --build-arg GIT_SHA=${GIT_SHA} -t ${IMAGE} .
docker push ${IMAGE}

# Update the Airflow Variable or env file:
# DATAPROC_CONTAINER_IMAGE=${IMAGE}
```

### 3. Build and push the downloader container

```bash
IMAGE_DL=europe-west1-docker.pkg.dev/${GCP_PROJECT}/spark/procurement-downloader:${GIT_SHA}
docker build -f Dockerfile.downloader --build-arg GIT_SHA=${GIT_SHA} -t ${IMAGE_DL} .
docker push ${IMAGE_DL}
```

### 4. Upload pipeline scripts to GCS

```bash
gsutil -m cp scripts/pipeline/*.py gs://${LAKEHOUSE_BUCKET}/jobs/
```

### 5. Create BigQuery external Iceberg tables

```bash
export RUNTIME_ENV=gcp
export LAKEHOUSE_BUCKET=your-project-lakehouse
export GCP_PROJECT=your-project-id
export BQ_DATASET=procurement_silver

python scripts/ops/setup_bq_external_tables.py --format iceberg
```

Re-run this script any time a new notice type appears or the silver schema
changes (new columns, new data-model splits).  BQ automatically resolves
the latest Iceberg snapshot — no further DDL updates are needed for
day-to-day data changes.

### 6. Deploy Cloud Workflows

```bash
export WORKFLOWS_SA=workflows-sa@${GCP_PROJECT}.iam.gserviceaccount.com

gcloud workflows deploy bzp-daily \
  --source workflows/daily.yaml \
  --location ${DATAPROC_REGION} \
  --service-account ${WORKFLOWS_SA}

gcloud workflows deploy bzp-backfill \
  --source workflows/backfill.yaml \
  --location ${DATAPROC_REGION} \
  --service-account ${WORKFLOWS_SA}
```

### 7. Create the Cloud Scheduler trigger (one-time)

This replaces the Cloud Composer schedule.  Run once; CI/CD keeps the
workflow source up to date but does not recreate this job.

```bash
gcloud scheduler jobs create http bzp-daily-trigger \
  --schedule "0 3 * * *" \
  --uri "https://workflowexecutions.googleapis.com/v1/projects/${GCP_PROJECT}/locations/${DATAPROC_REGION}/workflows/bzp-daily/executions" \
  --message-body "{\"argument\":\"{\\\"project\\\":\\\"${GCP_PROJECT}\\\",\\\"region\\\":\\\"${DATAPROC_REGION}\\\",\\\"bucket\\\":\\\"${LAKEHOUSE_BUCKET}\\\",\\\"container_image\\\":\\\"${IMAGE}\\\",\\\"subnet\\\":\\\"default\\\",\\\"jobs_prefix\\\":\\\"gs://${LAKEHOUSE_BUCKET}/jobs\\\",\\\"downloader_job_name\\\":\\\"bzp-downloader\\\"}\"}" \
  --oauth-service-account-email ${WORKFLOWS_SA} \
  --time-zone "UTC"
```

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
setup script after schema changes), see `docs/iceberg.md` Option B.

---

## CI/CD TODO

The following automation is planned but not yet implemented.  Add it once the
first version of the pipeline is stable and running.

```yaml
# .github/workflows/deploy.yml  (stub — not yet active)
#
# On merge to main:
#   1. Build and push Dockerfile.spark to Artifact Registry
#   2. Build and push Dockerfile.downloader to Artifact Registry
#   3. Upload scripts/pipeline/*.py to gs://{LAKEHOUSE_BUCKET}/jobs/
#   4. Deploy Cloud Workflows: gcloud workflows deploy bzp-daily + bzp-backfill
#   5. Run scripts/ops/setup_bq_external_tables.py to refresh BQ table schemas
#
# Required GitHub secrets:
#   GCP_PROJECT, LAKEHOUSE_BUCKET, DATAPROC_REGION,
#   WORKLOAD_IDENTITY_PROVIDER (for keyless auth via Workload Identity Federation),
#   WORKFLOWS_SA (service account email for Cloud Workflows execution)
```

See `.github/workflows/deploy.yml` for the stub file.

Note: The Cloud Scheduler job (step 7 of setup) is created once manually and
does not need to be re-created on each deploy — it always points at the latest
version of the deployed `bzp-daily` workflow.

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
