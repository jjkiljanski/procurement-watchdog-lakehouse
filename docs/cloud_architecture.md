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
| `gcp` | Dataproc Serverless (Spark batches) | GCS (bronze, silver) + BigQuery (external tables over silver) | Cloud Composer (managed Apache Airflow) |

Switching modes requires **only environment variables** — no code changes.

---

## GCP Architecture

```
                 BZP API
                    │
                    ▼
         ┌─────────────────────┐
         │  Cloud Run Job      │  Dockerfile.downloader
         │  (fetch_bzp_...)    │  triggered by Airflow daily DAG
         └──────────┬──────────┘
                    │ writes JSON
                    ▼
         ┌─────────────────────┐
         │  GCS bucket         │
         │  /bronze_raw/       │  bzp_YYYY-MM-DD.json
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│  Dockerfile.spark
         │  build_bronze.py    │  → /bronze/notices/noticeType=*/publicationDateDay=*/
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│
         │  build_silver_day   │  → /silver/notice_type_tables/ + /common_envelope/
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  Dataproc Serverless│
         │  build_silver_      │  → /silver/notice_update_deltas/
         │  update_deltas.py   │
         └──────────┬──────────┘
                    │
                    ▼
         ┌─────────────────────┐
         │  BigQuery external  │  created by setup_bq_external_tables.py
         │  tables over silver │  queryable via SQL from BigQuery + dbt
         └─────────────────────┘

   Cloud Composer (Airflow) orchestrates all steps
   dags/daily_dag.py   — 03:00 UTC daily
   dags/backfill_dag.py — manual trigger
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
| **Cloud Composer** | Managed Apache Airflow for DAG orchestration | Terraform |
| **Cloud Run** | Downloader job (fetch BZP API data) | Terraform |
| **Artifact Registry** | Docker image registry for Dataproc + Cloud Run | Terraform |
| **BigQuery** | External tables over silver Parquet | Terraform (dataset) + `setup_bq_external_tables.py` (tables) |
| **Cloud IAM** | Service accounts + roles | Terraform |

The Terraform repo provisions all of the above.  This repo owns only the
application code and DAG definitions.

---

## Terraform Repo Responsibilities

The separate Terraform repo must provision:

- GCS bucket (`{project}-lakehouse`) with standard storage class
- BigQuery dataset (`procurement_silver`) in the same region
- Dataproc Serverless API enabled + VPC/subnet configured
- Cloud Composer environment (Airflow 2.x) with GCS DAG bucket
- Cloud Run service account with `roles/storage.objectAdmin` on the GCS bucket
- Dataproc service account with:
    - `roles/dataproc.worker`
    - `roles/storage.objectAdmin` (on the GCS bucket)
    - `roles/bigquery.dataEditor` (on the BigQuery dataset)
- Artifact Registry repository for Docker images
- Cloud Run Job resource for the downloader

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

### 5. Create BigQuery external tables

```bash
export RUNTIME_ENV=gcp
export LAKEHOUSE_BUCKET=your-project-lakehouse
export GCP_PROJECT=your-project-id
export BQ_DATASET=procurement_silver

python scripts/ops/setup_bq_external_tables.py
```

Re-run this script any time the silver schema changes.

### 6. Sync DAGs to Cloud Composer

```bash
gcloud composer environments storage dags import \
  --environment your-composer-env \
  --location ${DATAPROC_REGION} \
  --source dags/
```

Set Airflow Variables (see `dags/daily_dag.py` docstring for the full list):

```bash
gcloud composer environments run your-composer-env \
  --location ${DATAPROC_REGION} variables set -- \
  gcp_project ${GCP_PROJECT} \
  dataproc_region ${DATAPROC_REGION} \
  lakehouse_bucket ${LAKEHOUSE_BUCKET} \
  dataproc_container_image ${IMAGE} \
  jobs_gcs_prefix gs://${LAKEHOUSE_BUCKET}/jobs
```

---

## Operating Modes

### Daily (automated)

The `bzp_daily` DAG runs at 03:00 UTC every day.  It processes the previous
calendar day through download → bronze → silver → deltas.  No manual
intervention needed.

### Backfill (manual)

Trigger the `bzp_backfill` DAG from the Airflow UI:

1. Open the DAG in the Composer UI
2. Click **Trigger DAG w/ config**
3. Set params: `start_date`, `end_date`, optionally `force: true`

The DAG runs the full four-step pipeline for the given date range:

```
fetch_range (single task)
  ↓
bronze[0..N]  →  silver[0..N]  →  deltas[0..N]   (dynamic task mapping, per date)
```

**Step 1 — fetch_range** executes the `bzp-downloader` Cloud Run Job with
`DOWNLOADER_COMMAND_TEMPLATE=python scripts/pipeline/fetch_bzp_range.py`,
`START_DATE`, and `END_DATE` env var overrides.  `fetch_bzp_range.py` iterates
over the date window and, for each date, checks the `fetch` processed-date
manifest — skipping dates already downloaded with the same script version.
All bronze_raw downloads complete before any Dataproc batch is submitted.

**Steps 2–4 — bronze / silver / deltas** submit one Dataproc Serverless batch
per date.  Each task performs the same hash-based skip check before submitting.

**Hash-based skipping**: before submitting each batch, the DAG reads the
processed-date manifest at `gs://{LAKEHOUSE_BUCKET}/_processed/{layer}/{date}.json`
and compares the stored `script_hash` against the SHA-256 of the currently
deployed script on GCS.  If they match and `force=False`, the batch is skipped.
Set `force=true` to reprocess all dates regardless of manifest state (e.g.
after deploying a new script version).

**Required Airflow Variables** (in addition to the daily DAG set):

| Variable | Purpose | Default |
|---|---|---|
| `downloader_job_name` | Cloud Run Job name for the range downloader | `bzp-downloader` |
| `cloud_run_region` | Region for Cloud Run Job execution | falls back to `dataproc_region` |

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
#   4. Sync dags/ to the Cloud Composer DAG bucket
#   5. Run scripts/ops/setup_bq_external_tables.py to refresh BQ table schemas
#
# Required GitHub secrets:
#   GCP_PROJECT, LAKEHOUSE_BUCKET, DATAPROC_REGION,
#   WORKLOAD_IDENTITY_PROVIDER (for keyless auth via Workload Identity Federation)
```

See `.github/workflows/deploy.yml` for the stub file.

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
