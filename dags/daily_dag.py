"""Daily BZP pipeline DAG.

Schedule: every day at 03:00 UTC.
Processes the *previous* calendar day (yesterday) through the full pipeline:

  1. download   — fetch BZP API data for yesterday → GCS bronze_raw
  2. bronze     — validate + write Bronze Parquet → GCS bronze/notices/
  3. silver     — parse HTML sections → GCS silver/notice_type_tables/ + common_envelope/
  4. deltas     — build NoticeUpdateNotice change deltas → GCS silver/notice_update_deltas/

Steps 2–4 are submitted to Dataproc Serverless as independent batch jobs.
Step 1 is a Cloud Run Job (Dockerfile.downloader image) because it only calls
the BZP HTTP API and writes a small JSON file — no Spark needed.

Configuration
-------------
All operator parameters are read from Airflow Variables at DAG parse time.
Set these in the Cloud Composer environment:

  ``gcp_project``              GCP project ID
  ``dataproc_region``          Dataproc Serverless region (e.g. europe-west1)
  ``lakehouse_bucket``         GCS bucket name (no gs:// prefix)
  ``dataproc_container_image`` Artifact Registry image URI for Spark batches
  ``dataproc_subnet``          VPC subnet (default: default)
  ``dataproc_service_account`` Service account email (optional)
  ``bq_dataset``               BigQuery dataset for silver external tables
  ``jobs_gcs_prefix``          GCS prefix where pipeline scripts are uploaded
                               (e.g. gs://my-bucket/jobs)

TODO (CI/CD): add a step to the deploy workflow that uploads scripts/pipeline/
to ``jobs_gcs_prefix`` on each merge to main.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunCreateJobOperator,
    CloudRunExecuteJobOperator,
)
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Config from Airflow Variables
# ---------------------------------------------------------------------------

GCP_PROJECT = Variable.get("gcp_project")
DATAPROC_REGION = Variable.get("dataproc_region")
LAKEHOUSE_BUCKET = Variable.get("lakehouse_bucket")
CONTAINER_IMAGE = Variable.get("dataproc_container_image")
SUBNET = Variable.get("dataproc_subnet", default_var="default")
SERVICE_ACCOUNT = Variable.get("dataproc_service_account", default_var="")
JOBS_PREFIX = Variable.get("jobs_gcs_prefix")  # e.g. gs://bucket/jobs

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RUNTIME_ENV = {
    "RUNTIME_ENV": "gcp",
    "LAKEHOUSE_BUCKET": LAKEHOUSE_BUCKET,
    "GCP_PROJECT": GCP_PROJECT,
    "DATAPROC_REGION": DATAPROC_REGION,
    "DATAPROC_CONTAINER_IMAGE": CONTAINER_IMAGE,
    "DATAPROC_SUBNET": SUBNET,
    **({"DATAPROC_SERVICE_ACCOUNT": SERVICE_ACCOUNT} if SERVICE_ACCOUNT else {}),
}


def _batch_env_vars() -> dict[str, str]:
    return _RUNTIME_ENV


def _execution_config() -> dict:
    cfg: dict = {"subnetwork_uri": SUBNET}
    if SERVICE_ACCOUNT:
        cfg["service_account"] = SERVICE_ACCOUNT
    return cfg


def _spark_batch(
    task_id: str,
    script_name: str,
    extra_args: list[str],
    batch_id_prefix: str,
) -> DataprocCreateBatchOperator:
    """Return a DataprocCreateBatchOperator for a pipeline script."""
    return DataprocCreateBatchOperator(
        task_id=task_id,
        project_id=GCP_PROJECT,
        region=DATAPROC_REGION,
        batch={
            "pyspark_batch": {
                "main_python_file_uri": f"{JOBS_PREFIX}/{script_name}",
                "args": extra_args,
            },
            "runtime_config": {
                "container_image": CONTAINER_IMAGE,
            },
            "environment_config": {
                "execution_config": _execution_config(),
                "peripherals_config": {},
            },
        },
        batch_id=f"{batch_id_prefix}-{{{{ ds_nodash }}}}",
        # Retry the batch once on transient Dataproc errors.
        retries=1,
        retry_delay=timedelta(minutes=5),
    )


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "procurement-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="bzp_daily",
    description="Daily BZP pipeline: download → bronze → silver → deltas",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    tags=["bzp", "daily"],
) as dag:

    # ------------------------------------------------------------------
    # Step 1: Download yesterday's BZP data → GCS bronze_raw
    # ------------------------------------------------------------------
    # Uses the Dockerfile.downloader image via a Cloud Run Job.
    # The job calls `apps/downloader/main.py` which invokes
    # scripts/pipeline/fetch_bzp_yesterday.py with --output-dir resolved
    # from RUNTIME_ENV=gcp + LAKEHOUSE_BUCKET.
    #
    # TODO: replace with CloudRunCreateJobOperator + CloudRunExecuteJobOperator
    # once the Cloud Run Job resource is provisioned by Terraform.
    # For now, the download step runs as a BashOperator placeholder.
    from airflow.operators.bash import BashOperator

    download = BashOperator(
        task_id="download",
        bash_command=(
            "gcloud run jobs execute bzp-downloader "
            "--region={{ var.value.dataproc_region }} "
            "--wait "
            "--update-env-vars TARGET_DATE={{ ds }}"
        ),
        # TODO: replace with CloudRunExecuteJobOperator once Terraform
        # provisions the Cloud Run Job resource.
    )

    # ------------------------------------------------------------------
    # Step 2: Build Bronze Parquet
    # ------------------------------------------------------------------
    bronze = _spark_batch(
        task_id="bronze",
        script_name="build_bronze.py",
        extra_args=["{{ ds }}"],
        batch_id_prefix="bzp-bronze",
    )

    # ------------------------------------------------------------------
    # Step 3: Build Silver
    # ------------------------------------------------------------------
    silver = _spark_batch(
        task_id="silver",
        script_name="build_silver_day.py",
        extra_args=["{{ ds }}"],
        batch_id_prefix="bzp-silver",
    )

    # ------------------------------------------------------------------
    # Step 4: Build notice-change deltas
    # ------------------------------------------------------------------
    deltas = _spark_batch(
        task_id="deltas",
        script_name="build_silver_update_deltas.py",
        extra_args=["{{ ds }}"],
        batch_id_prefix="bzp-deltas",
    )

    # ------------------------------------------------------------------
    # Pipeline order
    # ------------------------------------------------------------------
    download >> bronze >> silver >> deltas
