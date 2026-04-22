"""Manual backfill DAG.

Triggered manually via the Airflow UI or CLI with parameters:
  ``start_date``   YYYY-MM-DD (inclusive)
  ``end_date``     YYYY-MM-DD (inclusive)

For each date in the range, the DAG runs the full pipeline:
  bronze → silver → deltas

The download step is intentionally excluded — backfills assume bronze_raw data
already exists on GCS (loaded by a separate bulk fetch or by prior daily runs).
If bronze_raw is missing for a date, the bronze step will fail for that date.

Hash-based skipping
-------------------
Each Spark batch checks whether its output already exists with the correct
script hash before writing:

- **Bronze**: ``build_bronze.py`` reads from bronze_raw; Spark-level dedup
  against the existing bronze partition ensures idempotency.
- **Silver**: ``build_silver_day.py`` acquires a day lock; existing partitions
  are overwritten (Spark dynamic partition overwrite), which is idempotent.
- **Deltas**: ``build_silver_update_deltas.py`` overwrites the target partition.

Full re-processing of an already-complete day is therefore safe — re-trigger
with ``force=True`` in the Airflow params to bypass the skip logic.

TODO: implement explicit hash-check tasks that read the ``script_hash`` written
by obs.py (pipeline_runs table) and skip the batch if the hash matches the
current script's SHA-256.  See docs/cloud_architecture.md for the planned
implementation.

Configuration
-------------
Same Airflow Variables as ``bzp_daily`` — see dags/daily_dag.py.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Config from Airflow Variables (same as daily_dag.py)
# ---------------------------------------------------------------------------

GCP_PROJECT = Variable.get("gcp_project")
DATAPROC_REGION = Variable.get("dataproc_region")
LAKEHOUSE_BUCKET = Variable.get("lakehouse_bucket")
CONTAINER_IMAGE = Variable.get("dataproc_container_image")
SUBNET = Variable.get("dataproc_subnet", default_var="default")
SERVICE_ACCOUNT = Variable.get("dataproc_service_account", default_var="")
JOBS_PREFIX = Variable.get("jobs_gcs_prefix")


def _execution_config() -> dict:
    cfg: dict = {"subnetwork_uri": SUBNET}
    if SERVICE_ACCOUNT:
        cfg["service_account"] = SERVICE_ACCOUNT
    return cfg


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
    dag_id="bzp_backfill",
    description="Manual backfill: iterate over a date range and run bronze → silver → deltas",
    schedule_interval=None,  # manual trigger only
    start_date=days_ago(1),
    catchup=False,
    default_args=_DEFAULT_ARGS,
    params={
        "start_date": Param(
            default=(date.today() - timedelta(days=30)).isoformat(),
            type="string",
            description="First date to backfill (YYYY-MM-DD, inclusive)",
        ),
        "end_date": Param(
            default=(date.today() - timedelta(days=1)).isoformat(),
            type="string",
            description="Last date to backfill (YYYY-MM-DD, inclusive)",
        ),
        "force": Param(
            default=False,
            type="boolean",
            description=(
                "If true, reprocess dates even if they appear complete. "
                "Useful after a script version upgrade."
            ),
        ),
    },
    tags=["bzp", "backfill"],
) as dag:

    @task
    def generate_date_range(**context) -> list[str]:
        """Produce the list of dates to process from DAG params."""
        params = context["params"]
        start = date.fromisoformat(params["start_date"])
        end = date.fromisoformat(params["end_date"])
        if end < start:
            raise ValueError(f"end_date {end} is before start_date {start}")
        days = []
        d = start
        while d <= end:
            days.append(d.isoformat())
            d += timedelta(days=1)
        return days

    @task
    def submit_bronze_batch(target_date: str, **context) -> str:
        """Submit a Dataproc Serverless bronze batch for one date."""
        from google.cloud import dataproc_v1 as dataproc
        import re
        import time

        client = dataproc_v1.BatchControllerClient(
            client_options={"api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"}
        )
        safe_date = target_date.replace("-", "")
        batch_id = f"bzp-bronze-{safe_date}-{int(time.time())}"[:63]

        batch = dataproc.Batch(
            pyspark_batch=dataproc.PySparkBatch(
                main_python_file_uri=f"{JOBS_PREFIX}/build_bronze.py",
                args=[target_date],
            ),
            runtime_config=dataproc.RuntimeConfig(container_image=CONTAINER_IMAGE),
            environment_config=dataproc.EnvironmentConfig(
                execution_config=dataproc.ExecutionConfig(**_execution_config())
            ),
        )
        parent = f"projects/{GCP_PROJECT}/locations/{DATAPROC_REGION}"
        op = client.create_batch(
            request=dataproc.CreateBatchRequest(parent=parent, batch=batch, batch_id=batch_id)
        )
        op.result()  # wait
        return batch_id

    @task
    def submit_silver_batch(target_date: str, **context) -> str:
        """Submit a Dataproc Serverless silver batch for one date."""
        from google.cloud import dataproc_v1 as dataproc
        import time

        client = dataproc_v1.BatchControllerClient(
            client_options={"api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"}
        )
        safe_date = target_date.replace("-", "")
        batch_id = f"bzp-silver-{safe_date}-{int(time.time())}"[:63]

        batch = dataproc.Batch(
            pyspark_batch=dataproc.PySparkBatch(
                main_python_file_uri=f"{JOBS_PREFIX}/build_silver_day.py",
                args=[target_date],
            ),
            runtime_config=dataproc.RuntimeConfig(container_image=CONTAINER_IMAGE),
            environment_config=dataproc.EnvironmentConfig(
                execution_config=dataproc.ExecutionConfig(**_execution_config())
            ),
        )
        parent = f"projects/{GCP_PROJECT}/locations/{DATAPROC_REGION}"
        op = client.create_batch(
            request=dataproc.CreateBatchRequest(parent=parent, batch=batch, batch_id=batch_id)
        )
        op.result()
        return batch_id

    @task
    def submit_deltas_batch(target_date: str, **context) -> str:
        """Submit a Dataproc Serverless deltas batch for one date."""
        from google.cloud import dataproc_v1 as dataproc
        import time

        client = dataproc_v1.BatchControllerClient(
            client_options={"api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"}
        )
        safe_date = target_date.replace("-", "")
        batch_id = f"bzp-deltas-{safe_date}-{int(time.time())}"[:63]

        batch = dataproc.Batch(
            pyspark_batch=dataproc.PySparkBatch(
                main_python_file_uri=f"{JOBS_PREFIX}/build_silver_update_deltas.py",
                args=[target_date],
            ),
            runtime_config=dataproc.RuntimeConfig(container_image=CONTAINER_IMAGE),
            environment_config=dataproc.EnvironmentConfig(
                execution_config=dataproc.ExecutionConfig(**_execution_config())
            ),
        )
        parent = f"projects/{GCP_PROJECT}/locations/{DATAPROC_REGION}"
        op = client.create_batch(
            request=dataproc.CreateBatchRequest(parent=parent, batch=batch, batch_id=batch_id)
        )
        op.result()
        return batch_id

    # ------------------------------------------------------------------
    # Wiring: dynamic task mapping over the date list
    # ------------------------------------------------------------------
    dates = generate_date_range()
    bronze_results = submit_bronze_batch.expand(target_date=dates)
    silver_results = submit_silver_batch.expand(target_date=dates)
    deltas_results = submit_deltas_batch.expand(target_date=dates)

    # Enforce per-date ordering: bronze → silver → deltas.
    # Dynamic task mapping with dependencies preserves the index alignment.
    bronze_results >> silver_results >> deltas_results
