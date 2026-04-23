"""Manual backfill DAG.

Triggered manually via the Airflow UI or CLI with parameters:
  ``start_date``   YYYY-MM-DD (inclusive)
  ``end_date``     YYYY-MM-DD (inclusive)
  ``force``        boolean — reprocess even if the manifest hash matches

For each date in the range, the DAG runs the full pipeline:
  fetch → bronze → silver → deltas

Pipeline steps
--------------
**fetch** (single task, not per-date):
  Executes the ``bzp-range-downloader`` Cloud Run Job with ``START_DATE`` and
  ``END_DATE`` environment variable overrides.  The job runs
  ``fetch_bzp_range.py``, which iterates over the date range and downloads
  each date that does not already have a matching processed-date manifest.
  When ``force=True``, the script's own ``--force`` flag is propagated via the
  ``FORCE_FETCH=true`` env var override so all dates are re-fetched.

**bronze / silver / deltas** (dynamic task mapping, one instance per date):
  Each task checks the processed-date manifest before submitting a Dataproc
  Serverless batch.  Dates already processed with the current script version
  are skipped automatically (unless ``force=True``).

Hash-based skipping
-------------------
Before submitting each Dataproc batch, the DAG checks a processed-date
manifest stored at ``gs://{LAKEHOUSE_BUCKET}/_processed/{layer}/{date}.json``.
The manifest is written by each pipeline script on success and contains the
SHA-256 hash of that script file.

Skip logic (when ``force=False``):
- Download the current script from GCS (``gs://{JOBS_PREFIX}/{script}.py``).
- Compute its SHA-256.
- Read the manifest from GCS.
- If the manifest exists and its ``script_hash`` matches → skip the batch.

Re-run after a code upgrade:
- Set ``force=True`` when triggering the DAG.  All dates are reprocessed
  regardless of manifest state.

Configuration
-------------
Same Airflow Variables as ``bzp_daily`` — see dags/daily_dag.py.
Additional Variables:

  ``downloader_job_name``  Name of the Cloud Run Job for the range downloader
                           (default: ``bzp-downloader``)
  ``cloud_run_region``     Region for the Cloud Run Job (defaults to
                           ``dataproc_region`` when not set separately)
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from Airflow Variables (same as daily_dag.py)
# ---------------------------------------------------------------------------

GCP_PROJECT = Variable.get("gcp_project")
DATAPROC_REGION = Variable.get("dataproc_region")
LAKEHOUSE_BUCKET = Variable.get("lakehouse_bucket")
CONTAINER_IMAGE = Variable.get("dataproc_container_image")
SUBNET = Variable.get("dataproc_subnet", default_var="default")
SERVICE_ACCOUNT = Variable.get("dataproc_service_account", default_var="")
JOBS_PREFIX = Variable.get("jobs_gcs_prefix")  # e.g. gs://bucket/jobs

# Cloud Run Job for the range downloader
DOWNLOADER_JOB_NAME = Variable.get("downloader_job_name", default_var="bzp-downloader")
CLOUD_RUN_REGION = Variable.get("cloud_run_region", default_var=DATAPROC_REGION)


def _execution_config() -> dict:
    cfg: dict = {"subnetwork_uri": SUBNET}
    if SERVICE_ACCOUNT:
        cfg["service_account"] = SERVICE_ACCOUNT
    return cfg


# ---------------------------------------------------------------------------
# Manifest helpers (GCS-native, no procurement package required at import time)
# ---------------------------------------------------------------------------

def _gcs_blob_sha256(bucket: str, blob_name: str) -> str:
    """Download a GCS blob and return its SHA-256 hex digest."""
    from google.cloud import storage as gcs

    client = gcs.Client()
    content = client.bucket(bucket).blob(blob_name).download_as_bytes()
    return hashlib.sha256(content).hexdigest()


def _jobs_blob_name(script_filename: str) -> str:
    """Convert a script filename to the GCS blob name under the jobs prefix."""
    # JOBS_PREFIX is a full gs:// URI; extract the object path.
    # e.g. "gs://my-bucket/jobs" → "jobs"
    prefix = JOBS_PREFIX
    if prefix.startswith("gs://"):
        prefix = prefix[len("gs://"):]
        # drop bucket name
        prefix = prefix[prefix.index("/") + 1:]
    return f"{prefix}/{script_filename}"


def _check_manifest(layer: str, target_date: str, current_script_hash: str) -> bool:
    """Return True if the GCS manifest shows this date was processed with the current hash."""
    from google.cloud import storage as gcs
    from google.cloud.exceptions import NotFound

    client = gcs.Client()
    blob_name = f"_processed/{layer}/{target_date}.json"
    try:
        data = json.loads(client.bucket(LAKEHOUSE_BUCKET).blob(blob_name).download_as_text())
        return data.get("script_hash") == current_script_hash
    except NotFound:
        return False
    except Exception as exc:
        log.warning("Manifest read failed for %s/%s: %s — will run batch", layer, target_date, exc)
        return False


# ---------------------------------------------------------------------------
# Cloud Run Job execution (range downloader)
# ---------------------------------------------------------------------------

def _execute_range_cloud_run_job(start_date: str, end_date: str, force: bool) -> None:
    """Execute the bzp-range-downloader Cloud Run Job and block until completion.

    Overrides:
      DOWNLOADER_COMMAND_TEMPLATE → points to fetch_bzp_range.py
      START_DATE / END_DATE       → the backfill date window
      FORCE_FETCH                 → propagates force flag to the script
    """
    from google.cloud import run_v2

    client = run_v2.JobsClient()
    job_name = f"projects/{GCP_PROJECT}/locations/{CLOUD_RUN_REGION}/jobs/{DOWNLOADER_JOB_NAME}"

    force_str = "true" if force else "false"
    cmd_template = "python scripts/pipeline/fetch_bzp_range.py"
    if force:
        cmd_template += " --force"

    env_overrides = [
        run_v2.EnvVar(name="DOWNLOADER_COMMAND_TEMPLATE", value=cmd_template),
        run_v2.EnvVar(name="START_DATE", value=start_date),
        run_v2.EnvVar(name="END_DATE", value=end_date),
        run_v2.EnvVar(name="FORCE_FETCH", value=force_str),
    ]

    request = run_v2.RunJobRequest(
        name=job_name,
        overrides=run_v2.RunJobRequest.Overrides(
            container_overrides=[
                run_v2.RunJobRequest.Overrides.ContainerOverride(env=env_overrides)
            ]
        ),
    )
    log.info(
        "Submitting Cloud Run Job %s for range %s→%s (force=%s)",
        DOWNLOADER_JOB_NAME, start_date, end_date, force_str,
    )
    op = client.run_job(request=request)
    op.result()  # blocks until SUCCEEDED or raises on failure
    log.info("Cloud Run Job %s completed", DOWNLOADER_JOB_NAME)


# ---------------------------------------------------------------------------
# Dataproc batch submission helper
# ---------------------------------------------------------------------------

def _submit_and_wait(script_filename: str, target_date: str, batch_id_prefix: str) -> str:
    """Submit one Dataproc Serverless batch and block until it finishes."""
    import time

    from google.cloud import dataproc_v1 as dataproc

    client = dataproc.BatchControllerClient(
        client_options={"api_endpoint": f"{DATAPROC_REGION}-dataproc.googleapis.com:443"}
    )
    safe_date = target_date.replace("-", "")
    batch_id = f"{batch_id_prefix}-{safe_date}-{int(time.time())}"[:63]

    batch = dataproc.Batch(
        pyspark_batch=dataproc.PySparkBatch(
            main_python_file_uri=f"{JOBS_PREFIX}/{script_filename}",
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
    op.result()  # blocks until SUCCEEDED or raises on failure
    return batch_id


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
    description="Manual backfill: download a date range then run bronze → silver → deltas per date",
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
                "If true, reprocess dates even if the manifest hash matches. "
                "Use after deploying a new script version."
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
    def fetch_range(**context) -> str:
        """Download bronze_raw for the full date range via the Cloud Run range downloader.

        Executes the bzp-downloader Cloud Run Job with DOWNLOADER_COMMAND_TEMPLATE
        pointing to fetch_bzp_range.py and START_DATE/END_DATE env var overrides.
        The script skips dates that already have a matching processed-date manifest
        (unless force=True).
        """
        params = context["params"]
        start_date = params["start_date"]
        end_date = params["end_date"]
        force = params.get("force", False)

        _execute_range_cloud_run_job(start_date, end_date, force)
        return f"fetch_range:{start_date}:{end_date}"

    @task
    def submit_bronze_batch(target_date: str, **context) -> str:
        """Submit (or skip) the bronze batch for one date based on manifest check."""
        force = context["params"].get("force", False)
        if not force:
            script_hash = _gcs_blob_sha256(
                LAKEHOUSE_BUCKET, _jobs_blob_name("build_bronze.py")
            )
            if _check_manifest("bronze", target_date, script_hash):
                log.info(
                    "Skipping bronze for %s — manifest hash matches current script",
                    target_date,
                )
                return "skipped"

        return _submit_and_wait("build_bronze.py", target_date, "bzp-bronze")

    @task
    def submit_silver_batch(target_date: str, **context) -> str:
        """Submit (or skip) the silver batch for one date based on manifest check."""
        force = context["params"].get("force", False)
        if not force:
            script_hash = _gcs_blob_sha256(
                LAKEHOUSE_BUCKET, _jobs_blob_name("build_silver_day.py")
            )
            if _check_manifest("silver", target_date, script_hash):
                log.info(
                    "Skipping silver for %s — manifest hash matches current script",
                    target_date,
                )
                return "skipped"

        return _submit_and_wait("build_silver_day.py", target_date, "bzp-silver")

    @task
    def submit_deltas_batch(target_date: str, **context) -> str:
        """Submit (or skip) the deltas batch for one date based on manifest check."""
        force = context["params"].get("force", False)
        if not force:
            script_hash = _gcs_blob_sha256(
                LAKEHOUSE_BUCKET, _jobs_blob_name("build_silver_update_deltas.py")
            )
            if _check_manifest("deltas", target_date, script_hash):
                log.info(
                    "Skipping deltas for %s — manifest hash matches current script",
                    target_date,
                )
                return "skipped"

        return _submit_and_wait("build_silver_update_deltas.py", target_date, "bzp-deltas")

    # ------------------------------------------------------------------
    # Wiring:
    #   fetch_range (single task, full date window)
    #     → bronze / silver / deltas (dynamic task mapping, per date)
    # ------------------------------------------------------------------
    dates = generate_date_range()
    fetch = fetch_range()
    bronze_results = submit_bronze_batch.expand(target_date=dates)
    silver_results = submit_silver_batch.expand(target_date=dates)
    deltas_results = submit_deltas_batch.expand(target_date=dates)

    # All dates must be downloaded before any bronze batch starts.
    # Within the Spark steps, per-date ordering is preserved by index alignment.
    fetch >> bronze_results >> silver_results >> deltas_results
