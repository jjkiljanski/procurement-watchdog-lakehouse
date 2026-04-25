"""Google Cloud Platform provider.

Used when ``RUNTIME_ENV=gcp``.

Storage model
-------------
- ``bronze_raw`` and ``bronze``  →  GCS Parquet/JSON  (``gs://{LAKEHOUSE_BUCKET}/…``)
- ``silver`` and deltas           →  GCS Parquet  (``gs://{LAKEHOUSE_BUCKET}/silver/…``)
- External BigQuery tables        →  created once via
  ``scripts/ops/setup_bq_external_tables.py``
- Observability writes            →  TODO: extend obs.py to write directly to
  GCS; for now obs writes are skipped (``obs_path()`` returns ``None``).

Iceberg note
------------
Silver writes use Apache Iceberg tables on the ``HadoopCatalog`` warehouse at
``gs://{LAKEHOUSE_BUCKET}/iceberg/``.  The Iceberg Spark runtime JAR is
included in the container image at ``/opt/iceberg-spark-runtime.jar``
(see ``Dockerfile.spark``) and is referenced via ``jar_file_uris`` in every
Dataproc Serverless batch so executors also have it on the classpath.

BigQuery access to silver data is via ``FORMAT='ICEBERG'`` external tables
created by ``scripts/ops/setup_bq_external_tables.py --format iceberg``.
See ``docs/iceberg.md`` and ``docs/cloud_architecture.md`` for details.

Spark jobs
----------
Jobs are submitted to Dataproc Serverless via the ``google-cloud-dataproc``
client library.  The batch is created with the runtime container image
specified by ``DATAPROC_CONTAINER_IMAGE`` and waits for completion by default.

Required environment variables
-------------------------------
``LAKEHOUSE_BUCKET``
    GCS bucket name (no ``gs://`` prefix).
``GCP_PROJECT``
    GCP project ID.
``DATAPROC_REGION``
    Dataproc region, e.g. ``europe-west1``.
``DATAPROC_CONTAINER_IMAGE``
    Full Artifact Registry image URI, e.g.
    ``europe-west1-docker.pkg.dev/my-project/spark/procurement-spark:latest``.

Optional environment variables
-------------------------------
``DATAPROC_SUBNET``
    VPC subnet URI or short name (default: ``default``).
``DATAPROC_SERVICE_ACCOUNT``
    Service account email for Dataproc batches.
``BQ_DATASET``
    BigQuery dataset name (default: ``procurement_silver``).
``SPARK_APP_EXTRA_CONFIG``
    JSON object of extra Spark conf key/value pairs.
"""

from __future__ import annotations

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

from procurement.runtime.base import RuntimeConfig, SparkLauncher, StateBackend, StorageProvider


# ---------------------------------------------------------------------------
# GCSStorageProvider
# ---------------------------------------------------------------------------


class GCSStorageProvider(StorageProvider):
    """Resolves logical paths to ``gs://<bucket>/<path>`` URIs."""

    def __init__(self, bucket: str) -> None:
        self._bucket = bucket

    @property
    def bucket(self) -> str:
        return self._bucket

    def resolve(self, logical_path: str) -> str:
        """Return the fully-qualified GCS URI for *logical_path*."""
        return f"gs://{self._bucket}/{logical_path}"

    def exists(self, logical_path: str) -> bool:
        from google.cloud import storage as gcs

        client = gcs.Client()
        blob_prefix = logical_path.rstrip("/") + "/"
        blobs = client.list_blobs(self._bucket, prefix=blob_prefix, max_results=1)
        return any(True for _ in blobs)

    def read_json(self, logical_path: str) -> dict[str, Any]:
        from google.cloud import storage as gcs
        from google.cloud.exceptions import NotFound

        client = gcs.Client()
        blob = client.bucket(self._bucket).blob(logical_path)
        try:
            data = blob.download_as_text(encoding="utf-8")
        except NotFound:
            return {}
        return json.loads(data)

    def write_json(self, logical_path: str, data: dict[str, Any]) -> None:
        from google.cloud import storage as gcs

        client = gcs.Client()
        blob = client.bucket(self._bucket).blob(logical_path)
        blob.upload_from_string(
            json.dumps(data, ensure_ascii=False, indent=2),
            content_type="application/json",
        )

    def list_prefixes(self, logical_path: str) -> list[str]:
        """Return immediate child prefix names (virtual directory listing)."""
        from google.cloud import storage as gcs

        client = gcs.Client()
        prefix = logical_path.rstrip("/") + "/"
        blobs = client.list_blobs(self._bucket, prefix=prefix, delimiter="/")
        # Consume pages to populate .prefixes
        _ = list(blobs)
        return sorted(p[len(prefix) :].rstrip("/") for p in blobs.prefixes)

    @contextmanager
    def acquire_lock(self, key: str) -> Generator[None, None, None]:
        """Acquire a GCS object lock.

        Creates a zero-byte object at ``_locks/{key}`` and deletes it on
        exit.  In Airflow (where Dataproc batch tasks are serialised) this
        guard is rarely needed but kept for safety.
        """
        import time

        from google.cloud import storage as gcs
        from google.cloud.exceptions import Conflict

        client = gcs.Client()
        lock_blob_name = f"_locks/{key}.lock"
        bucket = client.bucket(self._bucket)
        blob = bucket.blob(lock_blob_name)

        deadline = time.time() + 300
        while True:
            try:
                blob.upload_from_string(b"", if_generation_match=0)
                break
            except Conflict:
                if time.time() > deadline:
                    raise TimeoutError(f"Could not acquire GCS lock {lock_blob_name} within 300 s")
                time.sleep(5)
        try:
            yield
        finally:
            blob.delete(if_generation_not_match=None)

    def obs_path(self) -> Path | None:
        # Returning None signals to pipeline scripts that local-Parquet obs
        # writes are not used.  obs.py detects RUNTIME_ENV=gcp and writes to
        # BigQuery instead (see src/procurement/obs.py: _use_bq()).
        return None


# ---------------------------------------------------------------------------
# DataprocServerlessLauncher
# ---------------------------------------------------------------------------


class DataprocServerlessLauncher(SparkLauncher):
    """Submits PySpark jobs to Dataproc Serverless Batches.

    ``get_session()`` is used when the *current process* is already running
    inside a Dataproc batch.  ``submit_batch()`` is called by the Airflow DAG
    operators to launch new batches.
    """

    def __init__(
        self,
        project: str,
        region: str,
        bucket: str,
        container_image: str,
        subnet: str = "default",
        service_account: str | None = None,
        extra_config: dict[str, str] | None = None,
    ) -> None:
        self._project = project
        self._region = region
        self._bucket = bucket
        self._container_image = container_image
        self._subnet = subnet
        self._service_account = service_account
        self._extra_config = extra_config or {}

    def _require_spark_env(self) -> None:
        """Raise early with a clear message if Spark-specific vars are absent."""
        missing = [v for v in ("DATAPROC_REGION", "DATAPROC_CONTAINER_IMAGE") if not os.environ.get(v, "").strip()]
        if missing:
            raise EnvironmentError(
                f"Missing required Spark environment variable(s): {missing}. "
                "These are needed for Dataproc Serverless jobs but are not required "
                "for storage-only workloads (e.g. the downloader Cloud Run Job)."
            )

    def get_session(self, app_name: str, **extra_config: str):
        """Return a SparkSession configured for GCS + Iceberg on GCS.

        When running inside a Dataproc batch, the GCS connector is already
        on the classpath; we just need to set the Iceberg catalog config.
        """
        self._require_spark_env()
        from pyspark.sql import SparkSession

        warehouse = f"gs://{self._bucket}/iceberg"

        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.ansi.enabled", "false")
            .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
            .config("spark.scheduler.mode", "FAIR")
            # Iceberg extensions — see docs/iceberg.md for migration to native
            # Iceberg table writes.
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.silver.type", "hadoop")
            .config("spark.sql.catalog.silver.warehouse", warehouse)
        )
        for k, v in {**self._extra_config, **extra_config}.items():
            builder = builder.config(k, v)
        return builder.getOrCreate()

    def submit_batch(
        self,
        script_path: str,
        args: list[str],
        job_id: str,
        *,
        wait: bool = True,
    ) -> int:
        """Submit a Dataproc Serverless batch and optionally wait for it.

        Parameters
        ----------
        script_path:
            GCS URI of the PySpark entry-point (e.g.
            ``gs://{bucket}/jobs/build_bronze.py``).
        args:
            CLI arguments forwarded to the script.
        job_id:
            Used as the Dataproc batch ID suffix (must be unique per project
            per region; alphanumeric + hyphens only).

        Notes
        -----
        The Airflow DAG uses ``DataprocCreateBatchOperator`` instead of this
        method for richer retry/monitoring.  This method is provided for
        direct programmatic use (e.g. testing, one-off backfill scripts).
        """
        self._require_spark_env()
        import re
        import time

        from google.cloud import dataproc_v1 as dataproc

        client = dataproc.BatchControllerClient(
            client_options={"api_endpoint": f"{self._region}-dataproc.googleapis.com:443"}
        )

        # Batch IDs must be lowercase alphanumeric + hyphens, max 63 chars.
        safe_id = re.sub(r"[^a-z0-9-]", "-", job_id.lower())[:55]
        batch_id = f"{safe_id}-{int(time.time())}"

        batch = dataproc.Batch()
        batch.pyspark_batch = dataproc.PySparkBatch(
            main_python_file_uri=script_path,
            args=args,
            # Deliver the Iceberg Spark runtime JAR to all executors.
            # The JAR is baked into the container image at this path; referencing
            # it via jar_file_uris ensures the Dataproc executor classpath picks
            # it up (PYSPARK_SUBMIT_ARGS / SPARK_EXTRA_CLASSPATH are driver-only
            # in Dataproc Serverless and do not reach executors).
            jar_file_uris=["file:///opt/iceberg-spark-runtime.jar"],
        )
        batch.runtime_config = dataproc.RuntimeConfig(
            container_image=self._container_image,
        )
        batch.environment_config = dataproc.EnvironmentConfig(
            execution_config=dataproc.ExecutionConfig(
                subnetwork_uri=self._subnet,
                **(
                    {"service_account": self._service_account}
                    if self._service_account
                    else {}
                ),
            )
        )

        parent = f"projects/{self._project}/locations/{self._region}"
        operation = client.create_batch(
            request=dataproc.CreateBatchRequest(
                parent=parent,
                batch=batch,
                batch_id=batch_id,
            )
        )

        if not wait:
            return 0

        result = operation.result()  # blocks until done
        state = result.state
        # State 4 = SUCCEEDED
        return 0 if state == dataproc.Batch.State.SUCCEEDED else 1


# ---------------------------------------------------------------------------
# GCSStateBackend
# ---------------------------------------------------------------------------


class GCSStateBackend(StateBackend):
    """Stores pipeline state as JSON objects under ``gs://{bucket}/_state/``."""

    def __init__(self, storage: GCSStorageProvider) -> None:
        self._storage = storage

    def _state_logical_path(self, state_key: str) -> str:
        return f"_state/{state_key}.json"

    def load(self, state_key: str) -> dict[str, Any]:
        return self._storage.read_json(self._state_logical_path(state_key))

    def save(self, state_key: str, data: dict[str, Any]) -> None:
        self._storage.write_json(self._state_logical_path(state_key), data)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise EnvironmentError(
            f"Required environment variable {name!r} is not set. "
            "See config/runtime_gcp.env.example for all required variables."
        )
    return value


def build_gcp_runtime() -> RuntimeConfig:
    """Construct a :class:`RuntimeConfig` for Google Cloud Platform.

    ``LAKEHOUSE_BUCKET`` and ``GCP_PROJECT`` are required for all GCP roles.
    Spark-specific vars (``DATAPROC_REGION``, ``DATAPROC_CONTAINER_IMAGE``,
    etc.) are only validated when the Spark launcher is actually invoked, so
    storage-only workloads (e.g. the Cloud Run downloader job) don't need them.
    """
    bucket = _require_env("LAKEHOUSE_BUCKET")
    project = _require_env("GCP_PROJECT")

    # Spark vars — read now but validated lazily inside the launcher.
    region = os.environ.get("DATAPROC_REGION", "").strip()
    container_image = os.environ.get("DATAPROC_CONTAINER_IMAGE", "").strip()
    subnet = os.environ.get("DATAPROC_SUBNET", "default")
    service_account = os.environ.get("DATAPROC_SERVICE_ACCOUNT") or None

    extra_config_raw = os.environ.get("SPARK_APP_EXTRA_CONFIG", "")
    extra_config: dict[str, str] = {}
    if extra_config_raw.strip():
        try:
            extra_config = json.loads(extra_config_raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"SPARK_APP_EXTRA_CONFIG is not valid JSON: {exc}") from exc

    storage = GCSStorageProvider(bucket=bucket)
    spark = DataprocServerlessLauncher(
        project=project,
        region=region,
        bucket=bucket,
        container_image=container_image,
        subnet=subnet,
        service_account=service_account,
        extra_config=extra_config,
    )
    state = GCSStateBackend(storage=storage)

    return RuntimeConfig(env="gcp", storage=storage, spark=spark, state=state)
