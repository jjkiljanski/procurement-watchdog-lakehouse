"""Runtime factory.

Reads ``RUNTIME_ENV`` (default: ``"local"``) and returns a
:class:`~procurement.runtime.base.RuntimeConfig` whose provider
implementations match the target environment.

Environment variables
---------------------
``RUNTIME_ENV``
    ``"local"`` (default) or ``"gcp"``.

Local-specific variables (see ``config/runtime_local.env``)
    ``LOCAL_DATA_ROOT`` — root directory for all data paths (default: ``data``).
    ``SPARK_MASTER``    — Spark master string (default: ``local[*]``).

GCP-specific variables (see ``config/runtime_gcp.env.example``)
    ``LAKEHOUSE_BUCKET``           — GCS bucket name (required).
    ``GCP_PROJECT``                — GCP project ID (required).
    ``DATAPROC_REGION``            — Dataproc region, e.g. ``europe-west1`` (required).
    ``DATAPROC_SUBNET``            — VPC subnet name or URI (default: ``default``).
    ``DATAPROC_CONTAINER_IMAGE``   — Artifact Registry image URI for Spark jobs (required).
    ``DATAPROC_SERVICE_ACCOUNT``   — Service account email for Dataproc batches (optional).
    ``BQ_DATASET``                 — BigQuery dataset name for external tables (default: ``procurement_silver``).

Usage
-----
::

    from procurement.runtime import get_runtime

    rt = get_runtime()
    bronze_path = rt.storage.resolve("bronze")          # "/abs/data/bronze" or "gs://…/bronze"
    spark       = rt.spark.get_session("my-app")
    state       = rt.state.load("backfill_state")
"""

from __future__ import annotations

import os

from procurement.runtime.base import RuntimeConfig

# Spark conf key → env var name.
# Dataproc Serverless blocks spark.kubernetes.driverEnv.*, so we pass config
# as spark.procurement.* properties and copy them into os.environ here so the
# rest of the runtime code can use os.environ as normal.
_SPARK_CONF_TO_ENV: dict[str, str] = {
    "spark.procurement.runtime_env": "RUNTIME_ENV",
    "spark.procurement.lakehouse_bucket": "LAKEHOUSE_BUCKET",
    "spark.procurement.gcp_project": "GCP_PROJECT",
    "spark.procurement.dataproc_region": "DATAPROC_REGION",
    "spark.procurement.bq_obs_dataset": "BQ_OBS_DATASET",
}


def _bootstrap_from_spark_conf() -> None:
    """Copy spark.procurement.* Spark conf properties into os.environ.

    No-op when pyspark is not importable (Cloud Run Job, local dev).
    Only sets a variable if it is not already present in the environment,
    so explicit env vars always win.
    """
    try:
        from pyspark import SparkConf
        conf = SparkConf()
    except Exception:
        return
    for spark_key, env_key in _SPARK_CONF_TO_ENV.items():
        if not os.environ.get(env_key):
            try:
                val = conf.get(spark_key)
                if val:
                    os.environ[env_key] = val
            except Exception:
                pass


def get_runtime() -> RuntimeConfig:
    """Build and return the :class:`RuntimeConfig` for the current environment.

    The result is **not** cached — callers that need a stable reference should
    store the returned object themselves.
    """
    _bootstrap_from_spark_conf()
    env = os.environ.get("RUNTIME_ENV", "local").strip().lower()

    if env == "local":
        from procurement.runtime.providers.local import build_local_runtime

        return build_local_runtime()

    if env == "gcp":
        from procurement.runtime.providers.gcp import build_gcp_runtime

        return build_gcp_runtime()

    raise ValueError(
        f"Unknown RUNTIME_ENV={env!r}. Supported values: 'local', 'gcp'."
        " To add a new provider, implement StorageProvider / SparkLauncher / "
        "StateBackend in src/procurement/runtime/providers/<name>.py and add "
        "an elif branch here."
    )
