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


# CLI flag → env var name.
# The workflow passes these as extra script_args so the driver sets them in
# os.environ before SparkContext (and thus the JVM) is available.
_ARGV_TO_ENV: dict[str, str] = {
    "--runtime-env": "RUNTIME_ENV",
    "--lakehouse-bucket": "LAKEHOUSE_BUCKET",
    "--gcp-project": "GCP_PROJECT",
    "--dataproc-region": "DATAPROC_REGION",
    "--bq-obs-dataset": "BQ_OBS_DATASET",
}

_SPARK_CONF_PATHS = [
    "/etc/spark/conf/spark-defaults.conf",
    "/usr/lib/spark/conf/spark-defaults.conf",
    "/usr/local/spark/conf/spark-defaults.conf",
]


# Run immediately at import time so the flags are consumed before any script's
# argparse runs (scripts import get_runtime at module level, before main()).
def _bootstrap_from_argv() -> None:
    """Extract gcp env flags from sys.argv and set os.environ.

    Recognises ``--runtime-env=X``, ``--lakehouse-bucket=X``, etc.  Consumes
    those flags from ``sys.argv`` so the calling script's argparse doesn't see
    them as unrecognised arguments.  Only sets a variable when it is not
    already present in the environment, so explicit env vars always win.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(add_help=False)
    for flag in _ARGV_TO_ENV:
        parser.add_argument(flag, default=None)

    namespace, remaining = parser.parse_known_args(sys.argv[1:])

    applied = False
    for flag, env_key in _ARGV_TO_ENV.items():
        attr = flag.lstrip("-").replace("-", "_")
        val = getattr(namespace, attr, None)
        if val and not os.environ.get(env_key):
            os.environ[env_key] = val
            applied = True

    if applied:
        sys.argv[1:] = remaining


_bootstrap_from_argv()


def _bootstrap_from_spark_conf() -> None:
    """Copy spark.procurement.* Spark conf properties into os.environ.

    Reads spark-defaults.conf directly as text — no JVM required, so this
    works before SparkContext is initialized. Falls back to pyspark.SparkConf
    as a secondary attempt (only works after SparkContext exists).

    Only sets a variable if it is not already present in the environment,
    so explicit env vars always win.
    """
    import pathlib

    props: dict[str, str] = {}
    for path_str in _SPARK_CONF_PATHS:
        try:
            text = pathlib.Path(path_str).read_text()
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split(None, 1)
                if len(parts) == 2:
                    props[parts[0]] = parts[1]
        except OSError:
            pass

    # Secondary fallback: pyspark SparkConf (only works after SparkContext).
    try:
        from pyspark import SparkConf
        conf = SparkConf()
        for spark_key in _SPARK_CONF_TO_ENV:
            try:
                val = conf.get(spark_key, "")
                if val:
                    props.setdefault(spark_key, val)
            except Exception:
                pass
    except Exception:
        pass

    for spark_key, env_key in _SPARK_CONF_TO_ENV.items():
        if not os.environ.get(env_key):
            val = props.get(spark_key, "").strip()
            if val:
                os.environ[env_key] = val


def get_runtime() -> RuntimeConfig:
    """Build and return the :class:`RuntimeConfig` for the current environment.

    The result is **not** cached — callers that need a stable reference should
    store the returned object themselves.
    """
    _bootstrap_from_argv()
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
