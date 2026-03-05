"""Session-scoped Spark fixture shared across all Tier 5 tests.

Tests that require a real Spark session are automatically skipped when PySpark
is unavailable (e.g. on a dev laptop without Java).  They are expected to pass
inside the Docker container built from Dockerfile.launcher (DEV=1).
"""

from __future__ import annotations

import pytest


def _spark_available() -> bool:
    try:
        import pyspark  # noqa: F401
        import subprocess
        result = subprocess.run(["java", "-version"], capture_output=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for Tier 5 integration tests."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-silver-tier5")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=WARN")
        .getOrCreate()
    )
    yield session
    session.stop()
