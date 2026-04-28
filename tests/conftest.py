"""Session-scoped Spark fixture shared across all Tier 5 tests.

Tests that require a real Spark session are automatically skipped when PySpark
is unavailable (e.g. on a dev laptop without Java).  They are expected to pass
inside the Docker container built from Dockerfile.launcher (DEV=1).
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for Tier 5 integration tests.

    Automatically skipped when Spark cannot execute tasks (e.g. PySpark version
    mismatch, missing Java, or Windows environment restrictions).
    """
    try:
        import subprocess

        import pyspark  # noqa: F401
        result = subprocess.run(["java", "-version"], capture_output=True, timeout=5)
        if result.returncode != 0:
            pytest.skip("Java not available")
    except Exception:
        pytest.skip("PySpark or Java not available")

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
    try:
        from pyspark.sql.types import StringType, StructField, StructType
        session.createDataFrame(
            [("x",)],
            schema=StructType([StructField("v", StringType())]),
        ).collect()
    except Exception:
        session.stop()
        pytest.skip("Spark cannot execute tasks in this environment")
    yield session
    session.stop()
