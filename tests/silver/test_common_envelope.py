"""Tests for Silver envelope validation and schema."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.silver.common_envelope import (  # noqa: E402
    ENVELOPE_COLUMNS,
    build_envelope_df,
    validate_envelope_schema,
)


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.appName("silver-validation-tests")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_validate_envelope_schema_all_present(spark):
    df = spark.createDataFrame(
        [{"objectId": "x", "noticeType": "ContractNotice"}]
    )
    # Add all expected columns as nulls to simulate a full envelope
    for col in ENVELOPE_COLUMNS:
        if col not in df.columns:
            from pyspark.sql.functions import lit
            df = df.withColumn(col, lit(None).cast("string"))

    result = validate_envelope_schema(df)
    assert result["missing_columns"] == [], result["missing_columns"]


def test_validate_envelope_schema_warns_on_missing(spark):
    # DataFrame with only objectId — everything else missing
    df = spark.createDataFrame([{"objectId": "x"}])
    result = validate_envelope_schema(df)
    assert len(result["missing_columns"]) > 0
    assert "noticeType" in result["missing_columns"]


_ENVELOPE_TEST_SCHEMA = StructType([
    StructField("objectId", StringType()),
    StructField("noticeType", StringType()),
    StructField("tenderId", StringType()),
    StructField("clientType", StringType()),
    StructField("organizationProvince", StringType()),
    StructField("publicationDate", StringType()),
    StructField("publicationDateDay", StringType()),
])


def test_build_envelope_df_notice_stage(spark):
    rows = [
        ("1", "ContractNotice", None, None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
        ("2", "TenderResultNotice", "t2", None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
        ("3", "ContractPerformingNotice", None, None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
        ("4", "NoticeUpdateNotice", "t4", None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
    ]
    df_in = spark.createDataFrame(rows, schema=_ENVELOPE_TEST_SCHEMA)
    df_out = build_envelope_df(df_in)

    stage_by_id = {r["objectId"]: r["noticeStage"] for r in df_out.select("objectId", "noticeStage").collect()}
    assert stage_by_id["1"] == "INIT"
    assert stage_by_id["2"] == "RESULT"
    assert stage_by_id["3"] == "EXECUTION"
    assert stage_by_id["4"] == "UPDATE"


def test_build_envelope_df_case_id_fallback(spark):
    rows = [
        ("o1", "ContractNotice", "t1", None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
        ("o2", "ContractNotice", None, None, None, "2025-10-01T00:00:00Z", "2025-10-01"),
    ]
    df_out = build_envelope_df(spark.createDataFrame(rows, schema=_ENVELOPE_TEST_SCHEMA))
    case_by_id = {r["objectId"]: r["caseId"] for r in df_out.select("objectId", "caseId").collect()}
    assert case_by_id["o1"] == "t1"
    assert case_by_id["o2"] == "o2"
