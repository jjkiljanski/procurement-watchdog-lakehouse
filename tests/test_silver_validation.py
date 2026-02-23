"""Tests for Silver Spark-side validation rules."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.validation import (  # noqa: E402
    summarize_notice_validation,
    validate_common_envelope,
    with_notice_validation_errors,
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
        .getOrCreate()
    )
    yield session
    session.stop()


def test_common_envelope_postal_rule_depends_on_country(spark):
    df = spark.createDataFrame(
        [
            {
                "street": "ul. Testowa 1",
                "postal_code": "00123",  # invalid PL format
                "organizationCountry": "PL",
            },
            {
                "street": "Rue de Test 2",
                "postal_code": "75008",  # valid abroad (non-PL rule)
                "organizationCountry": "FR",
            },
        ]
    )

    metrics = validate_common_envelope(df, target_date="2025-07-03")
    assert metrics["total_rows"] == 2
    assert metrics["postal_present_rows"] == 2
    assert metrics["postal_invalid_rows"] == 1


def test_notice_validation_postal_rule_depends_on_country(spark):
    df = spark.createDataFrame(
        [
            {
                "objectId": "o1",
                "publicationDate": "2025-07-03T10:00:00Z",
                "noticeType": "ContractNotice",
                "organizationId": "1",
                "caseId": "c1",
                "street": "ul. Testowa 1",
                "postal_code": "00123",  # invalid for PL
                "organizationCountry": "PL",
            },
            {
                "objectId": "o2",
                "publicationDate": "2025-07-03T11:00:00Z",
                "noticeType": "ContractNotice",
                "organizationId": "2",
                "caseId": "c2",
                "street": "Rue de Test 2",
                "postal_code": "75008",  # accepted for non-PL
                "organizationCountry": "FR",
            },
        ]
    )

    df_err, rules = with_notice_validation_errors(df, target_date="2025-07-03", notice_type="ContractNotice")
    metrics = summarize_notice_validation(
        df_with_errors=df_err,
        target_date="2025-07-03",
        notice_type="ContractNotice",
        rules=rules,
    )
    assert metrics["postal_invalid_rows"] == 1
    assert metrics["invalid_rows"] >= 1
