"""Tests for Gold Spark transforms."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.gold.spark_transforms import (  # noqa: E402
    build_gold_buyer_mart,
    build_gold_case_mart,
    build_gold_market_mart,
    build_gold_signals_buyer_daily,
)


@pytest.fixture(scope="session")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.appName("gold-tests")
        .master("local[1]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def silver_df(spark):
    lot_schema = StructType(
        [
            StructField("lot_id", StringType(), True),
            StructField("contract_value", DoubleType(), True),
            StructField("estimated_value", DoubleType(), True),
            StructField("lowest_bid", DoubleType(), True),
            StructField("highest_bid", DoubleType(), True),
            StructField("winning_bid", DoubleType(), True),
            StructField("winner", StringType(), True),
        ]
    )
    values_schema = StructType(
        [
            StructField("contract_value", DoubleType(), True),
            StructField("winning_bid", DoubleType(), True),
            StructField("total_paid", DoubleType(), True),
        ]
    )
    contract_execution_schema = StructType([StructField("execution_end_date", StringType(), True)])
    schema = StructType(
        [
            StructField("caseId", StringType(), True),
            StructField("objectId", StringType(), True),
            StructField("organizationId", StringType(), True),
            StructField("provinceName", StringType(), True),
            StructField("noticeType", StringType(), True),
            StructField("noticeStage", StringType(), True),
            StructField("publicationDate", StringType(), True),
            StructField("cpvCodes", ArrayType(StringType()), True),
            StructField("procedureResultParsed", ArrayType(StringType()), True),
            StructField("biddingWindowDays", LongType(), True),
            StructField("priceWeight", LongType(), True),
            StructField("paidRatio", DoubleType(), True),
            StructField("executionDelayed", BooleanType(), True),
            StructField("executionRiskFlag", BooleanType(), True),
            StructField("deadlineChanged", BooleanType(), True),
            StructField("criteriaChanged", BooleanType(), True),
            StructField("scopeChanged", BooleanType(), True),
            StructField("contractorNameNormalized", ArrayType(StringType()), True),
            StructField(
                "htmlExtracted",
                StructType(
                    [
                        StructField("nuts3_code", StringType(), True),
                        StructField("lots", ArrayType(lot_schema), True),
                        StructField("values", values_schema, True),
                        StructField("contract_execution", contract_execution_schema, True),
                    ]
                ),
                True,
            ),
        ]
    )

    rows = [
        {
            "caseId": "case-1",
            "objectId": "obj-1-cn",
            "organizationId": "buyer-1",
            "provinceName": "mazowieckie",
            "noticeType": "ContractNotice",
            "noticeStage": "INIT",
            "publicationDate": "2025-01-01T00:00:00Z",
            "cpvCodes": ["45000000-7 (Roboty budowlane)"],
            "procedureResultParsed": None,
            "biddingWindowDays": 10,
            "priceWeight": 60,
            "paidRatio": None,
            "executionDelayed": None,
            "executionRiskFlag": None,
            "deadlineChanged": False,
            "criteriaChanged": False,
            "scopeChanged": False,
            "contractorNameNormalized": None,
            "htmlExtracted": {
                "nuts3_code": "PL911",
                "lots": None,
                "values": None,
                "contract_execution": None,
            },
        },
        {
            "caseId": "case-1",
            "objectId": "obj-1-upd",
            "organizationId": "buyer-1",
            "provinceName": "mazowieckie",
            "noticeType": "NoticeUpdateNotice",
            "noticeStage": "UPDATE",
            "publicationDate": "2025-01-03T00:00:00Z",
            "cpvCodes": ["45000000-7 (Roboty budowlane)"],
            "procedureResultParsed": None,
            "biddingWindowDays": None,
            "priceWeight": None,
            "paidRatio": None,
            "executionDelayed": None,
            "executionRiskFlag": None,
            "deadlineChanged": True,
            "criteriaChanged": False,
            "scopeChanged": True,
            "contractorNameNormalized": None,
            "htmlExtracted": {
                "nuts3_code": "PL911",
                "lots": None,
                "values": None,
                "contract_execution": None,
            },
        },
        {
            "caseId": "case-1",
            "objectId": "obj-1-trn",
            "organizationId": "buyer-1",
            "provinceName": "mazowieckie",
            "noticeType": "TenderResultNotice",
            "noticeStage": "RESULT",
            "publicationDate": "2025-01-10T00:00:00Z",
            "cpvCodes": ["45000000-7 (Roboty budowlane)"],
            "procedureResultParsed": ["Otrzymano jedną ofertę"],
            "biddingWindowDays": None,
            "priceWeight": None,
            "paidRatio": None,
            "executionDelayed": None,
            "executionRiskFlag": None,
            "deadlineChanged": False,
            "criteriaChanged": False,
            "scopeChanged": False,
            "contractorNameNormalized": ["firma a", "firma b"],
            "htmlExtracted": {
                "nuts3_code": "PL911",
                "lots": [
                    {
                        "lot_id": "1",
                        "contract_value": 100.0,
                        "estimated_value": 120.0,
                        "lowest_bid": 95.0,
                        "highest_bid": 130.0,
                        "winning_bid": 100.0,
                        "winner": "firma a",
                    },
                    {
                        "lot_id": "2",
                        "contract_value": 50.0,
                        "estimated_value": 70.0,
                        "lowest_bid": 45.0,
                        "highest_bid": 80.0,
                        "winning_bid": 50.0,
                        "winner": "firma b",
                    },
                ],
                "values": {"contract_value": 150.0, "winning_bid": 150.0, "total_paid": None},
                "contract_execution": None,
            },
        },
        {
            "caseId": "case-1",
            "objectId": "obj-1-cpn",
            "organizationId": "buyer-1",
            "provinceName": "mazowieckie",
            "noticeType": "ContractPerformingNotice",
            "noticeStage": "EXECUTION",
            "publicationDate": "2025-01-20T00:00:00Z",
            "cpvCodes": ["45000000-7 (Roboty budowlane)"],
            "procedureResultParsed": None,
            "biddingWindowDays": None,
            "priceWeight": None,
            "paidRatio": 1.2,
            "executionDelayed": True,
            "executionRiskFlag": True,
            "deadlineChanged": False,
            "criteriaChanged": False,
            "scopeChanged": False,
            "contractorNameNormalized": None,
            "htmlExtracted": {
                "nuts3_code": "PL911",
                "lots": None,
                "values": {"contract_value": 100.0, "winning_bid": 100.0, "total_paid": 120.0},
                "contract_execution": {"execution_end_date": "2025-01-20"},
            },
        },
        {
            "caseId": "case-2",
            "objectId": "obj-2-trn",
            "organizationId": "buyer-1",
            "provinceName": "mazowieckie",
            "noticeType": "TenderResultNotice",
            "noticeStage": "RESULT",
            "publicationDate": "2025-01-11T00:00:00Z",
            "cpvCodes": ["45000000-7 (Roboty budowlane)"],
            "procedureResultParsed": ["Wiele ofert"],
            "biddingWindowDays": None,
            "priceWeight": None,
            "paidRatio": None,
            "executionDelayed": None,
            "executionRiskFlag": None,
            "deadlineChanged": False,
            "criteriaChanged": False,
            "scopeChanged": False,
            "contractorNameNormalized": ["firma a"],
            "htmlExtracted": {
                "nuts3_code": "PL911",
                "lots": [
                    {
                        "lot_id": "1",
                        "contract_value": 200.0,
                        "estimated_value": 220.0,
                        "lowest_bid": 190.0,
                        "highest_bid": 260.0,
                        "winning_bid": 200.0,
                        "winner": "firma a",
                    }
                ],
                "values": {"contract_value": 200.0, "winning_bid": 200.0, "total_paid": None},
                "contract_execution": None,
            },
        },
        {
            "caseId": "case-3",
            "objectId": "obj-3-trn",
            "organizationId": "buyer-2",
            "provinceName": "malopolskie",
            "noticeType": "TenderResultNotice",
            "noticeStage": "RESULT",
            "publicationDate": "2025-01-09T00:00:00Z",
            "cpvCodes": ["72000000-5 (Usługi IT)"],
            "procedureResultParsed": ["Postępowanie unieważnione"],
            "biddingWindowDays": None,
            "priceWeight": None,
            "paidRatio": None,
            "executionDelayed": None,
            "executionRiskFlag": None,
            "deadlineChanged": False,
            "criteriaChanged": False,
            "scopeChanged": False,
            "contractorNameNormalized": None,
            "htmlExtracted": {
                "nuts3_code": "PL213",
                "lots": [
                    {
                        "lot_id": "status",
                        "contract_value": None,
                        "estimated_value": None,
                        "lowest_bid": None,
                        "highest_bid": None,
                        "winning_bid": None,
                        "winner": None,
                    }
                ],
                "values": {"contract_value": None, "winning_bid": None, "total_paid": None},
                "contract_execution": None,
            },
        },
    ]
    return spark.createDataFrame(rows, schema=schema)


def test_case_mart_basic_metrics(silver_df):
    out = build_gold_case_mart(silver_df, "2025-01-31")
    assert out.select("caseId").distinct().count() == out.count()

    case1 = out.where("caseId = 'case-1'").collect()[0]
    assert case1.num_notices == 4
    assert case1.num_updates == 1
    assert case1.has_init is True
    assert case1.has_result is True
    assert case1.has_execution is True
    assert case1.time_to_award_days == 9
    assert case1.award_to_completion_days == 10
    assert case1.deadline_changed_count == 1
    assert case1.scope_changed_count == 1


def test_buyer_mart_aggregates(silver_df):
    out = build_gold_buyer_mart(silver_df, "2025-01-31")
    buyer1 = out.where("organizationId = 'buyer-1'").collect()[0]
    assert buyer1.notices_total == 5
    assert buyer1.cases_total == 2
    assert buyer1.results_total == 2
    assert buyer1.executions_total == 1
    assert buyer1.updates_total == 1
    assert buyer1.single_bid_rate == pytest.approx(0.5, abs=1e-6)
    assert buyer1.concentration_top1_share == pytest.approx(300.0 / 350.0, abs=1e-6)
    assert buyer1.hhi == pytest.approx((300.0 / 350.0) ** 2 + (50.0 / 350.0) ** 2, abs=1e-6)


def test_market_and_signals_shapes(silver_df):
    market = build_gold_market_mart(silver_df, "2025-01-31")
    signals = build_gold_signals_buyer_daily(silver_df, "2025-01-31")

    cpv45 = market.where("cpv_2digit = '45'").collect()[0]
    assert cpv45.cases_total == 2
    assert cpv45.results_total == 2
    assert cpv45.value_total == pytest.approx(350.0, abs=1e-6)

    buyer1_signal = signals.where("buyer_id = 'buyer-1'").collect()[0]
    assert buyer1_signal.single_bid_rate_today == pytest.approx(0.5, abs=1e-6)
    assert buyer1_signal.update_intensity_today == pytest.approx(0.5, abs=1e-6)
