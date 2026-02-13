"""Build gold marts from silver Parquet using PySpark.

Reads:
  data/silver/bzp_YYYY-MM-DD.parquet
Writes:
  data/gold/case_mart/date=YYYY-MM-DD/
  data/gold/buyer_mart/date=YYYY-MM-DD/
  data/gold/market_mart/date=YYYY-MM-DD/
  data/gold/signals_buyer_daily/date=YYYY-MM-DD/
"""

import logging
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, lit, when

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.gold.spark_transforms import (  # noqa: E402
    build_gold_buyer_mart,
    build_gold_case_mart,
    build_gold_market_mart,
    build_gold_signals_buyer_daily,
)
from procurement.gold.utils import has_field  # noqa: E402
from procurement.logging import setup_logging  # noqa: E402

setup_logging()
log = logging.getLogger(__name__)


def _log_schema_probe(df: DataFrame) -> None:
    checks = [
        "caseId",
        "noticeType",
        "noticeStage",
        "cpvCodes",
        "procedureResultParsed",
        "htmlExtracted",
        "htmlExtracted.lots",
        "htmlExtracted.values",
        "htmlExtracted.contract_execution",
        "htmlExtracted.notice_change",
    ]
    log.info("Silver schema snapshot:")
    df.printSchema()
    for field_name in checks:
        log.info("field_present %s=%s", field_name, has_field(df, field_name))

    sample_cols = [
        c
        for c in [
            "caseId",
            "objectId",
            "organizationId",
            "noticeType",
            "noticeStage",
            "publicationDate",
            "cpvCodes",
            "procedureResultParsed",
        ]
        if has_field(df, c)
    ]
    if sample_cols:
        log.info("Silver sample columns=%s", sample_cols)
        df.select(*sample_cols).show(5, truncate=False)


def _write_partition(df: DataFrame, dataset_name: str, target_date: str) -> int:
    out_path = Path("data/gold") / dataset_name / f"date={target_date}"
    if out_path.exists():
        shutil.rmtree(out_path)
    df.write.mode("overwrite").parquet(str(out_path))
    return df.count()


def _nonnull_rate(df: DataFrame, field_name: str) -> float:
    return (
        df.select(avg(when(col(field_name).isNotNull(), lit(1.0)).otherwise(lit(0.0))).alias("r"))
        .collect()[0]
        .r
    )


def _log_basic_quality(case_mart: DataFrame, buyer_mart: DataFrame, market_mart: DataFrame) -> None:
    case_null_rate = _nonnull_rate(case_mart, "caseId")
    buyer_null_rate = _nonnull_rate(buyer_mart, "organizationId")
    market_null_rate = _nonnull_rate(market_mart, "cpv_2digit")
    negative_durations = case_mart.filter(
        (col("time_to_award_days") < 0) | (col("award_to_completion_days") < 0)
    ).count()

    log.warning("quality_non_null_rate caseId=%.4f", case_null_rate)
    log.warning("quality_non_null_rate organizationId=%.4f", buyer_null_rate)
    log.warning("quality_non_null_rate cpv_2digit=%.4f", market_null_rate)
    log.warning("quality_negative_duration_rows=%d", negative_durations)


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_path = Path("data/silver") / f"bzp_{target_date}.parquet"
    if not silver_path.exists():
        log.error("Silver data not found: %s", silver_path)
        sys.exit(1)

    required_cols = ["caseId", "organizationId", "noticeType", "publicationDate", "cpvCodes"]

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bzp-gold")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        df_silver = spark.read.parquet(str(silver_path))
        log.info("Loaded %d silver records", df_silver.count())
        _log_schema_probe(df_silver)

        missing = [c for c in required_cols if c not in df_silver.columns]
        if missing:
            log.error("Missing required Silver columns: %s", missing)
            sys.exit(1)

        case_mart = build_gold_case_mart(df_silver, target_date)
        buyer_mart = build_gold_buyer_mart(df_silver, target_date)
        market_mart = build_gold_market_mart(df_silver, target_date)
        signals_buyer_daily = build_gold_signals_buyer_daily(df_silver, target_date)

        case_rows = _write_partition(case_mart, "case_mart", target_date)
        buyer_rows = _write_partition(buyer_mart, "buyer_mart", target_date)
        market_rows = _write_partition(market_mart, "market_mart", target_date)
        signals_rows = _write_partition(signals_buyer_daily, "signals_buyer_daily", target_date)

        log.info("Wrote case_mart rows=%d", case_rows)
        log.info("Wrote buyer_mart rows=%d", buyer_rows)
        log.info("Wrote market_mart rows=%d", market_rows)
        log.info("Wrote signals_buyer_daily rows=%d", signals_rows)

        _log_basic_quality(case_mart, buyer_mart, market_mart)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
