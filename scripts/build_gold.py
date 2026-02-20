"""Build gold marts from silver Parquet using PySpark.

Reads:
  data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/
  data/silver/notice_type_tables/noticeType=*/publicationDateDay=YYYY-MM-DD/
Writes:
  data/gold/case_mart/date=YYYY-MM-DD/
  data/gold/buyer_mart/date=YYYY-MM-DD/
  data/gold/market_mart/date=YYYY-MM-DD/
  data/gold/signals_buyer_daily/date=YYYY-MM-DD/
"""

import logging
import os
import shutil
import sys
from datetime import date, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, coalesce, col, lit, struct, when

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.gold.spark_transforms import (  # noqa: E402
    build_gold_buyer_mart,
    build_gold_case_mart,
    build_gold_market_mart,
    build_gold_signals_buyer_daily,
)
from procurement.gold.utils import has_field, safe_col  # noqa: E402
from procurement.logging import setup_logging  # noqa: E402

setup_logging()
log = logging.getLogger(__name__)


def _silver_partition_daily_paths(silver_dir: Path, target_date: str) -> list[str]:
    return sorted(
        str(p)
        for p in silver_dir.glob(f"notice_type_tables/noticeType=*/publicationDateDay={target_date}")
        if p.is_dir()
    )


def _silver_partition_asof_paths(silver_dir: Path, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted(silver_dir.glob("notice_type_tables/noticeType=*/publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        if token <= target_date:
            out.append(str(p))
    return out


def _silver_envelope_daily_paths(silver_dir: Path, target_date: str) -> list[str]:
    path = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
    return [str(path)] if path.is_dir() else []


def _silver_envelope_asof_paths(silver_dir: Path, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted((silver_dir / "common_envelope").glob("publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        if token <= target_date:
            out.append(str(p))
    return out


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


def _write_partition(df: DataFrame, dataset_name: str, target_date: str, gold_dir: Path) -> int:
    out_path = gold_dir / dataset_name / f"date={target_date}"
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


def _normalize_for_gold(frame: DataFrame) -> DataFrame:
    normalized_html = struct(
        safe_col(
            frame,
            "htmlExtracted.contract_execution",
            "struct<contract_date:string,contract_executed:boolean,executed_on_time:boolean,executed_properly:boolean,execution_end_date:string,execution_period:string,num_changes:bigint>",
        ).alias("contract_execution"),
        safe_col(
            frame,
            "htmlExtracted.lots",
            "array<struct<contract_value:double,estimated_value:double,highest_bid:double,lot_id:string,lowest_bid:double,winner:string,winning_bid:double>>",
        ).alias("lots"),
        safe_col(
            frame,
            "htmlExtracted.values",
            "struct<contract_value:double,currency:string,estimated_value:double,highest_bid:double,lowest_bid:double,total_paid:double,winning_bid:double>",
        ).alias("values"),
        safe_col(frame, "htmlExtracted.nuts3_code", "string").alias("nuts3_code"),
    ).alias("htmlExtracted")
    return (
        frame.withColumn("paidRatio", safe_col(frame, "paidRatio", "double").cast("double"))
        .select(
            safe_col(frame, "clientType", "string").alias("clientType"),
            safe_col(frame, "orderType", "string").alias("orderType"),
            safe_col(frame, "tenderType", "string").alias("tenderType"),
            safe_col(frame, "noticeType", "string").alias("noticeType"),
            safe_col(frame, "noticeNumber", "string").alias("noticeNumber"),
            safe_col(frame, "bzpNumber", "string").alias("bzpNumber"),
            safe_col(frame, "publicationDate", "string").alias("publicationDate"),
            safe_col(frame, "submittingOffersDate", "string").alias("submittingOffersDate"),
            safe_col(frame, "organizationId", "string").alias("organizationId"),
            safe_col(frame, "organizationNationalId", "string").alias("organizationNationalId"),
            safe_col(frame, "provinceName", "string").alias("provinceName"),
            safe_col(frame, "clientTypeName", "string").alias("clientTypeName"),
            safe_col(frame, "street", "string").alias("street"),
            safe_col(frame, "postal_code", "string").alias("postal_code"),
            safe_col(frame, "caseId", "string").alias("caseId"),
            safe_col(frame, "objectId", "string").alias("objectId"),
            safe_col(frame, "noticeStage", "string").alias("noticeStage"),
            safe_col(frame, "cpvCodes", "array<string>").alias("cpvCodes"),
            safe_col(frame, "procedureResultParsed", "array<string>").alias("procedureResultParsed"),
            safe_col(
                frame,
                "contractors",
                "array<struct<contractorCity:string,contractorCountry:string,contractorName:string,contractorNationalId:string,contractorProvince:string>>",
            ).alias("contractors"),
            safe_col(frame, "biddingWindowDays", "long").alias("biddingWindowDays"),
            safe_col(frame, "priceWeight", "double").alias("priceWeight"),
            safe_col(frame, "nonPriceWeightSum", "double").alias("nonPriceWeightSum"),
            safe_col(frame, "deadlineChanged", "boolean").alias("deadlineChanged"),
            safe_col(frame, "criteriaChanged", "boolean").alias("criteriaChanged"),
            safe_col(frame, "scopeChanged", "boolean").alias("scopeChanged"),
            safe_col(
                frame,
                "changes",
                "array<struct<changed_section:string,change_description:string>>",
            ).alias("changes"),
            safe_col(frame, "paidRatio", "double").alias("paidRatio"),
            safe_col(frame, "executionDelayed", "boolean").alias("executionDelayed"),
            safe_col(frame, "executionRiskFlag", "boolean").alias("executionRiskFlag"),
            safe_col(frame, "contractorNameNormalized", "array<string>").alias("contractorNameNormalized"),
            normalized_html,
        )
    )


def _read_union_raw(spark: "SparkSession", paths: list[str], base_path: str | None = None) -> DataFrame:
    frames = []
    for path in paths:
        reader = spark.read
        if base_path is not None:
            reader = reader.option("basePath", base_path)
        frames.append(reader.parquet(path))
    if not frames:
        raise ValueError("No silver paths to read")
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)


def _read_silver_union(
    spark: "SparkSession", paths: list[str], base_path: str | None = None
) -> DataFrame:
    return _normalize_for_gold(_read_union_raw(spark, paths, base_path=base_path))


def _read_silver_split_layout(
    spark: "SparkSession",
    envelope_paths: list[str],
    specific_paths: list[str],
) -> DataFrame:
    envelope_raw = _read_union_raw(
        spark,
        envelope_paths,
        base_path=str(Path(envelope_paths[0]).parent.parent) if envelope_paths else None,
    )
    envelope_slim = envelope_raw.select(
        safe_col(envelope_raw, "objectId", "string").alias("objectId"),
        safe_col(envelope_raw, "clientType", "string").alias("env_clientType"),
        safe_col(envelope_raw, "orderType", "string").alias("env_orderType"),
        safe_col(envelope_raw, "tenderType", "string").alias("env_tenderType"),
        safe_col(envelope_raw, "organizationId", "string").alias("env_organizationId"),
        safe_col(envelope_raw, "organizationNationalId", "string").alias("env_organizationNationalId"),
        safe_col(envelope_raw, "provinceName", "string").alias("env_provinceName"),
        safe_col(envelope_raw, "clientTypeName", "string").alias("env_clientTypeName"),
        safe_col(envelope_raw, "submittingOffersDate", "string").alias("env_submittingOffersDate"),
    )
    specific_raw = _read_union_raw(
        spark,
        specific_paths,
        base_path=str(Path(specific_paths[0]).parent.parent) if specific_paths else None,
    )
    merged = specific_raw.join(envelope_slim, on="objectId", how="left")

    merged = (
        merged.withColumn(
            "clientType",
            coalesce(safe_col(merged, "clientType", "string"), col("env_clientType")),
        )
        .withColumn(
            "orderType",
            coalesce(safe_col(merged, "orderType", "string"), col("env_orderType")),
        )
        .withColumn(
            "tenderType",
            coalesce(safe_col(merged, "tenderType", "string"), col("env_tenderType")),
        )
        .withColumn(
            "organizationId",
            coalesce(safe_col(merged, "organizationId", "string"), col("env_organizationId")),
        )
        .withColumn(
            "organizationNationalId",
            coalesce(
                safe_col(merged, "organizationNationalId", "string"),
                col("env_organizationNationalId"),
            ),
        )
        .withColumn(
            "provinceName",
            coalesce(safe_col(merged, "provinceName", "string"), col("env_provinceName")),
        )
        .withColumn(
            "clientTypeName",
            coalesce(safe_col(merged, "clientTypeName", "string"), col("env_clientTypeName")),
        )
        .withColumn(
            "submittingOffersDate",
            coalesce(
                safe_col(merged, "submittingOffersDate", "string"),
                col("env_submittingOffersDate"),
            ),
        )
    )
    return _normalize_for_gold(merged)


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
    if len(sys.argv) > 1 and not sys.argv[1].startswith("--"):
        target_date = sys.argv[1]
        extra_args = sys.argv[2:]
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()
        extra_args = sys.argv[1:]

    silver_dir = Path("data/silver")
    gold_dir = Path("data/gold")
    spark_master = os.environ.get("SPARK_MASTER", "local[*]")
    scope = "daily"
    for i, token in enumerate(extra_args):
        if token == "--silver-dir" and i + 1 < len(extra_args):
            silver_dir = Path(extra_args[i + 1])
        if token == "--gold-dir" and i + 1 < len(extra_args):
            gold_dir = Path(extra_args[i + 1])
        if token == "--spark-master" and i + 1 < len(extra_args):
            spark_master = extra_args[i + 1]
        if token == "--scope" and i + 1 < len(extra_args):
            scope = extra_args[i + 1]

    if scope not in {"daily", "asof"}:
        log.error("Unsupported --scope=%s (expected daily|asof)", scope)
        sys.exit(1)

    legacy_silver_path = silver_dir / f"bzp_{target_date}.parquet"
    silver_paths: list[str]
    daily_silver_paths: list[str]
    envelope_paths: list[str] = []
    daily_envelope_paths: list[str] = []
    use_split_layout = False
    if scope == "daily":
        daily_silver_paths = _silver_partition_daily_paths(silver_dir, target_date)
        daily_envelope_paths = _silver_envelope_daily_paths(silver_dir, target_date)
        if daily_silver_paths and daily_envelope_paths:
            use_split_layout = True
            silver_paths = daily_silver_paths
        elif legacy_silver_path.exists():
            # Compatibility with pre-partitioned Silver layout.
            silver_paths = [str(legacy_silver_path)]
            daily_silver_paths = [str(legacy_silver_path)]
        else:
            log.error(
                "Silver data not found for date=%s in partitioned or legacy layout under %s",
                target_date,
                silver_dir,
            )
            sys.exit(1)
    else:
        if str(gold_dir) == "data/gold":
            gold_dir = Path("data/gold_asof")
        silver_paths = _silver_partition_asof_paths(silver_dir, target_date)
        daily_silver_paths = _silver_partition_daily_paths(silver_dir, target_date)
        envelope_paths = _silver_envelope_asof_paths(silver_dir, target_date)
        daily_envelope_paths = _silver_envelope_daily_paths(silver_dir, target_date)
        if silver_paths and envelope_paths and daily_silver_paths and daily_envelope_paths:
            use_split_layout = True
        elif not silver_paths:
            # Compatibility with pre-partitioned Silver layout.
            all_paths = sorted(silver_dir.glob("bzp_*.parquet"))
            silver_paths = []
            for p in all_paths:
                day = p.stem.replace("bzp_", "")
                if day <= target_date:
                    silver_paths.append(str(p))
            if legacy_silver_path.exists():
                daily_silver_paths = [str(legacy_silver_path)]
        if not silver_paths:
            log.error("No silver files found in %s up to date=%s", silver_dir, target_date)
            sys.exit(1)
        if not daily_silver_paths:
            log.error(
                "Daily silver input for signals not found for date=%s under %s",
                target_date,
                silver_dir,
            )
            sys.exit(1)
        if use_split_layout:
            log.info(
                "Using split Silver layout asof specific=%d envelope=%d",
                len(silver_paths),
                len(envelope_paths),
            )
        else:
            log.info("Using legacy Silver layout asof files=%d", len(silver_paths))

    required_cols = ["caseId", "organizationId", "noticeType", "publicationDate", "cpvCodes"]

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bzp-gold")
        .master(spark_master)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )

    try:
        if use_split_layout:
            marts_envelope = envelope_paths if scope == "asof" else daily_envelope_paths
            df_silver_for_marts = _read_silver_split_layout(
                spark,
                envelope_paths=marts_envelope,
                specific_paths=silver_paths,
            )
            df_silver_daily = _read_silver_split_layout(
                spark,
                envelope_paths=daily_envelope_paths,
                specific_paths=daily_silver_paths,
            )
        else:
            silver_base = str(silver_dir) if any("publicationDateDay=" in p for p in silver_paths) else None
            daily_base = (
                str(silver_dir) if any("publicationDateDay=" in p for p in daily_silver_paths) else None
            )
            df_silver_for_marts = _read_silver_union(spark, silver_paths, base_path=silver_base)
            df_silver_daily = _read_silver_union(spark, daily_silver_paths, base_path=daily_base)
        log.info("Loaded marts scope records=%d", df_silver_for_marts.count())
        log.info("Loaded daily scope records=%d", df_silver_daily.count())
        _log_schema_probe(df_silver_for_marts)

        missing = [c for c in required_cols if c not in df_silver_for_marts.columns]
        if missing:
            log.error("Missing required Silver columns: %s", missing)
            sys.exit(1)

        case_mart = build_gold_case_mart(df_silver_for_marts, target_date)
        buyer_mart = build_gold_buyer_mart(df_silver_for_marts, target_date)
        market_mart = build_gold_market_mart(df_silver_for_marts, target_date)
        signals_buyer_daily = build_gold_signals_buyer_daily(df_silver_daily, target_date)

        case_rows = _write_partition(case_mart, "case_mart", target_date, gold_dir)
        buyer_rows = _write_partition(buyer_mart, "buyer_mart", target_date, gold_dir)
        market_rows = _write_partition(market_mart, "market_mart", target_date, gold_dir)
        signals_rows = _write_partition(signals_buyer_daily, "signals_buyer_daily", target_date, gold_dir)

        log.info("Wrote case_mart rows=%d", case_rows)
        log.info("Wrote buyer_mart rows=%d", buyer_rows)
        log.info("Wrote market_mart rows=%d", market_rows)
        log.info("Wrote signals_buyer_daily rows=%d", signals_rows)

        _log_basic_quality(case_mart, buyer_mart, market_mart)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
