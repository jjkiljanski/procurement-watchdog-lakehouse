"""Build Silver case-derived lifecycle facts.

Modes:
  full:
    Rebuild case_derived_facts snapshot from all Silver notices up to target date.
  incremental:
    Recompute only cases touched on target date and merge with previous snapshot.

Reads:
  data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/
  data/silver/notice_type_tables/noticeType=*/publicationDateDay=YYYY-MM-DD/

Writes:
  data/silver/case_derived_facts/asOfDate=YYYY-MM-DD/
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
from datetime import date, datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    count,
    datediff,
    expr,
    first,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import ArrayType, DataType, StructType

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def has_field(df: DataFrame, field_path: str) -> bool:
    current: DataType = df.schema
    for part in field_path.split("."):
        if isinstance(current, StructType):
            field = next((f for f in current.fields if f.name == part), None)
            if field is None:
                return False
            current = field.dataType
            continue
        if isinstance(current, ArrayType):
            current = current.elementType
            if isinstance(current, StructType):
                field = next((f for f in current.fields if f.name == part), None)
                if field is None:
                    return False
                current = field.dataType
                continue
        return False
    return True


def safe_col(df: DataFrame, field_path: str, cast_to: str | None = None):
    from pyspark.sql.functions import lit

    if has_field(df, field_path):
        return col(field_path)
    if cast_to is None:
        return lit(None)
    return lit(None).cast(cast_to)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Silver case_derived_facts snapshot.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Build mode for case_derived_facts",
    )
    parser.add_argument("--silver-dir", default="data/silver", help="Silver root directory")
    parser.add_argument(
        "--output-dir",
        default="data/silver/case_derived_facts",
        help="Output directory for case_derived_facts snapshots",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[2])",
    )
    return parser.parse_args()


def _union_paths(spark: "SparkSession", paths: list[str], base_path: str | None = None) -> DataFrame:
    frames = []
    for path in paths:
        reader = spark.read
        if base_path:
            reader = reader.option("basePath", base_path)
        frames.append(reader.parquet(path))
    if not frames:
        raise ValueError("No paths to read")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def _paths_up_to(base_dir: Path, partition_key: str, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted(base_dir.glob(f"{partition_key}=*")):
        if not p.is_dir():
            continue
        token = p.name.replace(f"{partition_key}=", "")
        if token <= target_date:
            out.append(str(p))
    return out


def _specific_paths_up_to(silver_dir: Path, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted(silver_dir.glob("notice_type_tables/noticeType=*/publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        if token <= target_date:
            out.append(str(p))
    return out


def _read_notices_merged(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    envelope_paths = _paths_up_to(envelope_root, "publicationDateDay", target_date)
    specific_paths = _specific_paths_up_to(silver_dir, target_date)
    if not envelope_paths or not specific_paths:
        raise ValueError(
            f"Missing silver inputs for <= {target_date}: envelope={len(envelope_paths)} specific={len(specific_paths)}"
    )

    envelope_raw = _union_paths(spark, envelope_paths, base_path=str(envelope_root))
    # Avoid duplicate partition/data column warnings on noticeType by reading paths directly.
    specific_raw = _union_paths(spark, specific_paths)

    envelope_slim = envelope_raw.select(
        safe_col(envelope_raw, "objectId", "string").alias("objectId"),
        safe_col(envelope_raw, "caseId", "string").alias("env_caseId"),
        safe_col(envelope_raw, "organizationId", "string").alias("env_organizationId"),
        safe_col(envelope_raw, "noticeType", "string").alias("env_noticeType"),
        safe_col(envelope_raw, "publicationDate", "string").alias("env_publicationDate"),
        safe_col(envelope_raw, "submittingOffersDate", "string").alias("submittingOffersDate"),
    )

    merged = specific_raw.join(envelope_slim, on="objectId", how="left").select(
        safe_col(specific_raw, "objectId", "string").alias("objectId"),
        coalesce(
            safe_col(specific_raw, "caseId", "string"),
            safe_col(envelope_slim, "env_caseId", "string"),
        ).alias("caseId"),
        coalesce(
            safe_col(specific_raw, "organizationId", "string"),
            safe_col(envelope_slim, "env_organizationId", "string"),
        ).alias("organizationId"),
        coalesce(
            safe_col(specific_raw, "noticeType", "string"),
            safe_col(envelope_slim, "env_noticeType", "string"),
        ).alias("noticeType"),
        coalesce(
            safe_col(specific_raw, "publicationDate", "string"),
            safe_col(envelope_slim, "env_publicationDate", "string"),
        ).alias("publicationDate"),
        safe_col(envelope_slim, "submittingOffersDate", "string").alias("submittingOffersDate"),
        safe_col(specific_raw, "htmlExtracted", "struct<notice_change:struct<changes:array<struct<changed_section:string,change_description:string>>>,contract_execution:struct<contract_date:string,executed_on_time:boolean,executed_properly:boolean,execution_end_date:string,execution_period:string,num_changes:bigint>,values:struct<contract_value:double,total_paid:double>>").alias(
            "htmlExtracted"
        ),
    )

    if case_ids is not None:
        merged = merged.join(case_ids.select("caseId"), on="caseId", how="inner")

    return merged.filter(col("caseId").isNotNull())


def _build_case_derived(notices: DataFrame) -> DataFrame:
    with_metrics = (
        notices.withColumn("publication_date", to_date(col("publicationDate")))
        .withColumn(
            "biddingWindowDays",
            datediff(to_timestamp(col("submittingOffersDate")), to_timestamp(col("publicationDate"))),
        )
        .withColumn(
            "updateDeltaText",
            lower(
                expr(
                    "concat_ws(' ', transform(coalesce(htmlExtracted.notice_change.changes, array()), "
                    "x -> concat_ws(' ', coalesce(x.changed_section, ''), coalesce(x.change_description, ''))))"
                )
            ),
        )
        .withColumn(
            "deadlineChanged",
            col("updateDeltaText").rlike("termin|deadline|skladania ofert|otwarcia ofert"),
        )
        .withColumn(
            "criteriaChanged",
            col("updateDeltaText").rlike("kryter|cena|waga"),
        )
        .withColumn(
            "scopeChanged",
            col("updateDeltaText").rlike("zakres|przedmiot|opis"),
        )
        .withColumn(
            "executionDurationDays",
            coalesce(
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:dni|dzien|days?)', 1) as int)"
                    ),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:tygod\\w*|weeks?)', 1) as int)"
                    )
                    * lit(7),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:miesi\\w*|months?)', 1) as int)"
                    )
                    * lit(30),
                ),
                when(
                    col("htmlExtracted.contract_execution.contract_date").isNotNull()
                    & col("htmlExtracted.contract_execution.execution_end_date").isNotNull(),
                    datediff(
                        to_date(col("htmlExtracted.contract_execution.execution_end_date")),
                        to_date(col("htmlExtracted.contract_execution.contract_date")),
                    ),
                ),
            ),
        )
        .withColumn(
            "paidRatio",
            when(
                col("htmlExtracted.values.contract_value").isNotNull()
                & (col("htmlExtracted.values.contract_value") != 0)
                & col("htmlExtracted.values.total_paid").isNotNull(),
                col("htmlExtracted.values.total_paid") / col("htmlExtracted.values.contract_value"),
            ),
        )
        .withColumn(
            "executionDelayed",
            when(
                col("htmlExtracted.contract_execution.executed_on_time").isNotNull(),
                ~col("htmlExtracted.contract_execution.executed_on_time"),
            ),
        )
        .withColumn(
            "executionRiskFlag",
            when(
                col("noticeType") == lit("ContractPerformingNotice"),
                coalesce(col("executionDelayed"), lit(False))
                | (coalesce(col("paidRatio"), lit(0.0)) > lit(1.05))
                | (coalesce(col("htmlExtracted.contract_execution.num_changes"), lit(0)) > lit(0))
                | (col("htmlExtracted.contract_execution.executed_properly") == lit(False)),
            ),
        )
        .withColumn(
            "init_date",
            when(
                col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"),
                col("publication_date"),
            ),
        )
        .withColumn(
            "result_date",
            when(col("noticeType") == lit("TenderResultNotice"), col("publication_date")),
        )
        .withColumn(
            "execution_completion_date",
            when(
                col("noticeType") == lit("ContractPerformingNotice"),
                coalesce(
                    to_date(col("htmlExtracted.contract_execution.execution_end_date")),
                    col("publication_date"),
                ),
            ),
        )
    )

    return (
        with_metrics.groupBy("caseId")
        .agg(
            first("organizationId", ignorenulls=True).alias("buyer_id"),
            spark_min("publication_date").alias("first_publicationDate"),
            spark_max("publication_date").alias("last_publicationDate"),
            count(lit(1)).alias("num_notices"),
            spark_sum(when(col("noticeType") == lit("NoticeUpdateNotice"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("num_updates"),
            spark_max(when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_init"),
            spark_max(when(col("noticeType") == lit("TenderResultNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_result"),
            spark_max(when(col("noticeType") == lit("ContractPerformingNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_execution"),
            spark_min("init_date").alias("first_init_date"),
            spark_min("result_date").alias("first_result_date"),
            spark_min("execution_completion_date").alias("first_execution_completion_date"),
            spark_sum(when(col("deadlineChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "deadline_changed_count"
            ),
            spark_sum(when(col("criteriaChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "criteria_changed_count"
            ),
            spark_sum(when(col("scopeChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "scope_changed_count"
            ),
            spark_max(when(col("executionDelayed"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_delayed_any"),
            spark_max(when(col("executionRiskFlag"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_risk_any"),
            spark_max("paidRatio").alias("paid_ratio_max"),
            percentile_approx(col("paidRatio"), 0.5, 1000).alias("paid_ratio_median"),
            percentile_approx(col("biddingWindowDays"), 0.5, 1000).alias("bidding_window_days_median"),
            percentile_approx(col("executionDurationDays"), 0.5, 1000).alias(
                "execution_duration_days_median"
            ),
        )
        .withColumn(
            "time_to_award_days",
            when(
                col("first_init_date").isNotNull() & col("first_result_date").isNotNull(),
                datediff(col("first_result_date"), col("first_init_date")),
            ),
        )
        .withColumn(
            "award_to_completion_days",
            when(
                col("first_result_date").isNotNull() & col("first_execution_completion_date").isNotNull(),
                datediff(col("first_execution_completion_date"), col("first_result_date")),
            ),
        )
        .drop("first_init_date", "first_result_date", "first_execution_completion_date")
    )


def _list_snapshot_dates(output_dir: Path) -> list[str]:
    if not output_dir.exists():
        return []
    out = []
    for p in output_dir.glob("asOfDate=*"):
        if p.is_dir():
            out.append(p.name.replace("asOfDate=", ""))
    return sorted(out)


def _latest_snapshot_before(output_dir: Path, target_date: str) -> str | None:
    dates = [d for d in _list_snapshot_dates(output_dir) if d < target_date]
    return dates[-1] if dates else None


def _earliest_snapshot_after(output_dir: Path, target_date: str) -> str | None:
    dates = [d for d in _list_snapshot_dates(output_dir) if d > target_date]
    return dates[0] if dates else None


def _parse_iso_day(day: str) -> date:
    return datetime.strptime(day, "%Y-%m-%d").date()


def _write_snapshot(df: DataFrame, output_dir: Path, target_date: str) -> int:
    out_path = output_dir / f"asOfDate={target_date}"
    if out_path.exists():
        shutil.rmtree(out_path)
    df.write.mode("overwrite").parquet(str(out_path))
    return df.count()


def _touched_case_ids(spark: "SparkSession", silver_dir: Path, target_date: str) -> DataFrame:
    daily_envelope = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
    if not daily_envelope.exists():
        raise ValueError(f"Missing daily envelope partition: {daily_envelope}")
    daily_df = spark.read.parquet(str(daily_envelope))
    return daily_df.select(safe_col(daily_df, "caseId", "string").alias("caseId")).filter(
        col("caseId").isNotNull()
    ).distinct()


def _touched_case_ids_in_range(
    spark: "SparkSession",
    silver_dir: Path,
    start_exclusive: str,
    end_inclusive: str,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    if not envelope_root.exists():
        raise ValueError(f"Missing envelope root: {envelope_root}")

    start_day = _parse_iso_day(start_exclusive)
    end_day = _parse_iso_day(end_inclusive)
    if start_day >= end_day:
        return spark.createDataFrame([], "caseId string")

    paths: list[str] = []
    for p in sorted(envelope_root.glob("publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        day = _parse_iso_day(token)
        if start_day < day <= end_day:
            paths.append(str(p))

    if not paths:
        return spark.createDataFrame([], "caseId string")

    daily_df = _union_paths(spark, paths, base_path=str(envelope_root))
    return (
        daily_df.select(safe_col(daily_df, "caseId", "string").alias("caseId"))
        .filter(col("caseId").isNotNull())
        .distinct()
    )


def main() -> None:
    args = _parse_args()
    if args.target_date:
        target_date = args.target_date
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_dir = Path(args.silver_dir)
    output_dir = Path(args.output_dir)

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bzp-silver-case-derived")
        .master(args.spark_master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        if args.mode == "full":
            notices = _read_notices_merged(spark, silver_dir, target_date)
            case_df = _build_case_derived(notices).withColumn("asOfDate", lit(target_date))
            rows = _write_snapshot(case_df, output_dir, target_date)
            log.info("Built full case_derived_facts snapshot asOfDate=%s rows=%d", target_date, rows)
            return

        prev_date = _latest_snapshot_before(output_dir, target_date)
        next_date = _earliest_snapshot_after(output_dir, target_date)

        if prev_date is None and next_date is None:
            log.warning("No neighboring snapshot found around %s, falling back to full mode", target_date)
            notices = _read_notices_merged(spark, silver_dir, target_date)
            case_df = _build_case_derived(notices).withColumn("asOfDate", lit(target_date))
            rows = _write_snapshot(case_df, output_dir, target_date)
            log.info("Built full (fallback) case_derived_facts snapshot asOfDate=%s rows=%d", target_date, rows)
            return

        chosen_direction = "forward"
        anchor_date = prev_date
        if prev_date is None:
            chosen_direction = "backward"
            anchor_date = next_date
        elif next_date is not None:
            # Choose nearer anchor to minimize recomputation window.
            target_day = _parse_iso_day(target_date)
            prev_gap = (target_day - _parse_iso_day(prev_date)).days
            next_gap = (_parse_iso_day(next_date) - target_day).days
            if next_gap < prev_gap:
                chosen_direction = "backward"
                anchor_date = next_date

        assert anchor_date is not None
        anchor_df = spark.read.parquet(str(output_dir / f"asOfDate={anchor_date}"))

        if chosen_direction == "forward":
            affected = _touched_case_ids_in_range(spark, silver_dir, anchor_date, target_date)
        else:
            affected = _touched_case_ids_in_range(spark, silver_dir, target_date, anchor_date)
        affected_count = affected.count()

        if affected_count == 0:
            log.info(
                "No affected cases between %s and %s; cloning snapshot",
                anchor_date,
                target_date,
            )
            out = anchor_df.drop("asOfDate").withColumn("asOfDate", lit(target_date))
            rows = _write_snapshot(out, output_dir, target_date)
            log.info(
                "Built incremental case_derived_facts asOfDate=%s rows=%d direction=%s affected_cases=0",
                target_date,
                rows,
                chosen_direction,
            )
            return

        notices_affected = _read_notices_merged(spark, silver_dir, target_date, case_ids=affected)
        recomputed = _build_case_derived(notices_affected)
        unchanged = anchor_df.join(affected, on="caseId", how="left_anti").drop("asOfDate")
        out = unchanged.unionByName(recomputed, allowMissingColumns=True).withColumn(
            "asOfDate", lit(target_date)
        )
        rows = _write_snapshot(out, output_dir, target_date)
        log.info(
            "Built incremental case_derived_facts asOfDate=%s rows=%d direction=%s anchor=%s affected_cases=%d",
            target_date,
            rows,
            chosen_direction,
            anchor_date,
            affected_count,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
