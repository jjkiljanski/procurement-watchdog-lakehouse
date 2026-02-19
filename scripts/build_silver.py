"""Build silver layer from raw BZP JSON using PySpark.

Reads:
  <raw-dir>/bzp_YYYY-MM-DD.json
Writes:
  <silver-dir>/common_envelope/publicationDateDay=YYYY-MM-DD/
  <silver-dir>/notice_type_tables/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/
"""

import argparse
import logging
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
# Also propagate to Spark worker processes via PYTHONPATH
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)

from procurement.silver.notice_types import (
    html_extracted_fields_for_notice_type,
    normalized_notice_type_token,
    specific_columns_for_notice_type,
)


ENVELOPE_COLUMNS = [
    "objectId",
    "noticeType",
    "noticeNumber",
    "bzpNumber",
    "publicationDate",
    "publicationDateDay",
    "isTenderAmountBelowEU",
    "orderObject",
    "clientType",
    "clientTypeName",
    "orderType",
    "tenderType",
    "submittingOffersDate",
    "organizationName",
    "organizationCity",
    "organizationProvince",
    "provinceName",
    "organizationCountry",
    "organizationNationalId",
    "organizationId",
    "tenderId",
    "caseId",
    "noticeStage",
    "hasTenderResult",
    "hasContractExecution",
    "organizationNameNormalized",
    "ulica",
    "kod_pocztowy",
]

def _select_existing(df: "DataFrame", columns: list[str]) -> "DataFrame":
    return df.select(*[c for c in columns if c in df.columns])


def _compact_html_extracted(
    df: "DataFrame",
    html_fields: list[str],
) -> "DataFrame":
    if "htmlExtracted" not in df.columns:
        return df
    if not html_fields:
        return df.drop("htmlExtracted")
    from pyspark.sql.functions import col, struct

    compact_cols = [col(f"htmlExtracted.{field_name}").alias(field_name) for field_name in html_fields]
    return df.withColumn("htmlExtracted", struct(*compact_cols))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument("--raw-dir", default="data/raw", help="Directory with raw daily JSON files")
    parser.add_argument(
        "--silver-dir",
        default="data/silver",
        help="Output directory for silver parquet files",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[2])",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = Spark default)",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Repartition raw DataFrame before heavy HTML parsing (0 = auto by defaultParallelism*2)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    if args.target_date:
        target_date = args.target_date
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    raw_path = Path(args.raw_dir) / f"bzp_{target_date}.json"
    if not raw_path.exists():
        log.error("Raw file not found: %s", raw_path)
        sys.exit(1)

    from pyspark.sql import SparkSession
    from pyspark.storagelevel import StorageLevel

    from procurement.silver.spark_transforms import build_silver_for_notice_type

    spark = (
        SparkSession.builder.appName("bzp-silver")
        .master(args.spark_master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    if args.shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        log.info("Set spark.sql.shuffle.partitions=%d", args.shuffle_partitions)

    try:
        from functools import reduce
        from pyspark.sql.functions import col, lit, to_date

        df_raw = spark.read.json(str(raw_path), multiLine=True)
        raw_count = df_raw.count()
        log.info("Loaded %d raw records", raw_count)
        current_parts = df_raw.rdd.getNumPartitions()
        max_parallel = max(2, spark.sparkContext.defaultParallelism * 2)
        size_based_target = max(2, raw_count // 2000)
        auto_target = min(max_parallel, size_based_target)
        target_parts = args.repartition if args.repartition > 0 else auto_target
        if args.shuffle_partitions <= 0:
            spark.conf.set("spark.sql.shuffle.partitions", str(target_parts))
            log.info("Auto-set spark.sql.shuffle.partitions=%d", target_parts)
        if target_parts > current_parts:
            df_raw = df_raw.repartition(target_parts)
            log.info(
                "Repartitioned raw DataFrame %d -> %d partitions for HTML parsing parallelism",
                current_parts,
                target_parts,
            )
        else:
            log.info(
                "Kept raw DataFrame partitions=%d (target=%d)",
                current_parts,
                target_parts,
            )
        df_raw = df_raw.persist(StorageLevel.MEMORY_AND_DISK)
        log.info("Persisted raw DataFrame for noticeType batch reuse")

        # Step 1: process Bronze in noticeType-sorted batches.
        notice_types = [
            row.noticeType for row in df_raw.select("noticeType").distinct().collect()
        ]
        notice_types_sorted = sorted(
            notice_types,
            key=lambda x: (x is None, "" if x is None else str(x)),
        )
        log.info("Processing noticeType batches in order: %s", notice_types_sorted)

        silver_dir = Path(args.silver_dir)
        envelope_root = str(silver_dir / "common_envelope")
        specific_root = silver_dir / "notice_type_tables"
        envelope_batches = []

        # Overwrite only touched publicationDateDay partitions.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        total_input_rows = raw_count
        persisted_batches = []
        for notice_type in notice_types_sorted:
            if notice_type is None:
                batch_raw = df_raw.filter(col("noticeType").isNull())
            else:
                batch_raw = df_raw.filter(col("noticeType") == lit(notice_type))
            notice_type_token = normalized_notice_type_token(notice_type)
            specific_columns = specific_columns_for_notice_type(notice_type)
            html_fields = html_extracted_fields_for_notice_type(notice_type)
            required_columns = set(ENVELOPE_COLUMNS) | set(specific_columns)
            batch_start = time.perf_counter()
            log.info("Processing batch noticeType=%s", notice_type_token)
            batch_silver = build_silver_for_notice_type(
                batch_raw,
                notice_type=notice_type,
                required_columns=required_columns,
            ).withColumn(
                "publicationDateDay",
                to_date(col("publicationDate")).cast("string"),
            )
            batch_silver = batch_silver.persist(StorageLevel.MEMORY_AND_DISK)
            persisted_batches.append(batch_silver)
            envelope_batches.append(_select_existing(batch_silver, ENVELOPE_COLUMNS))

            specific_df = _select_existing(batch_silver, specific_columns)
            specific_df = _compact_html_extracted(specific_df, html_fields)
            specific_out = str(specific_root / f"noticeType={notice_type_token}")
            (
                specific_df.write.mode("overwrite")
                .partitionBy("publicationDateDay")
                .parquet(specific_out)
            )
            log.info(
                "Wrote specific table noticeType=%s to %s (%.2fs)",
                notice_type_token,
                specific_out,
                time.perf_counter() - batch_start,
            )

        if not envelope_batches:
            log.warning("No Silver rows produced for %s", target_date)
            return

        envelope_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), envelope_batches)
        envelope_write_start = time.perf_counter()
        (
            envelope_df.coalesce(target_parts).write.mode("overwrite")
            .partitionBy("publicationDateDay")
            .parquet(envelope_root)
        )
        log.info(
            "Wrote common envelope to %s (%.2fs)",
            envelope_root,
            time.perf_counter() - envelope_write_start,
        )
        log.info("Completed Silver build total_input_rows=%d", total_input_rows)
        for cached_df in persisted_batches:
            cached_df.unpersist()
        df_raw.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
