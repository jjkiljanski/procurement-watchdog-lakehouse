"""Build silver layer from raw BZP JSON using PySpark.

Reads:
  Preferred: <bronze-dir>/notices/noticeType=*/publicationDateDay=YYYY-MM-DD/
  Fallback:  <raw-dir>/bzp_YYYY-MM-DD.json
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


def _auto_target_partitions(raw_count: int, default_parallelism: int) -> int:
    max_parallel = max(2, default_parallelism * 2)
    size_based_target = max(2, raw_count // 2000)
    return min(max_parallel, size_based_target)


def _maybe_repartition_batch(
    df: "DataFrame",
    row_count: int,
    args: argparse.Namespace,
    spark: "SparkSession",
    notice_type_token: str,
) -> "DataFrame":
    current_parts = df.rdd.getNumPartitions()
    target_parts = args.repartition if args.repartition > 0 else _auto_target_partitions(
        row_count, spark.sparkContext.defaultParallelism
    )
    # Keep tiny batches as-is to avoid shuffle overhead.
    if row_count < 2000:
        log.info(
            "Batch noticeType=%s kept partitions=%d (rows=%d; tiny batch)",
            notice_type_token,
            current_parts,
            row_count,
        )
        return df
    if target_parts > current_parts:
        log.info(
            "Batch noticeType=%s repartition %d -> %d (rows=%d)",
            notice_type_token,
            current_parts,
            target_parts,
            row_count,
        )
        return df.repartition(target_parts)
    log.info(
        "Batch noticeType=%s kept partitions=%d (target=%d, rows=%d)",
        notice_type_token,
        current_parts,
        target_parts,
        row_count,
    )
    return df


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Bronze root directory (expects notices/noticeType=*/publicationDateDay=YYYY-MM-DD)",
    )
    parser.add_argument(
        "--input-layer",
        choices=["auto", "bronze", "raw"],
        default="auto",
        help="Input source selection for silver build",
    )
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

    from pyspark.sql import SparkSession
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

        bronze_root = Path(args.bronze_dir) / "notices"
        bronze_paths = sorted(
            bronze_root.glob(f"noticeType=*/publicationDateDay={target_date}")
        )
        use_bronze = args.input_layer in ("auto", "bronze") and len(bronze_paths) > 0
        if args.input_layer == "bronze" and not bronze_paths:
            log.error("Bronze partitions for %s not found under %s", target_date, bronze_root)
            sys.exit(1)

        if not use_bronze:
            raw_path = Path(args.raw_dir) / f"bzp_{target_date}.json"
            if not raw_path.exists():
                log.error("Raw file not found: %s", raw_path)
                sys.exit(1)
            log.warning("Bronze input not available; falling back to raw JSON: %s", raw_path)
            df_raw = spark.read.json(str(raw_path), multiLine=True)

        # Step 1: process Bronze in noticeType-sorted batches.
        if use_bronze:
            # Process each physical Bronze partition path directly.
            notice_batches: list[tuple[str | None, str]] = []
            for p in bronze_paths:
                token = p.parent.name.replace("noticeType=", "")
                nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
                notice_batches.append((nt, str(p)))
            notice_batches.sort(key=lambda x: (x[0] is None, "" if x[0] is None else str(x[0])))
            log.info(
                "Processing Bronze partition batches in order: %s",
                [normalized_notice_type_token(nt) for nt, _ in notice_batches],
            )
        else:
            # Fallback mode: one raw JSON input, then type-filter.
            notice_types = [row.noticeType for row in df_raw.select("noticeType").distinct().collect()]
            notice_types_sorted = sorted(
                notice_types,
                key=lambda x: (x is None, "" if x is None else str(x)),
            )
            notice_batches = [(nt, None) for nt in notice_types_sorted]
            log.info("Processing raw noticeType batches in order: %s", notice_types_sorted)

        silver_dir = Path(args.silver_dir)
        envelope_root = str(silver_dir / "common_envelope")
        specific_root = silver_dir / "notice_type_tables"
        envelope_batches = []

        # Overwrite only touched publicationDateDay partitions.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        total_input_rows = 0
        for notice_type, batch_path in notice_batches:
            if use_bronze:
                assert batch_path is not None
                batch_raw = spark.read.option("basePath", str(bronze_root)).parquet(batch_path)
            else:
                if notice_type is None:
                    batch_raw = df_raw.filter(col("noticeType").isNull())
                else:
                    batch_raw = df_raw.filter(col("noticeType") == lit(notice_type))
            notice_type_token = normalized_notice_type_token(notice_type)
            batch_count = batch_raw.count()
            total_input_rows += batch_count
            if args.shuffle_partitions <= 0:
                per_batch_shuffle = args.repartition if args.repartition > 0 else _auto_target_partitions(
                    batch_count, spark.sparkContext.defaultParallelism
                )
                spark.conf.set("spark.sql.shuffle.partitions", str(per_batch_shuffle))
                log.info(
                    "Batch noticeType=%s set spark.sql.shuffle.partitions=%d",
                    notice_type_token,
                    per_batch_shuffle,
                )
            batch_raw = _maybe_repartition_batch(batch_raw, batch_count, args, spark, notice_type_token)
            specific_columns = specific_columns_for_notice_type(notice_type)
            html_fields = html_extracted_fields_for_notice_type(notice_type)
            required_columns = set(ENVELOPE_COLUMNS) | set(specific_columns)
            batch_start = time.perf_counter()
            log.info("Processing batch noticeType=%s rows=%d", notice_type_token, batch_count)
            batch_silver = build_silver_for_notice_type(
                batch_raw,
                notice_type=notice_type,
                required_columns=required_columns,
            ).withColumn(
                "publicationDateDay",
                to_date(col("publicationDate")).cast("string"),
            )
            envelope_batches.append(_select_existing(batch_silver, ENVELOPE_COLUMNS))

            specific_df = _select_existing(batch_silver, specific_columns)
            specific_df = _compact_html_extracted(specific_df, html_fields)
            specific_out = str(specific_root / f"noticeType={notice_type_token}")
            (
                specific_df.write.mode("overwrite")
                .partitionBy("publicationDateDay")
                .parquet(specific_out)
            )
            log.info("Wrote specific table noticeType=%s to %s (%.2fs)", notice_type_token, specific_out, time.perf_counter() - batch_start)

        if not envelope_batches:
            log.warning("No Silver rows produced for %s", target_date)
            return

        envelope_df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), envelope_batches)
        envelope_write_start = time.perf_counter()
        envelope_parts = args.repartition if args.repartition > 0 else _auto_target_partitions(
            total_input_rows, spark.sparkContext.defaultParallelism
        )
        (
            envelope_df.coalesce(envelope_parts).write.mode("overwrite")
            .partitionBy("publicationDateDay")
            .parquet(envelope_root)
        )
        log.info(
            "Wrote common envelope to %s (%.2fs)",
            envelope_root,
            time.perf_counter() - envelope_write_start,
        )
        log.info("Completed Silver build total_input_rows=%d", total_input_rows)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
