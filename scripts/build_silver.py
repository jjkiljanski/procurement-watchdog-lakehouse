"""Build silver layer from raw BZP JSON using PySpark.

Reads:
  <raw-dir>/bzp_YYYY-MM-DD.json
Writes:
  <silver-dir>/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/
"""

import argparse
import logging
import os
import sys
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

    from procurement.silver.spark_transforms import build_silver

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
        from pyspark.sql.functions import col, to_date

        df_raw = spark.read.json(str(raw_path), multiLine=True)
        log.info("Loaded %d raw records", df_raw.count())
        current_parts = df_raw.rdd.getNumPartitions()
        auto_target = max(1, spark.sparkContext.defaultParallelism * 2)
        target_parts = args.repartition if args.repartition > 0 else auto_target
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

        df_silver = build_silver(df_raw)
        df_silver = df_silver.withColumn(
            "publicationDateDay",
            to_date(col("publicationDate")).cast("string"),
        )

        # Overwrite only touched (noticeType, publicationDateDay) partitions.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        out_path = str(Path(args.silver_dir))
        (
            df_silver.repartition("noticeType", "publicationDateDay")
            .write.mode("overwrite")
            .partitionBy("noticeType", "publicationDateDay")
            .parquet(out_path)
        )

        log.info(
            "Wrote %d silver records to %s (partitioned by noticeType/publicationDateDay)",
            df_silver.count(),
            out_path,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
