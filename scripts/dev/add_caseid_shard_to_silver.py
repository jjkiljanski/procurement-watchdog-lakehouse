from __future__ import annotations

import argparse
import logging
import shutil
import sys
import uuid
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, when


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="One-off rewrite: add caseId_shard column to Silver partitions."
    )
    p.add_argument(
        "--silver-dir",
        default="data/silver",
        help="Silver root containing common_envelope and notice_type_tables",
    )
    p.add_argument(
        "--shard-count",
        type=int,
        default=64,
        help="Shard count for pmod(xxhash64(caseId), N)",
    )
    p.add_argument(
        "--spark-master",
        default="local[*]",
        help="Spark master (e.g. local[*])",
    )
    p.add_argument(
        "--start-date",
        default="",
        help="Optional lower bound publicationDateDay (YYYY-MM-DD)",
    )
    p.add_argument(
        "--end-date",
        default="",
        help="Optional upper bound publicationDateDay (YYYY-MM-DD)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Rewrite even if caseId_shard already exists",
    )
    return p.parse_args()


def _in_range(day_token: str, start_date: str, end_date: str) -> bool:
    if start_date and day_token < start_date:
        return False
    if end_date and day_token > end_date:
        return False
    return True


def collect_partition_dirs(silver_dir: Path, start_date: str, end_date: str) -> list[Path]:
    parts: list[Path] = []
    for p in sorted((silver_dir / "common_envelope").glob("publicationDateDay=*")):
        day = p.name.replace("publicationDateDay=", "")
        if _in_range(day, start_date, end_date):
            parts.append(p)
    for p in sorted((silver_dir / "notice_type_tables").glob("noticeType=*/publicationDateDay=*")):
        day = p.name.replace("publicationDateDay=", "")
        if _in_range(day, start_date, end_date):
            parts.append(p)
    return [p for p in parts if p.is_dir()]


def add_shard_column(df, shard_count: int):
    return df.withColumn(
        "caseId_shard",
        when(
            col("caseId").isNull(),
            lit(None).cast("int"),
        ).otherwise(expr(f"pmod(xxhash64(caseId), {int(shard_count)})").cast("int")),
    )


def rewrite_partition(spark: SparkSession, part_dir: Path, shard_count: int, force: bool) -> str:
    df = spark.read.parquet(str(part_dir))
    if "caseId" not in df.columns:
        return "skip_no_caseId"
    if "caseId_shard" in df.columns and not force:
        return "skip_already_has_caseId_shard"

    df2 = add_shard_column(df, shard_count)
    parent = part_dir.parent
    run_token = uuid.uuid4().hex[:8]
    tmp_dir = parent / f"__tmp_{part_dir.name}_{run_token}"
    bak_dir = parent / f"__bak_{part_dir.name}_{run_token}"

    df2.write.mode("overwrite").parquet(str(tmp_dir))

    shutil.move(str(part_dir), str(bak_dir))
    shutil.move(str(tmp_dir), str(part_dir))
    shutil.rmtree(bak_dir, ignore_errors=True)
    return "rewritten"


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    silver_dir = Path(args.silver_dir)
    if not silver_dir.exists():
        raise FileNotFoundError(f"silver-dir not found: {silver_dir}")
    if args.shard_count <= 0:
        raise ValueError("--shard-count must be > 0")

    parts = collect_partition_dirs(
        silver_dir=silver_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    logging.info("Found %d partition dirs to scan", len(parts))

    spark = (
        SparkSession.builder.appName("silver-add-caseid-shard")
        .master(args.spark_master)
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )
    try:
        stats = {
            "rewritten": 0,
            "skip_no_caseId": 0,
            "skip_already_has_caseId_shard": 0,
            "failed": 0,
        }
        for i, part in enumerate(parts, start=1):
            try:
                result = rewrite_partition(
                    spark=spark,
                    part_dir=part,
                    shard_count=args.shard_count,
                    force=args.force,
                )
                stats[result] = stats.get(result, 0) + 1
                logging.info("[%d/%d] %s -> %s", i, len(parts), part, result)
            except Exception as exc:
                stats["failed"] += 1
                logging.exception("[%d/%d] %s -> failed: %s", i, len(parts), part, exc)

        logging.info("Done. Stats: %s", stats)
        if stats["failed"] > 0:
            sys.exit(2)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
