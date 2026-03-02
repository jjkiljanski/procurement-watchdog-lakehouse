"""Build Silver for a single day (wrapper over core Silver build)."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging
from procurement.silver.build_core import CoreRunConfig, build_spark_session, run_silver_day_core

setup_logging()


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
    parser.add_argument("--silver-dir", default="data/silver", help="Output directory for silver parquet files")
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[2])",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = adaptive per batch)",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Force repartition count per batch (0 = adaptive)",
    )
    parser.add_argument("--profile-json", default="", help="Optional path to write profile JSON")
    parser.add_argument(
        "--lock-stale-minutes",
        type=int,
        default=360,
        help="Treat an existing day lock as stale after this many minutes",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    target_date = args.target_date or (date.today() - timedelta(days=1)).isoformat()

    spark = build_spark_session(master=args.spark_master, app_name="bzp-silver-day")
    try:
        cfg = CoreRunConfig(
            target_date=target_date,
            bronze_dir=args.bronze_dir,
            silver_dir=args.silver_dir,
            raw_dir=args.raw_dir,
            input_layer=args.input_layer,
            shuffle_partitions=args.shuffle_partitions,
            repartition=args.repartition,
            lock_stale_minutes=args.lock_stale_minutes,
            mode="day",
            profile_json=args.profile_json,
        )
        run_silver_day_core(
            spark=spark,
            cfg=cfg,
            command=sys.argv,
            args_dict=vars(args),
            script_paths=[Path(__file__).resolve()],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
