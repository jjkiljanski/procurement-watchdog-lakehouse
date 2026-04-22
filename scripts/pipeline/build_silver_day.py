"""Build Silver for a single day (wrapper over core Silver build).

Path resolution
---------------
``bronze-dir`` and ``silver-dir`` default to the runtime-resolved paths:

- **local** (``RUNTIME_ENV=local``): ``{LOCAL_DATA_ROOT}/bronze/`` and
  ``{LOCAL_DATA_ROOT}/silver/``
- **GCP**   (``RUNTIME_ENV=gcp``):  ``gs://{LAKEHOUSE_BUCKET}/bronze/`` and
  ``gs://{LAKEHOUSE_BUCKET}/silver/``

Pass ``--bronze-dir`` / ``--silver-dir`` explicitly to override.
"""

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
from procurement.runtime import get_runtime
from procurement.silver.pipeline_orchestrator import CoreRunConfig, run_silver_day_core

setup_logging()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help=(
            "Bronze root directory.  Defaults to the runtime-resolved 'bronze' path."
        ),
    )
    parser.add_argument(
        "--input-layer",
        choices=["auto", "bronze", "raw"],
        default="auto",
        help="Input source selection for silver build",
    )
    parser.add_argument(
        "--raw-dir",
        default=None,
        help="Directory with raw daily JSON files.  Defaults to the runtime-resolved 'bronze_raw' path.",
    )
    parser.add_argument(
        "--silver-dir",
        default=None,
        help="Output directory for silver parquet files.  Defaults to the runtime-resolved 'silver' path.",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*], local[2]).  Defaults to SPARK_MASTER env var.",
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
    parser.add_argument(
        "--max-batch-workers",
        type=int,
        default=0,
        help="Max concurrent notice-type batches (0 = default tuned cap)",
    )
    parser.add_argument(
        "--max-section-write-workers",
        type=int,
        default=0,
        help="Max concurrent section-model writes inside one batch (0 = default tuned cap)",
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

    rt = get_runtime()
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    silver_dir = args.silver_dir or rt.storage.resolve("silver")
    raw_dir = args.raw_dir or rt.storage.resolve("bronze_raw")

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    spark = rt.spark.get_session("bzp-silver-day", **extra)
    try:
        cfg = CoreRunConfig(
            target_date=target_date,
            bronze_dir=bronze_dir,
            silver_dir=silver_dir,
            raw_dir=raw_dir,
            input_layer=args.input_layer,
            shuffle_partitions=args.shuffle_partitions,
            repartition=args.repartition,
            max_batch_workers=args.max_batch_workers,
            max_section_write_workers=args.max_section_write_workers,
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
            obs_dir=rt.storage.obs_path(),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
