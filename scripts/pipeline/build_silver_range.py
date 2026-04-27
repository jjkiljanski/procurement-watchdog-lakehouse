"""Build Silver for a date range (efficient backfill).

Unlike ``build_silver_backfill.py`` which loops over dates (one Spark plan per
notice-type per date, i.e. ≈6 s × 14 × N_days overhead), this script loops
over **notice types** — for each notice type it reads *all* date partitions in
the range in a single Spark plan and writes them all at once.

  14 iterations × 6 s DAG prep = 84 s total overhead
  vs 365 × 14 × 6 s ≈ 8.5 hours with the date-loop approach.

For the daily pipeline (single day) use ``build_silver_day.py`` instead.

Per-(date, notice_type) manifests are written after each notice-type batch so
interrupted runs can resume efficiently.  Use ``--force`` to reprocess
everything regardless of manifest state.

Usage
-----
    python build_silver_range.py --start-date 2025-01-01 --end-date 2025-12-31
    python build_silver_range.py --start-date 2025-01-01 --end-date 2025-12-31 --force
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
_SRC_PKG = Path(_src) / "procurement"
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import get_stage_logger, setup_logging
from procurement.obs import sha256_paths
from procurement.runtime import get_runtime
from procurement.silver.pipeline_orchestrator import run_silver_range_core

setup_logging()
log = get_stage_logger(__name__, "silver")


def _date_range(start: str, end: str) -> list[str]:
    """Return sorted list of ISO date strings from *start* to *end* inclusive."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    if e < s:
        raise ValueError(f"end-date {end} is before start-date {start}")
    result: list[str] = []
    d = s
    while d <= e:
        result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Silver for a date range using one Spark plan per notice type."
    )
    parser.add_argument("--start-date", required=True, help="First date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="Last date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help="Bronze root directory.  Defaults to the runtime-resolved 'bronze' path.",
    )
    parser.add_argument(
        "--silver-dir",
        default=None,
        help="Silver root directory.  Defaults to the runtime-resolved 'silver' path.",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*]).  Defaults to SPARK_MASTER env var.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = adaptive).",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Force repartition count per notice-type batch (0 = adaptive).",
    )
    parser.add_argument(
        "--max-section-write-workers",
        type=int,
        default=0,
        help="Max concurrent section-model writes inside one batch (0 = default tuned cap).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all (date, notice_type) pairs even when manifests match.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Validate date range early.
    _date_range(args.start_date, args.end_date)

    rt = get_runtime()
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    silver_dir = args.silver_dir or rt.storage.resolve("silver")
    obs_dir = rt.storage.obs_path()
    script_hash = sha256_paths(Path(__file__), _SRC_PKG / "silver")

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    log.info(
        "Silver range: %s..%s (force=%s)",
        args.start_date, args.end_date, args.force,
    )

    spark = rt.spark.get_session("bzp-silver-range", **extra)
    try:
        result = run_silver_range_core(
            spark=spark,
            start_date=args.start_date,
            end_date=args.end_date,
            bronze_dir=bronze_dir,
            repartition=args.repartition,
            shuffle_partitions=args.shuffle_partitions,
            max_section_write_workers=args.max_section_write_workers,
            obs_dir=obs_dir,
            script_paths=[Path(__file__).resolve()],
            storage=rt.storage,
            script_hash=script_hash,
            force=args.force,
        )
        log.info(
            "Silver range complete: %s..%s rows=%d batches=%d",
            args.start_date, args.end_date,
            result["rows"], len(result["batches"]),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
