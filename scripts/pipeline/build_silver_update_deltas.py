"""Build notice-change delta records from NoticeUpdateNotice silver data.

For each NoticeUpdateNotice on the target day, resolves the type of the
original changed notice and produces one delta row whose schema matches the
original notice type's core silver table.  Changed sections contain parsed
values; unchanged sections are NULL.  A ``section_changes`` column preserves
the complete raw change list (section_prefix, label, before, after) verbatim.
Parse failures are recorded in a ``parse_errors`` JSON column rather than
typed column values.

Reads from Apache Iceberg (catalog ``silver``):
  silver.notice_type_tables.notice_update_notice__core
  silver.notice_type_tables.notice_update_notice__part
  silver.notice_type_tables.notice_update_notice__part_part
  silver.common.common_envelope  (year-scoped)

Writes to Apache Iceberg:
  silver.notice_update_deltas.{target_notice_type_snake_case}
  Partitioned by publicationDateDay (= NUN publication date).

Usage:
  # Single day
  python scripts/pipeline/build_silver_update_deltas.py 2025-04-25

  # Bounded date range backfill (builds BZP index once, reuses across all days)
  python scripts/pipeline/build_silver_update_deltas.py --start-date 2025-01-01 --end-date 2025-12-31

  # Full year backfill (builds BZP index once, reuses across all days)
  python scripts/pipeline/build_silver_update_deltas.py --all
  python scripts/pipeline/build_silver_update_deltas.py --all --year 2025
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
_SRC_PKG = Path(_src) / "procurement"
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import get_stage_logger, setup_logging
from procurement.manifests import write_processed_manifest
from procurement.obs import sha256_paths
from procurement.runtime import get_runtime
from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
from procurement.silver.update_deltas.delta_builder import (
    _build_section_index,
    build_update_deltas,
    load_bzp_index,
    load_nun_rows,
    write_deltas,
)

setup_logging()
log = get_stage_logger(__name__, "deltas")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build notice-change delta records from NoticeUpdateNotice silver data."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("target_date", nargs="?", help="Target date in YYYY-MM-DD format")
    group.add_argument("--all", action="store_true", help="Process all available NUN days")
    group.add_argument(
        "--start-date",
        metavar="YYYY-MM-DD",
        help="First date of a bounded backfill range (use with --end-date).",
    )
    parser.add_argument(
        "--end-date",
        metavar="YYYY-MM-DD",
        help="Last date of a bounded backfill range (use with --start-date).",
    )
    parser.add_argument(
        "--year",
        help="Filter to a specific year when using --all (e.g. 2025)",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*]).  Defaults to SPARK_MASTER env var.",
    )
    return parser.parse_args()


def _discover_nun_days(
    spark,
    year: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[str]:
    """Return sorted list of dates that have NUN core data in Iceberg.

    Exactly one of *year* or (*start_date*, *end_date*) should be supplied for
    filtered queries.  All three being ``None`` returns all available days.
    """
    table = "silver.notice_type_tables.notice_update_notice__core"
    try:
        spark.sql(f"DESCRIBE TABLE {table}")
    except Exception:
        return []

    if start_date and end_date:
        df = spark.sql(
            f"SELECT DISTINCT publicationDateDay FROM {table} "
            f"WHERE publicationDateDay >= '{start_date}' "
            f"AND publicationDateDay <= '{end_date}'"
        )
    elif year:
        df = spark.sql(
            f"SELECT DISTINCT publicationDateDay FROM {table} "
            f"WHERE publicationDateDay LIKE '{year}%'"
        )
    else:
        df = spark.sql(f"SELECT DISTINCT publicationDateDay FROM {table}")
    return sorted(row.publicationDateDay for row in df.collect())


def _run_day(
    spark,
    target_date: str,
    section_index: dict,
    bzp_index: dict | None,
) -> None:
    core_rows, part_rows, part_part_rows = load_nun_rows(spark, target_date)
    if not core_rows:
        log.info(
            "No NUN data for %s — nothing to do", target_date,
            extra={"date": target_date, "status": "skipped"},
        )
        return

    actual_bzp_index = bzp_index
    if actual_bzp_index is None:
        target_bzps = {r.get("section_3_2") for r in core_rows if r.get("section_3_2")}
        years: set[str] = set()
        for bzp in target_bzps:
            m = re.match(r"^(\d{4})/", bzp or "")
            if m:
                years.add(m.group(1))
        if not years:
            log.warning("Could not extract any year from section_3_2 values — aborting")
            return
        actual_bzp_index = load_bzp_index(spark, years)

    deltas_by_type = build_update_deltas(
        target_date=target_date,
        core_rows=core_rows,
        part_rows=part_rows,
        part_part_rows=part_part_rows,
        section_index=section_index,
        bzp_index=actual_bzp_index,
    )
    if deltas_by_type:
        write_deltas(spark, target_date, deltas_by_type, section_index)
    else:
        log.info("No delta rows produced for %s — nothing written", target_date)


def main() -> None:
    args = _parse_args()

    rt = get_runtime()

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    spark = rt.spark.get_session("bzp-silver-deltas", **extra)
    try:
        log.info("build_silver_update_deltas started")
        t0 = time.perf_counter()

        all_profiles = load_all_profiles()
        section_index = _build_section_index(all_profiles)

        script_hash = sha256_paths(Path(__file__), _SRC_PKG / "silver")

        if args.all or (args.start_date and args.end_date):
            if args.start_date and args.end_date:
                days = _discover_nun_days(spark, start_date=args.start_date, end_date=args.end_date)
                log.info(
                    "Processing NUN days in range %s..%s (%d days found)",
                    args.start_date, args.end_date, len(days),
                )
            else:
                year = args.year
                days = _discover_nun_days(spark, year=year)
                log.info("Processing %d NUN days (year filter: %s)", len(days), year or "none")

            if not days:
                log.info("No NUN days found — nothing to do")
            else:
                years = {d[:4] for d in days}
                log.info("Building BZP index for years: %s", sorted(years))
                bzp_index = load_bzp_index(spark, years)

                for i, day in enumerate(days, 1):
                    t_day = time.perf_counter()
                    _run_day(spark, day, section_index, bzp_index)
                    write_processed_manifest(
                        layer="deltas",
                        target_date=day,
                        script_hash=script_hash,
                        storage=rt.storage,
                    )
                    log.info(
                        "Day %d/%d (%s) done in %.1fs",
                        i, len(days), day, time.perf_counter() - t_day,
                        extra={"date": day, "status": "ok"},
                    )

        else:
            target_date = args.target_date
            log.info(
                "build_silver_update_deltas started: date=%s", target_date,
                extra={"date": target_date, "status": "started"},
            )
            _run_day(spark, target_date, section_index, bzp_index=None)
            write_processed_manifest(
                layer="deltas",
                target_date=target_date,
                script_hash=script_hash,
                storage=rt.storage,
            )
            log.info(
                "build_silver_update_deltas done: date=%s", target_date,
                extra={"date": target_date, "status": "ok"},
            )

        elapsed = time.perf_counter() - t0
        log.info("build_silver_update_deltas finished: elapsed=%.1fs", elapsed)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
