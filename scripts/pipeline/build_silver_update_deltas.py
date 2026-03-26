"""Build notice-change delta records from NoticeUpdateNotice silver data.

For each NoticeUpdateNotice on the target day, resolves the type of the
original changed notice and produces one delta row whose schema matches the
original notice type's core silver table.  Changed sections contain parsed
values; unchanged sections are NULL.  A ``section_changes`` column preserves
the complete raw change list (section_prefix, label, before, after) verbatim.
Parse failures are recorded in a ``parse_errors`` JSON column rather than
typed column values.

Reads:
  <silver-dir>/notice_type_tables/noticeType=NoticeUpdateNotice/data_model=core/...
  <silver-dir>/notice_type_tables/noticeType=NoticeUpdateNotice/data_model=part/...
  <silver-dir>/notice_type_tables/noticeType=NoticeUpdateNotice/data_model=part_part/...
  <silver-dir>/common_envelope/  (year-scoped)

Writes:
  <silver-dir>/notice_update_deltas/noticeType=<OriginalType>/publicationDateDay=<D>/

Usage:
  # Single day
  python scripts/pipeline/build_silver_update_deltas.py 2025-04-25
  python scripts/pipeline/build_silver_update_deltas.py 2025-04-25 --silver-dir data/silver

  # Full year backfill (builds BZP index once, reuses across all days)
  python scripts/pipeline/build_silver_update_deltas.py --all --silver-dir data/silver
  python scripts/pipeline/build_silver_update_deltas.py --all --year 2025 --silver-dir data/silver
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)

from procurement.logging import setup_logging
from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
from procurement.silver.update_deltas.delta_builder import (
    _build_section_index,
    _load_bzp_index,
    build_update_deltas,
    write_deltas,
)

setup_logging()
log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build notice-change delta records from NoticeUpdateNotice silver data."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("target_date", nargs="?", help="Target date in YYYY-MM-DD format")
    group.add_argument("--all", action="store_true", help="Process all available NUN days")
    parser.add_argument(
        "--year",
        help="Filter to a specific year when using --all (e.g. 2025)",
    )
    parser.add_argument(
        "--silver-dir",
        default="data/silver",
        help="Silver layer root directory (default: data/silver)",
    )
    return parser.parse_args()


def _discover_nun_days(silver_dir: Path, year: str | None) -> list[str]:
    """Return sorted list of dates that have NUN core data."""
    core_dir = silver_dir / "notice_type_tables" / "noticeType=NoticeUpdateNotice" / "data_model=core"
    if not core_dir.exists():
        return []
    days = sorted(
        p.name.replace("publicationDateDay=", "")
        for p in core_dir.iterdir()
        if p.is_dir() and (year is None or p.name.endswith(year) or p.name.startswith(f"publicationDateDay={year}"))
    )
    return days


def _run_day(
    target_date: str,
    silver_dir: Path,
    section_index: dict,
    bzp_index: dict,
) -> None:
    deltas_by_type = build_update_deltas(
        target_date=target_date,
        silver_dir=silver_dir,
        bzp_index=bzp_index,
    )
    if deltas_by_type:
        write_deltas(
            target_date=target_date,
            deltas_by_type=deltas_by_type,
            silver_dir=silver_dir,
            section_index=section_index,
        )
    else:
        log.info("No delta rows produced for %s — nothing written", target_date)


def main() -> None:
    args = _parse_args()
    silver_dir = Path(args.silver_dir)

    log.info("build_silver_update_deltas started: silver_dir=%s", silver_dir)
    t0 = time.perf_counter()

    all_profiles = load_all_profiles()
    section_index = _build_section_index(all_profiles)

    if args.all:
        year = args.year
        days = _discover_nun_days(silver_dir, year)
        if not days:
            log.info("No NUN days found under %s (year filter: %s)", silver_dir, year)
            return
        log.info("Processing %d NUN days (year filter: %s)", len(days), year or "none")

        # Build BZP index once for all years present in the data
        years = {d[:4] for d in days}
        log.info("Building BZP index for years: %s", sorted(years))
        bzp_index = _load_bzp_index(silver_dir, years)

        for i, day in enumerate(days, 1):
            t_day = time.perf_counter()
            _run_day(day, silver_dir, section_index, bzp_index)
            log.info("Day %d/%d (%s) done in %.1fs", i, len(days), day, time.perf_counter() - t_day)

    else:
        target_date = args.target_date
        log.info("Processing single day: %s", target_date)
        bzp_index = None  # will be built inside build_update_deltas
        _run_day(target_date, silver_dir, section_index, bzp_index)

    elapsed = time.perf_counter() - t0
    log.info("build_silver_update_deltas finished: elapsed=%.1fs", elapsed)


if __name__ == "__main__":
    main()
