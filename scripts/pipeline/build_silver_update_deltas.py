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
  python scripts/pipeline/build_silver_update_deltas.py 2025-04-25
  python scripts/pipeline/build_silver_update_deltas.py 2025-04-25 --silver-dir data/silver
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
    build_update_deltas,
    write_deltas,
)

setup_logging()
log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build notice-change delta records from NoticeUpdateNotice silver data."
    )
    parser.add_argument("target_date", help="Target date in YYYY-MM-DD format")
    parser.add_argument(
        "--silver-dir",
        default="data/silver",
        help="Silver layer root directory (default: data/silver)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    silver_dir = Path(args.silver_dir)
    target_date = args.target_date

    log.info("build_silver_update_deltas started: date=%s silver_dir=%s", target_date, silver_dir)
    t0 = time.perf_counter()

    all_profiles = load_all_profiles()
    section_index = _build_section_index(all_profiles)

    deltas_by_type = build_update_deltas(target_date=target_date, silver_dir=silver_dir)

    if deltas_by_type:
        write_deltas(
            target_date=target_date,
            deltas_by_type=deltas_by_type,
            silver_dir=silver_dir,
            section_index=section_index,
        )
    else:
        log.info("No delta rows produced for %s — nothing written", target_date)

    elapsed = time.perf_counter() - t0
    log.info("build_silver_update_deltas finished: date=%s elapsed=%.1fs", target_date, elapsed)


if __name__ == "__main__":
    main()
