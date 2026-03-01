"""Batch-fetch BZP data for a date range and build Bronze Parquet.

Usage (run from the directory where you want data/ created):
    cd E:\\git_projects\\procurement-watchdog-api-exploration
    python E:\\git_projects\\procurement-watchdog-lakehouse\\scripts\\ops\\batch_fetch.py 2025-10-01 2025-12-31

Skips dates whose Bronze-Raw file already exists, so it is safe to re-run.
"""

from __future__ import annotations

import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPTS_DIR.parent / "pipeline"

sys.path.insert(0, str(SCRIPTS_DIR.parent / "src"))
from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def daterange(start: date, end: date):
    """Yield each date from start to end (inclusive)."""
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} START_DATE END_DATE")
        print(f"  e.g. python {sys.argv[0]} 2025-10-01 2025-12-31")
        sys.exit(1)

    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2])
    total_days = (end - start).days + 1

    log.info("Batch fetch: %s to %s (%d days)", start, end, total_days)

    fetch_script = str(PIPELINE_DIR / "fetch_bzp_yesterday.py")
    bronze_script = str(PIPELINE_DIR / "build_bronze.py")

    for i, d in enumerate(daterange(start, end), 1):
        ds = d.isoformat()
        raw_path = Path("data/bronze_raw") / f"bzp_{ds}.json"

        if raw_path.exists():
            log.info("[%d/%d] %s - bronze_raw file exists, skipping fetch", i, total_days, ds)
        else:
            log.info("[%d/%d] %s - fetching...", i, total_days, ds)
            rc = subprocess.run([sys.executable, fetch_script, ds], capture_output=False).returncode
            if rc != 0:
                log.error("[%d/%d] %s - fetch FAILED (exit %d), skipping bronze", i, total_days, ds, rc)
                continue

        bronze_daily_partitions = list(
            Path("data/bronze/notices").glob(f"noticeType=*/publicationDateDay={ds}")
        )
        if bronze_daily_partitions:
            log.info(
                "[%d/%d] %s - bronze partitions exist (%d), skipping",
                i,
                total_days,
                ds,
                len(bronze_daily_partitions),
            )
        else:
            log.info("[%d/%d] %s - building bronze...", i, total_days, ds)
            rc = subprocess.run([sys.executable, bronze_script, ds], capture_output=False).returncode
            if rc != 0:
                log.error("[%d/%d] %s - bronze FAILED (exit %d)", i, total_days, ds, rc)

    log.info("Batch complete")


if __name__ == "__main__":
    main()
