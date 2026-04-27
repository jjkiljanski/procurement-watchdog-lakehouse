"""Fetch BZP notices for a date range and dump to bronze_raw JSON.

Designed for backfill runs.  For each date in the range the script:

1. Checks whether a processed-date manifest already exists for ``layer="fetch"``
   with a matching script hash.  If so, the date is skipped.
2. Downloads all notice types from the BZP API for that date (with exponential
   backoff for transient errors).
3. Writes the raw JSON to ``bronze_raw/bzp_<date>.json``.
4. Writes a processed-date manifest so the date is skipped on the next run.

Output path resolution
----------------------
Same logic as ``fetch_bzp_yesterday.py``:

- **local**  (``RUNTIME_ENV=local``):  ``{LOCAL_DATA_ROOT}/bronze_raw/``
- **GCP**    (``RUNTIME_ENV=gcp``):    ``gs://{LAKEHOUSE_BUCKET}/bronze_raw/``

Override with ``--output-dir`` for one-off runs.

Date range inputs
-----------------
Priority order (first wins):

1. CLI positional args: ``start_date end_date``
2. Environment variables: ``START_DATE`` / ``END_DATE``

Usage
-----
::

    python scripts/pipeline/fetch_bzp_range.py 2025-01-01 2025-03-31
    python scripts/pipeline/fetch_bzp_range.py 2025-01-01 2025-03-31 --output-dir gs://my-bucket/bronze_raw

    # Via environment variables (Cloud Run / Docker)
    START_DATE=2025-01-01 END_DATE=2025-03-31 python scripts/pipeline/fetch_bzp_range.py
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
_SRC_PKG = Path(_src) / "procurement"
sys.path.insert(0, _src)

from procurement.fetch.bzp_api import (
    NOTICE_TYPES,
    fetch_notices_for_type,
    filter_and_dedup_daily,
    write_output,
)
from procurement.logging import get_stage_logger, setup_logging
from procurement.manifests import is_already_processed, write_processed_manifest
from procurement.obs import git_commit_sha, now_utc_iso, sha256_paths, write_pipeline_run
from procurement.runtime import get_runtime

setup_logging()
log = get_stage_logger(__name__, "fetch")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch BZP notices for a date range, skipping already-fetched dates."
    )
    parser.add_argument(
        "start_date",
        nargs="?",
        help="First date in range (YYYY-MM-DD). Falls back to START_DATE env var.",
    )
    parser.add_argument(
        "end_date",
        nargs="?",
        help="Last date in range (YYYY-MM-DD, inclusive). Falls back to END_DATE env var.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Output directory for raw JSON payloads.  Defaults to the "
            "runtime-resolved 'bronze_raw' path."
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Re-fetch even if a matching manifest already exists.",
    )
    return parser.parse_args()


def _resolve_dates(args: argparse.Namespace) -> tuple[date, date]:
    start_str = args.start_date or os.environ.get("START_DATE")
    end_str = args.end_date or os.environ.get("END_DATE")
    if not start_str or not end_str:
        raise ValueError(
            "start_date and end_date are required — pass as CLI args or START_DATE/END_DATE env vars"
        )
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    if end < start:
        raise ValueError(f"end_date {end} is before start_date {start}")
    return start, end


def _fetch_one_day(
    target_date: date,
    output_dir_str: str,
    session: requests.Session,
    rt,
    script_hash: str,
) -> None:
    """Fetch, filter, write output, and record manifest for a single date."""
    started_at = now_utc_iso()
    date_str = target_date.isoformat()
    date_from = f"{date_str}T00:00:00"
    date_to = f"{date_str}T23:59:59"

    all_notices: list[dict] = []
    for notice_type in NOTICE_TYPES:
        log.info("  [%s] Fetching %s", date_str, notice_type)
        notices, _ = fetch_notices_for_type(notice_type, date_from, date_to, session)
        all_notices.extend(notices)

    log.info("[%s] Total fetched (raw): %d", date_str, len(all_notices))

    filtered_notices, dropped_by_day, dropped_duplicates = filter_and_dedup_daily(
        all_notices, date_str
    )
    log.info(
        "[%s] Kept: %d (dropped by day: %d, dropped duplicates: %d)",
        date_str, len(filtered_notices), dropped_by_day, dropped_duplicates,
        extra={"date": date_str, "status": "ok"},
    )

    write_output(output_dir_str, f"bzp_{date_str}.json", filtered_notices)

    obs_dir = rt.storage.obs_path()
    write_pipeline_run(
        layer="fetch",
        target_date=date_str,
        run_id=f"fetch_range_{date_str}_{os.getpid()}",
        started_at=started_at,
        completed_at=now_utc_iso(),
        status="ok",
        counts={
            "fetched_raw": len(all_notices),
            "dropped_by_day": dropped_by_day,
            "dropped_duplicates": dropped_duplicates,
            "kept_for_day": len(filtered_notices),
        },
        git_commit=git_commit_sha(),
        script_hash=script_hash,
        obs_dir=obs_dir,
    )

    write_processed_manifest(
        layer="fetch",
        target_date=date_str,
        script_hash=script_hash,
        storage=rt.storage,
    )
    log.info("[%s] Manifest written", date_str)


def main() -> None:
    args = _parse_args()
    start, end = _resolve_dates(args)

    rt = get_runtime()
    output_dir_str = args.output_dir or rt.storage.resolve("bronze_raw")
    script_hash = sha256_paths(Path(__file__), _SRC_PKG / "fetch")

    total_days = (end - start).days + 1
    log.info(
        "fetch_bzp_range started: %s → %s (%d days) runtime=%s output_dir=%s",
        start.isoformat(), end.isoformat(), total_days, rt.env, output_dir_str,
        extra={"runtime": rt.env},
    )

    session = requests.Session()
    session.headers["Accept"] = "application/json"

    fetched, skipped = 0, 0
    d = start
    while d <= end:
        date_str = d.isoformat()
        if not args.force and is_already_processed("fetch", date_str, script_hash, rt.storage):
            log.info(
                "[%s] Skipping — manifest hash matches current script", date_str,
                extra={"date": date_str, "status": "skipped"},
            )
            skipped += 1
        else:
            log.info(
                "[%s] Fetching (%d/%d)", date_str, fetched + skipped + 1, total_days,
                extra={"date": date_str, "status": "started"},
            )
            _fetch_one_day(d, output_dir_str, session, rt, script_hash)
            fetched += 1
        d += timedelta(days=1)

    log.info(
        "fetch_bzp_range finished: fetched=%d skipped=%d total=%d",
        fetched, skipped, total_days,
        extra={"status": "ok"},
    )


if __name__ == "__main__":
    main()
