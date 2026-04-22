"""Fetch all BZP notices for target day and dump to bronze_raw JSON.

Output path resolution
----------------------
The output directory is resolved by the runtime provider (see
``src/procurement/runtime/``):

- **local**  (``RUNTIME_ENV=local``):  ``{LOCAL_DATA_ROOT}/bronze_raw/``
- **GCP**    (``RUNTIME_ENV=gcp``):    ``gs://{LAKEHOUSE_BUCKET}/bronze_raw/``

The resolved path can be overridden with ``--output-dir`` for one-off runs.

Usage
-----
::

    python scripts/pipeline/fetch_bzp_yesterday.py
    python scripts/pipeline/fetch_bzp_yesterday.py 2025-10-01
    python scripts/pipeline/fetch_bzp_yesterday.py 2025-10-01 --output-dir gs://my-bucket/bronze_raw
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.obs import git_commit_sha, now_utc_iso, sha256_file, write_pipeline_run
from procurement.logging import setup_logging
from procurement.manifests import write_processed_manifest
from procurement.runtime import get_runtime

setup_logging()
log = logging.getLogger(__name__)

BASE_URL = "https://ezamowienia.gov.pl/mo-board/api/v1/notice"

NOTICE_TYPES = [
    "ContractNotice",
    "AgreementIntentionNotice",
    "TenderResultNotice",
    "CompetitionNotice",
    "CompetitionResultNotice",
    "NoticeUpdateNotice",
    "AgreementUpdateNotice",
    "ContractPerformingNotice",
    "CircumstancesFulfillmentNotice",
    "SmallContractNotice",
    "ConcessionNotice",
    "ConcessionIntentionAgreementNotice",
    "NoticeUpdateConcession",
    "ConcessionAgreementNotice",
    "ConcessionUpdateAgreementNotice",
]

PAGE_SIZE = 500


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch BZP notices for a target date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--output-dir",
        default=None,
        help=(
            "Output directory for raw JSON payload.  Defaults to the "
            "runtime-resolved 'bronze_raw' path (local or GCS depending on "
            "RUNTIME_ENV)."
        ),
    )
    return parser.parse_args()


def fetch_notices_for_type(
    notice_type: str,
    date_from: str,
    date_to: str,
    session: requests.Session,
) -> tuple[list[dict], list[dict]]:
    """Fetch all pages for one notice type and return notices + query log."""
    all_notices: list[dict] = []
    page_queries: list[dict] = []
    search_after: str | None = None

    while True:
        params: dict = {
            "NoticeType": notice_type,
            "PublicationDateFrom": date_from,
            "PublicationDateTo": date_to,
            "PageSize": PAGE_SIZE,
        }
        if search_after:
            params["SearchAfter"] = search_after

        resp = session.get(BASE_URL, params=params, timeout=60)
        resp.raise_for_status()
        page = resp.json()

        page_queries.append(
            {
                "requested_at": now_utc_iso(),
                "url": BASE_URL,
                "params": dict(params),
                "response_count": len(page),
                "first_object_id": page[0].get("objectId") if page else None,
                "last_object_id": page[-1].get("objectId") if page else None,
            }
        )

        if not page:
            break

        all_notices.extend(page)
        log.info(
            "  %s - fetched page (%d records, %d total so far)",
            notice_type,
            len(page),
            len(all_notices),
        )

        if len(page) < PAGE_SIZE:
            break

        last_object_id = page[-1].get("objectId")
        if not last_object_id:
            break
        search_after = last_object_id

    return all_notices, page_queries


def _same_day(publication_date: str | None, target_day: str) -> bool:
    if not publication_date or not isinstance(publication_date, str):
        return False
    return publication_date[:10] == target_day


def _filter_and_dedup_daily(notices: list[dict], target_day: str) -> tuple[list[dict], int, int]:
    filtered = [n for n in notices if _same_day(n.get("publicationDate"), target_day)]
    dropped_by_day = len(notices) - len(filtered)

    deduped: list[dict] = []
    seen: set[str] = set()
    dropped_duplicates = 0
    for notice in filtered:
        object_id = notice.get("objectId")
        key = object_id if isinstance(object_id, str) and object_id else None
        if key is None:
            deduped.append(notice)
            continue
        if key in seen:
            dropped_duplicates += 1
            continue
        seen.add(key)
        deduped.append(notice)
    return deduped, dropped_by_day, dropped_duplicates


def _write_output(output_dir_str: str, filename: str, data: list[dict]) -> None:
    """Write JSON output to either a local path or a GCS URI."""
    serialised = json.dumps(data, ensure_ascii=False, indent=2)

    if output_dir_str.startswith("gs://"):
        from google.cloud import storage as gcs

        # gs://bucket/path/to/dir  →  bucket="bucket", prefix="path/to/dir"
        without_scheme = output_dir_str[5:]
        bucket_name, _, prefix = without_scheme.partition("/")
        blob_name = f"{prefix.rstrip('/')}/{filename}" if prefix else filename

        client = gcs.Client()
        client.bucket(bucket_name).blob(blob_name).upload_from_string(
            serialised.encode("utf-8"),
            content_type="application/json",
        )
        log.info("Saved to %s/%s", output_dir_str, filename)
    else:
        out_path = Path(output_dir_str)
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / filename).write_text(serialised, encoding="utf-8")
        log.info("Saved to %s", out_path / filename)


def main() -> None:
    args = _parse_args()
    target_date = date.fromisoformat(args.target_date) if args.target_date else (date.today() - timedelta(days=1))
    started_at = now_utc_iso()

    rt = get_runtime()
    output_dir_str = args.output_dir or rt.storage.resolve("bronze_raw")

    date_from = f"{target_date.isoformat()}T00:00:00"
    date_to = f"{target_date.isoformat()}T23:59:59"

    log.info("Fetching BZP notices for %s", target_date.isoformat())
    session = requests.Session()
    session.headers["Accept"] = "application/json"

    all_notices: list[dict] = []
    fetch_queries: list[dict] = []
    for notice_type in NOTICE_TYPES:
        log.info("Fetching notice type: %s", notice_type)
        notices, queries = fetch_notices_for_type(notice_type, date_from, date_to, session)
        all_notices.extend(notices)
        fetch_queries.extend(queries)
        log.info("  %s - %d notices", notice_type, len(notices))

    log.info("Total notices fetched (raw): %d", len(all_notices))

    filtered_notices, dropped_by_day, dropped_duplicates = _filter_and_dedup_daily(
        all_notices,
        target_date.isoformat(),
    )
    log.info("Dropped notices with mismatched publicationDate day: %d", dropped_by_day)
    log.info("Dropped duplicate notices by objectId: %d", dropped_duplicates)
    log.info("Total notices kept for %s: %d", target_date.isoformat(), len(filtered_notices))

    _write_output(output_dir_str, f"bzp_{target_date.isoformat()}.json", filtered_notices)

    obs_dir = rt.storage.obs_path()
    write_pipeline_run(
        layer="fetch",
        target_date=target_date.isoformat(),
        run_id=f"fetch_{target_date.isoformat()}_{os.getpid()}",
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
        script_hash=sha256_file(Path(__file__)),
        obs_dir=obs_dir,
    )
    if obs_dir:
        log.info("Wrote fetch obs pipeline_run for %s", target_date.isoformat())
    else:
        log.info("Obs write skipped (obs_path=None for runtime env=%s)", rt.env)

    write_processed_manifest(
        layer="fetch",
        target_date=target_date.isoformat(),
        script_hash=sha256_file(Path(__file__)),
        storage=rt.storage,
    )
    log.info("Written processed manifest: layer=fetch date=%s", target_date.isoformat())


if __name__ == "__main__":
    main()
