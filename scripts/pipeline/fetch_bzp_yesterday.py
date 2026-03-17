"""Fetch all BZP notices for target day and dump to bronze_raw JSON."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.obs import git_commit_sha, now_utc_iso, write_pipeline_run
from procurement.logging import setup_logging

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
        default="data/bronze_raw",
        help="Directory for raw daily JSON payload",
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


def main() -> None:
    args = _parse_args()
    target_date = date.fromisoformat(args.target_date) if args.target_date else (date.today() - timedelta(days=1))
    started_at = now_utc_iso()

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

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"bzp_{target_date.isoformat()}.json"
    output_path.write_text(json.dumps(filtered_notices, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Saved to %s", output_path)

    import os
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
    )
    log.info("Wrote fetch obs pipeline_run for %s", target_date.isoformat())


if __name__ == "__main__":
    main()
