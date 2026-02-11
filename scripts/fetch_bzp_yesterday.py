"""Fetch all BZP notices published yesterday and dump to JSON."""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urlencode

import requests

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

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


def fetch_notices_for_type(
    notice_type: str,
    date_from: str,
    date_to: str,
    session: requests.Session,
) -> list[dict]:
    """Fetch all pages of a single notice type for the given date range."""
    all_notices: list[dict] = []
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

        if not page:
            break

        all_notices.extend(page)
        log.info(
            "  %s — fetched page (%d records, %d total so far)",
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

    return all_notices


def main() -> None:
    yesterday = date.today() - timedelta(days=1)
    date_from = f"{yesterday.isoformat()}T00:00:00"
    date_to = f"{yesterday.isoformat()}T23:59:59"

    log.info("Fetching BZP notices for %s", yesterday.isoformat())

    session = requests.Session()
    session.headers["Accept"] = "application/json"

    all_notices: list[dict] = []

    for notice_type in NOTICE_TYPES:
        log.info("Fetching notice type: %s", notice_type)
        notices = fetch_notices_for_type(notice_type, date_from, date_to, session)
        all_notices.extend(notices)
        log.info("  %s — %d notices", notice_type, len(notices))

    log.info("Total notices fetched: %d", len(all_notices))

    output_dir = Path("data/raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"bzp_{yesterday.isoformat()}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_notices, f, ensure_ascii=False, indent=2)

    log.info("Saved to %s", output_path)


if __name__ == "__main__":
    main()
