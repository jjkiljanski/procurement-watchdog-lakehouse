"""Shared BZP API fetch helpers used by both daily and range downloader scripts.

Provides:
- HTTP fetch with exponential backoff + jitter
- Per-notice-type pagination
- Same-day filtering and deduplication
- Output writing (local filesystem or GCS)
"""

from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path

import requests

from procurement.logging import get_stage_logger

log = get_stage_logger(__name__, "fetch")

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

_MAX_RETRIES = 5
_BASE_DELAY = 2.0  # seconds — doubles on each attempt, plus jitter

# HTTP status codes that warrant a retry (transient server errors / rate-limit)
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


def fetch_with_backoff(session: requests.Session, url: str, params: dict) -> requests.Response:
    """GET with exponential backoff + jitter for transient network/server errors."""
    for attempt in range(_MAX_RETRIES):
        try:
            resp = session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as exc:
            if attempt == _MAX_RETRIES - 1:
                raise
            delay = _BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
            log.warning(
                "BZP API transient error (attempt %d/%d): %s — retrying in %.1fs",
                attempt + 1, _MAX_RETRIES, exc, delay,
            )
            time.sleep(delay)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in _RETRYABLE_STATUS:
                if attempt == _MAX_RETRIES - 1:
                    raise
                delay = _BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                log.warning(
                    "BZP API HTTP %d (attempt %d/%d) — retrying in %.1fs",
                    exc.response.status_code, attempt + 1, _MAX_RETRIES, delay,
                )
                time.sleep(delay)
            else:
                raise
    raise RuntimeError("unreachable")  # pragma: no cover


def fetch_notices_for_type(
    notice_type: str,
    date_from: str,
    date_to: str,
    session: requests.Session,
) -> tuple[list[dict], list[dict]]:
    """Fetch all pages for one notice type and return (notices, query_log)."""
    from procurement.obs import now_utc_iso

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

        resp = fetch_with_backoff(session, BASE_URL, params)
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


def same_day(publication_date: str | None, target_day: str) -> bool:
    if not publication_date or not isinstance(publication_date, str):
        return False
    return publication_date[:10] == target_day


def filter_and_dedup_daily(
    notices: list[dict], target_day: str
) -> tuple[list[dict], int, int]:
    """Filter to target day and deduplicate by objectId.

    Returns (filtered_notices, dropped_by_day, dropped_duplicates).
    """
    filtered = [n for n in notices if same_day(n.get("publicationDate"), target_day)]
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


def write_output(output_dir_str: str, filename: str, data: list[dict]) -> None:
    """Write JSON output to either a local path or a GCS URI."""
    serialised = json.dumps(data, ensure_ascii=False, indent=2)
    encoded = serialised.encode("utf-8")
    size_kb = len(encoded) / 1024

    if output_dir_str.startswith("gs://"):
        from google.cloud import storage as gcs

        without_scheme = output_dir_str[5:]
        bucket_name, _, prefix = without_scheme.partition("/")
        blob_name = f"{prefix.rstrip('/')}/{filename}" if prefix else filename

        client = gcs.Client()
        client.bucket(bucket_name).blob(blob_name).upload_from_string(
            encoded,
            content_type="application/json",
        )
        log.info(
            "Saved %d records (%.1f KB) to gs://%s/%s",
            len(data), size_kb, bucket_name, blob_name,
        )
    else:
        out_path = Path(output_dir_str)
        out_path.mkdir(parents=True, exist_ok=True)
        (out_path / filename).write_text(serialised, encoding="utf-8")
        log.info("Saved %d records (%.1f KB) to %s", len(data), size_kb, out_path / filename)
