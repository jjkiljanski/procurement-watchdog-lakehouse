"""Tests for src/procurement/fetch/bzp_api.py.

Covers:
- fetch_with_backoff retry logic (connection errors, HTTP errors, success)
- filter_and_dedup_daily
- same_day
- write_output (local + GCS)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest
import requests

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))

from procurement.fetch.bzp_api import (
    filter_and_dedup_daily,
    fetch_with_backoff,
    same_day,
    write_output,
)


# ---------------------------------------------------------------------------
# same_day
# ---------------------------------------------------------------------------

class TestSameDay:
    def test_matching_prefix(self):
        assert same_day("2025-10-01T14:30:00", "2025-10-01") is True

    def test_different_day(self):
        assert same_day("2025-10-02T00:00:00", "2025-10-01") is False

    def test_none_returns_false(self):
        assert same_day(None, "2025-10-01") is False

    def test_non_string_returns_false(self):
        assert same_day(12345, "2025-10-01") is False  # type: ignore[arg-type]

    def test_empty_string_returns_false(self):
        assert same_day("", "2025-10-01") is False


# ---------------------------------------------------------------------------
# filter_and_dedup_daily
# ---------------------------------------------------------------------------

class TestFilterAndDedupDaily:
    def test_filters_to_target_day(self):
        notices = [
            {"publicationDate": "2025-10-01T10:00:00", "objectId": "A"},
            {"publicationDate": "2025-10-02T10:00:00", "objectId": "B"},
        ]
        result, dropped_day, dropped_dup = filter_and_dedup_daily(notices, "2025-10-01")
        assert len(result) == 1
        assert result[0]["objectId"] == "A"
        assert dropped_day == 1
        assert dropped_dup == 0

    def test_deduplicates_by_object_id(self):
        notices = [
            {"publicationDate": "2025-10-01T10:00:00", "objectId": "A"},
            {"publicationDate": "2025-10-01T11:00:00", "objectId": "A"},
            {"publicationDate": "2025-10-01T12:00:00", "objectId": "B"},
        ]
        result, dropped_day, dropped_dup = filter_and_dedup_daily(notices, "2025-10-01")
        assert len(result) == 2
        assert dropped_dup == 1

    def test_notices_without_object_id_pass_through(self):
        notices = [
            {"publicationDate": "2025-10-01T10:00:00"},  # no objectId
            {"publicationDate": "2025-10-01T10:00:00"},  # another no objectId
        ]
        result, _, _ = filter_and_dedup_daily(notices, "2025-10-01")
        assert len(result) == 2

    def test_empty_input(self):
        result, dropped_day, dropped_dup = filter_and_dedup_daily([], "2025-10-01")
        assert result == []
        assert dropped_day == 0
        assert dropped_dup == 0

    def test_all_wrong_day(self):
        notices = [{"publicationDate": "2025-09-30T23:59:59", "objectId": "X"}]
        result, dropped_day, dropped_dup = filter_and_dedup_daily(notices, "2025-10-01")
        assert result == []
        assert dropped_day == 1


# ---------------------------------------------------------------------------
# fetch_with_backoff
# ---------------------------------------------------------------------------

class TestFetchWithBackoff:
    def _make_session(self, responses):
        """Build a mock session whose get() returns responses in sequence."""
        session = MagicMock()
        session.get.side_effect = responses
        return session

    def test_success_on_first_attempt(self):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        session = self._make_session([resp])

        with patch("time.sleep"):
            result = fetch_with_backoff(session, "http://example.com", {"k": "v"})

        assert result is resp
        assert session.get.call_count == 1

    def test_retries_on_connection_error(self):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        session = self._make_session([
            requests.ConnectionError("timeout"),
            resp,
        ])

        with patch("time.sleep"):
            result = fetch_with_backoff(session, "http://example.com", {})

        assert result is resp
        assert session.get.call_count == 2

    def test_retries_on_retryable_http_status(self):
        bad_resp = MagicMock()
        bad_resp.status_code = 503
        bad_exc = requests.HTTPError(response=bad_resp)
        bad_resp.raise_for_status.side_effect = bad_exc

        good_resp = MagicMock()
        good_resp.raise_for_status.return_value = None

        session = MagicMock()
        # First call raises HTTPError with 503, second returns good response
        session.get.side_effect = [bad_resp, good_resp]
        bad_resp.raise_for_status.side_effect = bad_exc

        # Patch session.get to properly raise on first call
        call_count = [0]
        def _get(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise bad_exc
            return good_resp

        session.get.side_effect = _get

        with patch("time.sleep"):
            result = fetch_with_backoff(session, "http://example.com", {})

        assert result is good_resp

    def test_does_not_retry_on_4xx_non_retryable(self):
        bad_resp = MagicMock()
        bad_resp.status_code = 400
        bad_exc = requests.HTTPError(response=bad_resp)

        session = MagicMock()
        session.get.side_effect = bad_exc

        with patch("time.sleep"):
            with pytest.raises(requests.HTTPError):
                fetch_with_backoff(session, "http://example.com", {})

        # Should not retry on 400
        assert session.get.call_count == 1

    def test_raises_after_max_retries(self):
        session = MagicMock()
        session.get.side_effect = requests.ConnectionError("persistent")

        with patch("time.sleep"):
            with pytest.raises(requests.ConnectionError):
                fetch_with_backoff(session, "http://example.com", {})

        assert session.get.call_count == 5  # _MAX_RETRIES


# ---------------------------------------------------------------------------
# write_output — local
# ---------------------------------------------------------------------------

class TestWriteOutputLocal:
    def test_creates_file(self, tmp_path: Path):
        write_output(str(tmp_path), "out.json", [{"id": "1"}])
        assert (tmp_path / "out.json").exists()

    def test_content_is_valid_json(self, tmp_path: Path):
        data = [{"objectId": "A", "noticeType": "ContractNotice"}]
        write_output(str(tmp_path), "out.json", data)
        content = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
        assert content == data

    def test_creates_parent_dirs(self, tmp_path: Path):
        nested = tmp_path / "a" / "b" / "c"
        write_output(str(nested), "out.json", [])
        assert (nested / "out.json").exists()

    def test_unicode_preserved(self, tmp_path: Path):
        data = [{"name": "Łódź"}]
        write_output(str(tmp_path), "out.json", data)
        content = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
        assert content[0]["name"] == "Łódź"


# ---------------------------------------------------------------------------
# write_output — GCS
# ---------------------------------------------------------------------------

def _gcs_sys_modules():
    mock_gcs = MagicMock()
    mock_client = MagicMock()
    mock_gcs.Client.return_value = mock_client
    mock_google_cloud = MagicMock()
    mock_google_cloud.storage = mock_gcs
    mock_google = MagicMock()
    mock_google.cloud = mock_google_cloud
    return {
        "google": mock_google,
        "google.cloud": mock_google_cloud,
        "google.cloud.storage": mock_gcs,
    }, mock_gcs, mock_client


class TestWriteOutputGcs:
    def test_uploads_to_correct_blob(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value

        with patch.dict(sys.modules, modules):
            write_output("gs://my-bucket/bronze_raw", "bzp_2025-10-01.json", [{"id": "x"}])

        blob.upload_from_string.assert_called_once()

    def test_blob_name_includes_filename(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        with patch.dict(sys.modules, modules):
            write_output("gs://my-bucket/bronze_raw", "bzp_2025-10-01.json", [])

        blob_name = mock_client.bucket.return_value.blob.call_args[0][0]
        assert "bzp_2025-10-01.json" in blob_name

    def test_uploaded_content_is_valid_json(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value
        data = [{"objectId": "abc"}]

        with patch.dict(sys.modules, modules):
            write_output("gs://my-bucket/raw", "out.json", data)

        uploaded = blob.upload_from_string.call_args[0][0]
        assert json.loads(uploaded) == data

    def test_bucket_and_prefix_parsed_correctly(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        with patch.dict(sys.modules, modules):
            write_output("gs://specific-bucket/path/to/dir", "file.json", [])

        mock_client.bucket.assert_called_with("specific-bucket")
        blob_name = mock_client.bucket.return_value.blob.call_args[0][0]
        assert blob_name.startswith("path/to/dir/")
