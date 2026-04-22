"""Tests for helper functions in scripts/pipeline/build_bronze.py.

Covers _candidate_input_files, _load_raw_records, and _write_errors.
All local-path tests use tmp_path.  GCS paths are mocked via sys.modules.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add src and scripts root to path
_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))
sys.path.insert(0, str(_repo))

import importlib
import types


def _load_bronze_module():
    """Import build_bronze without executing main()."""
    spec = importlib.util.spec_from_file_location(
        "build_bronze",
        str(_repo / "scripts" / "pipeline" / "build_bronze.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    # Prevent the module-level setup_logging / log from failing
    sys.modules.setdefault("procurement.logging", MagicMock())
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# GCS mock helper (shared)
# ---------------------------------------------------------------------------

class _NotFound(Exception):
    pass


def _gcs_sys_modules():
    """Return a patch.dict context that mocks the google.cloud.storage namespace."""
    mock_gcs = MagicMock()
    mock_client = MagicMock()
    mock_gcs.Client.return_value = mock_client

    mock_google_cloud = MagicMock()
    mock_google_cloud.storage = mock_gcs
    mock_google_cloud.exceptions = MagicMock()
    mock_google_cloud.exceptions.NotFound = _NotFound

    mock_google = MagicMock()
    mock_google.cloud = mock_google_cloud

    modules = {
        "google": mock_google,
        "google.cloud": mock_google_cloud,
        "google.cloud.storage": mock_gcs,
        "google.cloud.exceptions": mock_google_cloud.exceptions,
    }
    return modules, mock_gcs, mock_client


# ---------------------------------------------------------------------------
# _candidate_input_files — local
# ---------------------------------------------------------------------------

class TestCandidateInputFilesLocal:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_returns_single_file_when_present(self, tmp_path: Path):
        f = tmp_path / "bzp_2025-10-01.json"
        f.write_text("[]")
        result = self.mod._candidate_input_files(str(tmp_path), "2025-10-01")
        assert result == [str(f)]

    def test_returns_empty_when_no_files(self, tmp_path: Path):
        result = self.mod._candidate_input_files(str(tmp_path), "2025-10-01")
        assert result == []

    def test_returns_chunked_files_sorted(self, tmp_path: Path):
        for suffix in ["_002", "_001", "_003"]:
            (tmp_path / f"bzp_2025-10-01{suffix}.json").write_text("[]")
        result = self.mod._candidate_input_files(str(tmp_path), "2025-10-01")
        assert result == [
            str(tmp_path / "bzp_2025-10-01_001.json"),
            str(tmp_path / "bzp_2025-10-01_002.json"),
            str(tmp_path / "bzp_2025-10-01_003.json"),
        ]

    def test_single_and_chunked_together(self, tmp_path: Path):
        (tmp_path / "bzp_2025-10-01.json").write_text("[]")
        (tmp_path / "bzp_2025-10-01_001.json").write_text("[]")
        result = self.mod._candidate_input_files(str(tmp_path), "2025-10-01")
        assert len(result) == 2

    def test_does_not_include_other_dates(self, tmp_path: Path):
        (tmp_path / "bzp_2025-10-01.json").write_text("[]")
        (tmp_path / "bzp_2025-10-02.json").write_text("[]")
        result = self.mod._candidate_input_files(str(tmp_path), "2025-10-01")
        assert all("2025-10-01" in r for r in result)


class TestCandidateInputFilesGcs:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_returns_matching_blob_uris(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        direct_blob = MagicMock()
        direct_blob.exists.return_value = True

        chunked_blob = MagicMock()
        chunked_blob.name = "bronze_raw/bzp_2025-10-01_001.json"

        mock_client.bucket.return_value.blob.return_value = direct_blob
        mock_client.list_blobs.return_value = iter([chunked_blob])

        with patch.dict(sys.modules, modules):
            result = self.mod._candidate_input_files(
                "gs://my-bucket/bronze_raw", "2025-10-01"
            )

        assert any("bzp_2025-10-01.json" in r for r in result)
        assert any("bzp_2025-10-01_001.json" in r for r in result)

    def test_returns_empty_when_no_blobs(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        direct_blob = MagicMock()
        direct_blob.exists.return_value = False
        mock_client.bucket.return_value.blob.return_value = direct_blob
        mock_client.list_blobs.return_value = iter([])

        with patch.dict(sys.modules, modules):
            result = self.mod._candidate_input_files(
                "gs://my-bucket/bronze_raw", "2025-10-01"
            )
        assert result == []


# ---------------------------------------------------------------------------
# _load_raw_records — local
# ---------------------------------------------------------------------------

class TestLoadRawRecordsLocal:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_loads_json_array(self, tmp_path: Path):
        records = [{"objectId": "1"}, {"objectId": "2"}]
        f = tmp_path / "bzp_2025-10-01.json"
        f.write_text(json.dumps(records))
        result = self.mod._load_raw_records([str(f)])
        assert result == records

    def test_concatenates_multiple_files(self, tmp_path: Path):
        f1 = tmp_path / "a.json"
        f2 = tmp_path / "b.json"
        f1.write_text('[{"objectId": "1"}]')
        f2.write_text('[{"objectId": "2"}]')
        result = self.mod._load_raw_records([str(f1), str(f2)])
        assert len(result) == 2

    def test_empty_array_file_returns_empty(self, tmp_path: Path):
        f = tmp_path / "empty.json"
        f.write_text("[]")
        assert self.mod._load_raw_records([str(f)]) == []

    def test_non_array_raises_value_error(self, tmp_path: Path):
        f = tmp_path / "bad.json"
        f.write_text('{"key": "value"}')
        with pytest.raises(ValueError, match="Expected JSON array"):
            self.mod._load_raw_records([str(f)])

    def test_returns_empty_for_no_files(self):
        assert self.mod._load_raw_records([]) == []


class TestLoadRawRecordsGcs:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_loads_from_gcs_uri(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value
        blob.download_as_text.return_value = '[{"objectId": "gcs-1"}]'

        with patch.dict(sys.modules, modules):
            result = self.mod._load_raw_records(
                ["gs://my-bucket/bronze_raw/bzp_2025-10-01.json"]
            )
        assert result == [{"objectId": "gcs-1"}]


# ---------------------------------------------------------------------------
# _write_errors — local
# ---------------------------------------------------------------------------

class TestWriteErrorsLocal:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_creates_errors_file(self, tmp_path: Path):
        errors = [{"objectId": "1", "error": "invalid field"}]
        self.mod._write_errors(str(tmp_path), "2025-10-01", errors)
        errors_file = tmp_path / "errors" / "bzp_2025-10-01_errors.json"
        assert errors_file.exists()

    def test_errors_file_content_is_valid_json(self, tmp_path: Path):
        errors = [{"objectId": "1", "error": "bad"}]
        self.mod._write_errors(str(tmp_path), "2025-10-01", errors)
        content = json.loads(
            (tmp_path / "errors" / "bzp_2025-10-01_errors.json").read_text()
        )
        assert content == errors

    def test_creates_errors_dir_if_missing(self, tmp_path: Path):
        self.mod._write_errors(str(tmp_path / "new_dir"), "2025-10-01", [{"x": 1}])
        assert (tmp_path / "new_dir" / "errors").exists()


class TestWriteErrorsGcs:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_bronze_module()

    def test_uploads_to_gcs(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value

        with patch.dict(sys.modules, modules):
            self.mod._write_errors(
                "gs://my-bucket/bronze", "2025-10-01", [{"objectId": "1"}]
            )

        blob.upload_from_string.assert_called_once()
        uploaded = blob.upload_from_string.call_args[0][0]
        assert json.loads(uploaded) == [{"objectId": "1"}]

    def test_blob_name_contains_errors_dir(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        with patch.dict(sys.modules, modules):
            self.mod._write_errors(
                "gs://my-bucket/bronze", "2025-10-01", [{"objectId": "1"}]
            )

        blob_name = mock_client.bucket.return_value.blob.call_args[0][0]
        assert "errors" in blob_name
        assert "2025-10-01" in blob_name
