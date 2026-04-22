"""Tests for _write_output() in scripts/pipeline/fetch_bzp_yesterday.py.

Covers both local filesystem and GCS paths (GCS mocked via sys.modules).
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))
sys.path.insert(0, str(_repo))


def _load_fetch_module():
    spec = importlib.util.spec_from_file_location(
        "fetch_bzp_yesterday",
        str(_repo / "scripts" / "pipeline" / "fetch_bzp_yesterday.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("procurement.logging", MagicMock())
    spec.loader.exec_module(mod)
    return mod


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


# ---------------------------------------------------------------------------
# _write_output — local
# ---------------------------------------------------------------------------

class TestWriteOutputLocal:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_fetch_module()

    def test_creates_file(self, tmp_path: Path):
        data = [{"objectId": "1"}]
        self.mod._write_output(str(tmp_path), "bzp_2025-10-01.json", data)
        assert (tmp_path / "bzp_2025-10-01.json").exists()

    def test_content_is_valid_json(self, tmp_path: Path):
        data = [{"objectId": "1", "noticeType": "ContractNotice"}]
        self.mod._write_output(str(tmp_path), "bzp_2025-10-01.json", data)
        content = json.loads((tmp_path / "bzp_2025-10-01.json").read_text())
        assert content == data

    def test_creates_parent_dirs(self, tmp_path: Path):
        output_dir = tmp_path / "new" / "nested"
        self.mod._write_output(str(output_dir), "bzp_2025-10-01.json", [])
        assert (output_dir / "bzp_2025-10-01.json").exists()

    def test_empty_list_writes_empty_array(self, tmp_path: Path):
        self.mod._write_output(str(tmp_path), "bzp_2025-10-01.json", [])
        content = json.loads((tmp_path / "bzp_2025-10-01.json").read_text())
        assert content == []

    def test_unicode_content_preserved(self, tmp_path: Path):
        data = [{"name": "Łódź"}]
        self.mod._write_output(str(tmp_path), "out.json", data)
        content = json.loads((tmp_path / "out.json").read_text(encoding="utf-8"))
        assert content[0]["name"] == "Łódź"


# ---------------------------------------------------------------------------
# _write_output — GCS
# ---------------------------------------------------------------------------

class TestWriteOutputGcs:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_fetch_module()

    def test_uploads_to_correct_blob(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value

        with patch.dict(sys.modules, modules):
            self.mod._write_output(
                "gs://my-bucket/bronze_raw",
                "bzp_2025-10-01.json",
                [{"objectId": "x"}],
            )

        blob.upload_from_string.assert_called_once()

    def test_blob_name_includes_filename(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        with patch.dict(sys.modules, modules):
            self.mod._write_output(
                "gs://my-bucket/bronze_raw",
                "bzp_2025-10-01.json",
                [],
            )

        blob_name = mock_client.bucket.return_value.blob.call_args[0][0]
        assert "bzp_2025-10-01.json" in blob_name

    def test_uploaded_content_is_valid_json(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()
        blob = mock_client.bucket.return_value.blob.return_value

        data = [{"objectId": "abc"}]
        with patch.dict(sys.modules, modules):
            self.mod._write_output("gs://my-bucket/raw", "out.json", data)

        uploaded_bytes = blob.upload_from_string.call_args[0][0]
        assert json.loads(uploaded_bytes) == data

    def test_bucket_and_prefix_parsed_correctly(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        with patch.dict(sys.modules, modules):
            self.mod._write_output(
                "gs://specific-bucket/path/to/dir",
                "file.json",
                [],
            )

        mock_client.bucket.assert_called_with("specific-bucket")
        blob_name = mock_client.bucket.return_value.blob.call_args[0][0]
        assert blob_name.startswith("path/to/dir/")
