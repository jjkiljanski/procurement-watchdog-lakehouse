"""Tests for _discover_nun_days() in scripts/pipeline/build_silver_update_deltas.py.

Covers both local filesystem and GCS paths (GCS mocked via sys.modules).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))
sys.path.insert(0, str(_repo))


def _load_deltas_module():
    spec = importlib.util.spec_from_file_location(
        "build_silver_update_deltas",
        str(_repo / "scripts" / "pipeline" / "build_silver_update_deltas.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("procurement.logging", MagicMock())
    # Stub heavy silver imports that aren't needed for this function
    sys.modules.setdefault(
        "procurement.silver.section_pipeline.notice_schema_reader", MagicMock()
    )
    sys.modules.setdefault(
        "procurement.silver.update_deltas.delta_builder", MagicMock()
    )
    spec.loader.exec_module(mod)
    return mod


_NUN_CORE = "notice_type_tables/noticeType=NoticeUpdateNotice/data_model=core"


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
# _discover_nun_days — local
# ---------------------------------------------------------------------------

class TestDiscoverNunDaysLocal:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_deltas_module()

    def _make_core_dir(self, silver_dir: Path, date: str) -> None:
        d = silver_dir / _NUN_CORE / f"publicationDateDay={date}"
        d.mkdir(parents=True, exist_ok=True)

    def test_returns_empty_when_no_nun_data(self, tmp_path: Path):
        result = self.mod._discover_nun_days(str(tmp_path), None)
        assert result == []

    def test_returns_sorted_dates(self, tmp_path: Path):
        for d in ["2025-10-03", "2025-10-01", "2025-10-02"]:
            self._make_core_dir(tmp_path, d)
        result = self.mod._discover_nun_days(str(tmp_path), None)
        assert result == ["2025-10-01", "2025-10-02", "2025-10-03"]

    def test_year_filter_applied(self, tmp_path: Path):
        self._make_core_dir(tmp_path, "2025-10-01")
        self._make_core_dir(tmp_path, "2024-10-01")
        result = self.mod._discover_nun_days(str(tmp_path), "2025")
        assert result == ["2025-10-01"]

    def test_year_filter_none_returns_all(self, tmp_path: Path):
        self._make_core_dir(tmp_path, "2025-10-01")
        self._make_core_dir(tmp_path, "2024-10-01")
        result = self.mod._discover_nun_days(str(tmp_path), None)
        assert len(result) == 2

    def test_non_partition_dirs_ignored(self, tmp_path: Path):
        self._make_core_dir(tmp_path, "2025-10-01")
        # Add a directory that doesn't match the partition naming convention
        (tmp_path / _NUN_CORE / "metadata").mkdir(parents=True, exist_ok=True)
        result = self.mod._discover_nun_days(str(tmp_path), None)
        assert result == ["2025-10-01"]


# ---------------------------------------------------------------------------
# _discover_nun_days — GCS
# ---------------------------------------------------------------------------

class TestDiscoverNunDaysGcs:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_deltas_module()

    def test_returns_dates_from_gcs_prefixes(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        blobs_iter = MagicMock()
        blobs_iter.__iter__ = MagicMock(return_value=iter([]))
        blobs_iter.prefixes = [
            "silver/notice_type_tables/noticeType=NoticeUpdateNotice/data_model=core/publicationDateDay=2025-10-01/",
            "silver/notice_type_tables/noticeType=NoticeUpdateNotice/data_model=core/publicationDateDay=2025-10-02/",
        ]
        mock_client.list_blobs.return_value = blobs_iter

        with patch.dict(sys.modules, modules):
            result = self.mod._discover_nun_days("gs://my-bucket/silver", None)

        assert result == ["2025-10-01", "2025-10-02"]

    def test_gcs_year_filter(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        blobs_iter = MagicMock()
        blobs_iter.__iter__ = MagicMock(return_value=iter([]))
        blobs_iter.prefixes = [
            "silver/core/publicationDateDay=2025-10-01/",
            "silver/core/publicationDateDay=2024-10-01/",
        ]
        mock_client.list_blobs.return_value = blobs_iter

        with patch.dict(sys.modules, modules):
            result = self.mod._discover_nun_days("gs://my-bucket/silver", "2025")

        assert result == ["2025-10-01"]

    def test_returns_empty_when_no_prefixes(self):
        modules, mock_gcs, mock_client = _gcs_sys_modules()

        blobs_iter = MagicMock()
        blobs_iter.__iter__ = MagicMock(return_value=iter([]))
        blobs_iter.prefixes = []
        mock_client.list_blobs.return_value = blobs_iter

        with patch.dict(sys.modules, modules):
            result = self.mod._discover_nun_days("gs://my-bucket/silver", None)

        assert result == []
