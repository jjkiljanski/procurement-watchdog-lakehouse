"""Tests for scripts/pipeline/fetch_bzp_range.py.

Covers:
- _resolve_dates — CLI args and env var fallback
- date range iteration and manifest-based skipping
- force flag bypasses manifest check
- _fetch_one_day calls write_output and write_processed_manifest
"""

from __future__ import annotations

import importlib
import os
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))
sys.path.insert(0, str(_repo))


def _load_range_module():
    spec = importlib.util.spec_from_file_location(
        "fetch_bzp_range",
        str(_repo / "scripts" / "pipeline" / "fetch_bzp_range.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("procurement.logging", MagicMock())
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# _resolve_dates
# ---------------------------------------------------------------------------

class TestResolveDates:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_range_module()

    def _args(self, start=None, end=None):
        ns = MagicMock()
        ns.start_date = start
        ns.end_date = end
        return ns

    def test_uses_cli_args(self):
        start, end = self.mod._resolve_dates(self._args("2025-01-01", "2025-01-31"))
        assert start == date(2025, 1, 1)
        assert end == date(2025, 1, 31)

    def test_falls_back_to_env_vars(self, monkeypatch):
        monkeypatch.setenv("START_DATE", "2025-06-01")
        monkeypatch.setenv("END_DATE", "2025-06-30")
        start, end = self.mod._resolve_dates(self._args(None, None))
        assert start == date(2025, 6, 1)
        assert end == date(2025, 6, 30)

    def test_cli_takes_priority_over_env(self, monkeypatch):
        monkeypatch.setenv("START_DATE", "2024-01-01")
        monkeypatch.setenv("END_DATE", "2024-12-31")
        start, end = self.mod._resolve_dates(self._args("2025-03-01", "2025-03-31"))
        assert start == date(2025, 3, 1)

    def test_raises_when_dates_missing(self, monkeypatch):
        monkeypatch.delenv("START_DATE", raising=False)
        monkeypatch.delenv("END_DATE", raising=False)
        with pytest.raises(ValueError, match="start_date and end_date are required"):
            self.mod._resolve_dates(self._args(None, None))

    def test_raises_when_end_before_start(self):
        with pytest.raises(ValueError, match="end_date .* is before start_date"):
            self.mod._resolve_dates(self._args("2025-10-31", "2025-10-01"))


# ---------------------------------------------------------------------------
# main — manifest-based skipping and fetching
# ---------------------------------------------------------------------------

class TestMainManifestSkipping:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_range_module()

    def _run_main(self, tmp_path: Path, start: str, end: str, force: bool = False):
        """Run main() with mocked runtime, API calls, and manifest checks."""
        mock_rt = MagicMock()
        mock_rt.storage.resolve.return_value = str(tmp_path / "bronze_raw")
        mock_rt.storage.obs_path.return_value = None

        with (
            patch.object(self.mod, "get_runtime", return_value=mock_rt),
            patch.object(self.mod, "write_output") as mock_write,
            patch.object(self.mod, "write_processed_manifest") as mock_manifest,
            patch.object(self.mod, "write_pipeline_run"),
            patch.object(self.mod, "sha256_file", return_value="testhash"),
            patch.object(self.mod, "fetch_notices_for_type", return_value=([], [])),
            patch.object(self.mod, "filter_and_dedup_daily", return_value=([], 0, 0)),
            patch.object(
                self.mod,
                "is_already_processed",
                return_value=False,  # nothing processed yet
            ),
        ):
            args = MagicMock()
            args.start_date = start
            args.end_date = end
            args.output_dir = None
            args.force = force
            with patch.object(self.mod, "_parse_args", return_value=args):
                self.mod.main()

        return mock_write, mock_manifest

    def test_fetches_all_dates_in_range(self, tmp_path: Path):
        mock_write, mock_manifest = self._run_main(tmp_path, "2025-10-01", "2025-10-03")
        # 3 dates → 3 write_output calls
        assert mock_write.call_count == 3

    def test_output_filenames_match_dates(self, tmp_path: Path):
        mock_write, _ = self._run_main(tmp_path, "2025-10-01", "2025-10-02")
        filenames = [c.args[1] for c in mock_write.call_args_list]
        assert "bzp_2025-10-01.json" in filenames
        assert "bzp_2025-10-02.json" in filenames

    def test_manifest_written_per_date(self, tmp_path: Path):
        _, mock_manifest = self._run_main(tmp_path, "2025-10-01", "2025-10-03")
        assert mock_manifest.call_count == 3

    def test_skips_already_processed_dates(self, tmp_path: Path):
        mock_rt = MagicMock()
        mock_rt.storage.resolve.return_value = str(tmp_path / "bronze_raw")
        mock_rt.storage.obs_path.return_value = None

        call_log = []

        def _is_processed(layer, date_str, hash_, storage):
            # Only 2025-10-02 is already processed
            return date_str == "2025-10-02"

        with (
            patch.object(self.mod, "get_runtime", return_value=mock_rt),
            patch.object(self.mod, "write_output") as mock_write,
            patch.object(self.mod, "write_processed_manifest"),
            patch.object(self.mod, "write_pipeline_run"),
            patch.object(self.mod, "sha256_file", return_value="testhash"),
            patch.object(self.mod, "fetch_notices_for_type", return_value=([], [])),
            patch.object(self.mod, "filter_and_dedup_daily", return_value=([], 0, 0)),
            patch.object(self.mod, "is_already_processed", side_effect=_is_processed),
        ):
            args = MagicMock()
            args.start_date = "2025-10-01"
            args.end_date = "2025-10-03"
            args.output_dir = None
            args.force = False
            with patch.object(self.mod, "_parse_args", return_value=args):
                self.mod.main()

        # Only 2 dates fetched (2025-10-01 and 2025-10-03); 2025-10-02 skipped
        assert mock_write.call_count == 2
        fetched_dates = {c.args[1] for c in mock_write.call_args_list}
        assert "bzp_2025-10-01.json" in fetched_dates
        assert "bzp_2025-10-03.json" in fetched_dates
        assert "bzp_2025-10-02.json" not in fetched_dates

    def test_force_bypasses_manifest_check(self, tmp_path: Path):
        mock_rt = MagicMock()
        mock_rt.storage.resolve.return_value = str(tmp_path / "bronze_raw")
        mock_rt.storage.obs_path.return_value = None

        mock_is_processed = MagicMock(return_value=True)  # all "already processed"

        with (
            patch.object(self.mod, "get_runtime", return_value=mock_rt),
            patch.object(self.mod, "write_output") as mock_write,
            patch.object(self.mod, "write_processed_manifest"),
            patch.object(self.mod, "write_pipeline_run"),
            patch.object(self.mod, "sha256_file", return_value="testhash"),
            patch.object(self.mod, "fetch_notices_for_type", return_value=([], [])),
            patch.object(self.mod, "filter_and_dedup_daily", return_value=([], 0, 0)),
            patch.object(self.mod, "is_already_processed", mock_is_processed),
        ):
            args = MagicMock()
            args.start_date = "2025-10-01"
            args.end_date = "2025-10-02"
            args.output_dir = None
            args.force = True  # force re-fetch
            with patch.object(self.mod, "_parse_args", return_value=args):
                self.mod.main()

        # Both dates fetched even though manifest says processed
        assert mock_write.call_count == 2
        mock_is_processed.assert_not_called()

    def test_single_date_range(self, tmp_path: Path):
        mock_write, mock_manifest = self._run_main(tmp_path, "2025-10-15", "2025-10-15")
        assert mock_write.call_count == 1
        assert mock_manifest.call_count == 1
