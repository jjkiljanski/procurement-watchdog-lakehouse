"""Tests for write_output() — now in src/procurement/fetch/bzp_api.py.

The function was extracted from scripts/pipeline/fetch_bzp_yesterday.py into
the shared bzp_api module so both the daily and range downloader scripts can
use it.  Comprehensive coverage lives in tests/fetch/test_bzp_api.py.

This file keeps a thin smoke test to guard against accidental removal of the
re-export from fetch_bzp_yesterday and to verify the daily script still wires
up to the shared helper correctly.
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

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


class TestFetchBzpYesterdayWiresSharedHelpers:
    """Smoke tests: the daily script imports and re-exports the shared API helpers."""

    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_fetch_module()

    def test_write_output_accessible(self):
        """write_output must be importable from the daily script namespace."""
        assert callable(self.mod.write_output)

    def test_fetch_notices_for_type_accessible(self):
        assert callable(self.mod.fetch_notices_for_type)

    def test_filter_and_dedup_daily_accessible(self):
        assert callable(self.mod.filter_and_dedup_daily)

    def test_notice_types_list_present(self):
        assert isinstance(self.mod.NOTICE_TYPES, list)
        assert len(self.mod.NOTICE_TYPES) == 15

    def test_write_output_works_locally(self, tmp_path: Path):
        """End-to-end: write_output via the daily script namespace writes valid JSON."""
        data = [{"objectId": "X"}]
        self.mod.write_output(str(tmp_path), "smoke.json", data)
        content = json.loads((tmp_path / "smoke.json").read_text(encoding="utf-8"))
        assert content == data
