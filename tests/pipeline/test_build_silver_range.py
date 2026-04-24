"""Tests for scripts/pipeline/build_silver_range.py — pure-Python helpers only.

Covers the _date_range utility function.  The Spark-dependent run_silver_range_core
integration is not tested here.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "build_silver_range",
        str(_repo / "scripts" / "pipeline" / "build_silver_range.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_mod = None


def _get_mod():
    global _mod
    if _mod is None:
        _mod = _load_module()
    return _mod


def _date_range(start: str, end: str) -> list[str]:
    return _get_mod()._date_range(start, end)


# ---------------------------------------------------------------------------
# _date_range
# ---------------------------------------------------------------------------

class TestDateRange:
    def test_single_day(self):
        assert _date_range("2025-10-01", "2025-10-01") == ["2025-10-01"]

    def test_consecutive_days(self):
        result = _date_range("2025-10-01", "2025-10-03")
        assert result == ["2025-10-01", "2025-10-02", "2025-10-03"]

    def test_thirty_day_range_length(self):
        result = _date_range("2025-01-01", "2025-01-30")
        assert len(result) == 30

    def test_month_boundary(self):
        result = _date_range("2025-03-30", "2025-04-02")
        assert result == ["2025-03-30", "2025-03-31", "2025-04-01", "2025-04-02"]

    def test_leap_year_includes_feb_29(self):
        result = _date_range("2024-02-28", "2024-03-01")
        assert "2024-02-29" in result

    def test_result_is_sorted(self):
        result = _date_range("2025-05-01", "2025-05-10")
        assert result == sorted(result)

    def test_all_entries_are_valid_iso_dates(self):
        from datetime import date
        for d in _date_range("2025-11-01", "2025-11-05"):
            date.fromisoformat(d)

    def test_end_before_start_raises_value_error(self):
        with pytest.raises(ValueError):
            _date_range("2025-10-10", "2025-10-01")

    def test_year_rollover(self):
        result = _date_range("2025-12-31", "2026-01-01")
        assert result == ["2025-12-31", "2026-01-01"]
