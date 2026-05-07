"""Tests for scripts/pipeline/build_bronze_range.py — pure-Python helpers only.

Covers the _date_range utility function.  Spark-dependent _process_date is
not tested here (integration concern).
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
        "build_bronze_range",
        str(_repo / "scripts" / "pipeline" / "build_bronze_range.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    # Prevent main() from running and avoid heavy imports at module load.
    # We only need _date_range which depends only on stdlib.
    sys.modules.setdefault("build_bronze", type(sys)("build_bronze"))
    spec.loader.exec_module(mod)
    return mod


# Lazy import — evaluated once per test session.
_mod = None


def _get_mod():
    global _mod
    if _mod is None:
        _mod = _load_module()
    return _mod


def _date_range(start: str, end: str) -> list[str]:
    return _get_mod()._date_range(start, end)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return _get_mod()._chunks(items, size)


# ---------------------------------------------------------------------------
# _date_range
# ---------------------------------------------------------------------------

class TestDateRange:
    def test_single_day(self):
        result = _date_range("2025-10-01", "2025-10-01")
        assert result == ["2025-10-01"]

    def test_two_days(self):
        result = _date_range("2025-10-01", "2025-10-02")
        assert result == ["2025-10-01", "2025-10-02"]

    def test_full_week(self):
        result = _date_range("2025-10-01", "2025-10-07")
        assert len(result) == 7
        assert result[0] == "2025-10-01"
        assert result[-1] == "2025-10-07"

    def test_month_boundary(self):
        result = _date_range("2025-01-30", "2025-02-02")
        assert result == ["2025-01-30", "2025-01-31", "2025-02-01", "2025-02-02"]

    def test_leap_year(self):
        result = _date_range("2024-02-28", "2024-03-01")
        assert result == ["2024-02-28", "2024-02-29", "2024-03-01"]

    def test_non_leap_year_skips_feb_29(self):
        result = _date_range("2025-02-28", "2025-03-01")
        assert result == ["2025-02-28", "2025-03-01"]

    def test_result_is_sorted(self):
        result = _date_range("2025-01-01", "2025-01-05")
        assert result == sorted(result)

    def test_all_dates_are_iso_format(self):
        from datetime import date
        result = _date_range("2025-06-01", "2025-06-05")
        for d in result:
            date.fromisoformat(d)  # raises on invalid format

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError):
            _date_range("2025-10-05", "2025-10-01")

    def test_year_boundary(self):
        result = _date_range("2024-12-30", "2025-01-02")
        assert result == ["2024-12-30", "2024-12-31", "2025-01-01", "2025-01-02"]


class TestChunks:
    def test_exact_multiple(self):
        assert _chunks(["a", "b", "c", "d"], 2) == [["a", "b"], ["c", "d"]]

    def test_partial_final_chunk(self):
        assert _chunks(["a", "b", "c", "d", "e"], 2) == [["a", "b"], ["c", "d"], ["e"]]

    def test_size_must_be_positive(self):
        with pytest.raises(ValueError):
            _chunks(["a"], 0)
