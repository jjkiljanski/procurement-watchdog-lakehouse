"""Tests for _discover_nun_days() in scripts/pipeline/build_silver_update_deltas.py.

The function now uses Spark SQL against the Iceberg catalog; tests use a mocked
SparkSession so no JVM is required.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock, call

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
    sys.modules.setdefault(
        "procurement.silver.section_pipeline.notice_schema_reader", MagicMock()
    )
    sys.modules.setdefault(
        "procurement.silver.update_deltas.delta_builder", MagicMock()
    )
    spec.loader.exec_module(mod)
    return mod


def _make_spark(dates: list[str], table_exists: bool = True):
    """Return a mock SparkSession whose sql() returns the given dates."""
    spark = MagicMock()

    def describe_side_effect(sql: str):
        if sql.startswith("DESCRIBE TABLE"):
            if not table_exists:
                raise Exception("table not found")
            return MagicMock()
        # SELECT DISTINCT query
        rows = [MagicMock(publicationDateDay=d) for d in dates]
        result_df = MagicMock()
        result_df.collect.return_value = rows
        return result_df

    spark.sql.side_effect = describe_side_effect
    return spark


# ---------------------------------------------------------------------------
# _discover_nun_days
# ---------------------------------------------------------------------------

class TestDiscoverNunDays:
    @pytest.fixture(autouse=True)
    def module(self):
        self.mod = _load_deltas_module()

    def test_returns_empty_when_table_does_not_exist(self):
        spark = _make_spark([], table_exists=False)
        result = self.mod._discover_nun_days(spark, None)
        assert result == []

    def test_returns_sorted_dates(self):
        spark = _make_spark(["2025-10-03", "2025-10-01", "2025-10-02"])
        result = self.mod._discover_nun_days(spark, None)
        assert result == ["2025-10-01", "2025-10-02", "2025-10-03"]

    def test_year_filter_included_in_sql(self):
        spark = _make_spark(["2025-10-01"])
        self.mod._discover_nun_days(spark, "2025")
        # The second sql() call (SELECT) should include a LIKE filter
        select_calls = [
            str(c) for c in spark.sql.call_args_list
            if "SELECT" in str(c)
        ]
        assert any("2025%" in c for c in select_calls)

    def test_no_year_filter_no_where_clause(self):
        spark = _make_spark(["2025-10-01", "2024-05-10"])
        self.mod._discover_nun_days(spark, None)
        select_calls = [
            str(c) for c in spark.sql.call_args_list
            if "SELECT" in str(c)
        ]
        assert any("LIKE" not in c for c in select_calls)

    def test_returns_empty_when_no_rows(self):
        spark = _make_spark([])
        result = self.mod._discover_nun_days(spark, None)
        assert result == []
