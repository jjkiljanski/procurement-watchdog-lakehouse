"""Tests for Iceberg helper functions in pipeline_orchestrator.py.

Covers pure-Python helpers that do not require a SparkSession:
- _iceberg_notice_type_table_name
- _discover_bronze_partitions (local filesystem branch)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_repo = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_repo / "src"))

from procurement.silver.pipeline_orchestrator import (
    _iceberg_notice_type_table_name,
    _discover_bronze_partitions,
)


# ---------------------------------------------------------------------------
# _iceberg_notice_type_table_name
# ---------------------------------------------------------------------------

class TestIcebergNoticeTypeTableName:
    def test_camel_case_to_snake(self):
        assert _iceberg_notice_type_table_name("ContractNotice", "core") == "contract_notice__core"

    def test_data_model_dot_replaced(self):
        assert _iceberg_notice_type_table_name("ContractNotice", "part.core") == "contract_notice__part_core"

    def test_complex_notice_type(self):
        assert _iceberg_notice_type_table_name("TenderResultNotice", "core") == "tender_result_notice__core"

    def test_null_token_becomes_unknown(self):
        assert _iceberg_notice_type_table_name("__NULL__", "core") == "unknown__core"

    def test_empty_token_becomes_unknown(self):
        assert _iceberg_notice_type_table_name("__EMPTY__", "core") == "unknown__core"

    def test_simple_notice_type(self):
        assert _iceberg_notice_type_table_name("SmallContractNotice", "core") == "small_contract_notice__core"

    def test_data_model_already_lowercase(self):
        result = _iceberg_notice_type_table_name("ContractNotice", "core")
        assert result == result.lower()

    def test_multiple_nested_model(self):
        result = _iceberg_notice_type_table_name("ContractNotice", "parts_criteria")
        assert result == "contract_notice__parts_criteria"


# ---------------------------------------------------------------------------
# _discover_bronze_partitions (local filesystem)
# ---------------------------------------------------------------------------

class TestDiscoverBronzePartitionsLocal:
    def _make_partition(self, root: Path, notice_type: str, date: str) -> Path:
        p = root / f"noticeType={notice_type}" / f"publicationDateDay={date}"
        p.mkdir(parents=True, exist_ok=True)
        (p / "part-0.parquet").write_bytes(b"")
        return p

    def test_finds_existing_partition(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        self._make_partition(root, "ContractNotice", "2025-10-01")
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert len(result) == 1
        nt, path = result[0]
        assert nt == "ContractNotice"
        assert "2025-10-01" in path

    def test_ignores_other_dates(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        self._make_partition(root, "ContractNotice", "2025-10-01")
        self._make_partition(root, "ContractNotice", "2025-10-02")
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert len(result) == 1
        assert "2025-10-01" in result[0][1]

    def test_returns_multiple_notice_types(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        self._make_partition(root, "ContractNotice", "2025-10-01")
        self._make_partition(root, "TenderResultNotice", "2025-10-01")
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert len(result) == 2
        types = {nt for nt, _ in result}
        assert "ContractNotice" in types
        assert "TenderResultNotice" in types

    def test_null_partition_token_maps_to_none(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        self._make_partition(root, "__NULL__", "2025-10-01")
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert len(result) == 1
        nt, _ = result[0]
        assert nt is None

    def test_hive_default_partition_token_maps_to_none(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        self._make_partition(root, "__HIVE_DEFAULT_PARTITION__", "2025-10-01")
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        nt, _ = result[0]
        assert nt is None

    def test_empty_root_returns_empty_list(self, tmp_path: Path):
        root = tmp_path / "bronze" / "notices"
        root.mkdir(parents=True)
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert result == []

    def test_nonexistent_root_returns_empty_list(self, tmp_path: Path):
        root = tmp_path / "does_not_exist"
        result = _discover_bronze_partitions(str(root), "2025-10-01")
        assert result == []
