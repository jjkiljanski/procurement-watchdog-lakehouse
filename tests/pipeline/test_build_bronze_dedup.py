"""Tests for _deduplicate_via_spark() in scripts/pipeline/build_bronze.py.

Tier 5 — requires a live SparkSession.  Skipped automatically when PySpark
or Java is unavailable (same pattern as the rest of the Tier 5 suite).
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


def _load_bronze_module():
    spec = importlib.util.spec_from_file_location(
        "build_bronze",
        str(_repo / "scripts" / "pipeline" / "build_bronze.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("procurement.logging", MagicMock())
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Helpers to write minimal Bronze Parquet for dedup tests
# ---------------------------------------------------------------------------

def _write_bronze_parquet(spark, bronze_dir: Path, records: list[dict]) -> None:
    """Write a minimal Bronze Parquet partition for testing."""
    df = spark.createDataFrame(records)
    (
        df.write.mode("overwrite")
        .partitionBy("noticeType", "publicationDateDay")
        .parquet(str(bronze_dir / "notices"))
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

@pytest.mark.usefixtures("spark")
class TestDeduplicateViaSpark:
    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        self.spark = spark
        self.mod = _load_bronze_module()

    def test_no_existing_bronze_passes_all_records(self, tmp_path: Path):
        """First run: no existing Bronze partitions → all records pass through."""
        records = [
            {"objectId": "A", "publicationDateDay": "2025-10-01"},
            {"objectId": "B", "publicationDateDay": "2025-10-01"},
        ]
        bronze_uri = str(tmp_path / "bronze" / "notices")

        deduped, stats = self.mod._deduplicate_via_spark(
            self.spark, records, "2025-10-01", bronze_uri
        )

        assert len(deduped) == 2
        assert stats["dropped_duplicates_seen_index_other_day"] == 0

    def test_cross_day_duplicate_is_dropped(self, tmp_path: Path):
        """objectId seen on a *different* day should be excluded."""
        bronze_dir = tmp_path / "bronze"

        # Write objectId "A" on 2025-10-01
        _write_bronze_parquet(
            self.spark,
            bronze_dir,
            [{"objectId": "A", "noticeType": "ContractNotice", "publicationDateDay": "2025-10-01"}],
        )

        # New batch for 2025-10-02 contains "A" (cross-day dup) and "B" (new)
        records = [
            {"objectId": "A", "publicationDateDay": "2025-10-02"},
            {"objectId": "B", "publicationDateDay": "2025-10-02"},
        ]
        bronze_uri = str(bronze_dir / "notices")

        deduped, stats = self.mod._deduplicate_via_spark(
            self.spark, records, "2025-10-02", bronze_uri
        )

        object_ids = {r["objectId"] for r in deduped}
        assert "A" not in object_ids
        assert "B" in object_ids
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_same_day_record_is_not_dropped(self, tmp_path: Path):
        """objectId already present on the *same* day must pass through (idempotent re-run)."""
        bronze_dir = tmp_path / "bronze"

        _write_bronze_parquet(
            self.spark,
            bronze_dir,
            [{"objectId": "A", "noticeType": "ContractNotice", "publicationDateDay": "2025-10-01"}],
        )

        records = [{"objectId": "A", "publicationDateDay": "2025-10-01"}]
        bronze_uri = str(bronze_dir / "notices")

        deduped, stats = self.mod._deduplicate_via_spark(
            self.spark, records, "2025-10-01", bronze_uri
        )

        assert len(deduped) == 1
        assert stats["dropped_duplicates_seen_index_other_day"] == 0

    def test_in_file_duplicates_are_dropped(self, tmp_path: Path):
        """Duplicate objectIds within the same input file (same-file dedup)."""
        bronze_uri = str(tmp_path / "bronze" / "notices")

        records = [
            {"objectId": "A", "publicationDateDay": "2025-10-01"},
            {"objectId": "A", "publicationDateDay": "2025-10-01"},  # dup
            {"objectId": "B", "publicationDateDay": "2025-10-01"},
        ]

        deduped, stats = self.mod._deduplicate_via_spark(
            self.spark, records, "2025-10-01", bronze_uri
        )

        object_ids = [r["objectId"] for r in deduped]
        assert object_ids.count("A") == 1
        assert stats["dropped_duplicates_in_input"] == 1

    def test_records_without_object_id_pass_through(self, tmp_path: Path):
        """Records with no objectId should not be deduped away."""
        bronze_uri = str(tmp_path / "bronze" / "notices")
        records = [{"publicationDateDay": "2025-10-01"}]  # no objectId

        deduped, stats = self.mod._deduplicate_via_spark(
            self.spark, records, "2025-10-01", bronze_uri
        )

        assert len(deduped) == 1
