"""Tests for bronze cross-day deduplication logic.

Tier 1  — pure Python (no Spark required):
    Tests for ``apply_dedup_filter`` from ``procurement.bronze.dedup``.
    These always run.

Tier 5  — Spark required (auto-skipped when PySpark / Java unavailable):
    Tests for ``_collect_pre_range_object_ids`` and
    ``_collect_object_ids_for_date`` in ``build_bronze_range``, plus
    integration tests for the running-seen-set pattern.

Test classes:
    TestApplyDedupFilter                 — Tier 1 (21 tests)
    TestCollectPreRangeObjectIds         — Tier 5 (4 tests)
    TestCollectObjectIdsForDate          — Tier 5 (3 tests)
    TestRunningSetIntegration            — Tier 5 (4 tests)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# --- path setup -----------------------------------------------------------
_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "src"))
sys.path.insert(0, str(_REPO / "scripts" / "pipeline"))

# --- always-importable pure-Python module ---------------------------------
from procurement.bronze.dedup import apply_dedup_filter

# --- conditional import of script modules (require PySpark) ---------------
try:
    import build_bronze as _bb
    import build_bronze_range as _br

    _SCRIPTS_AVAILABLE = True
except Exception:
    _SCRIPTS_AVAILABLE = False
    _bb = None  # type: ignore[assignment]
    _br = None  # type: ignore[assignment]

_requires_scripts = pytest.mark.skipif(
    not _SCRIPTS_AVAILABLE,
    reason="build_bronze scripts require PySpark",
)


# ===========================================================================
# Helpers
# ===========================================================================

def _rec(object_id: str | None, **extra) -> dict:
    """Build a minimal record dict."""
    r: dict = {}
    if object_id is not None:
        r["objectId"] = object_id
    r.update(extra)
    return r


def _write_test_bronze(spark, bronze_notices_uri: str, rows: list[dict]) -> None:
    """Write minimal bronze-like parquet partitioned by noticeType/publicationDateDay."""
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([
        StructField("objectId", StringType(), False),
        StructField("noticeType", StringType(), False),
        StructField("publicationDateDay", StringType(), False),
    ])
    df = spark.createDataFrame(
        [(r["objectId"], r["noticeType"], r["publicationDateDay"]) for r in rows],
        schema=schema,
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite").partitionBy("noticeType", "publicationDateDay").parquet(
        bronze_notices_uri
    )


# ===========================================================================
# Tier 1 — apply_dedup_filter (pure Python, no Spark)
# ===========================================================================

class TestApplyDedupFilter:
    # --- cross-day dedup ---------------------------------------------------

    def test_drops_ids_present_in_seen_set(self):
        records = [_rec("A"), _rec("B"), _rec("C")]
        seen = {"A", "C"}
        filtered, stats = apply_dedup_filter(records, seen)
        ids = [r["objectId"] for r in filtered]
        assert ids == ["B"]

    def test_keeps_records_not_in_seen_set(self):
        records = [_rec("X"), _rec("Y")]
        filtered, _ = apply_dedup_filter(records, set())
        assert len(filtered) == 2

    def test_empty_seen_set_keeps_all_records(self):
        records = [_rec("A"), _rec("B"), _rec("C")]
        filtered, stats = apply_dedup_filter(records, set())
        assert len(filtered) == 3
        assert stats["dropped_duplicates_seen_index_other_day"] == 0

    def test_all_records_seen_returns_empty(self):
        records = [_rec("A"), _rec("B")]
        filtered, stats = apply_dedup_filter(records, {"A", "B"})
        assert filtered == []
        assert stats["dropped_duplicates_seen_index_other_day"] == 2

    # --- within-file dedup -------------------------------------------------

    def test_in_file_dedup_drops_second_occurrence(self):
        records = [_rec("A"), _rec("A"), _rec("B")]
        filtered, stats = apply_dedup_filter(records, set())
        ids = [r["objectId"] for r in filtered]
        assert ids == ["A", "B"]
        assert stats["dropped_duplicates_in_input"] == 1

    def test_in_file_dedup_keeps_first_occurrence(self):
        records = [_rec("A", value=1), _rec("A", value=2)]
        filtered, _ = apply_dedup_filter(records, set())
        assert filtered[0]["value"] == 1

    def test_in_file_dedup_handles_triple_occurrence(self):
        records = [_rec("A"), _rec("A"), _rec("A")]
        filtered, stats = apply_dedup_filter(records, set())
        assert len(filtered) == 1
        assert stats["dropped_duplicates_in_input"] == 2

    # --- missing / empty objectId pass-through -----------------------------

    def test_none_object_id_passes_through(self):
        records = [_rec(None, value="no-id")]
        filtered, _ = apply_dedup_filter(records, {"anything"})
        assert len(filtered) == 1

    def test_missing_object_id_key_passes_through(self):
        records = [{"value": "no-id-key"}]
        filtered, _ = apply_dedup_filter(records, {"anything"})
        assert len(filtered) == 1

    def test_empty_string_object_id_passes_through(self):
        records = [_rec("", value="empty")]
        filtered, _ = apply_dedup_filter(records, set())
        assert len(filtered) == 1

    def test_whitespace_only_object_id_passes_through(self):
        records = [_rec("   ", value="spaces")]
        filtered, _ = apply_dedup_filter(records, set())
        assert len(filtered) == 1

    # --- objectId normalisation --------------------------------------------

    def test_objectid_whitespace_stripped_before_comparison(self):
        records = [_rec(" A "), _rec("B")]
        seen = {"A"}  # no surrounding spaces
        filtered, stats = apply_dedup_filter(records, seen)
        # " A " strips to "A" which is in seen → dropped
        ids = [r["objectId"] for r in filtered]
        assert ids == ["B"]
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_in_file_dedup_also_strips_whitespace(self):
        records = [_rec("A"), _rec(" A ")]
        filtered, stats = apply_dedup_filter(records, set())
        assert len(filtered) == 1
        assert stats["dropped_duplicates_in_input"] == 1

    # --- stats dict --------------------------------------------------------

    def test_stats_input_rows_count(self):
        records = [_rec("A"), _rec("B"), _rec("C")]
        _, stats = apply_dedup_filter(records, set())
        assert stats["input_rows"] == 3

    def test_stats_output_rows_count(self):
        records = [_rec("A"), _rec("B"), _rec("C")]
        filtered, stats = apply_dedup_filter(records, {"B"})
        assert stats["output_rows"] == 2
        assert len(filtered) == stats["output_rows"]

    def test_stats_dropped_in_input(self):
        records = [_rec("A"), _rec("A"), _rec("B"), _rec("B"), _rec("B")]
        _, stats = apply_dedup_filter(records, set())
        assert stats["dropped_duplicates_in_input"] == 3

    def test_stats_dropped_seen_other_day(self):
        records = [_rec("A"), _rec("B"), _rec("C")]
        _, stats = apply_dedup_filter(records, {"A", "C"})
        assert stats["dropped_duplicates_seen_index_other_day"] == 2

    # --- combined scenarios ------------------------------------------------

    def test_combined_in_file_and_cross_day_dedup(self):
        # A: in seen set (cross-day drop)
        # B: appears twice in file (in-file drop on second)
        # C: clean pass-through
        records = [_rec("A"), _rec("B"), _rec("B"), _rec("C")]
        filtered, stats = apply_dedup_filter(records, {"A"})
        ids = [r["objectId"] for r in filtered]
        assert ids == ["B", "C"]
        assert stats["dropped_duplicates_in_input"] == 1
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_empty_input_returns_empty(self):
        filtered, stats = apply_dedup_filter([], {"A", "B"})
        assert filtered == []
        assert stats["input_rows"] == 0
        assert stats["output_rows"] == 0

    def test_mixed_none_and_valid_ids(self):
        records = [_rec(None), _rec("A"), _rec(None), _rec("B")]
        filtered, stats = apply_dedup_filter(records, {"A"})
        # None records always pass through; A is dropped
        assert len(filtered) == 3  # 2 × None + B
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_order_preserved(self):
        records = [_rec("C"), _rec("A"), _rec("B")]
        filtered, _ = apply_dedup_filter(records, set())
        assert [r["objectId"] for r in filtered] == ["C", "A", "B"]


# ===========================================================================
# Tier 5 — Spark-backed helpers (auto-skipped without PySpark / Java)
# ===========================================================================

@_requires_scripts
class TestCollectPreRangeObjectIds:
    def test_returns_empty_when_no_bronze_exists(self, spark, tmp_path):
        result = _br._collect_pre_range_object_ids(
            spark,
            str(tmp_path / "bronze_notices"),
            "2025-03-01",
        )
        assert result == set()

    def test_reads_only_dates_before_start_date(self, spark, tmp_path):
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "A", "noticeType": "T", "publicationDateDay": "2025-02-28"},
            {"objectId": "B", "noticeType": "T", "publicationDateDay": "2025-03-01"},
            {"objectId": "C", "noticeType": "T", "publicationDateDay": "2025-03-02"},
        ])
        result = _br._collect_pre_range_object_ids(spark, uri, "2025-03-01")
        assert result == {"A"}

    def test_excludes_start_date_itself(self, spark, tmp_path):
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "A", "noticeType": "T", "publicationDateDay": "2025-03-01"},
        ])
        result = _br._collect_pre_range_object_ids(spark, uri, "2025-03-01")
        assert result == set()

    def test_returns_all_ids_from_multiple_notice_types(self, spark, tmp_path):
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "A", "noticeType": "T1", "publicationDateDay": "2025-02-01"},
            {"objectId": "B", "noticeType": "T2", "publicationDateDay": "2025-02-01"},
            {"objectId": "C", "noticeType": "T1", "publicationDateDay": "2025-02-15"},
        ])
        result = _br._collect_pre_range_object_ids(spark, uri, "2025-03-01")
        assert result == {"A", "B", "C"}


@_requires_scripts
class TestCollectObjectIdsForDate:
    def test_returns_empty_when_no_bronze_exists(self, spark, tmp_path):
        result = _br._collect_object_ids_for_date(
            spark,
            str(tmp_path / "bronze_notices"),
            "2025-03-01",
        )
        assert result == set()

    def test_returns_ids_for_exactly_that_date(self, spark, tmp_path):
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "A", "noticeType": "T", "publicationDateDay": "2025-03-01"},
            {"objectId": "B", "noticeType": "T", "publicationDateDay": "2025-03-01"},
            {"objectId": "C", "noticeType": "T", "publicationDateDay": "2025-03-02"},
        ])
        result = _br._collect_object_ids_for_date(spark, uri, "2025-03-01")
        assert result == {"A", "B"}

    def test_returns_empty_when_no_data_for_date(self, spark, tmp_path):
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "A", "noticeType": "T", "publicationDateDay": "2025-03-01"},
        ])
        result = _br._collect_object_ids_for_date(spark, uri, "2025-04-01")
        assert result == set()


# ===========================================================================
# Tier 5 — Running-set integration
# ===========================================================================

@_requires_scripts
class TestRunningSetIntegration:
    """Verify the running-set pattern correctly deduplicates across dates."""

    def test_cross_day_duplicate_dropped_by_running_set(self):
        """An ID present in the seen set is dropped from a later date's records."""
        seen = {"X", "Y"}
        records = [{"objectId": "X"}, {"objectId": "Z"}]
        filtered, stats = apply_dedup_filter(records, seen)
        ids = [r["objectId"] for r in filtered]
        assert ids == ["Z"]
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_seen_set_grows_correctly_after_each_date(self):
        """Simulates two sequential date processing steps and verifies accumulation."""
        seen_ids: set[str] = set()

        # Day 1: records A, B, C — none in seen yet
        day1_records = [{"objectId": "A"}, {"objectId": "B"}, {"objectId": "C"}]
        filtered1, _ = apply_dedup_filter(day1_records, seen_ids)
        new_ids1 = {r["objectId"] for r in filtered1}
        seen_ids |= new_ids1
        assert seen_ids == {"A", "B", "C"}

        # Day 2: records B (dup), C (dup), D (new)
        day2_records = [{"objectId": "B"}, {"objectId": "C"}, {"objectId": "D"}]
        filtered2, stats2 = apply_dedup_filter(day2_records, seen_ids)
        ids2 = [r["objectId"] for r in filtered2]
        assert ids2 == ["D"]
        assert stats2["dropped_duplicates_seen_index_other_day"] == 2

    def test_pre_range_ids_block_cross_day_duplicates_on_first_range_date(
        self, spark, tmp_path
    ):
        """IDs from bronze before the range prevent duplication on range day 1."""
        uri = str(tmp_path / "bronze_notices")
        _write_test_bronze(spark, uri, [
            {"objectId": "EARLY", "noticeType": "T", "publicationDateDay": "2025-02-28"},
        ])

        seen_ids = _br._collect_pre_range_object_ids(spark, uri, "2025-03-01")
        assert "EARLY" in seen_ids

        # Day 1 of range includes EARLY (cross-day dup) and NEW
        records = [{"objectId": "EARLY"}, {"objectId": "NEW"}]
        filtered, stats = apply_dedup_filter(records, seen_ids)
        ids = [r["objectId"] for r in filtered]
        assert ids == ["NEW"]
        assert stats["dropped_duplicates_seen_index_other_day"] == 1

    def test_skipped_date_ids_available_for_subsequent_dates(self, spark, tmp_path):
        """A skipped date's IDs are collected and block duplicates on the next date."""
        uri = str(tmp_path / "bronze_notices")

        # Simulate: start_date=2025-03-01, date 2025-03-01 already processed (skipped).
        _write_test_bronze(spark, uri, [
            {"objectId": "SKIP_ID", "noticeType": "T", "publicationDateDay": "2025-03-01"},
        ])

        # Pre-range scan covers nothing (nothing before 2025-03-01)
        seen_ids = _br._collect_pre_range_object_ids(spark, uri, "2025-03-01")
        assert seen_ids == set()

        # Skipped date: collect its IDs and merge
        skipped_ids = _br._collect_object_ids_for_date(spark, uri, "2025-03-01")
        seen_ids |= skipped_ids
        assert "SKIP_ID" in seen_ids

        # Day 2: SKIP_ID should be dropped
        records = [{"objectId": "SKIP_ID"}, {"objectId": "NEW_ID"}]
        filtered, stats = apply_dedup_filter(records, seen_ids)
        ids = [r["objectId"] for r in filtered]
        assert ids == ["NEW_ID"]
        assert stats["dropped_duplicates_seen_index_other_day"] == 1
