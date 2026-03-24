"""Tier 5 tests for section_pipeline/spark_table_builder.py.

Requires a real SparkSession (run inside the procurement-silver Docker container).

Run with:
    docker run --rm \\
        -v "$(pwd)/src:/app/src:ro" \\
        -v "$(pwd)/tests:/app/tests:ro" \\
        -v "$(pwd)/refs:/app/refs:ro" \\
        procurement-silver:deps \\
        sh -c "pip install pytest -q && python -m pytest tests/silver/section_pipeline/test_spark_table_builder.py -v"
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from procurement.silver.section_pipeline.spark_table_builder import (
    apply_column_parsers,
    build_section_tables,
    detect_section_parse_error_quarantine,
    detect_unknown_section_quarantine,
    make_html_sections_udf,
)

# ---------------------------------------------------------------------------
# Minimal test fixtures
# ---------------------------------------------------------------------------

# Each section number "X.Y" → field_name "section_X_Y" (see section_to_field_name)
# col_name in the profile must match this so the JSON keys align.

_CORE_PROFILE = {
    "1.1": {"col_name": "section_1_1", "data_model": "core", "section_header": "Field A"},
    "1.2": {"col_name": "section_1_2", "data_model": "core", "section_header": "Field B"},
}

_MULTI_PROFILE = {
    "1.1": {"col_name": "section_1_1", "data_model": "core",   "section_header": "Core A"},
    "2.1": {"col_name": "section_2_1", "data_model": "client", "section_header": "Client A"},
    "2.2": {"col_name": "section_2_2", "data_model": "client", "section_header": "Client B"},
}

_PART_PROFILE = {
    "1.1": {"col_name": "section_1_1", "data_model": "core", "section_header": "Core A"},
    "4.1": {"col_name": "section_4_1", "data_model": "part", "section_header": "Part A"},
    "4.2": {"col_name": "section_4_2", "data_model": "part", "section_header": "Part B"},
}

_PART_CRITERION_PROFILE = {
    "4.1": {"col_name": "section_4_1", "data_model": "part.core", "section_header": "Part A"},
    "6.1": {"col_name": "section_6_1", "data_model": "part.criterion", "section_header": "Criterion A"},
    "6.2": {"col_name": "section_6_2", "data_model": "part.criterion", "section_header": "Criterion B"},
}

_TEST_NOTICE_TYPE = "TestNotice"

# all_profiles dict passed to make_html_sections_udf — keys are notice_type strings
_ALL_PROFILES = {
    _TEST_NOTICE_TYPE: _CORE_PROFILE,
    "TestMulti": _MULTI_PROFILE,
    "TestPart": _PART_PROFILE,
    "TestPartCriterion": _PART_CRITERION_PROFILE,
}


def _html(*sections: tuple[str, str]) -> str:
    """Produce minimal BZP-format HTML: one <h3> per (section_number, value) pair."""
    parts = []
    for num, val in sections:
        parts.append(f'<h3>{num}) <span class="normal">{val}</span></h3>')
    return "\n".join(parts)


_INPUT_SCHEMA = StructType([
    StructField("objectId",           StringType()),
    StructField("publicationDateDay", StringType()),
    StructField("htmlBody",           StringType()),
])


def _make_df(spark, rows: list[tuple[str, str, str]]):
    return spark.createDataFrame(rows, schema=_INPUT_SCHEMA)


# ===========================================================================
# make_html_sections_udf
# ===========================================================================


class TestMakeHtmlSectionsUdf:
    def test_returns_non_null_for_valid_input(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "alpha"), ("1.2", "beta"))),
        ])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE))
        ).collect()[0]["sections"]
        assert result is not None

    def test_returns_valid_json(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "alpha"))),
        ])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE))
        ).collect()[0]["sections"]
        parsed = json.loads(result)
        assert isinstance(parsed, dict)

    def test_output_contains_core_key(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "alpha"), ("1.2", "beta"))),
        ])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE))
        ).collect()[0]["sections"]
        parsed = json.loads(result)
        assert "core" in parsed

    def test_core_section_values_captured(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "alpha"), ("1.2", "beta"))),
        ])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE))
        ).collect()[0]["sections"]
        parsed = json.loads(result)
        assert parsed["core"]["section_1_1"] == "alpha"
        assert parsed["core"]["section_1_2"] == "beta"

    def test_null_html_returns_null(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", None)])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE))
        ).collect()[0]["sections"]
        assert result is None

    def test_null_notice_type_returns_null(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", _html(("1.1", "val")))])
        from pyspark.sql.functions import col, lit
        null_col = lit(None).cast(StringType())
        result = df.withColumn(
            "sections", udf(col("htmlBody"), null_col)
        ).collect()[0]["sections"]
        assert result is None

    def test_unregistered_notice_type_returns_empty_core(self, spark):
        """Unknown notice type → profile is {} → no sections matched → empty core dict."""
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", _html(("1.1", "val")))])
        from pyspark.sql.functions import col, lit
        result = df.withColumn(
            "sections", udf(col("htmlBody"), lit("UnknownNotice"))
        ).collect()[0]["sections"]
        parsed = json.loads(result)
        assert parsed["core"] == {}
        # Sections present in HTML but absent from profile appear in _unknown_sections
        assert "1.1" in parsed.get("_unknown_sections", [])

    def test_multiple_rows_processed_independently(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "row1_val"))),
            ("obj2", "2025-01-02", _html(("1.1", "row2_val"))),
        ])
        from pyspark.sql.functions import col, lit
        rows = (
            df.withColumn("sections", udf(col("htmlBody"), lit(_TEST_NOTICE_TYPE)))
            .orderBy("objectId")
            .collect()
        )
        assert json.loads(rows[0]["sections"])["core"]["section_1_1"] == "row1_val"
        assert json.loads(rows[1]["sections"])["core"]["section_1_1"] == "row2_val"


# ===========================================================================
# build_section_tables — guard cases
# ===========================================================================


class TestBuildSectionTablesGuards:
    def test_empty_profile_returns_empty_dict_and_none(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", "<html/>")])
        tables, sections_df = build_section_tables(df, _TEST_NOTICE_TYPE, {}, udf)
        assert tables == {}
        assert sections_df is None

    def test_none_notice_type_returns_empty_dict_and_none(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", "<html/>")])
        tables, sections_df = build_section_tables(df, None, _CORE_PROFILE, udf)
        assert tables == {}
        assert sections_df is None

    def test_sections_df_is_returned_for_valid_input(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [("obj1", "2025-01-01", _html(("1.1", "v")))])
        tables, sections_df = build_section_tables(
            df, _TEST_NOTICE_TYPE, _CORE_PROFILE, udf
        )
        try:
            assert sections_df is not None
        finally:
            if sections_df is not None:
                sections_df.unpersist()


# ===========================================================================
# build_section_tables — core model
# ===========================================================================


class TestBuildSectionTablesCore:
    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "alpha"), ("1.2", "beta"))),
            ("obj2", "2025-01-02", _html(("1.1", "gamma"), ("1.2", "delta"))),
        ])
        self._tables, self._sections_df = build_section_tables(
            df, _TEST_NOTICE_TYPE, _CORE_PROFILE, udf
        )
        yield
        if self._sections_df:
            self._sections_df.unpersist()

    def test_core_table_is_in_result(self):
        assert "core" in self._tables

    def test_core_table_has_correct_row_count(self):
        assert self._tables["core"].count() == 2

    def test_core_table_has_objectId_column(self):
        assert "objectId" in self._tables["core"].columns

    def test_core_table_has_publicationDateDay_column(self):
        assert "publicationDateDay" in self._tables["core"].columns

    def test_core_table_has_section_columns(self):
        cols = self._tables["core"].columns
        assert "section_1_1" in cols
        assert "section_1_2" in cols

    def test_core_table_values_are_correct(self):
        rows = (
            self._tables["core"]
            .orderBy("objectId")
            .collect()
        )
        assert rows[0]["section_1_1"] == "alpha"
        assert rows[0]["section_1_2"] == "beta"
        assert rows[1]["section_1_1"] == "gamma"
        assert rows[1]["section_1_2"] == "delta"

    def test_core_table_preserves_objectId(self):
        ids = {r["objectId"] for r in self._tables["core"].collect()}
        assert ids == {"obj1", "obj2"}

    def test_core_table_no_internal_columns(self):
        """Intermediate columns like _sections_json must not leak into output."""
        for c in self._tables["core"].columns:
            assert not c.startswith("_"), f"Internal column leaked: {c!r}"


# ===========================================================================
# build_section_tables — repeating model (client)
# ===========================================================================


class TestBuildSectionTablesRepeating:
    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        # One notice with one client occurrence
        udf = make_html_sections_udf({"TestMulti": _MULTI_PROFILE})
        html = _html(("1.1", "core_v"), ("2.1", "client_a"), ("2.2", "client_b"))
        df = _make_df(spark, [("obj1", "2025-01-01", html)])
        self._tables, self._sections_df = build_section_tables(
            df, "TestMulti", _MULTI_PROFILE, udf
        )
        yield
        if self._sections_df:
            self._sections_df.unpersist()

    def test_client_table_in_result(self):
        assert "client" in self._tables

    def test_client_table_one_row_per_occurrence(self):
        assert self._tables["client"].count() == 1

    def test_client_table_has_ordinal_column(self):
        assert "client_ordinal" in self._tables["client"].columns

    def test_client_ordinal_starts_at_1(self):
        row = self._tables["client"].collect()[0]
        assert row["client_ordinal"] == 1

    def test_client_table_has_section_columns(self):
        cols = self._tables["client"].columns
        assert "section_2_1" in cols
        assert "section_2_2" in cols

    def test_client_table_values_correct(self):
        row = self._tables["client"].collect()[0]
        assert row["section_2_1"] == "client_a"
        assert row["section_2_2"] == "client_b"

    def test_client_table_no_internal_columns(self):
        for c in self._tables["client"].columns:
            assert not c.startswith("_"), f"Internal column leaked: {c!r}"


class TestBuildSectionTablesMultipleOccurrences:
    """Two notices, each with two parts → 4 rows in the part table."""

    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        udf = make_html_sections_udf({"TestPart": _PART_PROFILE})
        # Two parts per notice: same section numbers repeated → triggers new occurrence
        two_parts = _html(
            ("1.1", "core_v"),
            ("4.1", "part1_A"), ("4.2", "part1_B"),
            ("4.1", "part2_A"), ("4.2", "part2_B"),
        )
        one_part = _html(
            ("1.1", "core_v2"),
            ("4.1", "only_A"), ("4.2", "only_B"),
        )
        df = _make_df(spark, [
            ("obj1", "2025-01-01", two_parts),
            ("obj2", "2025-01-02", one_part),
        ])
        self._tables, self._sections_df = build_section_tables(
            df, "TestPart", _PART_PROFILE, udf
        )
        yield
        if self._sections_df:
            self._sections_df.unpersist()

    def test_part_table_exists(self):
        assert "part" in self._tables

    def test_total_rows_equal_sum_of_occurrences(self):
        # obj1 has 2 parts, obj2 has 1 part → 3 rows
        assert self._tables["part"].count() == 3

    def test_ordinals_start_at_1_per_notice(self):
        rows = (
            self._tables["part"]
            .orderBy("objectId", "part_ordinal")
            .collect()
        )
        obj1_rows = [r for r in rows if r["objectId"] == "obj1"]
        obj2_rows = [r for r in rows if r["objectId"] == "obj2"]
        assert [r["part_ordinal"] for r in obj1_rows] == [1, 2]
        assert [r["part_ordinal"] for r in obj2_rows] == [1]


class TestBuildSectionTablesMultipleModels:
    """Profile with core + client → both tables present in result."""

    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        udf = make_html_sections_udf({"TestMulti": _MULTI_PROFILE})
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "cv"), ("2.1", "ca"), ("2.2", "cb"))),
        ])
        self._tables, self._sections_df = build_section_tables(
            df, "TestMulti", _MULTI_PROFILE, udf
        )
        yield
        if self._sections_df:
            self._sections_df.unpersist()

    def test_both_models_present(self):
        assert "core" in self._tables
        assert "client" in self._tables

    def test_two_models_returned(self):
        assert len(self._tables) == 2


class TestBuildSectionTablesNestedChildModel:
    @pytest.fixture(autouse=True)
    def _setup(self, spark):
        udf = make_html_sections_udf({"TestPartCriterion": _PART_CRITERION_PROFILE})
        html = _html(
            ("4.1", "part_1"),
            ("6.1", "criterion_1_a"), ("6.2", "criterion_1_b"),
            ("6.1", "criterion_2_a"), ("6.2", "criterion_2_b"),
        )
        df = _make_df(spark, [("obj1", "2025-01-01", html)])
        self._tables, self._sections_df = build_section_tables(
            df, "TestPartCriterion", _PART_CRITERION_PROFILE, udf
        )
        yield
        if self._sections_df:
            self._sections_df.unpersist()

    def test_parent_and_child_tables_are_both_present(self):
        assert "part" in self._tables
        assert "part_criterion" in self._tables

    def test_parent_table_has_no_nested_items_array(self):
        assert "criterion_items" not in self._tables["part"].columns

    def test_child_table_has_parent_and_child_ordinals(self):
        cols = self._tables["part_criterion"].columns
        assert "part_ordinal" in cols
        assert "part_criterion_ordinal" in cols

    def test_child_table_has_one_row_per_criterion(self):
        assert self._tables["part_criterion"].count() == 2

    def test_child_rows_keep_parent_linkage(self):
        rows = self._tables["part_criterion"].orderBy("part_criterion_ordinal").collect()
        assert [r["part_ordinal"] for r in rows] == [1, 1]
        assert [r["part_criterion_ordinal"] for r in rows] == [1, 2]
        assert rows[0]["section_6_1"] == "criterion_1_a"
        assert rows[1]["section_6_1"] == "criterion_2_a"


# ===========================================================================
# detect_unknown_section_quarantine
# ===========================================================================


class TestDetectUnknownSectionQuarantine:
    """detect_unknown_section_quarantine — quarantines rows with unknown sections."""

    def test_none_sections_df_returns_none(self):
        from procurement.silver.section_pipeline.spark_table_builder import detect_unknown_section_quarantine
        assert detect_unknown_section_quarantine(None, "ContractNotice") is None

    def test_none_notice_type_returns_none(self, spark):
        from procurement.silver.section_pipeline.spark_table_builder import detect_unknown_section_quarantine
        from pyspark.sql.types import StringType, StructField, StructType
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {}, "_unknown_sections": ["9.9"]}')],
            schema=StructType([
                StructField("objectId", StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("_sections_json", StringType()),
            ]),
        )
        assert detect_unknown_section_quarantine(df, None) is None

    def test_no_unknown_sections_returns_empty_df(self, spark):
        from procurement.silver.section_pipeline.spark_table_builder import detect_unknown_section_quarantine
        from pyspark.sql.types import StringType, StructField, StructType
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {"section_1_1": "alpha"}}')],
            schema=StructType([
                StructField("objectId", StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("_sections_json", StringType()),
            ]),
        )
        result = detect_unknown_section_quarantine(df, "ContractNotice")
        assert result is not None
        assert result.count() == 0

    def test_unknown_section_row_quarantined(self, spark):
        from procurement.silver.section_pipeline.spark_table_builder import detect_unknown_section_quarantine
        from pyspark.sql.types import StringType, StructField, StructType
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {}, "_unknown_sections": ["9.9", "8.8"]}')],
            schema=StructType([
                StructField("objectId", StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("_sections_json", StringType()),
            ]),
        )
        result = detect_unknown_section_quarantine(df, "ContractNotice")
        assert result is not None
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["objectId"] == "obj1"
        assert rows[0]["notice_type"] == "ContractNotice"
        assert rows[0]["data_model"] == "unknown"
        errors = rows[0]["_parse_errors"]
        assert any("unknown section: 9.9" in e for e in errors)
        assert any("unknown section: 8.8" in e for e in errors)

    def test_only_rows_with_unknown_sections_quarantined(self, spark):
        from procurement.silver.section_pipeline.spark_table_builder import detect_unknown_section_quarantine
        from pyspark.sql.types import StringType, StructField, StructType
        df = spark.createDataFrame(
            [
                ("obj1", "2025-01-01", '{"core": {}, "_unknown_sections": ["9.9"]}'),
                ("obj2", "2025-01-02", '{"core": {"section_1_1": "ok"}}'),
            ],
            schema=StructType([
                StructField("objectId", StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("_sections_json", StringType()),
            ]),
        )
        result = detect_unknown_section_quarantine(df, "ContractNotice")
        assert result is not None
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["objectId"] == "obj1"


# ===========================================================================
# detect_section_parse_error_quarantine
# ===========================================================================


_SECTIONS_SCHEMA = StructType([
    StructField("objectId",           StringType()),
    StructField("publicationDateDay", StringType()),
    StructField("_sections_json",     StringType()),
])


class TestDetectSectionParseErrorQuarantine:
    """detect_section_parse_error_quarantine — quarantines rows with HTML parse errors."""

    def test_none_sections_df_returns_none(self):
        assert detect_section_parse_error_quarantine(None, "ContractNotice") is None

    def test_none_notice_type_returns_none(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {}, "_parse_errors": ["duplicate_core_section: ..."]}')],
            schema=_SECTIONS_SCHEMA,
        )
        assert detect_section_parse_error_quarantine(df, None) is None

    def test_no_parse_errors_returns_empty_df(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {"section_1_1": "alpha"}}')],
            schema=_SECTIONS_SCHEMA,
        )
        result = detect_section_parse_error_quarantine(df, "ContractNotice")
        assert result is not None
        assert result.count() == 0

    def test_parse_error_row_quarantined(self, spark):
        error_msg = "duplicate_core_section: notice_type='AgreementUpdateNotice' section='3.7' field='section_3_7'"
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", f'{{"core": {{}}, "_parse_errors": ["{error_msg}"]}}')],
            schema=_SECTIONS_SCHEMA,
        )
        result = detect_section_parse_error_quarantine(df, "AgreementUpdateNotice")
        assert result is not None
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["objectId"] == "obj1"
        assert rows[0]["notice_type"] == "AgreementUpdateNotice"
        assert rows[0]["data_model"] == "core"
        errors = rows[0]["_parse_errors"]
        assert any("duplicate_core_section" in e for e in errors)

    def test_only_error_rows_quarantined(self, spark):
        error_msg = "duplicate_core_section: ..."
        df = spark.createDataFrame(
            [
                ("obj1", "2025-01-01", f'{{"core": {{}}, "_parse_errors": ["{error_msg}"]}}'),
                ("obj2", "2025-01-02", '{"core": {"section_1_1": "ok"}}'),
            ],
            schema=_SECTIONS_SCHEMA,
        )
        result = detect_section_parse_error_quarantine(df, "ContractNotice")
        assert result is not None
        rows = result.collect()
        assert len(rows) == 1
        assert rows[0]["objectId"] == "obj1"

    def test_no_internal_columns_in_output(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", '{"core": {}, "_parse_errors": ["err"]}')],
            schema=_SECTIONS_SCHEMA,
        )
        result = detect_section_parse_error_quarantine(df, "ContractNotice")
        assert result is not None
        for c in result.columns:
            assert not c.startswith("_html_"), f"Internal column leaked: {c!r}"


class TestBuildSectionTablesParseErrorExclusion:
    """Rows with HTML _parse_errors must be excluded from section model tables."""

    def test_duplicate_core_section_row_excluded_from_core_table(self, spark):
        # obj1: valid notice
        # obj2: duplicate core section 1.1 → _parse_errors in sections JSON
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "good"), ("1.2", "also_good"))),
            ("obj2", "2025-01-02", _html(("1.1", "first"), ("1.1", "duplicate"))),
        ])
        tables, sections_df = build_section_tables(df, _TEST_NOTICE_TYPE, _CORE_PROFILE, udf)
        try:
            core = tables["core"]
            rows = {r["objectId"] for r in core.collect()}
            assert "obj1" in rows
            assert "obj2" not in rows, "Row with duplicate section must be excluded from core table"
        finally:
            if sections_df:
                sections_df.unpersist()

    def test_valid_rows_unaffected_by_errored_peers(self, spark):
        udf = make_html_sections_udf(_ALL_PROFILES)
        df = _make_df(spark, [
            ("obj1", "2025-01-01", _html(("1.1", "good"))),
            ("obj2", "2025-01-02", _html(("1.1", "first"), ("1.1", "dup"))),
        ])
        tables, sections_df = build_section_tables(df, _TEST_NOTICE_TYPE, _CORE_PROFILE, udf)
        try:
            row = tables["core"].filter("objectId = 'obj1'").collect()[0]
            assert row["section_1_1"] == "good"
        finally:
            if sections_df:
                sections_df.unpersist()


# ===========================================================================
# apply_column_parsers
# ===========================================================================


class TestApplyColumnParsers:
    def test_empty_parsers_profile_returns_same_tables(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", "Tak")],
            schema=StructType([
                StructField("objectId", StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1", StringType()),
            ]),
        )
        tables = {"core": df}
        result_tables, quarantine_df, _ = apply_column_parsers(tables, _CORE_PROFILE, "ContractNotice")
        # _CORE_PROFILE has no parsers → same object returned, no quarantine
        assert result_tables is tables
        assert quarantine_df is None

    def test_empty_section_tables_returns_unchanged(self, spark):
        result_tables, quarantine_df, _ = apply_column_parsers({}, _CORE_PROFILE, "ContractNotice")
        assert result_tables == {}
        assert quarantine_df is None

    def test_parser_converts_string_column_to_typed(self, spark):
        """parse_tak_nie parser applied → column becomes BooleanType."""
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", "Tak"), ("obj2", "2025-01-02", "Nie")],
            schema=StructType([
                StructField("objectId",           StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1",        StringType()),
            ]),
        )
        profile_with_parser = {
            "1.1": {
                "col_name": "section_1_1",
                "data_model": "core",
                "section_header": "Header",
                "parser": {"fn": "parse_tak_nie"},
            },
        }
        tables = {"core": df}
        result_tables, quarantine_df, _ = apply_column_parsers(tables, profile_with_parser, "ContractNotice")
        col_type = dict(result_tables["core"].dtypes)["section_1_1"]
        assert col_type == "boolean"

    def test_parser_produces_correct_boolean_values(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", "Tak"), ("obj2", "2025-01-02", "Nie")],
            schema=StructType([
                StructField("objectId",           StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1",        StringType()),
            ]),
        )
        profile_with_parser = {
            "1.1": {
                "col_name": "section_1_1",
                "data_model": "core",
                "section_header": "Header",
                "parser": {"fn": "parse_tak_nie"},
            },
        }
        result_tables, _, _ = apply_column_parsers({"core": df}, profile_with_parser, "ContractNotice")
        rows = {r["objectId"]: r["section_1_1"] for r in result_tables["core"].collect()}
        assert rows["obj1"] is True
        assert rows["obj2"] is False

    def test_parser_null_input_gives_null_output(self, spark):
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", None)],
            schema=StructType([
                StructField("objectId",           StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1",        StringType()),
            ]),
        )
        profile_with_parser = {
            "1.1": {
                "col_name": "section_1_1",
                "data_model": "core",
                "section_header": "Header",
                "parser": {"fn": "parse_tak_nie"},
            },
        }
        result_tables, _, _ = apply_column_parsers({"core": df}, profile_with_parser, "ContractNotice")
        row = result_tables["core"].collect()[0]
        assert row["section_1_1"] is None

    def test_unparsed_columns_remain_string(self, spark):
        """Only the column with a parser should change type; others stay StringType."""
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", "Tak", "unchanged")],
            schema=StructType([
                StructField("objectId",           StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1",        StringType()),
                StructField("section_1_2",        StringType()),
            ]),
        )
        profile = {
            "1.1": {
                "col_name": "section_1_1",
                "data_model": "core",
                "section_header": "Header",
                "parser": {"fn": "parse_tak_nie"},
            },
            "1.2": {
                "col_name": "section_1_2",
                "data_model": "core",
                "section_header": "Header",
                # no parser
            },
        }
        result_tables, _, _ = apply_column_parsers({"core": df}, profile, "ContractNotice")
        dtypes = dict(result_tables["core"].dtypes)
        assert dtypes["section_1_1"] == "boolean"
        assert dtypes["section_1_2"] == "string"

    def test_col_not_in_dataframe_silently_skipped(self, spark):
        """Parser configured for a column not in the DataFrame → no error."""
        df = spark.createDataFrame(
            [("obj1", "2025-01-01", "Tak")],
            schema=StructType([
                StructField("objectId",           StringType()),
                StructField("publicationDateDay", StringType()),
                StructField("section_1_1",        StringType()),
            ]),
        )
        profile = {
            "1.1": {
                "col_name": "section_1_1",
                "data_model": "core",
                "section_header": "Header",
                "parser": {"fn": "parse_tak_nie"},
            },
            "9.9": {
                "col_name": "section_9_9",       # not in df
                "data_model": "core",
                "section_header": "Missing",
                "parser": {"fn": "parse_tak_nie"},
            },
        }
        result_tables, _, _ = apply_column_parsers({"core": df}, profile, "ContractNotice")
        # should not raise; section_1_1 is still parsed
        assert dict(result_tables["core"].dtypes)["section_1_1"] == "boolean"
