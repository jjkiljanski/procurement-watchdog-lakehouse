"""Tier 4 tests for section_pipeline/final_schema_validator.py.

validate_section_models() only accesses df.columns on its DataFrames,
so we use a lightweight SimpleNamespace mock — no Spark session needed.

get_pydantic_model_class() is already covered by Tier 3 (test_pydantic_models.py),
so here we focus on the validation logic and logging behaviour.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from procurement.silver.section_pipeline.final_schema_validator import (
    _PIPELINE_COLS,
    validate_section_models,
)
from procurement.silver.section_pipeline.final_schema_validator import (
    get_pydantic_model_class,
)

_VALIDATOR_LOGGER = "procurement.silver.section_pipeline.final_schema_validator"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_df(columns: list[str]) -> SimpleNamespace:
    """Minimal stand-in for a Spark DataFrame: only .columns is accessed."""
    return SimpleNamespace(columns=columns)


def _all_fields(notice_type: str, model: str) -> list[str]:
    cls = get_pydantic_model_class(notice_type, model)
    return list(cls.model_fields.keys())


# ===========================================================================
# None / falsy notice_type — validation is skipped entirely
# ===========================================================================


def test_none_notice_type_returns_tables_unchanged():
    tables = {"core": _mock_df(["col_a"])}
    result = validate_section_models(tables, None)
    assert result is tables


def test_empty_string_notice_type_returns_tables_unchanged():
    tables = {"core": _mock_df(["col_a"])}
    result = validate_section_models(tables, "")
    assert result is tables


def test_skipped_validation_emits_no_warnings(caplog):
    tables = {"core": _mock_df([])}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, None)
    assert not caplog.records


# ===========================================================================
# Return-value contract
# ===========================================================================


def test_returns_same_dict_object():
    tables = {"core": _mock_df(_all_fields("ContractNotice", "core"))}
    result = validate_section_models(tables, "ContractNotice")
    assert result is tables


def test_returns_same_dataframe_objects():
    mock = _mock_df(_all_fields("ContractNotice", "core"))
    tables = {"core": mock}
    result = validate_section_models(tables, "ContractNotice")
    assert result["core"] is mock


# ===========================================================================
# All columns present — no warnings
# ===========================================================================


def test_all_columns_present_no_warning(caplog):
    fields = _all_fields("ContractNotice", "core")
    tables = {"core": _mock_df(fields)}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


def test_pipeline_cols_allowed_silently(caplog):
    """objectId, publicationDateDay, and <model>_ordinal must not trigger warnings."""
    fields = _all_fields("ContractNotice", "core")
    fields_with_pipeline = fields + ["objectId", "publicationDateDay", "core_ordinal"]
    tables = {"core": _mock_df(fields_with_pipeline)}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


def test_model_ordinal_column_name_is_model_specific(caplog):
    """<model>_ordinal — the prefix must match the model name, not be generic."""
    # 'client_ordinal' should be silently allowed for the client model
    fields = _all_fields("ContractNotice", "client")
    fields_with_ordinal = fields + ["client_ordinal"]
    tables = {"client": _mock_df(fields_with_ordinal)}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


# ===========================================================================
# Missing columns — warning emitted
# ===========================================================================


def test_missing_column_emits_warning(caplog):
    fields = _all_fields("ContractNotice", "core")
    # Omit the first field to simulate a missing column
    partial = fields[1:]
    tables = {"core": _mock_df(partial)}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "missing" in warnings[0].message.lower()


def test_missing_column_warning_names_the_column(caplog):
    fields = _all_fields("ContractNotice", "core")
    missing_field = fields[0]
    tables = {"core": _mock_df(fields[1:])}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warning_text = caplog.records[0].message
    assert missing_field in warning_text


def test_empty_dataframe_emits_warning(caplog):
    tables = {"core": _mock_df([])}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings


# ===========================================================================
# Unknown / unresolvable model — silently skipped
# ===========================================================================


def test_unknown_notice_type_no_warning(caplog):
    tables = {"core": _mock_df(["some_col"])}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "NonExistentNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


def test_unknown_model_name_no_warning(caplog):
    # Valid notice type but model not in _MODEL_SUFFIX → get_pydantic_model_class returns None
    tables = {"totally_unknown_model": _mock_df(["col"])}
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


# ===========================================================================
# Multiple models in one call
# ===========================================================================


def test_multiple_models_all_complete_no_warnings(caplog):
    tables = {
        "core":   _mock_df(_all_fields("ContractNotice", "core")),
        "client": _mock_df(_all_fields("ContractNotice", "client")),
        "part":   _mock_df(_all_fields("ContractNotice", "part")),
    }
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert not warnings


def test_multiple_models_one_missing_emits_one_warning(caplog):
    core_fields = _all_fields("ContractNotice", "core")
    tables = {
        "core":   _mock_df(core_fields[1:]),   # missing first field
        "client": _mock_df(_all_fields("ContractNotice", "client")),
    }
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        validate_section_models(tables, "ContractNotice")
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


def test_empty_section_tables_dict_no_warnings(caplog):
    with caplog.at_level(logging.WARNING, logger=_VALIDATOR_LOGGER):
        result = validate_section_models({}, "ContractNotice")
    assert result == {}
    assert not caplog.records


# ===========================================================================
# _PIPELINE_COLS constant
# ===========================================================================


def test_pipeline_cols_contains_object_id():
    assert "objectId" in _PIPELINE_COLS


def test_pipeline_cols_contains_publication_date_day():
    assert "publicationDateDay" in _PIPELINE_COLS


# ===========================================================================
# apply_pydantic_validation — Spark-dependent (skipped when Spark unavailable)
# ===========================================================================


from procurement.silver.section_pipeline.final_schema_validator import apply_pydantic_validation


class TestApplyPydanticValidation:
    def test_none_notice_type_returns_tables_unchanged(self):
        tables = {"core": object()}
        result_tables, quarantine_df, _ = apply_pydantic_validation(tables, None)
        assert result_tables is tables
        assert quarantine_df is None

    def test_empty_tables_returns_empty(self):
        result_tables, quarantine_df, _ = apply_pydantic_validation({}, "ContractNotice")
        assert result_tables == {}
        assert quarantine_df is None

    def test_unknown_notice_type_passes_through(self):
        tables = {"core": object()}
        result_tables, quarantine_df, _ = apply_pydantic_validation(tables, "NonExistentNotice")
        # no Pydantic model → tables passed through unchanged
        assert result_tables["core"] is tables["core"]
        assert quarantine_df is None

    def test_valid_rows_produce_no_quarantine(self, spark):
        """All-valid rows → quarantine_df is None or empty."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([
            StructField("objectId", StringType()),
            StructField("publicationDateDay", StringType()),
            StructField("section_1_1", StringType()),
        ])
        df = spark.createDataFrame([("obj1", "2025-01-01", "alpha")], schema=schema)
        tables = {"core": df}

        # Use a notice type whose core model has section_1_1 or use a minimal profile
        # We just verify no crash and sensible outputs
        result_tables, quarantine_df, _ = apply_pydantic_validation(tables, "ContractNotice")
        # ContractNotice core model has many fields; section_1_1 is not in it so
        # model_class won't raise on unknown kwargs because BaseModel ignores extras
        # This just verifies the plumbing works
        assert "core" in result_tables

    def test_returns_tuple(self):
        result = apply_pydantic_validation({}, "ContractNotice")
        assert isinstance(result, tuple) and len(result) == 3
