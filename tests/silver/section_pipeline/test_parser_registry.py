"""Tier 4 tests for section_pipeline/parser_registry.py.

No Spark session needed — the registry stores Python callables and Spark
DataType *class instances*, both of which are plain Python objects.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

import procurement.silver.section_pipeline.parser_registry as registry
from procurement.silver.section_pipeline.parser_registry import (
    COMMON_PARSERS,
    get_parser_entry,
    registered_fn_names,
)

# ---------------------------------------------------------------------------
# All 6 common parser names that must always be registered
# ---------------------------------------------------------------------------

_EXPECTED_COMMON_PARSERS = {
    "parse_tak_nie",
    "parse_pln_value",
    "parse_criterion_weight",
    "parse_cpv_codes",
    "parse_date_from_text",
    "parse_int_from_text",
}


# ===========================================================================
# COMMON_PARSERS — structure and completeness
# ===========================================================================


def test_all_expected_common_parsers_are_registered():
    assert _EXPECTED_COMMON_PARSERS <= set(COMMON_PARSERS)


def test_every_common_entry_is_a_two_tuple():
    for fn_name, entry in COMMON_PARSERS.items():
        assert isinstance(entry, tuple) and len(entry) == 2, (
            f"{fn_name}: expected (callable, DataType) tuple, got {entry!r}"
        )


def test_every_common_entry_callable_is_callable():
    for fn_name, (fn, _) in COMMON_PARSERS.items():
        assert callable(fn), f"{fn_name}: first element is not callable"


@pytest.mark.parametrize("fn_name,expected_type", [
    ("parse_tak_nie",        BooleanType),
    ("parse_pln_value",      DoubleType),
    ("parse_criterion_weight", IntegerType),
    ("parse_cpv_codes",      ArrayType),
    ("parse_date_from_text", StringType),
    ("parse_int_from_text",  IntegerType),
])
def test_common_parser_spark_type(fn_name, expected_type):
    _, spark_type = COMMON_PARSERS[fn_name]
    assert isinstance(spark_type, expected_type), (
        f"{fn_name}: expected {expected_type.__name__}, got {type(spark_type).__name__}"
    )


# ===========================================================================
# Callables produce correct output (smoke-test each parser via the registry)
# ===========================================================================


@pytest.mark.parametrize("fn_name,raw,expected", [
    ("parse_tak_nie",        "Tak",          True),
    ("parse_tak_nie",        "Nie",          False),
    ("parse_tak_nie",        None,           None),
    ("parse_pln_value",      "100,00 PLN",   100.0),
    ("parse_pln_value",      None,           None),
    ("parse_criterion_weight", "60",         60),
    ("parse_criterion_weight", None,         None),
    ("parse_date_from_text", "2025-10-01",   "2025-10-01"),
    ("parse_date_from_text", None,           None),
    ("parse_int_from_text",  "42",           42),
    ("parse_int_from_text",  None,           None),
])
def test_common_parser_callable_produces_correct_output(fn_name, raw, expected):
    fn, _ = COMMON_PARSERS[fn_name]
    assert fn(raw) == expected


def test_parse_cpv_codes_callable():
    fn, _ = COMMON_PARSERS["parse_cpv_codes"]
    assert fn("45000000-7 Roboty") == ["45000000-7"]


# ===========================================================================
# get_parser_entry
# ===========================================================================


@pytest.mark.parametrize("fn_name", _EXPECTED_COMMON_PARSERS)
def test_get_parser_entry_returns_entry_for_common_fn(fn_name):
    entry = get_parser_entry(fn_name, None)
    assert entry is not None
    assert entry == COMMON_PARSERS[fn_name]


@pytest.mark.parametrize("fn_name", _EXPECTED_COMMON_PARSERS)
def test_get_parser_entry_with_notice_type_falls_back_to_common(fn_name):
    # No type-specific parsers registered → common parsers still returned
    entry = get_parser_entry(fn_name, "ContractNotice")
    assert entry == COMMON_PARSERS[fn_name]


def test_get_parser_entry_unknown_fn_returns_none():
    assert get_parser_entry("nonexistent_fn", None) is None


def test_get_parser_entry_unknown_fn_with_notice_type_returns_none():
    assert get_parser_entry("nonexistent_fn", "ContractNotice") is None


def test_get_parser_entry_type_specific_takes_precedence_over_common(monkeypatch):
    """A notice-type-specific entry shadows the common entry for the same fn_name."""
    def _override_fn(raw):
        return "overridden"

    monkeypatch.setitem(
        registry.NOTICE_TYPE_PARSERS,
        "ContractNotice",
        {"parse_tak_nie": (_override_fn, StringType())},
    )

    entry = get_parser_entry("parse_tak_nie", "ContractNotice")
    assert entry is not None
    fn, spark_type = entry
    assert fn is _override_fn
    assert isinstance(spark_type, StringType)


def test_get_parser_entry_type_specific_does_not_affect_other_types(monkeypatch):
    """Override for ContractNotice must not change what TenderResultNotice sees."""
    def _override_fn(raw):
        return "overridden"

    monkeypatch.setitem(
        registry.NOTICE_TYPE_PARSERS,
        "ContractNotice",
        {"parse_tak_nie": (_override_fn, StringType())},
    )

    entry = get_parser_entry("parse_tak_nie", "TenderResultNotice")
    fn, _ = entry
    assert fn is not _override_fn  # falls back to common


def test_get_parser_entry_type_specific_new_fn_not_visible_to_none(monkeypatch):
    """A fn registered only for a specific type must not appear for notice_type=None."""
    def _type_only_fn(raw):
        return raw

    monkeypatch.setitem(
        registry.NOTICE_TYPE_PARSERS,
        "ContractNotice",
        {"type_only_fn": (_type_only_fn, StringType())},
    )

    assert get_parser_entry("type_only_fn", None) is None


# ===========================================================================
# registered_fn_names
# ===========================================================================


def test_registered_fn_names_none_returns_all_common():
    names = registered_fn_names(None)
    assert _EXPECTED_COMMON_PARSERS <= names


def test_registered_fn_names_known_type_with_no_overrides_equals_common():
    assert registered_fn_names("ContractNotice") == registered_fn_names(None)


def test_registered_fn_names_type_with_override_includes_extra_fn(monkeypatch):
    def _new_fn(raw):
        return raw

    monkeypatch.setitem(
        registry.NOTICE_TYPE_PARSERS,
        "ContractNotice",
        {"brand_new_fn": (_new_fn, StringType())},
    )

    names = registered_fn_names("ContractNotice")
    assert "brand_new_fn" in names
    # Common parsers still present
    assert _EXPECTED_COMMON_PARSERS <= names


def test_registered_fn_names_type_override_not_in_other_type(monkeypatch):
    monkeypatch.setitem(
        registry.NOTICE_TYPE_PARSERS,
        "ContractNotice",
        {"cn_only_fn": (lambda x: x, StringType())},
    )

    assert "cn_only_fn" not in registered_fn_names("TenderResultNotice")
    assert "cn_only_fn" not in registered_fn_names(None)
