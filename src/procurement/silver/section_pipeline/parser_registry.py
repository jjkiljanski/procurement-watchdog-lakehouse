"""Registry of column-level parser functions for section DataFrames.

Each registered parser transforms a single raw StringType section value into a
typed output value.  Format:  fn_name -> (callable, Spark DataType).

Allowed source modules
----------------------
- common   : procurement.silver.section_value_parsers.common
- per-type : procurement.silver.section_value_parsers.<snake_type_name>

Configuring a parser for a column (via "parser" key in the sections profile
JSON) will change that column's Spark type.  The corresponding Pydantic section
model field should be updated to match when Gold types are introduced.
"""

from __future__ import annotations

from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, StringType

from procurement.silver.section_value_parsers.common import (
    _parse_criterion_weight,
    _parse_pln_value,
    _parse_tak_nie,
    compute_contract_end_date,
    compute_duration_days,
    parse_cpv_codes,
    parse_date_from_text,
    parse_int_from_text,
    parse_national_id_type,
    parse_national_id_value,
    parse_nuts3_code,
    parse_nuts3_name,
)

# ---------------------------------------------------------------------------
# Common parsers — available to all notice types
# ---------------------------------------------------------------------------

COMMON_PARSERS: dict[str, tuple] = {
    "parse_tak_nie": (_parse_tak_nie, BooleanType()),
    "parse_pln_value": (_parse_pln_value, DoubleType()),
    "parse_criterion_weight": (_parse_criterion_weight, IntegerType()),
    "parse_cpv_codes": (parse_cpv_codes, ArrayType(StringType())),
    "parse_date_from_text": (parse_date_from_text, StringType()),
    "parse_int_from_text": (parse_int_from_text, IntegerType()),
    "parse_nuts3_code": (parse_nuts3_code, StringType()),
    "parse_nuts3_name": (parse_nuts3_name, StringType()),
    "parse_national_id_value": (parse_national_id_value, StringType()),
    "parse_national_id_type": (parse_national_id_type, StringType()),
}

# ---------------------------------------------------------------------------
# Multi-source computed parsers — take N column values, produce one output.
# Registered separately from single-arg parsers because their UDFs are called
# with multiple column arguments by apply_column_parsers.
# ---------------------------------------------------------------------------

COMPUTED_PARSERS: dict[str, tuple] = {
    "compute_duration_days":      (compute_duration_days,      IntegerType()),
    "compute_contract_end_date":  (compute_contract_end_date,  StringType()),
}


def get_computed_entry(fn_name: str) -> tuple | None:
    """Return (callable, Spark DataType) for a multi-arg computed function."""
    return COMPUTED_PARSERS.get(fn_name)


def registered_computed_fn_names() -> set[str]:
    """Return all registered computed function names."""
    return set(COMPUTED_PARSERS)


# ---------------------------------------------------------------------------
# Notice-type-specific parsers — extend or override common parsers.
# Keyed by camelCase notice type name (same as NOTICE_TYPE_TO_PROFILE_KEY keys).
# Populate when notice-type-specific column parsers are implemented in
# section_value_parsers/<snake_type_name>.py and registered here.
# ---------------------------------------------------------------------------

NOTICE_TYPE_PARSERS: dict[str, dict[str, tuple]] = {
    # "ContractNotice": {
    #     "my_custom_fn": (my_custom_fn, StringType()),
    # },
}


def get_parser_entry(fn_name: str, notice_type: str | None) -> tuple | None:
    """Return (callable, Spark DataType) for fn_name, or None if not registered.

    Notice-type-specific parsers take precedence over common parsers when both
    register the same fn_name.
    """
    if notice_type:
        entry = NOTICE_TYPE_PARSERS.get(notice_type, {}).get(fn_name)
        if entry is not None:
            return entry
    return COMMON_PARSERS.get(fn_name)


def registered_fn_names(notice_type: str | None = None) -> set[str]:
    """Return all registered function names visible to a given notice type."""
    names = set(COMMON_PARSERS)
    if notice_type:
        names |= set(NOTICE_TYPE_PARSERS.get(notice_type, {}))
    return names
