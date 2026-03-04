"""Registry of column-level parser functions for section DataFrames.

Each registered parser transforms a single raw StringType section value into a
typed output value.  Format:  fn_name -> (callable, Spark DataType).

Allowed source modules
----------------------
- common   : procurement.silver.html_value_parsers.common_values
- per-type : procurement.silver.html_value_parsers.<snake_type_name>

Configuring a parser for a column (via "parser" key in the sections profile
JSON) will change that column's Spark type.  The corresponding Pydantic section
model field should be updated to match when Gold types are introduced.
"""

from __future__ import annotations

from pyspark.sql.types import ArrayType, BooleanType, DoubleType, IntegerType, StringType

from procurement.silver.html_value_parsers.common_values import (
    _parse_criterion_weight,
    _parse_pln_value,
    _parse_tak_nie,
    parse_cpv_codes,
    parse_date_from_text,
    parse_int_from_text,
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
}

# ---------------------------------------------------------------------------
# Notice-type-specific parsers — extend or override common parsers.
# Keyed by camelCase notice type name (same as NOTICE_TYPE_TO_PROFILE_KEY keys).
# Populate when notice-type-specific column parsers are implemented in
# html_value_parsers/<snake_type>.py and expose them here.
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
