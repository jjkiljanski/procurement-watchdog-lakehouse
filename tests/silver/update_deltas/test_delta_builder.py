"""Tests for silver/update_deltas/delta_builder.py."""

from __future__ import annotations

import pytest
from pyspark.sql.types import ArrayType, BooleanType, StructType

from procurement.silver.update_deltas.delta_builder import (
    _apply_parser,
    _build_col_type_map,
    _build_delta_spark_schema,
    _build_section_index,
    _extract_section_num,
    build_update_deltas,
)

# ---------------------------------------------------------------------------
# _extract_section_num
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label, expected", [
    ("4.2.2.  Krótki opis przedmiotu zamówienia", "4.2.2"),
    ("3.1.) Nazwa zmienianego ogłoszenia",        "3.1"),
    ("10.1.3 Jakiś nagłówek",                    "10.1.3"),
    ("SEKCJA IV - bez numeru",                    None),
    ("",                                           None),
    (None,                                         None),
    ("  5.4.  Warunki udziału",                   "5.4"),
])
def test_extract_section_num(label, expected):
    assert _extract_section_num(label) == expected


# ---------------------------------------------------------------------------
# _apply_parser
# ---------------------------------------------------------------------------

def test_apply_parser_no_fn():
    val, err = _apply_parser(None, "raw text")
    assert val == "raw text"
    assert err is None


def test_apply_parser_none_input():
    val, err = _apply_parser("parse_tak_nie", None)
    assert val is None
    assert err is None


def test_apply_parser_tak_nie_success():
    val, err = _apply_parser("parse_tak_nie", "Tak")
    assert val is True
    assert err is None


def test_apply_parser_tak_nie_failure():
    val, err = _apply_parser("parse_tak_nie", "free text that is not yes or no")
    assert val is None
    assert err is not None
    assert "parse_tak_nie" in err


def test_apply_parser_unknown_fn():
    val, err = _apply_parser("nonexistent_parser", "some text")
    # Falls back to returning raw text + error note
    assert val == "some text"
    assert err is not None
    assert "nonexistent_parser" in err


def test_apply_parser_pln_value():
    val, err = _apply_parser("parse_pln_value", "1 234,56 PLN")
    assert err is None
    assert abs(val - 1234.56) < 0.01


def test_apply_parser_int_from_text():
    val, err = _apply_parser("parse_int_from_text", "12 miesięcy")
    assert err is None
    assert val == 12


# ---------------------------------------------------------------------------
# _build_section_index
# ---------------------------------------------------------------------------

def test_build_section_index_excludes_nun():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    assert "NoticeUpdateNotice" not in idx


def test_build_section_index_only_core_sections():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    # ContractNotice has both core and part sections; only core should appear
    cn_idx = idx.get("ContractNotice", {})
    # Section 4.2.2 is part-level in ContractNotice → must NOT appear
    assert "4.2.2" not in cn_idx
    # Section 2.5 is core-level in ContractNotice → must appear
    assert "2.5" in cn_idx


def test_build_section_index_derived_cols_expanded():
    """Sections with derived_cols should map to the derived col names, not the source."""
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    # Section 1.4.6 in ContractNotice has derived_cols for nuts3 code and name
    # The entries should reference derived col names, not 'section_1_4_6'
    for notice_type, type_idx in idx.items():
        entries = type_idx.get("1.4.6")
        if entries is None:
            continue
        col_names = [col for col, _ in entries]
        assert "section_1_4_6" not in col_names, (
            f"{notice_type}: source col 'section_1_4_6' should be replaced by derived cols"
        )
        assert any("nuts3" in c or "1_4_6" in c for c in col_names)


# ---------------------------------------------------------------------------
# _build_delta_spark_schema
# ---------------------------------------------------------------------------

def test_build_delta_spark_schema_contains_meta_fields():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    col_type_map = _build_col_type_map("ContractNotice", idx)
    schema = _build_delta_spark_schema(col_type_map)
    field_names = [f.name for f in schema.fields]
    for meta in ["publicationDateDay", "nun_objectId", "nun_section_3_2", "section_changes", "parse_errors"]:
        assert meta in field_names


def test_build_delta_spark_schema_section_changes_type():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    col_type_map = _build_col_type_map("ContractNotice", idx)
    schema = _build_delta_spark_schema(col_type_map)
    sc_field = schema["section_changes"]
    assert isinstance(sc_field.dataType, ArrayType)
    assert isinstance(sc_field.dataType.elementType, StructType)


def test_build_delta_spark_schema_tak_nie_col_is_bool():
    """Any column backed by parse_tak_nie should be BooleanType in the schema."""
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    for nt, type_idx in idx.items():
        for entries in type_idx.values():
            for col_name, fn_name in entries:
                if fn_name == "parse_tak_nie":
                    col_type_map = _build_col_type_map(nt, idx)
                    schema = _build_delta_spark_schema(col_type_map)
                    field = schema[col_name]
                    assert isinstance(field.dataType, BooleanType), (
                        f"{nt}.{col_name}: expected BooleanType, got {field.dataType}"
                    )
                    return  # one case is enough


# ---------------------------------------------------------------------------
# build_update_deltas — pure Python tests (no file I/O, no Spark)
# ---------------------------------------------------------------------------

def _make_section_index():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    return _build_section_index(load_all_profiles())


def _make_bzp_index():
    return {"2025/BZP 00099999": ("CN-001", "ContractNotice", "2025-01-15")}


def test_build_update_deltas_basic():
    section_index = _make_section_index()
    core_rows = [{"objectId": "NUN-001", "section_3_2": "2025/BZP 00099999", "section_3_3": "01"}]
    part_rows = [{"objectId": "NUN-001", "part_ordinal": 0, "section_3_4": "SEKCJA II - OPIS"}]
    part_part_rows = [
        {
            "objectId": "NUN-001", "part_ordinal": 0, "part_part_ordinal": 0,
            "section_3_4_1_label": "2.5.  Numer ogłoszenia",
            "section_3_4_1_before": "2025/BZP 00099999/01",
            "section_3_4_1_after": "2025/BZP 00099999/02",
        },
        {
            "objectId": "NUN-001", "part_ordinal": 0, "part_part_ordinal": 1,
            "section_3_4_1_label": "SEKCJA BEZ NUMERU - nie mapowalna",
            "section_3_4_1_before": "stare",
            "section_3_4_1_after": "nowe",
        },
    ]

    result = build_update_deltas(
        target_date="2025-04-25",
        core_rows=core_rows,
        part_rows=part_rows,
        part_part_rows=part_part_rows,
        section_index=section_index,
        bzp_index=_make_bzp_index(),
    )

    assert "ContractNotice" in result
    rows = result["ContractNotice"]
    assert len(rows) == 1
    row = rows[0]

    assert row["publicationDateDay"] == "2025-04-25"
    assert row["nun_objectId"] == "NUN-001"
    assert row["target_objectId"] == "CN-001"
    assert row["target_publicationDateDay"] == "2025-01-15"

    # section_changes must contain both change items (including unmappable one)
    changes = row["section_changes"]
    assert len(changes) == 2
    assert all(c["section_prefix"] == "SEKCJA II - OPIS" for c in changes)

    # The mappable section (2.5) should populate a column
    assert row.get("section_2_5") == "2025/BZP 00099999/02"

    # The unmappable item should not raise; it just stays in section_changes
    assert row.get("parse_errors") is None or "section_2_5" not in (row.get("parse_errors") or "")


def test_build_update_deltas_no_nun_data():
    """When core_rows is empty, build returns empty dict."""
    section_index = _make_section_index()
    result = build_update_deltas(
        target_date="2025-04-25",
        core_rows=[],
        part_rows=[],
        part_part_rows=[],
        section_index=section_index,
        bzp_index={},
    )
    assert result == {}


def test_build_update_deltas_unresolvable_bzp():
    """NUN rows whose section_3_2 is not in the index produce no delta rows."""
    section_index = _make_section_index()
    core_rows = [{"objectId": "NUN-X", "section_3_2": "2025/BZP 99999999", "section_3_3": "01"}]
    # BZP index does NOT contain the above number
    bzp_index = {"2025/BZP 00000001": ("CN-X", "ContractNotice", "2025-03-01")}

    result = build_update_deltas(
        target_date="2025-04-25",
        core_rows=core_rows,
        part_rows=[],
        part_part_rows=[],
        section_index=section_index,
        bzp_index=bzp_index,
    )
    assert result == {}


def test_build_update_deltas_includes_publication_date_day():
    """Every delta row must include publicationDateDay = target_date."""
    section_index = _make_section_index()
    core_rows = [{"objectId": "NUN-001", "section_3_2": "2025/BZP 00099999", "section_3_3": "01"}]

    result = build_update_deltas(
        target_date="2025-06-15",
        core_rows=core_rows,
        part_rows=[],
        part_part_rows=[],
        section_index=section_index,
        bzp_index=_make_bzp_index(),
    )

    rows = result.get("ContractNotice", [])
    assert len(rows) == 1
    assert rows[0]["publicationDateDay"] == "2025-06-15"
