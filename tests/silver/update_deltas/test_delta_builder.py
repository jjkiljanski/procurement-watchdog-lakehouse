"""Tests for silver/update_deltas/delta_builder.py."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from procurement.silver.update_deltas.delta_builder import (
    _apply_parser,
    _build_col_type_map,
    _build_schema,
    _build_section_index,
    _extract_section_num,
    _rows_to_table,
    build_update_deltas,
    write_deltas,
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
# _build_schema
# ---------------------------------------------------------------------------

def test_build_schema_contains_meta_fields():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    col_type_map = _build_col_type_map("ContractNotice", idx)
    schema = _build_schema("ContractNotice", col_type_map)
    field_names = [f.name for f in schema]
    for meta in ["nun_objectId", "nun_section_3_2", "section_changes", "parse_errors"]:
        assert meta in field_names


def test_build_schema_section_changes_type():
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    col_type_map = _build_col_type_map("ContractNotice", idx)
    schema = _build_schema("ContractNotice", col_type_map)
    sc_field = next(f for f in schema if f.name == "section_changes")
    assert pa.types.is_list(sc_field.type)
    assert pa.types.is_struct(sc_field.type.value_type)


def test_build_schema_tak_nie_col_is_bool():
    """Any column backed by parse_tak_nie should be pa.bool_() in the schema."""
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    all_profiles = load_all_profiles()
    idx = _build_section_index(all_profiles)
    # Find a notice type that has a parse_tak_nie column
    for nt, type_idx in idx.items():
        for entries in type_idx.values():
            for col_name, fn_name in entries:
                if fn_name == "parse_tak_nie":
                    col_type_map = _build_col_type_map(nt, idx)
                    schema = _build_schema(nt, col_type_map)
                    field = next((f for f in schema if f.name == col_name), None)
                    assert field is not None
                    assert field.type == pa.bool_(), (
                        f"{nt}.{col_name}: expected bool, got {field.type}"
                    )
                    return  # one case is enough


# ---------------------------------------------------------------------------
# build_update_deltas — unit test with fixture parquet data
# ---------------------------------------------------------------------------

def _write_parquet(path: Path, schema: pa.Schema, rows: list[dict]) -> None:
    path.mkdir(parents=True, exist_ok=True)
    arrays = [pa.array([r.get(f.name) for r in rows], type=f.type) for f in schema]
    table = pa.table({f.name: arr for f, arr in zip(schema, arrays)}, schema=schema)
    pq.write_table(table, str(path / "part-0.parquet"))


def _make_fixture_silver(tmp_path: Path, target_date: str) -> None:
    """Write minimal fixture silver data for one NUN that changes a ContractNotice."""
    nun_root = tmp_path / "notice_type_tables" / "noticeType=NoticeUpdateNotice"
    date_part = f"publicationDateDay={target_date}"

    # --- NUN core ---
    core_schema = pa.schema([
        pa.field("objectId", pa.string()),
        pa.field("section_3_2", pa.string()),
        pa.field("section_3_3", pa.string()),
    ])
    _write_parquet(
        nun_root / "data_model=core" / date_part,
        core_schema,
        [{"objectId": "NUN-001", "section_3_2": "2025/BZP 00099999", "section_3_3": "01"}],
    )

    # --- NUN part (section groups) ---
    part_schema = pa.schema([
        pa.field("objectId", pa.string()),
        pa.field("part_ordinal", pa.int64()),
        pa.field("section_3_4", pa.string()),
    ])
    _write_parquet(
        nun_root / "data_model=part" / date_part,
        part_schema,
        [{"objectId": "NUN-001", "part_ordinal": 0, "section_3_4": "SEKCJA II - OPIS"}],
    )

    # --- NUN part_part (individual change items) ---
    pp_schema = pa.schema([
        pa.field("objectId", pa.string()),
        pa.field("part_ordinal", pa.int64()),
        pa.field("part_part_ordinal", pa.int64()),
        pa.field("section_3_4_1_label", pa.string()),
        pa.field("section_3_4_1_before", pa.string()),
        pa.field("section_3_4_1_after", pa.string()),
    ])
    _write_parquet(
        nun_root / "data_model=part_part" / date_part,
        pp_schema,
        [
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
        ],
    )

    # --- common_envelope ---
    env_schema = pa.schema([
        pa.field("bzpNumber", pa.string()),
        pa.field("objectId", pa.string()),
        pa.field("noticeType", pa.string()),
    ])
    _write_parquet(
        tmp_path / "common_envelope" / f"publicationDateDay=2025-01-15",
        env_schema,
        [{"bzpNumber": "2025/BZP 00099999", "objectId": "CN-001", "noticeType": "ContractNotice"}],
    )


def test_build_update_deltas_basic(tmp_path):
    target_date = "2025-04-25"
    _make_fixture_silver(tmp_path, target_date)

    result = build_update_deltas(target_date=target_date, silver_dir=tmp_path)

    assert "ContractNotice" in result
    rows = result["ContractNotice"]
    assert len(rows) == 1
    row = rows[0]

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


def test_build_update_deltas_no_nun_data(tmp_path):
    """When there is no NUN data for the date, build returns empty dict."""
    result = build_update_deltas(target_date="2025-04-25", silver_dir=tmp_path)
    assert result == {}


def test_build_update_deltas_unresolvable_bzp(tmp_path):
    """NUN rows whose section_3_2 is not in the envelope produce no delta rows."""
    target_date = "2025-04-25"
    nun_root = tmp_path / "notice_type_tables" / "noticeType=NoticeUpdateNotice"
    date_part = f"publicationDateDay={target_date}"
    core_schema = pa.schema([
        pa.field("objectId", pa.string()),
        pa.field("section_3_2", pa.string()),
        pa.field("section_3_3", pa.string()),
    ])
    _write_parquet(
        nun_root / "data_model=core" / date_part,
        core_schema,
        [{"objectId": "NUN-X", "section_3_2": "2025/BZP 99999999", "section_3_3": "01"}],
    )
    # common_envelope exists but does NOT have the above BZP number
    env_schema = pa.schema([
        pa.field("bzpNumber", pa.string()),
        pa.field("objectId", pa.string()),
        pa.field("noticeType", pa.string()),
    ])
    _write_parquet(
        tmp_path / "common_envelope" / "publicationDateDay=2025-03-01",
        env_schema,
        [{"bzpNumber": "2025/BZP 00000001", "objectId": "CN-X", "noticeType": "ContractNotice"}],
    )
    result = build_update_deltas(target_date=target_date, silver_dir=tmp_path)
    assert result == {}


# ---------------------------------------------------------------------------
# write_deltas — round-trip test
# ---------------------------------------------------------------------------

def test_write_deltas_round_trip(tmp_path):
    from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
    target_date = "2025-04-25"
    _make_fixture_silver(tmp_path, target_date)

    all_profiles = load_all_profiles()
    section_index = _build_section_index(all_profiles)
    deltas_by_type = build_update_deltas(target_date=target_date, silver_dir=tmp_path)

    write_deltas(
        target_date=target_date,
        deltas_by_type=deltas_by_type,
        silver_dir=tmp_path,
        section_index=section_index,
    )

    out_path = (
        tmp_path / "notice_update_deltas"
        / "noticeType=ContractNotice"
        / f"publicationDateDay={target_date}"
        / "part-0.parquet"
    )
    assert out_path.exists()

    import pyarrow.dataset as ds
    table = ds.dataset(str(out_path.parent), format="parquet").to_table()
    assert table.num_rows == 1
    assert "nun_objectId" in table.schema.names
    assert "section_changes" in table.schema.names
    assert "parse_errors" in table.schema.names

    row = table.to_pylist()[0]
    assert row["nun_objectId"] == "NUN-001"
    assert len(row["section_changes"]) == 2
