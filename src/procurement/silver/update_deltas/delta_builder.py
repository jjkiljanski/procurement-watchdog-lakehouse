"""Build notice-change delta records from NoticeUpdateNotice silver data.

For each day's NoticeUpdateNotice rows the builder:

1. Joins NUN core + part + part_part tables (all within the same day) to
   produce one change-item row per section changed per NUN.
2. Resolves the original notice type by joining ``section_3_2`` (target BZP
   number) against ``bzpNumber`` in the common_envelope (scoped to the years
   found in the NUN data to avoid a full-history scan).
3. Extracts the section number from each 3.4.1 label (e.g. ``"4.2.2.  Krótki
   opis..."`` → ``"4.2.2"``), looks it up in the target notice type's profile,
   and runs the registered column parser on the ``after`` text.
4. Pivots to one wide delta row per NUN, with:
   - All core-schema columns of the original notice type (NULL = not changed,
     typed value = changed + parsed).
   - ``parse_errors`` (JSON string): ``{col_name: error}`` for columns where
     parsing failed.  NULL typed columns still appear with NULL; errors tell
     downstream consumers *why* they are NULL.
   - ``section_changes`` (list of structs): flat raw change records preserved
     verbatim for every 3.4.1 item, regardless of whether it could be mapped
     to a column.  Format: ``{section_prefix, label, before, after}``.
   - NUN metadata columns: ``nun_objectId``, ``nun_section_3_2``,
     ``nun_section_3_3``, ``target_objectId``, ``target_publicationDateDay``.

Output path:
    <silver_dir>/notice_update_deltas/
        noticeType=<OriginalType>/publicationDateDay=<NUN_day>/
"""

from __future__ import annotations

import json
import logging
import re
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from procurement.silver.section_pipeline.notice_schema_reader import (
    load_all_profiles,
    section_derived_cols,
)
from procurement.silver.section_pipeline.parser_registry import COMMON_PARSERS

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SECTION_NUM_RE = re.compile(r"^(\d+(?:\.\d+)*)")

# Maps parser function name → pyarrow output type.
# Functions not listed here produce plain strings.
_FN_TO_PA_TYPE: dict[str, pa.DataType] = {
    "parse_tak_nie": pa.bool_(),
    "parse_pln_value": pa.float64(),
    "parse_criterion_weight": pa.int64(),
    "parse_int_from_text": pa.int64(),
    "parse_duration_days_from_range": pa.int64(),
    "parse_cpv_codes": pa.list_(pa.string()),
    "parse_list_from_newlines": pa.list_(pa.string()),
}

_CHANGE_STRUCT = pa.struct([
    pa.field("section_prefix", pa.string()),
    pa.field("label", pa.string()),
    pa.field("before", pa.string()),
    pa.field("after", pa.string()),
])

_NUN_META_FIELDS: list[pa.Field] = [
    pa.field("nun_objectId", pa.string()),
    pa.field("nun_section_3_2", pa.string()),
    pa.field("nun_section_3_3", pa.string()),
    pa.field("target_objectId", pa.string()),
    pa.field("target_publicationDateDay", pa.string()),
    pa.field("section_changes", pa.list_(_CHANGE_STRUCT)),
    pa.field("parse_errors", pa.string()),  # JSON {col_name: error}
]


# ---------------------------------------------------------------------------
# Schema / index building
# ---------------------------------------------------------------------------

def _pa_type_for_fn(fn_name: str | None) -> pa.DataType:
    if fn_name is None:
        return pa.string()
    return _FN_TO_PA_TYPE.get(fn_name, pa.string())


def _build_section_index(
    all_profiles: dict,
) -> dict[str, dict[str, list[tuple[str, str | None]]]]:
    """Return ``{noticeType: {section_num: [(col_name, fn_name_or_None), ...]}}``.

    Only ``data_model="core"`` sections are included (part-level changes land
    in ``section_changes`` raw but are not mapped to typed columns in phase 1).
    When a section has ``derived_cols``, the derived col names replace the
    source col name in the index (matching the actual silver output schema).
    """
    index: dict[str, dict[str, list[tuple[str, str | None]]]] = {}
    for notice_type, profile in all_profiles.items():
        if notice_type == "NoticeUpdateNotice":
            continue
        derived = section_derived_cols(profile)  # {source_col: {derived_col: {fn:...}}}
        type_index: dict[str, list[tuple[str, str | None]]] = {}
        for section_num, cfg in profile.items():
            if section_num.startswith("_") or not isinstance(cfg, dict):
                continue
            if cfg.get("data_model") != "core":
                continue
            col_name = cfg.get("col_name")
            if not col_name:
                continue
            fn_name: str | None = (cfg.get("parser") or {}).get("fn") or None
            if col_name in derived:
                entries = [
                    (dcol, dcfg.get("fn") if isinstance(dcfg, dict) else None)
                    for dcol, dcfg in derived[col_name].items()
                ]
                type_index[section_num] = entries
            else:
                type_index[section_num] = [(col_name, fn_name)]
        index[notice_type] = type_index
    return index


def _build_col_type_map(
    notice_type: str,
    section_index: dict[str, dict[str, list[tuple[str, str | None]]]],
) -> dict[str, pa.DataType]:
    """Return ``{col_name: pa_type}`` for all core columns of a notice type."""
    col_types: dict[str, pa.DataType] = {}
    for entries in section_index.get(notice_type, {}).values():
        for col_name, fn_name in entries:
            if col_name not in col_types:
                col_types[col_name] = _pa_type_for_fn(fn_name)
    return col_types


def _build_schema(notice_type: str, col_type_map: dict[str, pa.DataType]) -> pa.Schema:
    """Build the pyarrow schema for a delta table (NUN metadata + typed target cols)."""
    fields = list(_NUN_META_FIELDS)
    for col_name, pa_type in col_type_map.items():
        fields.append(pa.field(col_name, pa_type, nullable=True))
    return pa.schema(fields)


# ---------------------------------------------------------------------------
# Parser application
# ---------------------------------------------------------------------------

def _apply_parser(fn_name: str | None, after_text: str | None) -> tuple[Any, str | None]:
    """Call ``fn_name`` on ``after_text``.  Returns ``(value, error_msg_or_None)``."""
    if after_text is None:
        return None, None
    if fn_name is None:
        return after_text, None
    entry = COMMON_PARSERS.get(fn_name)
    if entry is None:
        return after_text, f"unknown parser '{fn_name}'"
    fn, _spark_type = entry
    try:
        return fn(after_text), None
    except Exception as exc:
        return None, str(exc)


# ---------------------------------------------------------------------------
# Section number extraction from label text
# ---------------------------------------------------------------------------

def _extract_section_num(label: str | None) -> str | None:
    """Extract leading section number from a 3.4.1 label.

    E.g. ``"4.2.2.  Krótki opis..."`` → ``"4.2.2"``.
    Returns None when the label does not start with a dotted-numeric prefix.
    """
    if not label:
        return None
    m = _SECTION_NUM_RE.match(label.strip())
    return m.group(1) if m else None


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_table(path: Path, columns: list[str] | None = None) -> list[dict]:
    """Read a parquet partition into a list of row dicts."""
    if not path.exists():
        return []
    try:
        dataset = ds.dataset(str(path), format="parquet")
        return dataset.to_table(columns=columns).to_pylist()
    except Exception as exc:
        log.warning("Failed to read parquet at %s: %s", path, exc)
        return []


def _load_nun_tables(
    silver_dir: Path, target_date: str
) -> tuple[list[dict], list[dict], list[dict]]:
    """Load NUN core, part, and part_part rows for *target_date*.

    Returns ``(core_rows, part_rows, part_part_rows)``.
    """
    nun_dir = silver_dir / "notice_type_tables" / "noticeType=NoticeUpdateNotice"
    date_suffix = f"publicationDateDay={target_date}"

    core = _load_table(
        nun_dir / "data_model=core" / date_suffix,
        columns=["objectId", "section_3_2", "section_3_3"],
    )
    part = _load_table(
        nun_dir / "data_model=part" / date_suffix,
        columns=["objectId", "part_ordinal", "section_3_4"],
    )
    part_part = _load_table(
        nun_dir / "data_model=part_part" / date_suffix,
        columns=[
            "objectId", "part_ordinal", "part_part_ordinal",
            "section_3_4_1_label", "section_3_4_1_before", "section_3_4_1_after",
        ],
    )
    return core, part, part_part


def _load_bzp_index(
    silver_dir: Path, years: set[str]
) -> dict[str, tuple[str, str, str]]:
    """Load ``bzpNumber → (objectId, noticeType, publicationDateDay)`` from common_envelope.

    Only partitions whose ``publicationDateDay`` starts with a year in *years* are read,
    avoiding a full-history scan.  When the same BZP number appears more than once
    (re-published corrections), the first occurrence (chronologically) is kept.
    """
    envelope_dir = silver_dir / "common_envelope"
    if not envelope_dir.exists():
        log.warning("common_envelope not found at %s — cannot resolve target notice types", envelope_dir)
        return {}

    result: dict[str, tuple[str, str, str]] = {}
    partitions_read = 0
    for part_dir in sorted(envelope_dir.iterdir()):
        if not part_dir.is_dir():
            continue
        day = part_dir.name.replace("publicationDateDay=", "")
        if not any(day.startswith(y) for y in years):
            continue
        rows = _load_table(part_dir, columns=["bzpNumber", "objectId", "noticeType"])
        for row in rows:
            bzp = row.get("bzpNumber")
            if bzp and bzp not in result:
                result[bzp] = (
                    row.get("objectId") or "",
                    row.get("noticeType") or "",
                    day,
                )
        partitions_read += 1

    log.info(
        "BZP index: %d entries from %d envelope partition(s) (year filter: %s)",
        len(result), partitions_read, sorted(years),
    )
    return result


# ---------------------------------------------------------------------------
# Delta building
# ---------------------------------------------------------------------------

def build_update_deltas(
    target_date: str,
    silver_dir: Path,
) -> dict[str, list[dict]]:
    """Build delta records from NUN silver data for *target_date*.

    Returns ``{target_noticeType: [row_dict, ...]}``.  Row dicts use plain
    Python values (including ``None``); the caller is responsible for writing
    them to parquet with the correct schema.
    """
    all_profiles = load_all_profiles()
    section_index = _build_section_index(all_profiles)

    # --- Load NUN tables for the target day ---
    core_rows, part_rows, part_part_rows = _load_nun_tables(silver_dir, target_date)
    if not core_rows:
        log.info("No NoticeUpdateNotice data for %s", target_date)
        return {}

    log.info(
        "NUN for %s: %d core, %d part, %d part_part rows",
        target_date, len(core_rows), len(part_rows), len(part_part_rows),
    )

    # --- Index part rows: (objectId, part_ordinal) → section_prefix ---
    part_prefix: dict[tuple[str | None, int], str] = {}
    for row in part_rows:
        key = (row.get("objectId"), int(row.get("part_ordinal") or 0))
        part_prefix[key] = row.get("section_3_4") or ""

    # --- Index part_part rows by objectId ---
    part_part_by_oid: dict[str, list[dict]] = {}
    for row in part_part_rows:
        oid = row.get("objectId")
        if oid:
            part_part_by_oid.setdefault(oid, []).append(row)
    # Sort each group by (part_ordinal, part_part_ordinal) once
    for rows in part_part_by_oid.values():
        rows.sort(key=lambda r: (int(r.get("part_ordinal") or 0), int(r.get("part_part_ordinal") or 0)))

    # --- Determine years needed for envelope scan ---
    target_bzps = {r.get("section_3_2") for r in core_rows if r.get("section_3_2")}
    years: set[str] = set()
    for bzp in target_bzps:
        m = re.match(r"^(\d{4})/", bzp or "")
        if m:
            years.add(m.group(1))
    if not years:
        log.warning("Could not extract any year from section_3_2 values — aborting")
        return {}

    bzp_index = _load_bzp_index(silver_dir, years)

    # --- Build delta rows ---
    deltas_by_type: dict[str, list[dict]] = {}
    stats = {"resolved": 0, "unresolved_bzp": 0, "unresolved_type": 0}

    for core_row in core_rows:
        nun_oid = core_row.get("objectId")
        target_bzp = core_row.get("section_3_2")
        target_version = core_row.get("section_3_3")

        if not target_bzp:
            continue

        # Resolve original notice
        envelope_entry = bzp_index.get(target_bzp)
        if envelope_entry is None:
            log.debug("NUN %s: target BZP %r not in common_envelope", nun_oid, target_bzp)
            stats["unresolved_bzp"] += 1
            continue

        target_oid, target_nt, target_day = envelope_entry
        if target_nt not in section_index:
            log.debug("NUN %s: target type %r has no section index", nun_oid, target_nt)
            stats["unresolved_type"] += 1
            continue

        type_sec_index = section_index[target_nt]

        # Gather and sort change items for this NUN
        changes = part_part_by_oid.get(nun_oid, [])

        # Build section_changes list and typed column values
        section_changes: list[dict] = []
        col_values: dict[str, Any] = {}
        parse_errors: dict[str, str] = {}

        for ch in changes:
            label = ch.get("section_3_4_1_label")
            before = ch.get("section_3_4_1_before")
            after = ch.get("section_3_4_1_after")
            part_ordinal = int(ch.get("part_ordinal") or 0)
            section_prefix = part_prefix.get((nun_oid, part_ordinal), "")

            section_changes.append({
                "section_prefix": section_prefix,
                "label": label,
                "before": before,
                "after": after,
            })

            section_num = _extract_section_num(label)
            if section_num is None:
                continue  # no section number in label — raw record kept in section_changes

            entries = type_sec_index.get(section_num)
            if entries is None:
                continue  # section not tracked in target profile (part-level or unparsed)

            for col_name, fn_name in entries:
                value, error = _apply_parser(fn_name, after)
                col_values[col_name] = value  # last write wins for duplicate section nums
                if error:
                    parse_errors[col_name] = error
                elif col_name in parse_errors:
                    del parse_errors[col_name]

        stats["resolved"] += 1
        delta_row: dict[str, Any] = {
            "nun_objectId": nun_oid,
            "nun_section_3_2": target_bzp,
            "nun_section_3_3": target_version,
            "target_objectId": target_oid,
            "target_publicationDateDay": target_day,
            "section_changes": section_changes,
            "parse_errors": json.dumps(parse_errors, ensure_ascii=False) if parse_errors else None,
        }
        delta_row.update(col_values)
        deltas_by_type.setdefault(target_nt, []).append(delta_row)

    log.info(
        "Delta build for %s: %d resolved, %d unresolved BZP, %d unresolved type → "
        "%d delta rows across %d notice type(s)",
        target_date,
        stats["resolved"], stats["unresolved_bzp"], stats["unresolved_type"],
        sum(len(v) for v in deltas_by_type.values()),
        len(deltas_by_type),
    )
    return deltas_by_type


# ---------------------------------------------------------------------------
# Writing output
# ---------------------------------------------------------------------------

def _rows_to_table(
    rows: list[dict], schema: pa.Schema
) -> pa.Table:
    """Convert a list of row dicts to a pyarrow Table conforming to *schema*."""
    columns: dict[str, list] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))

    arrays: list[pa.Array] = []
    for field in schema:
        col_data = columns[field.name]
        try:
            arrays.append(pa.array(col_data, type=field.type))
        except (pa.ArrowInvalid, pa.ArrowTypeError) as exc:
            # Graceful fallback: cast the column to string rather than losing data.
            log.warning(
                "Type cast failed for field '%s' (%s) — falling back to string: %s",
                field.name, field.type, exc,
            )
            str_data = [str(v) if v is not None else None for v in col_data]
            arrays.append(pa.array(str_data, type=pa.string()).cast(pa.string()))

    return pa.table(dict(zip([f.name for f in schema], arrays)), schema=schema)


def write_deltas(
    target_date: str,
    deltas_by_type: dict[str, list[dict]],
    silver_dir: Path,
    section_index: dict[str, dict[str, list[tuple[str, str | None]]]],
) -> None:
    """Write delta records to ``silver/notice_update_deltas/`` partitioned by noticeType and day.

    Overwrites any existing output for *(target_date, noticeType)* pairs present in
    *deltas_by_type*.  Notice types with no deltas for this day are untouched.
    """
    out_root = silver_dir / "notice_update_deltas"

    for notice_type, rows in deltas_by_type.items():
        out_dir = out_root / f"noticeType={notice_type}" / f"publicationDateDay={target_date}"
        # Overwrite: remove previous run's output for this (type, day) before writing
        if out_dir.exists():
            shutil.rmtree(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        col_type_map = _build_col_type_map(notice_type, section_index)
        schema = _build_schema(notice_type, col_type_map)
        table = _rows_to_table(rows, schema)

        out_path = out_dir / f"part-0.parquet"
        pq.write_table(table, str(out_path), compression="snappy")
        log.info(
            "Wrote %d delta rows for %s/%s → %s",
            len(rows), notice_type, target_date, out_path,
        )
