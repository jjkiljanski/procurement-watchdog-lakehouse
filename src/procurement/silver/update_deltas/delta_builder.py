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

I/O uses Apache Iceberg (HadoopCatalog, catalog name ``silver``):

Reads:
  silver.notice_type_tables.notice_update_notice__core
  silver.notice_type_tables.notice_update_notice__part
  silver.notice_type_tables.notice_update_notice__part_part
  silver.common.common_envelope  (year-scoped)

Writes:
  silver.notice_update_deltas.{target_notice_type_snake_case}
  Partitioned by publicationDateDay (= NUN publication date).
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from procurement.silver.section_pipeline.notice_schema_reader import (
    load_all_profiles,
    section_derived_cols,
)
from procurement.silver.section_pipeline.parser_registry import COMMON_PARSERS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SECTION_NUM_RE = re.compile(r"^(\d+(?:\.\d+)*)")

# Maps parser function name → PySpark output type.
# Functions not listed here produce plain strings.
_FN_TO_SPARK_TYPE: dict[str, Any] = {
    "parse_tak_nie": BooleanType(),
    "parse_pln_value": DoubleType(),
    "parse_criterion_weight": LongType(),
    "parse_int_from_text": LongType(),
    "parse_duration_days_from_range": LongType(),
    "parse_cpv_codes": ArrayType(StringType()),
    "parse_list_from_newlines": ArrayType(StringType()),
}

_CHANGE_STRUCT_SPARK = StructType([
    StructField("section_prefix", StringType(), True),
    StructField("label", StringType(), True),
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
])

_NUN_META_FIELDS_SPARK: list[StructField] = [
    # publicationDateDay = the NUN publication date; used as the Iceberg partition column.
    StructField("publicationDateDay", StringType(), False),
    StructField("nun_objectId", StringType(), True),
    StructField("nun_section_3_2", StringType(), True),
    StructField("nun_section_3_3", StringType(), True),
    StructField("target_objectId", StringType(), True),
    StructField("target_publicationDateDay", StringType(), True),
    StructField("section_changes", ArrayType(_CHANGE_STRUCT_SPARK), True),
    StructField("parse_errors", StringType(), True),  # JSON {col_name: error}
]


# ---------------------------------------------------------------------------
# Schema / index building
# ---------------------------------------------------------------------------

def _spark_type_for_fn(fn_name: str | None) -> Any:
    if fn_name is None:
        return StringType()
    return _FN_TO_SPARK_TYPE.get(fn_name, StringType())


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
) -> dict[str, Any]:
    """Return ``{col_name: SparkDataType}`` for all core columns of a notice type."""
    col_types: dict[str, Any] = {}
    for entries in section_index.get(notice_type, {}).values():
        for col_name, fn_name in entries:
            if col_name not in col_types:
                col_types[col_name] = _spark_type_for_fn(fn_name)
    return col_types


def _build_delta_spark_schema(col_type_map: dict[str, Any]) -> StructType:
    """Build the Spark StructType for a delta table (NUN metadata + typed target cols)."""
    fields = list(_NUN_META_FIELDS_SPARK)
    for col_name, spark_type in col_type_map.items():
        fields.append(StructField(col_name, spark_type, True))
    return StructType(fields)


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
# Iceberg helpers
# ---------------------------------------------------------------------------

def _nt_to_snake(notice_type: str) -> str:
    """Convert CamelCase notice type to snake_case table name component."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", notice_type).lower()


def _iceberg_table_exists(spark: "SparkSession", full_table_name: str) -> bool:
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Data loading helpers (Spark / Iceberg)
# ---------------------------------------------------------------------------

def load_nun_rows(
    spark: "SparkSession",
    target_date: str,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Load NUN core, part, and part_part rows for *target_date* from Iceberg.

    Returns ``(core_rows, part_rows, part_part_rows)``.
    Returns empty lists if the Iceberg tables do not yet exist.
    """

    def _query(table: str, columns: list[str]) -> list[dict]:
        if not _iceberg_table_exists(spark, table):
            return []
        cols = ", ".join(columns)
        df = spark.sql(
            f"SELECT {cols} FROM {table} WHERE publicationDateDay = '{target_date}'"
        )
        return [row.asDict() for row in df.collect()]

    core = _query(
        "silver.notice_type_tables.notice_update_notice__core",
        ["objectId", "section_3_2", "section_3_3"],
    )
    part = _query(
        "silver.notice_type_tables.notice_update_notice__part",
        ["objectId", "part_ordinal", "section_3_4"],
    )
    part_part = _query(
        "silver.notice_type_tables.notice_update_notice__part_part",
        [
            "objectId", "part_ordinal", "part_part_ordinal",
            "section_3_4_1_label", "section_3_4_1_before", "section_3_4_1_after",
        ],
    )
    return core, part, part_part


def load_bzp_index(
    spark: "SparkSession",
    years: set[str],
) -> dict[str, tuple[str, str, str]]:
    """Load ``bzpNumber → (objectId, noticeType, publicationDateDay)`` from Iceberg common_envelope.

    Only rows whose ``publicationDateDay`` starts with a year in *years* are read,
    avoiding a full-history scan.  When the same BZP number appears more than once
    (re-published corrections), the earliest occurrence (chronologically) is kept.
    """
    if not years:
        return {}

    year_filter = " OR ".join(
        f"publicationDateDay LIKE '{y}%'" for y in sorted(years)
    )
    df = spark.sql(
        f"SELECT bzpNumber, objectId, noticeType, publicationDateDay "
        f"FROM silver.common.common_envelope "
        f"WHERE {year_filter} "
        f"ORDER BY publicationDateDay ASC"
    )

    result: dict[str, tuple[str, str, str]] = {}
    rows_read = 0
    for row in df.collect():
        bzp = row["bzpNumber"]
        if bzp and bzp not in result:
            result[bzp] = (
                row["objectId"] or "",
                row["noticeType"] or "",
                row["publicationDateDay"] or "",
            )
        rows_read += 1

    log.info(
        "BZP index: %d entries from %d envelope rows (year filter: %s)",
        len(result), rows_read, sorted(years),
    )
    return result


# ---------------------------------------------------------------------------
# Delta building (pure Python — no I/O)
# ---------------------------------------------------------------------------

def build_update_deltas(
    target_date: str,
    core_rows: list[dict],
    part_rows: list[dict],
    part_part_rows: list[dict],
    section_index: dict[str, dict[str, list[tuple[str, str | None]]]],
    bzp_index: dict[str, tuple[str, str, str]],
) -> dict[str, list[dict]]:
    """Build delta records from pre-loaded NUN data for *target_date*.

    Returns ``{target_noticeType: [row_dict, ...]}``.  Row dicts use plain
    Python values (including ``None``); the caller writes them to Iceberg via
    ``write_deltas()``.  Each row includes ``publicationDateDay = target_date``
    as the Iceberg partition value.
    """
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
            "publicationDateDay": target_date,
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
# Writing output (Spark / Iceberg)
# ---------------------------------------------------------------------------

def write_deltas(
    spark: "SparkSession",
    target_date: str,
    deltas_by_type: dict[str, list[dict]],
    section_index: dict[str, dict[str, list[tuple[str, str | None]]]],
) -> None:
    """Write delta records to Iceberg ``silver.notice_update_deltas.{notice_type}``.

    Partitioned by ``publicationDateDay``.  Overwrites only the target day's
    partition for each notice type present in *deltas_by_type*; other types
    and other days are untouched.
    """
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver.notice_update_deltas")

    for notice_type, rows in deltas_by_type.items():
        if not rows:
            continue

        col_type_map = _build_col_type_map(notice_type, section_index)
        spark_schema = _build_delta_spark_schema(col_type_map)

        nt_snake = _nt_to_snake(notice_type)
        full_table = f"silver.notice_update_deltas.{nt_snake}"

        df = spark.createDataFrame(rows, spark_schema)

        if not _iceberg_table_exists(spark, full_table):
            df.writeTo(full_table).partitionedBy("publicationDateDay").create()
        else:
            df.writeTo(full_table).overwritePartitions()

        log.info(
            "Wrote %d delta rows for %s/%s → %s",
            len(rows), notice_type, target_date, full_table,
        )
