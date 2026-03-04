"""Spark transforms for building per-data-model section tables from HTML.

Pipeline:
1. `make_html_sections_udf(all_profiles)` — create once per Spark run on the driver,
   serialise all_profiles into the UDF closure.
2. `build_section_tables(df, notice_type, profile, sections_udf)` — called once per
   notice-type batch; applies the UDF and returns one DataFrame per data_model.
3. `apply_column_parsers(section_tables, profile, notice_type)` — applies registered
   column-level UDFs to section columns that have a ``"parser"`` entry in the profile.

Output DataFrame shapes
-----------------------
core table    : one row per notice; columns = [objectId, publicationDateDay, <section cols>]
repeating table : one row per occurrence; columns = [objectId, publicationDateDay,
                  <model>_ordinal, <section cols>, (<sub_key>_items if two-level)]
"""

from __future__ import annotations

import json
import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, get_json_object, lit, posexplode
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import udf
from pyspark.storagelevel import StorageLevel

from procurement.silver.section_pipeline.column_parsers import get_parser_entry
from procurement.silver.section_pipeline.profile import (
    model_core_col_names,
    model_sub_info,
    section_parsers,
    top_level_models,
)

log = logging.getLogger(__name__)


def make_html_sections_udf(all_profiles: dict):
    """Return a Spark UDF that parses one HTML notice body into a sections-model JSON string.

    The returned function signature is:
        (htmlBody: StringType, noticeType: StringType) -> StringType (JSON)

    The profiles dict is serialised into the closure once on the driver side; each
    worker deserialises it lazily (first call) and caches it for the lifetime of the
    task to avoid repeated JSON parsing overhead.
    """
    profiles_json_str = json.dumps(all_profiles, ensure_ascii=False)

    # _cache is captured by reference; each worker process starts with {}
    # and populates it on the first invocation.
    _cache: dict = {}

    def _parse_html_sections(html: str | None, notice_type: str | None) -> str | None:
        if not html or not notice_type:
            return None

        # On the first call in each worker process, populate the cache with the
        # deserialized profiles and the imported function/class references.
        # Subsequent calls skip all imports and JSON parsing entirely.
        if "_profiles" not in _cache:
            import json as _json
            from bs4 import BeautifulSoup as _BeautifulSoup
            from procurement.silver.section_pipeline.html_extractor import (
                build_notice_sections_model as _build,
            )
            _cache["_profiles"] = _json.loads(profiles_json_str)
            _cache["_json"] = _json
            _cache["_BeautifulSoup"] = _BeautifulSoup
            _cache["_build"] = _build

        soup = _cache["_BeautifulSoup"](html, "lxml")
        result = _cache["_build"](soup, notice_type, _cache["_profiles"])
        return _cache["_json"].dumps(result, ensure_ascii=False)

    return udf(_parse_html_sections, StringType())


def _make_model_rows_udf(
    model: str,
    core_cols: list[str],
    sub_key: str | None,
    sub_cols: list[str],
):
    """Return a UDF that extracts one list-of-row-dicts for a repeating data model.

    Input : sections JSON string produced by make_html_sections_udf
    Output: ArrayType of StructType rows; each row has core section columns, and
            optionally a '<sub_key>_items' array column for two-level models.
    """
    # Build the return schema on the driver (required before UDF is created)
    row_fields: list[StructField] = [StructField(c, StringType()) for c in core_cols]
    if sub_key and sub_cols:
        sub_struct = StructType([StructField(c, StringType()) for c in sub_cols])
        row_fields.append(StructField(f"{sub_key}_items", ArrayType(sub_struct)))
    return_schema = ArrayType(StructType(row_fields))

    # Capture everything needed inside the closure
    _model = model
    _core_cols = list(core_cols)
    _sub_key = sub_key
    _sub_cols = list(sub_cols)

    def _extract_rows(sections_json: str | None) -> list:
        if not sections_json:
            return []
        import json as _json
        sections = _json.loads(sections_json)
        occurrences = sections.get(_model, [])
        rows = []
        for occ in occurrences:
            core_data = occ.get("core", {})
            row: dict = {c: core_data.get(c) for c in _core_cols}
            if _sub_key and _sub_cols:
                sub_items = occ.get(_sub_key, [])
                row[f"{_sub_key}_items"] = [
                    {c: item.get(c) for c in _sub_cols}
                    for item in sub_items
                ]
            rows.append(row)
        return rows

    return udf(_extract_rows, return_schema)


def build_section_tables(
    df: DataFrame,
    notice_type: str | None,
    profile: dict,
    sections_udf,
) -> tuple[dict[str, DataFrame], DataFrame | None]:
    """Build one DataFrame per data_model from the HTML sections of a notice-type batch.

    Parameters
    ----------
    df          : Bronze batch with at least objectId, publicationDateDay, htmlBody columns.
    notice_type : The notice type string (e.g. 'ContractNotice').
    profile     : The sections profile dict for this notice type
                  (from sections_profile.load_profile).
    sections_udf: Pre-built UDF from make_html_sections_udf.

    Returns
    -------
    (section_tables, sections_df) — sections_df is persisted in MEMORY_AND_DISK so that
    writing N model tables only parses HTML once instead of N times per notice type.
    The caller **must** call ``sections_df.unpersist()`` after all writes complete.
    Returns ({}, None) if profile is empty or notice_type is None.
    """
    if not profile or notice_type is None:
        return {}, None

    models = top_level_models(profile)
    if not models:
        return {}, None

    # Parse HTML into sections JSON once per row; persist so N model writes
    # reuse the cached result instead of re-parsing HTML each time.
    df_with_sections = df.withColumn(
        "_sections_json",
        sections_udf(col("htmlBody"), lit(notice_type)),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    result: dict[str, DataFrame] = {}

    for model in models:
        core_cols = model_core_col_names(profile, model)

        if model == "core":
            # One row per notice; extract all core fields via from_json
            if not core_cols:
                log.warning(
                    "notice_type=%s model=core has no columns in profile; skipping",
                    notice_type,
                )
                continue
            core_schema = StructType([StructField(c, StringType()) for c in core_cols])
            df_model = (
                df_with_sections
                .withColumn(
                    "_core_struct",
                    from_json(get_json_object(col("_sections_json"), "$.core"), core_schema),
                )
                .select(
                    col("objectId"),
                    col("publicationDateDay"),
                    *[col(f"_core_struct.{c}").alias(c) for c in core_cols],
                )
            )

        else:
            # One row per occurrence; explode the model's list from the sections JSON
            sub_key, sub_cols = model_sub_info(profile, model)
            rows_udf = _make_model_rows_udf(model, core_cols, sub_key, sub_cols)

            df_exploded = (
                df_with_sections
                .withColumn("_model_rows", rows_udf(col("_sections_json")))
                .select(
                    col("objectId"),
                    col("publicationDateDay"),
                    posexplode(col("_model_rows")).alias("_ordinal0", "_row"),
                )
                .withColumn(f"{model}_ordinal", col("_ordinal0") + lit(1))
                .drop("_ordinal0")
            )

            # Flatten struct fields into top-level columns
            for c in core_cols:
                df_exploded = df_exploded.withColumn(c, col(f"_row.{c}"))
            if sub_key and sub_cols:
                df_exploded = df_exploded.withColumn(
                    f"{sub_key}_items", col(f"_row.{sub_key}_items")
                )

            df_model = df_exploded.drop("_row")

        result[model] = df_model
        log.debug(
            "Built section table notice_type=%s model=%s cols=%d",
            notice_type,
            model,
            len(core_cols),
        )

    return result, df_with_sections


def apply_column_parsers(
    section_tables: dict[str, DataFrame],
    profile: dict,
    notice_type: str | None,
) -> dict[str, DataFrame]:
    """Apply registered column-level parsers to section DataFrames.

    For each section column that has a ``"parser"`` entry in the profile, the raw
    StringType column is replaced with a typed column produced by the registered UDF.
    Columns without a parser configuration remain as StringType (unchanged).

    The parser function must be registered in
    :mod:`procurement.silver.section_pipeline.column_parsers` under the given ``fn`` name.
    Notice-type-specific parsers take precedence over common ones when names clash.

    Parameters
    ----------
    section_tables:
        Output of :func:`build_section_tables`.
    profile:
        The notice type's sections profile dict (from :func:`sections_profile.load_profile`).
    notice_type:
        CamelCase notice type name (e.g. ``"ContractNotice"``).

    Returns
    -------
    dict[model, DataFrame] — same keys as input; columns with configured parsers
    now carry the parser's return type instead of StringType.
    """
    col_parsers = section_parsers(profile)
    if not col_parsers or not section_tables:
        return section_tables

    result: dict[str, DataFrame] = {}
    for model, df in section_tables.items():
        df_out = df
        for col_name, parser_cfg in col_parsers.items():
            if col_name not in df_out.columns:
                continue
            fn_name = parser_cfg.get("fn")
            if not fn_name:
                continue
            entry = get_parser_entry(fn_name, notice_type)
            if entry is None:
                log.warning(
                    "apply_column_parsers: unknown fn=%s notice_type=%s col=%s; skipping",
                    fn_name,
                    notice_type,
                    col_name,
                )
                continue
            parser_fn, return_type = entry
            parser_udf = udf(parser_fn, return_type)
            df_out = df_out.withColumn(col_name, parser_udf(col(col_name)))
            log.debug(
                "Applied parser fn=%s col=%s model=%s notice_type=%s",
                fn_name,
                col_name,
                model,
                notice_type,
            )
        result[model] = df_out

    return result
