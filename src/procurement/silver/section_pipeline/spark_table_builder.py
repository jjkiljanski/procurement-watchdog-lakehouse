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
                  <model>_ordinal, <section cols>]
nested child table : one row per child occurrence; columns = [objectId, publicationDateDay,
                    <parent_model>_ordinal, <child_model>_ordinal, <section cols>]
"""

from __future__ import annotations

import json
import logging
import threading

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, from_json, get_json_object, lit, posexplode, size, when
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import udf
from pyspark.storagelevel import StorageLevel

from procurement.silver.section_pipeline.parser_registry import get_computed_entry, get_parser_entry
from procurement.silver.section_pipeline.notice_schema_reader import (
    model_core_col_names,
    model_sub_infos,
    output_models,
    section_computed_cols,
    section_derived_cols,
    section_parsers,
    top_level_models,
)

log = logging.getLogger(__name__)

# Cache UDF objects across batches: same (fn, return_type) pair only registers once with
# the JVM, eliminating repeated Py4J round-trips for shared parsers (e.g. parse_nuts3_code
# appears in every notice type).  The lock prevents concurrent threads from racing to
# register the same UDF simultaneously, which serialises Py4J calls and avoids JVM
# contention during the parallel-batch phase.
_UDF_CACHE: dict = {}
_UDF_LOCK = threading.Lock()


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
            from procurement.silver.section_pipeline.raw_section_extractor import (
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
):
    """Return a UDF that extracts one list-of-row-dicts for a repeating data model.

    Input : sections JSON string produced by make_html_sections_udf
    Output: ArrayType of StructType rows; each row has only the model's own
            core section columns. Nested child models are emitted as separate
            tables by ``_make_child_model_rows_udf``.
    """
    # Build the return schema on the driver (required before UDF is created)
    row_fields: list[StructField] = [StructField(c, StringType()) for c in core_cols]
    return_schema = ArrayType(StructType(row_fields))

    # Capture everything needed inside the closure
    _model = model
    _core_cols = list(core_cols)

    def _extract_rows(sections_json: str | None) -> list:
        if not sections_json:
            return []
        import json as _json
        sections = _json.loads(sections_json)
        occurrences = sections.get(_model, [])
        rows = []
        for occ in occurrences:
            core_data = occ.get("core", {})
            rows.append({c: core_data.get(c) for c in _core_cols})
        return rows

    return udf(_extract_rows, return_schema)


def _make_child_model_rows_udf(
    parent_model: str,
    child_key: str,
    child_cols: list[str],
):
    """Return a UDF that extracts flattened child rows for a nested model."""
    row_fields: list[StructField] = [
        StructField(f"{parent_model}_ordinal", StringType()),
        *[StructField(c, StringType()) for c in child_cols],
    ]
    return_schema = ArrayType(StructType(row_fields))

    _parent_model = parent_model
    _child_key = child_key
    _child_cols = list(child_cols)

    def _extract_rows(sections_json: str | None) -> list:
        if not sections_json:
            return []
        import json as _json

        sections = _json.loads(sections_json)
        parent_occurrences = sections.get(_parent_model, [])
        rows = []
        for parent_idx, occ in enumerate(parent_occurrences, start=1):
            for item in occ.get(_child_key, []):
                row = {c: item.get(c) for c in _child_cols}
                row[f"{_parent_model}_ordinal"] = str(parent_idx)
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

    models = output_models(profile)
    top_models = set(top_level_models(profile))
    if not models:
        return {}, None

    # Parse HTML into sections JSON once per row; persist so N model writes
    # reuse the cached result instead of re-parsing HTML each time.
    df_with_sections = df.withColumn(
        "_sections_json",
        sections_udf(col("htmlBody"), lit(notice_type)),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    # Exclude rows that produced HTML parse errors (e.g. duplicate core sections)
    # from the model tables.  They are routed to quarantine via
    # detect_section_parse_error_quarantine called by the orchestrator.
    df_valid = df_with_sections.filter(
        get_json_object(col("_sections_json"), "$._parse_errors").isNull()
    )

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
                df_valid
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

        elif model in top_models:
            # One row per occurrence; explode the model's list from the sections JSON
            rows_udf = _make_model_rows_udf(model, core_cols)

            df_exploded = (
                df_valid
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

            df_model = df_exploded.drop("_row")
        else:
            parent_model, child_key = model.rsplit("_", 1)
            child_cols = []
            for leaf, cols in model_sub_infos(profile, parent_model):
                if leaf == child_key:
                    child_cols = cols
                    break
            rows_udf = _make_child_model_rows_udf(parent_model, child_key, child_cols)
            df_model = (
                df_valid
                .withColumn("_model_rows", rows_udf(col("_sections_json")))
                .select(
                    col("objectId"),
                    col("publicationDateDay"),
                    posexplode(col("_model_rows")).alias("_ordinal0", "_row"),
                )
                .withColumn(f"{model}_ordinal", col("_ordinal0") + lit(1))
                .drop("_ordinal0")
                .withColumn(f"{parent_model}_ordinal", col(f"_row.{parent_model}_ordinal").cast("int"))
            )
            for c in child_cols:
                df_model = df_model.withColumn(c, col(f"_row.{c}"))
            df_model = df_model.drop("_row")

        result[model] = df_model
        log.debug(
            "Built section table notice_type=%s model=%s cols=%d",
            notice_type,
            model,
            len(core_cols),
        )

    return result, df_with_sections


def detect_unknown_section_quarantine(
    sections_df: "DataFrame | None",
    notice_type: str | None,
) -> "DataFrame | None":
    """Return a quarantine DataFrame for rows that contain section numbers absent from the profile.

    Uses the ``_unknown_sections`` list embedded in the sections JSON by
    :func:`~procurement.silver.section_pipeline.raw_section_extractor.build_notice_sections_model`.

    Returns ``None`` when ``sections_df`` is ``None`` (no sections were parsed).
    Returns a (possibly empty) DataFrame otherwise — write it only when non-empty
    to avoid creating empty Parquet files.
    """
    if sections_df is None or notice_type is None:
        return None

    from pyspark.sql.functions import array_size, from_json, transform
    from pyspark.sql.functions import concat as spark_concat
    from pyspark.sql.types import ArrayType, StringType

    _unknown_schema = ArrayType(StringType())
    df_with_unknown = sections_df.withColumn(
        "_unknown_sections",
        from_json(get_json_object(col("_sections_json"), "$._unknown_sections"), _unknown_schema),
    )
    quarantine = (
        df_with_unknown
        .filter(
            col("_unknown_sections").isNotNull()
            & (array_size(col("_unknown_sections")) > 0)
        )
        .select(
            col("objectId"),
            col("publicationDateDay"),
            lit(notice_type).alias("notice_type"),
            lit("unknown").alias("data_model"),
            transform(
                col("_unknown_sections"),
                lambda s: spark_concat(lit("unknown section: "), s),
            ).alias("_parse_errors"),
        )
    )
    log.debug(
        "detect_unknown_section_quarantine built quarantine plan notice_type=%s",
        notice_type,
    )
    return quarantine


def detect_section_parse_error_quarantine(
    sections_df: "DataFrame | None",
    notice_type: str | None,
) -> "DataFrame | None":
    """Return a quarantine DataFrame for rows that had HTML parse errors during section extraction.

    Uses the ``_parse_errors`` list embedded in the sections JSON by
    :func:`~procurement.silver.section_pipeline.raw_section_extractor.build_notice_sections_model`
    when a structural problem is detected in the notice HTML (e.g. a core section
    appearing more than once).  These rows are excluded from all section model
    tables by :func:`build_section_tables`.

    Returns ``None`` when ``sections_df`` is ``None`` (no sections were parsed).
    Returns a (possibly empty) DataFrame otherwise — write it only when non-empty
    to avoid creating empty Parquet files.
    """
    if sections_df is None or notice_type is None:
        return None

    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.functions import from_json

    _errors_schema = ArrayType(StringType())
    df_with_errors = sections_df.withColumn(
        "_html_parse_errors",
        from_json(get_json_object(col("_sections_json"), "$._parse_errors"), _errors_schema),
    )
    quarantine = (
        df_with_errors
        .filter(
            col("_html_parse_errors").isNotNull()
            & (size(col("_html_parse_errors")) > 0)
        )
        .select(
            col("objectId"),
            col("publicationDateDay"),
            lit(notice_type).alias("notice_type"),
            lit("core").alias("data_model"),
            col("_html_parse_errors").alias("_parse_errors"),
        )
    )
    log.debug(
        "detect_section_parse_error_quarantine built quarantine plan notice_type=%s",
        notice_type,
    )
    return quarantine


def _make_fault_tolerant_udf(fn, return_type):
    """Wrap a parser or computed function in a struct(value, error) UDF.

    Accepts one or more column arguments (matching the wrapped function's arity).
    On success returns struct(typed_value, null).
    On any exception returns struct(null, error_string).

    This makes every section parser non-fatal: a failed column receives None
    and the error message is collected into the row-level ``parse_errors``
    column instead of routing the row to quarantine.

    Results are cached by (fn identity, return_type string) so that the same
    parser used across multiple notice types is only registered with the JVM once.
    """
    key = (id(fn), str(return_type))
    cached = _UDF_CACHE.get(key)
    if cached is not None:
        return cached
    with _UDF_LOCK:
        cached = _UDF_CACHE.get(key)
        if cached is not None:
            return cached
        _schema = StructType([StructField("value", return_type), StructField("error", StringType())])
        _fn = fn

        def _call(*args):
            if all(a is None for a in args):
                return (None, None)
            try:
                return (_fn(*args), None)
            except Exception as exc:
                return (None, str(exc))

        result = udf(_call, _schema)
        _UDF_CACHE[key] = result
        return result


def apply_column_parsers(
    section_tables: dict[str, DataFrame],
    profile: dict,
    notice_type: str | None,
) -> tuple[dict[str, DataFrame], "DataFrame | None", list[DataFrame]]:
    """Apply registered column-level parsers to section DataFrames.

    All parsers are fault-tolerant: a parse failure on any field sets that
    field to None and records the error in the row-level ``parse_errors``
    column (``ArrayType(StringType())``, null when no errors).  No rows are
    excluded from the output — downstream consumers filter on
    ``parse_errors IS NULL`` if they require fully clean rows.

    Parameters
    ----------
    section_tables:
        Output of :func:`build_section_tables`.
    profile:
        The notice type's sections profile dict.
    notice_type:
        CamelCase notice type name (e.g. ``"ContractNotice"``).

    Returns
    -------
    ``(result_tables, None, [])`` — second and third values are always empty
    (kept for call-site compatibility).  Every DataFrame in ``result_tables``
    gains a ``parse_errors`` column.
    """
    col_parsers = section_parsers(profile)
    derived = section_derived_cols(profile)
    computed_specs = section_computed_cols(profile)

    if not section_tables:
        return {}, None, []

    # No parsers of any kind — still add parse_errors: null for schema consistency.
    if not col_parsers and not derived and not computed_specs:
        return {
            model: df.withColumn("parse_errors", lit(None).cast(ArrayType(StringType())))
            for model, df in section_tables.items()
        }, None, []

    result: dict[str, DataFrame] = {}

    for model, df in section_tables.items():
        df_out = df

        # --- In-place column type replacement (fault-tolerant) ---
        # Build all struct UDF expressions first, then apply in two select()
        # calls (add structs, then extract value+error) to keep the Catalyst
        # plan flat and avoid N nested Project nodes.
        struct_exprs: dict[str, object] = {}  # col_name -> struct UDF expression
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
                    fn_name, notice_type, col_name,
                )
                continue
            parser_fn, return_type = entry
            struct_exprs[col_name] = _make_fault_tolerant_udf(parser_fn, return_type)(col(col_name))
            log.debug(
                "Registered fault-tolerant parser fn=%s col=%s model=%s notice_type=%s",
                fn_name, col_name, model, notice_type,
            )

        if struct_exprs:
            df_out = df_out.select(
                *[col(c) for c in df_out.columns],
                *[expr_val.alias(f"_ft_{c}") for c, expr_val in struct_exprs.items()],
            )
            df_out = df_out.select(
                *[
                    col(f"_ft_{c}").getField("value").alias(c)
                    if c in struct_exprs else col(c)
                    for c in df_out.columns if not c.startswith("_ft_")
                ],
                *[col(f"_ft_{c}").getField("error").alias(f"_perr_{c}") for c in struct_exprs],
            )

        # --- Derived columns (fault-tolerant) ---
        # Source col renamed to temp; all derived UDFs read from temp so that
        # a derived col reusing the source name replaces it cleanly.
        for source_col, derived_map in derived.items():
            if source_col not in df_out.columns:
                continue
            temp_col = f"_derived_src_{source_col}"
            df_out = df_out.withColumnRenamed(source_col, temp_col)

            ft_derived: dict[str, object] = {}  # derived_col -> struct UDF expr
            for derived_col, parser_cfg in derived_map.items():
                fn_name = parser_cfg.get("fn")
                if not fn_name:
                    continue
                entry = get_parser_entry(fn_name, notice_type)
                if entry is None:
                    log.warning(
                        "apply_column_parsers: unknown fn=%s notice_type=%s "
                        "derived_col=%s; skipping",
                        fn_name, notice_type, derived_col,
                    )
                    continue
                parser_fn, return_type = entry
                ft_derived[derived_col] = _make_fault_tolerant_udf(parser_fn, return_type)(col(temp_col))
                log.debug(
                    "Registered fault-tolerant derived parser fn=%s source=%s "
                    "derived=%s model=%s notice_type=%s",
                    fn_name, source_col, derived_col, model, notice_type,
                )

            if ft_derived:
                df_out = df_out.select(
                    *[col(c) for c in df_out.columns],
                    *[expr_val.alias(f"_ft_{dc}") for dc, expr_val in ft_derived.items()],
                )
                pass_through = [
                    col(c) for c in df_out.columns
                    if c != temp_col and not c.startswith("_ft_")
                ]
                df_out = df_out.select(
                    *pass_through,
                    *[col(f"_ft_{dc}").getField("value").alias(dc) for dc in ft_derived],
                    *[col(f"_ft_{dc}").getField("error").alias(f"_perr_{dc}") for dc in ft_derived],
                )
            else:
                df_out = df_out.drop(temp_col)

        # --- Computed columns (fault-tolerant, multi-source) ---
        computed_for_model = [
            s for s in computed_specs if s.get("data_model") == model
        ]
        if computed_for_model:
            all_output_names = {s["col_name"] for s in computed_for_model if "col_name" in s}
            all_sources = {src for s in computed_for_model for src in s.get("sources", [])}
            conflict_cols = (all_output_names & all_sources) & set(df_out.columns)

            temp_mapping: dict[str, str] = {}
            for conflict_col in conflict_cols:
                temp_name = f"_computed_src_{conflict_col}"
                df_out = df_out.withColumnRenamed(conflict_col, temp_name)
                temp_mapping[conflict_col] = temp_name

            ft_computed: dict[str, object] = {}  # out_col -> struct UDF expr
            for spec in computed_for_model:
                fn_name = spec.get("fn")
                out_col = spec.get("col_name")
                sources = spec.get("sources", [])
                if not fn_name or not out_col or not sources:
                    continue
                entry = get_computed_entry(fn_name)
                if entry is None:
                    log.warning(
                        "apply_column_parsers: unknown computed fn=%s "
                        "notice_type=%s col=%s; skipping",
                        fn_name, notice_type, out_col,
                    )
                    continue
                computed_fn, return_type = entry
                resolved = [col(temp_mapping.get(s, s)) for s in sources]
                ft_computed[out_col] = _make_fault_tolerant_udf(computed_fn, return_type)(*resolved)
                log.debug(
                    "Registered fault-tolerant computed fn=%s sources=%s out=%s "
                    "model=%s notice_type=%s",
                    fn_name, sources, out_col, model, notice_type,
                )

            if ft_computed:
                temp_names = set(temp_mapping.values())
                df_out = df_out.select(
                    *[col(c) for c in df_out.columns],
                    *[expr_val.alias(f"_ft_{oc}") for oc, expr_val in ft_computed.items()],
                )
                pass_through = [
                    col(c) for c in df_out.columns
                    if c not in temp_names and not c.startswith("_ft_")
                ]
                df_out = df_out.select(
                    *pass_through,
                    *[col(f"_ft_{oc}").getField("value").alias(oc) for oc in ft_computed],
                    *[col(f"_ft_{oc}").getField("error").alias(f"_perr_{oc}") for oc in ft_computed],
                )
            else:
                for temp_name in temp_mapping.values():
                    df_out = df_out.drop(temp_name)

        # --- Aggregate all _perr_* columns into parse_errors: array<string>|null ---
        err_cols = [c for c in df_out.columns if c.startswith("_perr_")]
        if err_cols:
            df_out = df_out.withColumn(
                "_parse_errors_raw",
                expr(f"filter(array({', '.join(err_cols)}), x -> x is not null)"),
            )
            df_out = df_out.withColumn(
                "parse_errors",
                when(size(col("_parse_errors_raw")) > 0, col("_parse_errors_raw")),
            )
            df_out = df_out.drop("_parse_errors_raw", *err_cols)
        else:
            df_out = df_out.withColumn("parse_errors", lit(None).cast(ArrayType(StringType())))

        # Persist each model DF so that subsequent steps (Pydantic validation,
        # quarantine scans) read from cache rather than recomputing the full
        # parser chain.  The cache is populated lazily when the first action
        # (section table write) triggers it, which happens in parallel for all
        # models via the orchestrator's ThreadPoolExecutor.
        df_out = df_out.persist(StorageLevel.MEMORY_AND_DISK)
        result[model] = df_out

    return result, None, list(result.values())


def prebuild_all_parser_udfs(all_profiles: dict) -> None:
    """Pre-register all fault-tolerant parser UDFs before batch processing starts.

    Iterates every parser/derived/computed entry across all notice-type profiles and
    calls :func:`_make_fault_tolerant_udf` for each (fn, return_type) pair.  The first
    call registers the UDF with the JVM via Py4J; subsequent calls for the same pair
    return the cached object instantly.

    Running this once on the driver thread (before the ThreadPoolExecutor starts) means
    that batch threads never race to register the same UDF simultaneously.  It also
    eliminates the Py4J round-trip overhead from the hot per-batch ``apply_column_parsers``
    path, since every UDF is already in ``_UDF_CACHE`` when the batches begin.
    """
    count_before = len(_UDF_CACHE)
    for notice_type, profile in all_profiles.items():
        for _col_name, parser_cfg in section_parsers(profile).items():
            fn_name = parser_cfg.get("fn")
            if not fn_name:
                continue
            entry = get_parser_entry(fn_name, notice_type)
            if entry is not None:
                _make_fault_tolerant_udf(entry[0], entry[1])

        for _src_col, derived_map in section_derived_cols(profile).items():
            for _derived_col, parser_cfg in derived_map.items():
                fn_name = parser_cfg.get("fn")
                if not fn_name:
                    continue
                entry = get_parser_entry(fn_name, notice_type)
                if entry is not None:
                    _make_fault_tolerant_udf(entry[0], entry[1])

        for spec in section_computed_cols(profile):
            fn_name = spec.get("fn")
            if not fn_name:
                continue
            entry = get_computed_entry(fn_name)
            if entry is not None:
                _make_fault_tolerant_udf(entry[0], entry[1])

    log.info(
        "prebuild_all_parser_udfs: registered %d new UDFs (%d total in cache)",
        len(_UDF_CACHE) - count_before,
        len(_UDF_CACHE),
    )
