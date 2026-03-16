"""Pydantic-model-based schema validation for section DataFrames.

Current behaviour (silver layer, all section fields ``str | None``):
- Driver-side schema check: imports the Pydantic model class for each
  ``(notice_type, model)`` pair and verifies that all expected section
  columns exist in the DataFrame.
- DataFrames are returned unchanged; only warnings/debug logs are emitted.

Upgrade path:
  When Gold types are introduced and column parsers are configured, this
  step will be upgraded to row-level validation via ``mapInPandas``, where
  each row is run through the Pydantic model and invalid rows are routed to
  a quarantine output.
"""

from __future__ import annotations

import logging
from importlib import import_module

from pyspark.sql import DataFrame

from procurement.silver.section_pipeline.notice_schema_reader import NOTICE_TYPE_TO_PROFILE_KEY

log = logging.getLogger(__name__)

# Maps lowercase model name -> Pydantic class name suffix
_MODEL_SUFFIX: dict[str, str] = {
    "core": "Core",
    "part": "Part",
    "client": "Client",
    "change_matter": "ChangeMatter",
    "criterion_procedure": "CriterionProcedure",
    "criterion_qualification": "CriterionQualification",
}

# Columns added by the pipeline that are not section payload columns
_PIPELINE_COLS: frozenset[str] = frozenset({"objectId", "publicationDateDay"})


def get_pydantic_model_class(notice_type: str, model: str):
    """Import and return the Pydantic section model class for ``(notice_type, model)``.

    Returns ``None`` if the module or class cannot be found.

    Examples
    --------
    >>> get_pydantic_model_class("ContractNotice", "core")
    <class 'ContractNoticeCoreModel'>
    >>> get_pydantic_model_class("ContractNotice", "part")
    <class 'ContractNoticePartModel'>
    """
    profile_key = NOTICE_TYPE_TO_PROFILE_KEY.get(notice_type)
    if profile_key is None:
        return None
    module_name = f"procurement.silver.notice_schemas.{profile_key}_models"
    suffix = _MODEL_SUFFIX.get(model, model.title())
    class_name = f"{notice_type}{suffix}Model"
    try:
        mod = import_module(module_name)
        cls = getattr(mod, class_name, None)
        if cls is None:
            log.debug("Class %s not found in %s", class_name, module_name)
        return cls
    except ModuleNotFoundError:
        log.debug("Model module not found: %s", module_name)
        return None


def validate_section_models(
    section_tables: dict[str, DataFrame],
    notice_type: str | None,
) -> dict[str, DataFrame]:
    """Validate section DataFrames against their Pydantic models (driver-side).

    For each ``(notice_type, model)`` pair, imports the corresponding Pydantic
    class generated from the sections profile and checks:

    - All expected section columns (model fields) are present in the DataFrame.
      Missing columns are logged as **warnings**.
    - Columns that are in the DataFrame but not in the model are logged at DEBUG
      level; the pipeline-internal columns ``objectId``, ``publicationDateDay``,
      and ``<model>_ordinal`` are silently allowed.

    DataFrames are returned **unchanged**.

    Parameters
    ----------
    section_tables:
        Output of :func:`~procurement.silver.section_pipeline.spark_table_builder.build_section_tables`
        (possibly after :func:`~procurement.silver.section_pipeline.spark_table_builder.apply_column_parsers`).
    notice_type:
        CamelCase notice type name, or ``None`` (in which case validation is skipped).

    Returns
    -------
    The same ``section_tables`` dict, unmodified.
    """
    if not notice_type:
        return section_tables

    for model, df in section_tables.items():
        model_class = get_pydantic_model_class(notice_type, model)
        if model_class is None:
            log.debug(
                "validate_section_models: no Pydantic model for notice_type=%s model=%s",
                notice_type,
                model,
            )
            continue

        expected = set(model_class.model_fields)
        actual = set(df.columns)
        pipeline_cols = _PIPELINE_COLS | {f"{model}_ordinal"}

        missing = expected - actual
        if missing:
            log.warning(
                "notice_type=%s model=%s: %d expected column(s) missing from DataFrame: %s",
                notice_type,
                model,
                len(missing),
                sorted(missing),
            )

        unexpected = actual - expected - pipeline_cols
        if unexpected:
            log.debug(
                "notice_type=%s model=%s: column(s) present but not in Pydantic model: %s",
                notice_type,
                model,
                sorted(unexpected),
            )

    return section_tables


def _make_partition_validator(model_class, payload_cols: list[str]):
    """Return a mapInPandas-compatible validator for one (notice_type, model) pair.

    The returned function takes a pandas DataFrame partition, runs each row
    through the Pydantic model, and appends a ``_validation_errors`` column
    (list of error strings, empty when the row is valid).
    """
    _cls = model_class
    _pcols = list(payload_cols)

    def _validate_partition(pdf):
        import pandas as _pd
        from pydantic import ValidationError as _VE

        errors_list: list[list[str]] = []
        for _, row in pdf.iterrows():
            row_dict = {
                c: row[c]
                for c in _pcols
                if c in row.index and not _pd.isna(row[c])
            }
            try:
                _cls(**row_dict)
                errors_list.append([])
            except _VE as exc:
                msgs = [
                    f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                    for e in exc.errors()
                ]
                errors_list.append(msgs)
        pdf = pdf.copy()
        pdf["_validation_errors"] = errors_list
        return pdf

    return _validate_partition


def apply_pydantic_validation(
    section_tables: dict[str, "DataFrame"],
    notice_type: str | None,
) -> "tuple[dict[str, DataFrame], DataFrame | None]":
    """Validate section rows against their Pydantic models via mapInPandas.

    For each ``(notice_type, model)`` pair, runs every row through the
    corresponding Pydantic model.  Rows that raise ``ValidationError`` are
    routed to a quarantine DataFrame; the rest continue through the pipeline.

    Requires pandas and a real Spark executor (skipped in the local test env).

    Parameters
    ----------
    section_tables:
        Output of :func:`apply_column_parsers`.
    notice_type:
        CamelCase notice type name, or ``None`` (validation skipped).

    Returns
    -------
    ``(valid_tables, quarantine_df)`` — same shape as the Case-2 quarantine.
    """
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col, lit, size
    from pyspark.sql.types import ArrayType, StringType, StructField

    if not notice_type or not section_tables:
        return section_tables, None

    result: dict[str, DataFrame] = {}
    quarantine_dfs: list[DataFrame] = []

    for model, df in section_tables.items():
        model_class = get_pydantic_model_class(notice_type, model)
        if model_class is None:
            log.debug(
                "apply_pydantic_validation: no Pydantic model for notice_type=%s model=%s; skipping",
                notice_type,
                model,
            )
            result[model] = df
            continue

        pipeline_cols = _PIPELINE_COLS | {f"{model}_ordinal", f"{model}_items"}
        payload_cols = [c for c in df.columns if c not in pipeline_cols]

        validate_partition = _make_partition_validator(model_class, payload_cols)
        new_schema = df.schema.add(StructField("_validation_errors", ArrayType(StringType())))

        df_validated = df.mapInPandas(validate_partition, schema=new_schema)

        quarantine_dfs.append(
            df_validated
            .filter(size(col("_validation_errors")) > 0)
            .select(
                col("objectId"),
                col("publicationDateDay"),
                lit(notice_type).alias("notice_type"),
                lit(model).alias("data_model"),
                col("_validation_errors").alias("_parse_errors"),
            )
        )
        result[model] = (
            df_validated
            .filter(size(col("_validation_errors")) == 0)
            .drop("_validation_errors")
        )
        log.debug(
            "apply_pydantic_validation built mapInPandas plan notice_type=%s model=%s",
            notice_type,
            model,
        )

    quarantine_df: DataFrame | None = None
    if quarantine_dfs:
        quarantine_df = quarantine_dfs[0]
        for qdf in quarantine_dfs[1:]:
            quarantine_df = quarantine_df.union(qdf)

    return result, quarantine_df
