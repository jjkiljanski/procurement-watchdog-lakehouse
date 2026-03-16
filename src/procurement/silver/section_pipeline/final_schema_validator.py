"""Pydantic-model-based schema validation for section DataFrames.

Two validation stages:

1. Driver-side schema check (``validate_section_models``): verifies that all
   expected section columns exist in each DataFrame; logs warnings on missing
   columns, returns DataFrames unchanged.

2. Row-level Pydantic validation (``apply_pydantic_validation``): registers a
   UDF per ``(notice_type, model)`` pair that constructs the Pydantic model for
   each row and collects ``ValidationError`` messages.  Rows with errors are
   routed to the quarantine output (Case 3); valid rows continue through the
   pipeline.  Same two-pass pattern as the Case-2 strict-parser UDF in
   ``spark_table_builder``.
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


def _make_pydantic_validator_fn(model_class, payload_cols: list[str]):
    """Return a UDF-compatible validator for one (notice_type, model) pair.

    The returned function takes one positional argument per payload column and
    returns a list of error strings (empty list means the row is valid).
    This mirrors the pattern used by :func:`_make_validator_fn` in
    ``spark_table_builder`` for Case-2 strict-parser validation.
    """
    _cls = model_class
    _cols = list(payload_cols)

    def _validate(*values) -> list[str]:
        from pydantic import ValidationError as _VE

        row_dict = {c: v for c, v in zip(_cols, values) if v is not None}
        try:
            _cls(**row_dict)
            return []
        except _VE as exc:
            return [
                f"{'.'.join(str(loc) for loc in e['loc'])}: {e['msg']}"
                for e in exc.errors()
            ]

    return _validate


def apply_pydantic_validation(
    section_tables: dict[str, "DataFrame"],
    notice_type: str | None,
) -> "tuple[dict[str, DataFrame], DataFrame | None]":
    """Validate section rows against their Pydantic models via a row-level UDF.

    For each ``(notice_type, model)`` pair, registers a UDF that constructs the
    Pydantic model from the row's payload columns and returns validation error
    strings.  Rows with at least one error are routed to the quarantine
    DataFrame; the rest continue through the pipeline.

    Follows the same two-pass pattern as Case-2 (``apply_column_parsers``):
    one ``.withColumn`` call adds ``_pydantic_errors``; the quarantine and
    valid DataFrames are then split by filtering on that column.

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
    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.functions import udf

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

        validator_fn = _make_pydantic_validator_fn(model_class, payload_cols)
        validator_udf = udf(validator_fn, ArrayType(StringType()))

        df_out = df.withColumn("_pydantic_errors", validator_udf(*[col(c) for c in payload_cols]))

        quarantine_dfs.append(
            df_out
            .filter(size(col("_pydantic_errors")) > 0)
            .select(
                col("objectId"),
                col("publicationDateDay"),
                lit(notice_type).alias("notice_type"),
                lit(model).alias("data_model"),
                col("_pydantic_errors").alias("_parse_errors"),
            )
        )
        result[model] = (
            df_out
            .filter(size(col("_pydantic_errors")) == 0)
            .drop("_pydantic_errors")
        )
        log.debug(
            "apply_pydantic_validation registered UDF notice_type=%s model=%s cols=%d",
            notice_type,
            model,
            len(payload_cols),
        )

    quarantine_df: DataFrame | None = None
    if quarantine_dfs:
        quarantine_df = quarantine_dfs[0]
        for qdf in quarantine_dfs[1:]:
            quarantine_df = quarantine_df.union(qdf)

    return result, quarantine_df
