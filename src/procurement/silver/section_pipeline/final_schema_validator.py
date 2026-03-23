"""Pydantic-model-based schema validation for section DataFrames.

Driver-side schema check (``validate_section_models``): verifies that all
expected section columns exist in each DataFrame; logs warnings on missing
columns, returns DataFrames unchanged.
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
_PIPELINE_COLS: frozenset[str] = frozenset({"objectId", "publicationDateDay", "parse_errors"})


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
        pipeline_cols = _PIPELINE_COLS | {f"{model}_ordinal"} | {c for c in actual if c.endswith("_items")}

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


