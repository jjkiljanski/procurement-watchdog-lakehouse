"""Tier 3 tests: Pydantic section model lookup and profile↔model consistency.

Checks:
  - get_pydantic_model_class() resolves the right class for every
    (notice_type, data_model) combination that exists in the profiles.
  - Every Pydantic model class can be instantiated with no arguments
    (all fields must be Optional).
  - For every model without a nested sub-list: the set of col_names from
    the profile exactly matches the set of fields in the Pydantic model.
  - For models with a nested sub-list (currently only NoticeUpdateNotice/part):
    the profile col_names for the core level are a subset of the model fields.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from procurement.silver.section_pipeline.final_schema_validator import get_pydantic_model_class
from procurement.silver.section_pipeline.notice_schema_reader import (
    NOTICE_TYPE_TO_PROFILE_KEY,
    load_all_profiles,
    model_core_col_names,
    model_sub_info,
    top_level_models,
)

# ---------------------------------------------------------------------------
# Build parametrize list at collection time so failures name the pair exactly
# ---------------------------------------------------------------------------

_profiles = load_all_profiles()

_ALL_NT_MODEL_PAIRS = [
    (nt, model)
    for nt in NOTICE_TYPE_TO_PROFILE_KEY
    for model in top_level_models(_profiles[nt])
]


# ===========================================================================
# get_pydantic_model_class
# ===========================================================================


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_pydantic_class_resolvable(notice_type, model):
    """Every (notice_type, data_model) pair must have a resolvable Pydantic class."""
    cls = get_pydantic_model_class(notice_type, model)
    assert cls is not None, (
        f"get_pydantic_model_class({notice_type!r}, {model!r}) returned None"
    )


def test_unknown_notice_type_returns_none():
    assert get_pydantic_model_class("NonExistentNotice", "core") is None


def test_unknown_model_returns_none():
    # Valid notice type but model name not in _MODEL_SUFFIX and no class with that suffix
    assert get_pydantic_model_class("ContractNotice", "nonexistent_model") is None


# ===========================================================================
# Instantiation (all fields must be Optional / have defaults)
# ===========================================================================


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_pydantic_model_instantiates_with_no_args(notice_type, model):
    """No-arg instantiation must succeed — all section fields are str | None."""
    cls = get_pydantic_model_class(notice_type, model)
    instance = cls()
    assert instance is not None


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_pydantic_model_has_at_least_one_field(notice_type, model):
    cls = get_pydantic_model_class(notice_type, model)
    assert len(cls.model_fields) >= 1, (
        f"{notice_type}/{model}: Pydantic model has no fields"
    )


# ===========================================================================
# Profile ↔ model field cross-check
# ===========================================================================


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_profile_col_names_present_in_pydantic_model(notice_type, model):
    """Every col_name declared in the profile must appear as a field in the model.

    This catches two classes of drift:
      - A section added to the JSON but the model not regenerated.
      - A section renamed in the JSON without updating the model.
    """
    profile = _profiles[notice_type]
    expected_cols = set(model_core_col_names(profile, model))
    cls = get_pydantic_model_class(notice_type, model)
    model_fields = set(cls.model_fields.keys())

    missing = expected_cols - model_fields
    assert not missing, (
        f"{notice_type}/{model}: {len(missing)} profile col(s) missing from "
        f"Pydantic model: {sorted(missing)}"
    )


@pytest.mark.parametrize("notice_type,model", [
    (nt, m)
    for nt, m in _ALL_NT_MODEL_PAIRS
    if model_sub_info(_profiles[nt], m)[0] is None  # no sub-list
])
def test_pydantic_model_fields_match_profile_exactly(notice_type, model):
    """For models without sub-lists, profile cols and model fields must be identical.

    This catches the reverse drift: a field in the Pydantic model that has no
    corresponding section in the profile JSON.
    """
    profile = _profiles[notice_type]
    expected_cols = set(model_core_col_names(profile, model))
    cls = get_pydantic_model_class(notice_type, model)
    model_fields = set(cls.model_fields.keys())

    extra_in_model = model_fields - expected_cols
    assert not extra_in_model, (
        f"{notice_type}/{model}: {len(extra_in_model)} field(s) in Pydantic model "
        f"have no matching profile entry: {sorted(extra_in_model)}"
    )
