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
  - Parsed example values from the profile pass Pydantic validation without
    type errors (catches parser output type ↔ model field type mismatches).
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
    model_output_col_names,
    output_models,
)
from procurement.silver.section_pipeline.parser_registry import get_parser_entry

# ---------------------------------------------------------------------------
# Build parametrize list at collection time so failures name the pair exactly
# ---------------------------------------------------------------------------

_profiles = load_all_profiles()

_ALL_NT_MODEL_PAIRS = [
    (nt, model)
    for nt in NOTICE_TYPE_TO_PROFILE_KEY
    for model in output_models(_profiles[nt])
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
    expected_cols = set(model_output_col_names(profile, model))
    cls = get_pydantic_model_class(notice_type, model)
    model_fields = set(cls.model_fields.keys())

    missing = expected_cols - model_fields
    assert not missing, (
        f"{notice_type}/{model}: {len(missing)} profile col(s) missing from "
        f"Pydantic model: {sorted(missing)}"
    )


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_pydantic_model_fields_match_profile_exactly(notice_type, model):
    """For models without sub-lists, profile cols and model fields must be identical.

    This catches the reverse drift: a field in the Pydantic model that has no
    corresponding section in the profile JSON.
    """
    profile = _profiles[notice_type]
    expected_cols = set(model_output_col_names(profile, model))
    cls = get_pydantic_model_class(notice_type, model)
    model_fields = set(cls.model_fields.keys())

    extra_in_model = model_fields - expected_cols
    assert not extra_in_model, (
        f"{notice_type}/{model}: {len(extra_in_model)} field(s) in Pydantic model "
        f"have no matching profile entry: {sorted(extra_in_model)}"
    )


# ===========================================================================
# Parser output type ↔ Pydantic model field type
# ===========================================================================


def _build_parsed_row(notice_type: str, model: str, profile: dict) -> dict:
    """Build a {col_name: parsed_value} dict from profile example_values.

    For each section belonging to ``model``:
    - If the section has ``derived_cols``, apply each derived parser function
      to the first example value and store the result under the output col name.
    - If the section has a ``parser``, apply it to the first example value and
      store the result under the section's col_name.
    - If neither, store the raw example string.

    Parser exceptions (e.g. ``ParseError`` on a bad example) are silenced and
    the field is omitted from the row — the test is about type compatibility,
    not parser correctness on every example.
    """
    # Col-names overwritten by _computed_cols must be excluded from the raw
    # string assignment — they are set by multi-source compute functions (e.g.
    # compute_duration_days) whose output type differs from the raw HTML string.
    computed_col_names: set[str] = set()
    computed_list = profile.get("_computed_cols") or []
    if isinstance(computed_list, list):
        for comp in computed_list:
            if isinstance(comp, dict):
                comp_dm = comp.get("data_model", "")
                if comp_dm.split(".")[0] == model:
                    out_col = comp.get("col_name")
                    if out_col:
                        computed_col_names.add(out_col)

    row: dict = {}
    for section_num, cfg in profile.items():
        if not isinstance(cfg, dict):
            continue
        dm = cfg.get("data_model", "")
        if dm.split(".")[0] != model:
            continue
        col_name = cfg.get("col_name")
        examples = cfg.get("example_values") or []
        if not col_name or not examples:
            continue
        raw = examples[0]

        derived_cols = cfg.get("derived_cols") or {}
        if derived_cols:
            for out_col, dcfg in derived_cols.items():
                fn_name = dcfg.get("fn") if isinstance(dcfg, dict) else None
                if not fn_name:
                    continue
                entry = get_parser_entry(fn_name, notice_type)
                if not entry:
                    continue
                fn, _ = entry
                try:
                    row[out_col] = fn(raw)
                except Exception:
                    pass  # bad example — skip; we only care about type compatibility
        else:
            parser_cfg = cfg.get("parser") or {}
            fn_name = parser_cfg.get("fn") if isinstance(parser_cfg, dict) else None
            if fn_name:
                entry = get_parser_entry(fn_name, notice_type)
                if entry:
                    fn, _ = entry
                    try:
                        row[col_name] = fn(raw)
                    except Exception:
                        pass
                else:
                    row[col_name] = raw
            elif col_name not in computed_col_names:
                row[col_name] = raw
    return row


@pytest.mark.parametrize("notice_type,model", _ALL_NT_MODEL_PAIRS)
def test_parsed_example_values_pass_pydantic_validation(notice_type, model):
    """Parsed profile example values must not raise Pydantic ValidationError.

    This test catches parser output type ↔ model field type mismatches such as
    a ``bool`` value (from ``parse_tak_nie``) landing in a ``str | None`` field,
    or a ``list[str]`` (from ``parse_cpv_codes``) landing in a ``str | None`` field.

    The row is built by applying each section's registered parser to the first
    ``example_value`` listed in the profile.  Fields where the parser raises
    (unusual example text, strict parser rejection) are omitted rather than
    failing the test — the goal is type compatibility, not exhaustive value coverage.
    """
    from pydantic import ValidationError

    profile = _profiles[notice_type]
    cls = get_pydantic_model_class(notice_type, model)
    row = _build_parsed_row(notice_type, model, profile)

    try:
        cls(**row)
    except ValidationError as exc:
        errors = exc.errors()
        lines = [f"  {e['loc']}: {e['msg']} (input={e.get('input')!r})" for e in errors]
        pytest.fail(
            f"{notice_type}/{model}: {len(errors)} field(s) failed Pydantic validation "
            f"after parser application — likely a parser output type mismatch:\n"
            + "\n".join(lines)
        )
