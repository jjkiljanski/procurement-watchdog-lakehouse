"""Tier 3 tests: profile JSON integrity and notice_schema_reader logic.

Two groups:
  1. Contract tests against the real profile JSON files — guards against
     structural drift (missing keys, unknown data_model values, parser
     fn references that aren't registered).
  2. Pure-logic unit tests for notice_schema_reader helper functions using
     hand-crafted profile dicts — independent of the real files.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from procurement.silver.section_pipeline.notice_schema_reader import (
    NOTICE_TYPE_TO_PROFILE_KEY,
    load_all_profiles,
    load_profile,
    model_core_col_names,
    model_sub_info,
    section_derived_cols,
    section_parsers,
    top_level_models,
)
from procurement.silver.section_pipeline.parser_registry import registered_fn_names

# ---------------------------------------------------------------------------
# Known good sets — update here when the pipeline intentionally gains new
# data_model types or new notice types.
# ---------------------------------------------------------------------------

_KNOWN_DATA_MODELS: frozenset[str] = frozenset({
    "core",
    "client",
    "part",
    "part.core",
    "part.part",
    "change_matter",
    "criterion_procedure",
    "criterion_qualification",
})

_ALL_NOTICE_TYPES = list(NOTICE_TYPE_TO_PROFILE_KEY.keys())


# ===========================================================================
# 1. Contract tests against real profile JSON files
# ===========================================================================


def test_all_profiles_load_non_empty():
    """Every registered notice type has a non-empty profile."""
    profiles = load_all_profiles()
    assert set(profiles.keys()) == set(_ALL_NOTICE_TYPES)
    for nt, profile in profiles.items():
        assert len(profile) > 0, f"{nt}: profile is empty"


def test_load_profile_unknown_type_returns_empty_dict():
    assert load_profile("NonExistentType") == {}


@pytest.mark.parametrize("notice_type", _ALL_NOTICE_TYPES)
def test_every_entry_has_required_keys(notice_type):
    """col_name, data_model, and section_header must be present and non-empty."""
    profile = load_profile(notice_type)
    for section_num, cfg in profile.items():
        assert "col_name" in cfg and cfg["col_name"], (
            f"{notice_type} section {section_num}: missing or empty col_name"
        )
        assert "data_model" in cfg and cfg["data_model"], (
            f"{notice_type} section {section_num}: missing or empty data_model"
        )
        assert "section_header" in cfg and cfg["section_header"], (
            f"{notice_type} section {section_num}: missing or empty section_header"
        )


@pytest.mark.parametrize("notice_type", _ALL_NOTICE_TYPES)
def test_all_data_model_values_are_known(notice_type):
    """No data_model value outside the declared set."""
    profile = load_profile(notice_type)
    for section_num, cfg in profile.items():
        dm = cfg.get("data_model", "")
        assert dm in _KNOWN_DATA_MODELS, (
            f"{notice_type} section {section_num}: unexpected data_model={dm!r}"
        )


@pytest.mark.parametrize("notice_type", _ALL_NOTICE_TYPES)
def test_col_names_unique_within_profile(notice_type):
    """Two different section numbers must not share a col_name."""
    profile = load_profile(notice_type)
    seen: dict[str, str] = {}
    for section_num, cfg in profile.items():
        col = cfg.get("col_name", "")
        assert col not in seen, (
            f"{notice_type}: col_name {col!r} used by both section "
            f"{seen[col]!r} and {section_num!r}"
        )
        seen[col] = section_num


@pytest.mark.parametrize("notice_type", _ALL_NOTICE_TYPES)
def test_parser_fn_references_are_registered(notice_type):
    """Every parser.fn value referenced in a profile must exist in the registry.

    Currently all profiles have parser=null, so this test passes trivially
    and will catch future typos the moment a parser is configured.
    """
    profile = load_profile(notice_type)
    parsers = section_parsers(profile)           # {col_name: {fn: "..."}}
    available = registered_fn_names(notice_type)
    for col_name, parser_cfg in parsers.items():
        fn = parser_cfg.get("fn", "")
        assert fn in available, (
            f"{notice_type} col {col_name!r}: parser fn {fn!r} is not registered"
        )


@pytest.mark.parametrize("notice_type", _ALL_NOTICE_TYPES)
def test_derived_cols_fn_references_are_registered(notice_type):
    """Every fn referenced inside a derived_cols mapping must exist in the registry."""
    profile = load_profile(notice_type)
    derived = section_derived_cols(profile)
    available = registered_fn_names(notice_type)
    for source_col, derived_map in derived.items():
        for derived_col, parser_cfg in derived_map.items():
            fn = parser_cfg.get("fn", "")
            assert fn in available, (
                f"{notice_type} source={source_col!r} derived={derived_col!r}: "
                f"fn {fn!r} is not registered"
            )


# ===========================================================================
# 2. Pure-logic unit tests for notice_schema_reader helpers
#    Use hand-crafted profile dicts to avoid depending on real JSON files.
# ===========================================================================

_MIXED_PROFILE = {
    "1.1": {"col_name": "col_1_1", "data_model": "core"},
    "1.2": {"col_name": "col_1_2", "data_model": "core"},
    "2.1": {"col_name": "col_2_1", "data_model": "client"},
    "2.2": {"col_name": "col_2_2", "data_model": "client"},
    "3.1": {"col_name": "col_3_1", "data_model": "part"},
    "3.2": {"col_name": "col_3_2", "data_model": "part"},
    "4.1": {"col_name": "col_4_1", "data_model": "part.core"},
    "5.1": {"col_name": "col_5_1", "data_model": "part.part"},
    "6.1": {"col_name": "col_6_1", "data_model": "criterion_procedure"},
}

_PARSER_PROFILE = {
    "1.1": {"col_name": "col_1_1", "data_model": "core", "parser": {"fn": "parse_tak_nie"}},
    "1.2": {"col_name": "col_1_2", "data_model": "core", "parser": None},
    "1.3": {"col_name": "col_1_3", "data_model": "core"},
}


class TestTopLevelModels:
    def test_returns_distinct_top_level_names(self):
        result = top_level_models(_MIXED_PROFILE)
        # part.core and part.part both → "part"; criterion_procedure → "criterion_procedure"
        assert result == ["client", "core", "criterion_procedure", "part"]

    def test_sorted_order(self):
        profile = {
            "1": {"col_name": "c1", "data_model": "part"},
            "2": {"col_name": "c2", "data_model": "core"},
            "3": {"col_name": "c3", "data_model": "client"},
        }
        assert top_level_models(profile) == ["client", "core", "part"]

    def test_empty_profile_returns_empty(self):
        assert top_level_models({}) == []

    def test_single_model_profile(self):
        profile = {"1.1": {"col_name": "c", "data_model": "core"}}
        assert top_level_models(profile) == ["core"]


class TestModelCoreCols:
    def test_core_model_returns_core_cols(self):
        result = model_core_col_names(_MIXED_PROFILE, "core")
        assert result == ["col_1_1", "col_1_2"]

    def test_single_token_model_treated_as_core(self):
        # data_model='client' (single token) → leaf defaults to "core"
        result = model_core_col_names(_MIXED_PROFILE, "client")
        assert result == ["col_2_1", "col_2_2"]

    def test_part_includes_both_part_and_part_core(self):
        # data_model='part' (leaf=core) and 'part.core' (leaf=core) both qualify
        result = model_core_col_names(_MIXED_PROFILE, "part")
        assert "col_3_1" in result
        assert "col_3_2" in result
        assert "col_4_1" in result

    def test_part_excludes_part_part(self):
        # data_model='part.part' → leaf='part', not 'core' → excluded
        result = model_core_col_names(_MIXED_PROFILE, "part")
        assert "col_5_1" not in result

    def test_unknown_model_returns_empty(self):
        assert model_core_col_names(_MIXED_PROFILE, "nonexistent") == []

    def test_no_duplicates(self):
        result = model_core_col_names(_MIXED_PROFILE, "part")
        assert len(result) == len(set(result))


class TestSectionParsers:
    def test_null_parser_entry_excluded(self):
        result = section_parsers(_PARSER_PROFILE)
        assert "col_1_2" not in result

    def test_missing_parser_key_excluded(self):
        result = section_parsers(_PARSER_PROFILE)
        assert "col_1_3" not in result

    def test_valid_fn_included(self):
        result = section_parsers(_PARSER_PROFILE)
        assert "col_1_1" in result
        assert result["col_1_1"]["fn"] == "parse_tak_nie"

    def test_empty_profile_returns_empty(self):
        assert section_parsers({}) == {}


class TestModelSubInfo:
    def test_no_sub_level_returns_none_empty(self):
        # 'part' sections with only 'part.core' — no 'part.part'
        profile = {
            "3.1": {"col_name": "col_3_1", "data_model": "part.core"},
        }
        sub_key, sub_cols = model_sub_info(profile, "part")
        assert sub_key is None
        assert sub_cols == []

    def test_detects_part_part_sub_list(self):
        profile = {
            "4.1": {"col_name": "col_4_1", "data_model": "part.core"},
            "5.1": {"col_name": "col_5_1", "data_model": "part.part"},
            "5.2": {"col_name": "col_5_2", "data_model": "part.part"},
        }
        sub_key, sub_cols = model_sub_info(profile, "part")
        assert sub_key == "part"
        assert "col_5_1" in sub_cols
        assert "col_5_2" in sub_cols

    def test_core_model_has_no_sub_level(self):
        sub_key, sub_cols = model_sub_info(_MIXED_PROFILE, "core")
        assert sub_key is None
        assert sub_cols == []

    def test_notice_update_notice_has_part_sub(self):
        """Real profile regression test: NoticeUpdateNotice/part has part.part sub-cols."""
        profiles = load_all_profiles()
        sub_key, sub_cols = model_sub_info(profiles["NoticeUpdateNotice"], "part")
        assert sub_key == "part"
        assert len(sub_cols) >= 1

    def test_contract_notice_has_no_sub_lists(self):
        """ContractNotice uses flat 'part' — no sub-list."""
        profiles = load_all_profiles()
        sub_key, sub_cols = model_sub_info(profiles["ContractNotice"], "part")
        assert sub_key is None
        assert sub_cols == []
