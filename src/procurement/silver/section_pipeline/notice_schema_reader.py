"""Load and inspect notice sections profile JSONs.

This module is the single source of truth for:
- mapping camelCase notice type names to their profile file keys
- loading the profile dicts (section_number -> {col_name, data_model, ...})
- deriving per-model column lists used to build Spark schemas and Pydantic models
"""

from __future__ import annotations

import json
from pathlib import Path

_PROFILES_DIR = Path(__file__).parent.parent / "notice_schemas"

# Maps camelCase notice type name -> snake_case profile file stem
NOTICE_TYPE_TO_PROFILE_KEY: dict[str, str] = {
    "AgreementIntentionNotice": "agreement_intention_notice",
    "AgreementUpdateNotice": "agreement_update_notice",
    "CircumstancesFulfillmentNotice": "circumstances_fulfillment_notice",
    "CompetitionNotice": "competition_notice",
    "ConcessionNotice": "concession_notice",
    "ContractNotice": "contract_notice",
    "ContractPerformingNotice": "contract_performing_notice",
    "NoticeUpdateNotice": "notice_update_notice",
    "SmallContractNotice": "small_contract_notice",
    "TenderResultNotice": "tender_result_notice",
}


def load_profile(notice_type: str) -> dict:
    """Load the sections profile JSON for one notice type.

    Returns empty dict if the type is unknown or its profile file is missing.
    """
    key = NOTICE_TYPE_TO_PROFILE_KEY.get(notice_type)
    if key is None:
        return {}
    path = _PROFILES_DIR / f"{key}_profile.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def load_all_profiles() -> dict[str, dict]:
    """Load all sections profiles, keyed by camelCase notice type name."""
    return {nt: load_profile(nt) for nt in NOTICE_TYPE_TO_PROFILE_KEY}


def top_level_models(profile: dict) -> list[str]:
    """Return sorted distinct top-level data model names present in a profile.

    'part.core' and 'part.part' both resolve to 'part'.
    Example result: ['client', 'core', 'part']
    """
    seen: set[str] = set()
    for cfg in profile.values():
        dm = cfg.get("data_model")
        if dm:
            seen.add(dm.split(".")[0])
    return sorted(seen)


def model_core_col_names(profile: dict, model: str) -> list[str]:
    """Return col_names for sections that belong to a model's 'core' level.

    - model='core'   → sections with data_model='core'
    - model='part'   → sections with data_model='part' or 'part.core'
    - model='client' → sections with data_model='client' or 'client.core'

    Order follows profile key iteration order (insertion order, Python 3.7+).
    """
    result: list[str] = []
    seen: set[str] = set()
    for cfg in profile.values():
        dm = cfg.get("data_model", "")
        tokens = dm.split(".")
        top = tokens[0]
        # leaf defaults to 'core' when there is no dot (single-level model)
        leaf = tokens[-1] if len(tokens) > 1 else "core"
        if top == model and leaf == "core":
            col = cfg.get("col_name")
            if col and col not in seen:
                result.append(col)
                seen.add(col)
    return result


def section_derived_cols(profile: dict) -> dict[str, dict[str, dict]]:
    """Return derived-column definitions keyed by their source col_name.

    For profile entries that have a ``"derived_cols"`` mapping, returns:
        {source_col: {derived_col_name: {fn: "<parser_fn>", ...}, ...}, ...}

    Entries without ``"derived_cols"`` are omitted.
    """
    result: dict[str, dict[str, dict]] = {}
    for cfg in profile.values():
        dc = cfg.get("derived_cols")
        if not isinstance(dc, dict) or not dc:
            continue
        col_name = cfg.get("col_name")
        if col_name:
            result[col_name] = dc
    return result


def model_output_col_names(profile: dict, model: str) -> list[str]:
    """Like :func:`model_core_col_names`, but expands ``derived_cols`` entries.

    Source cols that have a ``derived_cols`` mapping are replaced by their
    derived col names in the returned list.  Source cols without
    ``derived_cols`` are kept as-is.  Use this to compare against Pydantic
    model fields *after* :func:`~spark_table_builder.apply_column_parsers` has run.
    """
    derived = section_derived_cols(profile)
    result: list[str] = []
    seen: set[str] = set()
    for cfg in profile.values():
        dm = cfg.get("data_model", "")
        tokens = dm.split(".")
        top = tokens[0]
        leaf = tokens[-1] if len(tokens) > 1 else "core"
        if top != model or leaf != "core":
            continue
        col = cfg.get("col_name")
        if not col:
            continue
        if col in derived:
            for derived_col in derived[col]:
                if derived_col not in seen:
                    result.append(derived_col)
                    seen.add(derived_col)
        else:
            if col not in seen:
                result.append(col)
                seen.add(col)
    return result


def section_parsers(profile: dict) -> dict[str, dict]:
    """Return {col_name: parser_config} for sections that have a non-null parser.

    Parser config has at least ``{"fn": "<function_name>"}`` and optionally
    ``{"args": {...}}`` for extra keyword arguments passed to the function.

    Sections without a ``"parser"`` key, or with ``null``/missing ``"fn"``,
    are omitted.  All current profiles return ``{}`` (parsers are configured
    per-column when the Gold typing phase begins).
    """
    result: dict[str, dict] = {}
    for cfg in profile.values():
        parser = cfg.get("parser")
        if not isinstance(parser, dict) or not parser.get("fn"):
            continue
        col_name = cfg.get("col_name")
        if col_name:
            result[col_name] = parser
    return result


def model_sub_info(profile: dict, model: str) -> tuple[str | None, list[str]]:
    """Return (sub_key, col_names) for the two-level sub-list of a model.

    For 'part.part' sections the sub_key is 'part' and col_names are the
    col_name values of all such sections.

    Returns (None, []) if the model has no sub-level.
    """
    sub_entries: dict[str, list[str]] = {}
    seen: dict[str, set[str]] = {}
    for cfg in profile.values():
        dm = cfg.get("data_model", "")
        tokens = dm.split(".")
        if len(tokens) < 2:
            continue
        top = tokens[0]
        leaf = tokens[-1]
        if top != model or leaf == "core":
            continue
        col = cfg.get("col_name")
        if not col:
            continue
        if leaf not in seen:
            seen[leaf] = set()
            sub_entries[leaf] = []
        if col not in seen[leaf]:
            sub_entries[leaf].append(col)
            seen[leaf].add(col)

    if not sub_entries:
        return None, []
    # There should be at most one sub_key per parent model in practice
    sub_key = next(iter(sub_entries))
    return sub_key, sub_entries[sub_key]
