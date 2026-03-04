"""Profile-driven raw HTML section parser.

This module contains generic logic for splitting notice HTML into section
payloads based on notice-specific section profiles (`notice_dicts`).
"""

from __future__ import annotations

import re

from bs4 import NavigableString, Tag


def extract_contract_notice_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def section_to_field_name(section_number: str) -> str:
    return f"cn_section_{section_number.replace('.', '_')}"


def section_number_key(section_number: str) -> tuple[int, ...]:
    return tuple(int(token) for token in section_number.split("."))


def _span_value(h3: Tag | None) -> str | None:
    if h3 is None:
        return None
    span = h3.find("span", class_="normal")
    if span is None:
        return None
    text = span.get_text().strip()
    return text or None


def _text_after_h3(h3: Tag | None) -> str | None:
    if h3 is None:
        return None
    sibling = h3.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            text = str(sibling).strip()
            if text:
                return text
        elif hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "br":
                sibling = sibling.next_sibling
                continue
            break
        sibling = sibling.next_sibling
    return None


def _collect_p_values(h3: Tag | None) -> list[str]:
    if h3 is None:
        return []
    parts: list[str] = []
    sibling = h3.next_sibling
    while sibling is not None:
        if hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "p":
                text = sibling.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        sibling = sibling.next_sibling
    return parts


def extract_contract_notice_section_value(h3: Tag) -> str | None:
    value = _span_value(h3)
    if value:
        return value
    value = _text_after_h3(h3)
    if value:
        return value
    p_values = _collect_p_values(h3)
    if p_values:
        return " ".join(p_values)
    return None


def _parent_model_path(model_path: str) -> str | None:
    if "." not in model_path:
        return None
    return model_path.rsplit(".", 1)[0]


def _last_model_token(model_path: str) -> str:
    return model_path.split(".")[-1]


def _ensure_model_slot(
    model_values: dict[str, object],
    current_index: dict[str, int],
    model_path: str,
) -> dict[str, object]:
    parent_path = _parent_model_path(model_path)
    if parent_path is None:
        bucket = model_values.setdefault(model_path, [])
        assert isinstance(bucket, list)
        idx = current_index.get(model_path, 0)
        while len(bucket) <= idx:
            bucket.append({})
        slot = bucket[idx]
        assert isinstance(slot, dict)
        return slot

    parent_slot = _ensure_model_slot(model_values, current_index, parent_path)
    child_key = _last_model_token(model_path)
    child_bucket = parent_slot.setdefault(child_key, [])
    assert isinstance(child_bucket, list)
    idx = current_index.get(model_path, 0)
    while len(child_bucket) <= idx:
        child_bucket.append({})
    slot = child_bucket[idx]
    assert isinstance(slot, dict)
    return slot


def _reset_descendant_indices(current_index: dict[str, int], parent_model_path: str) -> None:
    prefix = f"{parent_model_path}."
    for key in list(current_index.keys()):
        if key.startswith(prefix):
            current_index[key] = 0


def _reset_descendant_section_state(last_section_by_model: dict[str, tuple[int, ...]], parent_model_path: str) -> None:
    prefix = f"{parent_model_path}."
    for key in list(last_section_by_model.keys()):
        if key.startswith(prefix):
            del last_section_by_model[key]


def build_notice_sections_model(soup, notice_type: str | None, notice_dicts: dict | None = None) -> dict[str, object]:
    notice_dicts = notice_dicts or {}
    notice_sections = notice_dicts.get(notice_type or "", {})
    model_values: dict[str, object] = {"core": {}}
    current_index: dict[str, int] = {}
    last_section_by_model: dict[str, tuple[int, ...]] = {}

    for h3 in soup.find_all("h3"):
        section_number = extract_contract_notice_section_number(h3.get_text(separator=" ", strip=True))
        if not section_number:
            continue
        value = extract_contract_notice_section_value(h3)
        if not value:
            continue
        section_cfg = notice_sections.get(section_number)
        section_model = section_cfg.get("data_model") if section_cfg else None
        if section_model is None:
            continue
        section_key = section_number_key(section_number)
        field_name = section_to_field_name(section_number)

        if section_model == "core":
            if field_name in model_values["core"]:
                raise ValueError(
                    f"Duplicate core section while parsing notice_type={notice_type!r}: "
                    f"section={section_number!r}, field={field_name!r}"
                )
            model_values["core"][field_name] = value
            last_section_by_model["core"] = section_key
            continue

        tokens = section_model.split(".")
        if len(tokens) < 2:
            entity_path = section_model
            leaf_kind = "core"
            model_path = f"{entity_path}.core"
        else:
            entity_path = ".".join(tokens[:-1])
            leaf_kind = tokens[-1]
            model_path = section_model

        prev_key = last_section_by_model.get(model_path)
        if prev_key is not None and section_key <= prev_key:
            if leaf_kind == "core":
                current_index[entity_path] = current_index.get(entity_path, 0) + 1
                _reset_descendant_indices(current_index, entity_path)
                _reset_descendant_section_state(last_section_by_model, entity_path)
            else:
                current_index[model_path] = current_index.get(model_path, 0) + 1
                _reset_descendant_indices(current_index, model_path)
                _reset_descendant_section_state(last_section_by_model, model_path)

        current_index.setdefault(entity_path, 0)
        entity_slot = _ensure_model_slot(model_values, current_index, entity_path)
        if leaf_kind == "core":
            core_slot = entity_slot.setdefault("core", {})
            assert isinstance(core_slot, dict)
            if field_name not in core_slot:
                core_slot[field_name] = value
        else:
            child_bucket = entity_slot.setdefault(leaf_kind, [])
            assert isinstance(child_bucket, list)
            current_index.setdefault(model_path, 0)
            child_idx = current_index[model_path]
            while len(child_bucket) <= child_idx:
                child_bucket.append({})
            child_slot = child_bucket[child_idx]
            assert isinstance(child_slot, dict)
            if field_name not in child_slot:
                child_slot[field_name] = value
        last_section_by_model[model_path] = section_key

    return model_values
