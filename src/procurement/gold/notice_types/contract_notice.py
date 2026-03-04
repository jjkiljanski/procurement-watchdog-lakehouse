"""Gold-layer extraction helpers for ContractNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input (replace soup-based parsing with column reads).
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup
from pydantic import ValidationError

from procurement.silver.parser_utils import (
    _find_h3,
    _span_value,
    _text_after_h3,
)
from procurement.silver.html_value_parsers.common_values import (
    _parse_criterion_weight,
    _parse_pln_value,
    _parse_tak_nie,
    parse_cpv_codes,
)
from procurement.silver.models import (
    ContractNoticePart,
    EvalCriterion,
    ExtractedValues,
)
from procurement.silver.raw_html_sections_parser import (
    extract_contract_notice_section_number as _extract_contract_notice_section_number,
    extract_contract_notice_section_value as _extract_contract_notice_section_value,
    section_to_field_name as _section_to_field_name,
)
# TODO: ContractNoticePartRaw will be removed; _extract_contract_notice_parts
#       must be rewritten to read from Silver section-column tables.
from procurement.silver.notice_types.contract_notice_split_models import (
    ContractNoticePartRaw,
)


def _extract_description(soup: BeautifulSoup) -> str | None:
    """Extract short procurement description from field 4.2.2."""
    h3 = _find_h3(soup, "4.2.2.")
    if h3 is None:
        return None
    p = h3.find_next_sibling("p")
    if p is None:
        return None
    text = p.get_text(separator=" ", strip=True)
    return text or None


def _extract_criteria(soup: BeautifulSoup) -> list[EvalCriterion] | None:
    """Extract bid evaluation criteria (name + weight) from SEKCJA IV."""
    criteria: list[EvalCriterion] = []
    h3s = soup.find_all("h3")

    ordinal = 1
    i = 0
    while i < len(h3s):
        text = h3s[i].get_text()
        if "4.3.5.)" in text:
            criterion_text = _span_value(h3s[i])
            # Next h3 should be 4.3.6 with weight
            weight = None
            if i + 1 < len(h3s) and "4.3.6.)" in h3s[i + 1].get_text():
                raw = _span_value(h3s[i + 1])
                weight = _parse_criterion_weight(raw)
            if criterion_text and weight is not None:
                criteria.append(EvalCriterion(no=ordinal, str=criterion_text, weight=weight))
                ordinal += 1
        i += 1

    return criteria or None


def _extract_criteria_aspects_4310(soup: BeautifulSoup) -> tuple[str | None, bool | None]:
    """Extract ContractNotice field 4.3.10 text + Tak/Nie flag."""
    h3 = _find_h3(soup, "4.3.10.")
    raw = _span_value(h3) or _text_after_h3(h3)
    return raw, _parse_tak_nie(raw)


def _extract_cn_partial_offers_allowed_418(soup: BeautifulSoup) -> bool | None:
    """Extract ContractNotice field 4.1.8 as boolean Tak/Nie/null."""
    h3 = _find_h3(soup, "4.1.8.")
    raw = _span_value(h3) or _text_after_h3(h3)
    return _parse_tak_nie(raw)


def _extract_cn_offers_scope_4110(soup: BeautifulSoup) -> str | None:
    """Extract ContractNotice field 4.1.10 and map to: wszystkie/kilka/jedna."""
    h3 = _find_h3(soup, "4.1.10.")
    if h3 is None:
        return None
    raw = _span_value(h3) or _text_after_h3(h3) or h3.get_text(separator=" ", strip=True)
    return _map_cn_offers_scope(raw)


def _map_cn_offers_scope(raw: str | None) -> str | None:
    """Map raw 4.1.10 content to canonical categories."""
    if not raw:
        return None
    from procurement.silver.parser_utils import _normalize_label_text
    normalized = _normalize_label_text(raw or "")
    normalized = re.sub(r"^\s*4\.1\.10\.\)\s*", "", normalized).strip()
    if not normalized:
        return None
    if "wszystkie" in normalized:
        return "wszystkie"
    if "kilka" in normalized:
        return "kilka"
    if "jedna" in normalized:
        return "jedna"
    return None


def _extract_values_contract_notice(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractNotice: fields 4.1.5 (total value), 4.1.6 (net of VAT)."""
    value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.1.5.")))
    if value_estimated_procurement is None:
        value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.1.6.")))
    if value_estimated_procurement is None:
        return None
    return ExtractedValues(value_estimated_procurement=value_estimated_procurement)


def _stringify_section_dict(payload: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in payload.items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def _extract_part_id_from_header(text: str) -> str | None:
    """Extract part identifier from headers like 'Czesc/Czesc nr/Czesc 1'."""
    lowered = text.lower()
    if "4.3." in lowered:
        return None
    if len(lowered) > 64:
        return None
    if not lowered.strip().startswith("cz"):
        return None

    match = re.search(r"(?:nr\s*)?([a-z0-9._/-]+)\s*$", lowered, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _extract_contract_notice_part_chunks(h3s):
    part_headers = []
    for idx, h3 in enumerate(h3s):
        text = h3.get_text(separator=" ", strip=True)
        part_id = _extract_part_id_from_header(text)
        if part_id:
            part_headers.append((idx, part_id))

    chunks = []
    if part_headers:
        for i, (start_idx, part_id) in enumerate(part_headers):
            end_idx = part_headers[i + 1][0] if i + 1 < len(part_headers) else len(h3s)
            chunks.append((part_id, h3s[start_idx:end_idx]))
    else:
        chunks.append((None, h3s))
    return chunks


def _extract_contract_notice_parts(
    soup: BeautifulSoup,
) -> tuple[list[ContractNoticePart] | None, list[dict[str, str]] | None]:
    """Extract per-part criteria + CPV blocks from ContractNotice SEKCJA IV.

    TODO: rewrite to read from Silver ContractNotice_part section-column table
          once ContractNoticePartRaw is removed.
    """
    h3s = soup.find_all("h3")
    if not h3s:
        return None, None

    parts: list[ContractNoticePart] = []
    parts_raw_sections: list[dict[str, str]] = []
    chunks = _extract_contract_notice_part_chunks(h3s)
    part_field_names = {
        name for name in ContractNoticePartRaw.model_fields.keys() if name.startswith("cn_section_")
    }

    for part_id, chunk in chunks:
        section_payload: dict[str, str] = {}
        for h3 in chunk:
            section_number = _extract_contract_notice_section_number(h3.get_text(separator=" ", strip=True))
            if not section_number:
                continue
            field_name = _section_to_field_name(section_number)
            if field_name not in part_field_names or field_name in section_payload:
                continue
            value = _extract_contract_notice_section_value(h3)
            if value:
                section_payload[field_name] = value

        has_marker = any(
            field_name in section_payload
            for field_name in (
                "cn_section_4_2_2",
                "cn_section_4_2_6",
                "cn_section_4_2_7",
                "cn_section_4_2_10",
                "cn_section_4_3_5",
                "cn_section_4_3_6",
                "cn_section_4_3_10",
            )
        )
        if not has_marker:
            continue

        try:
            part_raw = ContractNoticePartRaw(**section_payload)
        except ValidationError:
            part_raw = ContractNoticePartRaw()
        part_raw_map = _stringify_section_dict(part_raw.model_dump(exclude_none=True))

        criteria: list[EvalCriterion] = []
        opis: str | None = part_raw.cn_section_4_2_2
        main_cpv: str | None = None
        secondary_cpv: list[str] = []
        contract_planned_execution_date: str | None = part_raw.cn_section_4_2_10
        ordinal = 1
        j = 0
        while j < len(chunk):
            text = chunk[j].get_text()
            if "4.3.5.)" in text:
                criterion_text = _span_value(chunk[j])
                weight = None
                if j + 1 < len(chunk) and "4.3.6.)" in chunk[j + 1].get_text():
                    raw_weight = _span_value(chunk[j + 1])
                    weight = _parse_criterion_weight(raw_weight)
                if criterion_text and weight is not None:
                    criteria.append(EvalCriterion(no=ordinal, str=criterion_text, weight=weight))
                    ordinal += 1
            j += 1

        raw_main_cpv = part_raw.cn_section_4_2_6
        if raw_main_cpv:
            parsed = parse_cpv_codes(raw_main_cpv)
            main_cpv = parsed[0] if parsed else raw_main_cpv

        raw_secondary_cpv = part_raw.cn_section_4_2_7
        if raw_secondary_cpv:
            parsed = parse_cpv_codes(raw_secondary_cpv)
            if parsed:
                secondary_cpv.extend(parsed)
            else:
                secondary_cpv.append(raw_secondary_cpv)

        aspects_raw = part_raw.cn_section_4_3_10
        aspects_flag = _parse_tak_nie(aspects_raw)

        parts.append(
            ContractNoticePart(
                part_id=part_id,
                opis=opis,
                kryteria_oceny=criteria,
                mainCPV=main_cpv,
                secondaryCPV=list(dict.fromkeys(secondary_cpv)),
                contract_planned_execution_date=contract_planned_execution_date,
                criteria_aspects_4310=aspects_raw,
                criteria_aspects_4310_flag=aspects_flag if aspects_flag is not None else part_raw.cn_section_8_5_flag,
            )
        )
        parts_raw_sections.append(part_raw_map)

    return parts or None, parts_raw_sections or None
