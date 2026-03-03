"""Extract structured fields from BZP notice HTML.

Each notice type has a different HTML template with different field numbers.
This parser dispatches value extraction by notice type. See
docs/data_model/html_structure.md for the full field reference.
"""

from __future__ import annotations

import re
import unicodedata
from datetime import date
from html import unescape

from bs4 import BeautifulSoup, Tag
from pydantic import ValidationError

from procurement.silver.models import (
    ChangeEntry,
    ContractExecution,
    ContractNoticePart,
    EvalCriterion,
    ExtractedValues,
    HtmlExtracted,
    NoticeChange,
    TenderResultLot,
    TenderResultPart,
    TenderResultEnrichment,
)
from procurement.silver.notice_types.contract_notice_split_models import (
    ContractNoticeCoreRaw,
    ContractNoticePartRaw,
)
from procurement.silver.notice_types.section_models_registry import NOTICE_TYPE_SECTION_MODELS

# Regex for parsing "1234,56 PLN" style values from span text
_PLN_NUM_RE = re.compile(r"([\d\s\xa0,.]+?)\s*(?:\xa0)?\s*(?:PLN|EUR|USD|GBP|CHF)?$")
_SPAN_NORMAL_RE = re.compile(
    r"<span[^>]*class=['\"]normal['\"][^>]*>(.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _field_marker_re(field_num: str) -> re.Pattern[str]:
    """Regex for exact field markers like '4.4.)' (not matching '1.4.4.)')."""
    return re.compile(rf"(?<![\d.]){re.escape(field_num)}\)")


def _find_h3(soup: BeautifulSoup, field_num: str) -> Tag | None:
    """Find the first <h3> whose text starts with a given field number."""
    marker_re = _field_marker_re(field_num)
    for h3 in soup.find_all("h3"):
        if marker_re.search(h3.get_text()):
            return h3
    return None


def _find_all_h3(soup: BeautifulSoup, field_num: str) -> list[Tag]:
    """Find all <h3> tags matching a given field number."""
    results = []
    marker_re = _field_marker_re(field_num)
    for h3 in soup.find_all("h3"):
        if marker_re.search(h3.get_text()):
            results.append(h3)
    return results


def _span_value(h3: Tag | None) -> str | None:
    """Extract text from <span class='normal'> inside an h3."""
    if h3 is None:
        return None
    span = h3.find("span", class_="normal")
    if span is None:
        return None
    text = span.get_text().strip()
    return text or None


def _find_h3_by_label(soup: BeautifulSoup, patterns: list[str]) -> Tag | None:
    """Find h3 by case-insensitive label fragments in text content."""
    lowered_patterns = [_normalize_label_text(p) for p in patterns]
    for h3 in soup.find_all("h3"):
        text = _normalize_label_text(h3.get_text(separator=" ", strip=True))
        if all(pattern in text for pattern in lowered_patterns):
            return h3
    return None


def _normalize_label_text(text: str) -> str:
    """Normalize text for robust label matching across encoding variants."""
    lowered = text.casefold()
    # Common mojibake fragments seen in current fixtures/docs.
    replacements = {
        "Ä…": "a",
        "Ä‡": "c",
        "Ä™": "e",
        "Ĺ‚": "l",
        "Ĺ„": "n",
        "Ăł": "o",
        "Ĺ›": "s",
        "Ĺş": "z",
        "ĹĽ": "z",
        "Ă„â€¦": "a",
        "Ă„â€ˇ": "c",
        "Ă„â„˘": "e",
        "Äąâ€š": "l",
        "Äąâ€ž": "n",
        "Ä‚Ĺ‚": "o",
        "Äąâ€ş": "s",
        "ÄąÂş": "z",
        "ÄąÂĽ": "z",
    }
    for src, dst in replacements.items():
        lowered = lowered.replace(src, dst)
    lowered = unicodedata.normalize("NFKD", lowered)
    lowered = "".join(ch for ch in lowered if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", lowered).strip()


def _field_num(h3: Tag | None) -> str | None:
    """Extract a field number prefix from an h3 (e.g. 6.2.)."""
    if h3 is None:
        return None
    text = h3.get_text(separator=" ", strip=True)
    m = re.search(r"(\d+\.\d+(?:\.\d+)?\.)\)", text)
    if m is None:
        return None
    return m.group(1)


def _parse_pln_value(raw: str | None) -> float | None:
    """Parse a monetary value string into a float.

    Handles formats: "465163,88 PLN", "1 000 000,00 PLN", "130.000,00 PLN",
    "295590 PLN", "295590", "25399,50" (no currency suffix).
    """
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None

    match = _PLN_NUM_RE.match(raw)
    if match is None:
        # Try bare number (SmallContractNotice has no currency suffix)
        match = re.match(r"([\d\s\xa0,.]+)", raw)
        if match is None:
            return None

    num_str = match.group(1).strip()
    # Remove thousand separators (spaces, non-breaking spaces)
    num_str = num_str.replace("\xa0", "").replace(" ", "")
    # Handle dot as thousands separator when comma is also present
    if "." in num_str and "," in num_str:
        num_str = num_str.replace(".", "")
    # Convert comma decimal to dot
    num_str = num_str.replace(",", ".")
    try:
        return float(num_str)
    except ValueError:
        return None


def _extract_currency(soup: BeautifulSoup, field_num: str) -> str:
    """Extract currency code from a 'Kod waluty' field, default PLN."""
    raw = _span_value(_find_h3(soup, field_num))
    if raw and raw.strip() in ("PLN", "EUR", "USD", "GBP", "CHF"):
        return raw.strip()
    return "PLN"


def _parse_tak_nie(raw: str | None) -> bool | None:
    """Parse a Polish 'Tak'/'Nie' value into a boolean."""
    if raw is None:
        return None
    cleaned = raw.strip()
    if cleaned == "Tak":
        return True
    if cleaned == "Nie":
        return False
    return None


def _is_poland_country(raw: str | None) -> bool:
    if not raw:
        return False
    normalized = _normalize_label_text(raw)
    compact = re.sub(r"[^a-z]", "", normalized)
    return ("polska" in normalized) or ("poland" in normalized) or (compact == "pl")


def _digits_only(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def _validate_nip(digits: str) -> bool:
    if len(digits) != 10:
        return False
    vals = [int(ch) for ch in digits]
    weights = [6, 5, 7, 2, 3, 4, 5, 6, 7]
    checksum = sum(vals[i] * weights[i] for i in range(9)) % 11
    return checksum != 10 and checksum == vals[9]


def _validate_regon9(digits: str) -> bool:
    if len(digits) != 9:
        return False
    vals = [int(ch) for ch in digits]
    weights = [8, 9, 2, 3, 4, 5, 6, 7]
    checksum = sum(vals[i] * weights[i] for i in range(8)) % 11
    if checksum == 10:
        checksum = 0
    return checksum == vals[8]


def _validate_regon14(digits: str) -> bool:
    if len(digits) != 14:
        return False
    vals = [int(ch) for ch in digits]
    weights = [2, 3, 4, 5, 6, 7, 8, 9, 2, 3, 4, 5, 6]
    checksum = sum(vals[i] * weights[i] for i in range(13)) % 11
    if checksum == 10:
        checksum = 0
    return checksum == vals[13]


def _decode_pesel_date(digits: str) -> date | None:
    if len(digits) != 11:
        return None
    yy = int(digits[0:2])
    mm = int(digits[2:4])
    dd = int(digits[4:6])
    if 1 <= mm <= 12:
        year, month = 1900 + yy, mm
    elif 21 <= mm <= 32:
        year, month = 2000 + yy, mm - 20
    elif 41 <= mm <= 52:
        year, month = 2100 + yy, mm - 40
    elif 61 <= mm <= 72:
        year, month = 2200 + yy, mm - 60
    elif 81 <= mm <= 92:
        year, month = 1800 + yy, mm - 80
    else:
        return None
    try:
        return date(year, month, dd)
    except ValueError:
        return None


def _validate_pesel(digits: str) -> bool:
    if len(digits) != 11:
        return False
    vals = [int(ch) for ch in digits]
    weights = [1, 3, 7, 9, 1, 3, 7, 9, 1, 3]
    checksum = (10 - (sum(vals[i] * weights[i] for i in range(10)) % 10)) % 10
    return checksum == vals[10] and _decode_pesel_date(digits) is not None


def _classify_polish_contractor_id(raw_id: str) -> tuple[str | None, str]:
    # Prefer NIP when multiple IDs are present in one raw field.
    nip_candidates = re.findall(r"(?<!\d)(?:\d{3}[-\s]?\d{3}[-\s]?\d{2}[-\s]?\d{2}|\d{10})(?!\d)", raw_id)
    for cand in nip_candidates:
        digits = _digits_only(cand)
        if _validate_nip(digits):
            return digits, "NIP"

    regon_candidates = re.findall(r"(?<!\d)\d{14}(?!\d)|(?<!\d)\d{9}(?!\d)|(?<!\d)\d{8}(?!\d)", raw_id)
    for cand in regon_candidates:
        digits = _digits_only(cand)
        if len(digits) == 14 and _validate_regon14(digits):
            return digits, "REGON"
        if len(digits) == 9 and _validate_regon9(digits):
            return digits, "REGON"
        if len(digits) == 8:
            padded = f"0{digits}"
            if _validate_regon9(padded):
                return padded, "REGON"

    pesel_candidates = re.findall(r"(?<!\d)\d{11}(?!\d)", raw_id)
    for cand in pesel_candidates:
        if _validate_pesel(cand):
            return cand, "PESEL"
    # Fallback for operational typing: 11-digit national IDs are treated as PESEL
    # even when checksum/date sanity fails in source data.
    if pesel_candidates:
        return pesel_candidates[0], "PESEL"

    digits = _digits_only(raw_id)
    if len(digits) == 10 and _validate_nip(digits):
        return digits, "NIP"
    if len(digits) == 14 and _validate_regon14(digits):
        return digits, "REGON"
    if len(digits) == 9 and _validate_regon9(digits):
        return digits, "REGON"
    if len(digits) == 8:
        padded = f"0{digits}"
        if _validate_regon9(padded):
            return padded, "REGON"
    if len(digits) == 11:
        return digits, "PESEL"
    return None, "not_recognized"


def _classify_contractor_id(country: str | None, raw_id: str | None) -> tuple[str | None, str | None, str | None]:
    """Return (raw, parsed, type) for contractor ID."""
    if raw_id is None:
        return None, None, None
    raw = raw_id.strip()
    if not raw:
        return None, None, None
    if not _is_poland_country(country):
        return raw, raw, "foreign"
    parsed, id_type = _classify_polish_contractor_id(raw)
    return raw, parsed, id_type


def classify_contractor_id_for_notice(
    country: str | None,
    raw_id: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Public helper used across notice-specific transforms."""
    return _classify_contractor_id(country, raw_id)


def normalize_tender_result_contractors(
    contractors: list[dict] | None,
) -> list[dict] | None:
    """Normalize TRN contractor IDs to raw/parsed/type fields."""
    if not contractors:
        return contractors
    out: list[dict] = []
    for contractor in contractors:
        if isinstance(contractor, dict):
            row = dict(contractor)
        elif hasattr(contractor, "asDict"):
            row = contractor.asDict(recursive=True)
        else:
            continue
        raw_id = row.pop("contractorNationalId", None)
        country = row.get("contractorCountry")
        raw, parsed, id_type = _classify_contractor_id(country, raw_id)
        row["contractorNationalId_raw"] = raw
        row["contractorNationalId_parsed"] = parsed
        row["contractorNationalId_type"] = id_type
        out.append(row)
    return out


def _parse_criterion_weight(raw: str | None) -> int | None:
    """Parse criterion weight from strings like '60', '60,00', '40.00', '100 %'."""
    if raw is None:
        return None
    cleaned = raw.strip().replace("\xa0", " ")
    if not cleaned:
        return None

    # Keep only numeric prefix, allowing local decimal separators.
    m = re.search(r"([0-9][0-9\s.,]*)", cleaned)
    if m is None:
        return None

    num = m.group(1).replace(" ", "")
    if "." in num and "," in num:
        num = num.replace(".", "")
    num = num.replace(",", ".")

    try:
        return int(round(float(num)))
    except ValueError:
        return None


def _text_after_h3(h3: Tag | None) -> str | None:
    """Extract plain text that follows an <h3> as a sibling text node.

    Handles the pattern where the value is NOT inside a <span> but is a
    bare text node after the h3 (e.g. field 4.2 or 3.4).
    """
    if h3 is None:
        return None
    from bs4 import NavigableString

    sibling = h3.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            text = sibling.strip()
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


def _collect_p_text(h3: Tag) -> str | None:
    """Collect text from sibling <p> tags after an h3 until the next h3/h2.

    Used for change descriptions (3.4.1) that span multiple <p> elements.
    """
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
    return "\n".join(parts) if parts else None


def _collect_p_values(h3: Tag | None) -> list[str]:
    """Collect plain values from sibling <p> tags after an h3 until next h3/h2."""
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


# --- Address extraction (shared across types) ---


_ADDRESS_FIELD_NUMS_BY_TYPE: dict[str | None, dict[str, tuple[str, ...]]] = {
    None: {
        # Some templates use section 1.5.x, others 1.4.x.
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    # Most templates keep address fields in section I, but some have variants.
    "ContractNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "TenderResultNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "NoticeUpdateNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementUpdateNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementIntentionNotice": {
        # Legacy templates use 1.4.x, newer ones use 1.5.x.
        "ulica": ("1.4.1.", "1.5.1.", "5.1.2."),
        "kod_pocztowy": ("1.4.3.", "1.5.3.", "5.1.4."),
        "nuts3": ("1.4.6.", "1.5.6."),
    },
    # Seen variants in execution notices with section IV labels.
    "ContractPerformingNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "AgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CompetitionNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CompetitionResultNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "CircumstancesFulfillmentNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "SmallContractNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionIntentionAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "NoticeUpdateConcession": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
    "ConcessionUpdateAgreementNotice": {
        "ulica": ("1.5.1.", "1.4.1."),
        "kod_pocztowy": ("1.5.3.", "1.4.3."),
        "nuts3": ("1.5.6.", "1.4.6."),
    },
}


def _first_span_by_field_nums(soup: BeautifulSoup, field_nums: tuple[str, ...]) -> str | None:
    for field_num in field_nums:
        value = _span_value(_find_h3(soup, field_num))
        if value:
            return value
    return None


def _extract_address(soup: BeautifulSoup, notice_type: str | None = None) -> dict:
    """Extract street, postal code, NUTS3 with noticeType-aware + label fallback logic."""
    mapping = _ADDRESS_FIELD_NUMS_BY_TYPE.get(notice_type) or _ADDRESS_FIELD_NUMS_BY_TYPE[None]

    ulica = _first_span_by_field_nums(soup, mapping["ulica"])
    if not ulica:
        ulica = _span_value(_find_h3_by_label(soup, ["ulica"]))

    kod_pocztowy = _first_span_by_field_nums(soup, mapping["kod_pocztowy"])
    if not kod_pocztowy:
        kod_pocztowy = _span_value(_find_h3_by_label(soup, ["kod", "poczt"]))

    nuts3_code = None
    nuts3_name = None
    nuts3_raw = _first_span_by_field_nums(soup, mapping["nuts3"])
    if not nuts3_raw:
        nuts3_raw = _span_value(_find_h3_by_label(soup, ["nuts", "3"]))
    if nuts3_raw and " - " in nuts3_raw:
        nuts3_code, nuts3_name = nuts3_raw.split(" - ", 1)
        nuts3_code = nuts3_code.strip()
        nuts3_name = nuts3_name.strip()

    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "nuts3_code": nuts3_code,
        "nuts3_name": nuts3_name,
    }


def _extract_ogloszenie_dotyczy(soup: BeautifulSoup) -> str | None:
    """Extract the 'OgĹ‚oszenie dotyczy' field by label text.

    Field numbers are reused across notice types, so this must be label-based
    (not just 2.1.) to avoid capturing unrelated identifiers.
    """
    h3 = None
    for candidate in soup.find_all("h3"):
        raw = candidate.get_text(separator=" ", strip=True).lower()
        if (
            raw.strip().startswith("2.1.)")
            and "dotyczy" in raw
            and "dotyczy zmiany" not in raw
        ):
            h3 = candidate
            break
    if h3 is None:
        return None
    return _span_value(h3) or _text_after_h3(h3) or _collect_p_text(h3)


# --- Description and criteria (ContractNotice-specific) ---


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


def _extract_contract_notice_section_number(h3_text: str) -> str | None:
    match = re.search(r"(?<![\d.])(\d+\.\d+(?:\.\d+)?)\.?\)\s*", h3_text)
    return match.group(1) if match else None


def _extract_contract_notice_section_value(h3: Tag) -> str | None:
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


def _section_to_field_name(section_number: str) -> str:
    return f"cn_section_{section_number.replace('.', '_')}"


def _section_number_key(section_number: str) -> tuple[int, ...]:
    return tuple(int(token) for token in section_number.split("."))


def _extract_contract_notice_part_chunks(h3s: list[Tag]) -> list[tuple[str | None, list[Tag]]]:
    part_headers: list[tuple[int, str]] = []
    for idx, h3 in enumerate(h3s):
        text = h3.get_text(separator=" ", strip=True)
        part_id = _extract_part_id_from_header(text)
        if part_id:
            part_headers.append((idx, part_id))

    chunks: list[tuple[str | None, list[Tag]]] = []
    if part_headers:
        for i, (start_idx, part_id) in enumerate(part_headers):
            end_idx = part_headers[i + 1][0] if i + 1 < len(part_headers) else len(h3s)
            chunks.append((part_id, h3s[start_idx:end_idx]))
    else:
        chunks.append((None, h3s))
    return chunks


def _build_notice_sections_model(soup: BeautifulSoup, notice_type: str | None, notice_dicts):
    notice_sections = notice_dicts.get(notice_type or "", {})
    model_types = {section_dict["data_model"] for section_dict in notice_sections.values() if section_dict.get("data_model")}
    model_values = {"core": {}}
    non_core_values = {model_type:[{}] for model_type in model_types if model_type != "core"}
    model_values.update(non_core_values)
    mode = "core"
    last_section_number = None
    n_current_part = 0
    for h3 in soup.find_all("h3"):
        section_number = _extract_contract_notice_section_number(h3.get_text(separator=" ", strip=True))
        if not section_number:
            continue
        value = _extract_contract_notice_section_value(h3)
        if not value:
            continue
        section_cfg = notice_sections.get(section_number)
        section_model = section_cfg.get("data_model") if section_cfg else None
        if section_model is None:
            continue
        if mode=="core": # Standard mode, not currently in a non-core section
            if section_model == "core":
                model_values["core"][_section_to_field_name(section_number)] = value
            else:
                mode = section_model
                model_values[mode][n_current_part][_section_to_field_name(section_number)] = value
        else: # Currently in a non-core section, check if we should switch to a different model or back to core
            if section_model != mode:
                n_current_part = 0
                mode = section_model
                if mode=="core":
                    model_values["core"][_section_to_field_name(section_number)] = value
                else:
                    model_values[mode][n_current_part][_section_to_field_name(section_number)] = value
            else:
                if last_section_number is not None and _section_number_key(section_number) > _section_number_key(last_section_number):
                    field_name = _section_to_field_name(section_number)
                    if field_name not in model_values[mode][n_current_part]:
                        model_values[mode][n_current_part][field_name] = value
                else:
                    n_current_part += 1
                    if len(model_values[mode])<=n_current_part:
                        model_values[mode].append({_section_to_field_name(section_number): value})
                    else:
                        model_values[mode][n_current_part][_section_to_field_name(section_number)] = value

        last_section_number = section_number
        
    return model_values

def _build_tender_result_notice_sections_model(soup: BeautifulSoup, model_cls):
    """Build nested TenderResultNotice sections model with repeating buyers/parts."""
    h3_entries: list[tuple[str, str]] = []
    for h3 in soup.find_all("h3"):
        section_number = _extract_contract_notice_section_number(h3.get_text(separator=" ", strip=True))
        if not section_number:
            continue
        value = _extract_contract_notice_section_value(h3)
        if not value:
            continue
        h3_entries.append((_section_to_field_name(section_number), value))

    buyer_fields = {
        "cn_section_1_2",
        "cn_section_1_3",
        "cn_section_1_4",
        "cn_section_1_5",
        "cn_section_1_5_1",
        "cn_section_1_5_2",
        "cn_section_1_5_3",
        "cn_section_1_5_4",
        "cn_section_1_5_5",
        "cn_section_1_5_6",
        "cn_section_1_5_7",
        "cn_section_1_5_8",
        "cn_section_1_5_9",
        "cn_section_1_5_10",
    }
    part_fields = {
        "cn_section_4_5_1",
        "cn_section_4_5_3",
        "cn_section_4_5_4",
        "cn_section_4_5_5",
        "cn_section_5_1",
        "cn_section_5_2",
        "cn_section_5_2_1",
        "cn_section_6_1",
        "cn_section_6_1_1",
        "cn_section_6_1_2",
        "cn_section_6_1_3",
        "cn_section_6_1_4",
        "cn_section_6_1_5",
        "cn_section_6_1_6",
        "cn_section_6_1_7",
        "cn_section_6_2",
        "cn_section_6_3",
        "cn_section_6_4",
        "cn_section_6_5",
        "cn_section_6_6",
        "cn_section_6_7",
        "cn_section_7_1",
        "cn_section_7_2",
        "cn_section_7_3_1",
        "cn_section_7_3_2",
        "cn_section_7_3_3",
        "cn_section_7_3_4",
        "cn_section_7_3_5",
        "cn_section_7_3_6",
        "cn_section_7_3_7",
        "cn_section_7_3_8",
        "cn_section_7_3_9",
        "cn_section_7_4",
        "cn_section_7_4_1",
        "cn_section_8_1",
        "cn_section_8_2",
        "cn_section_8_3",
        "cn_section_8_4",
    }

    buyers: list[dict[str, str]] = []
    current_buyer: dict[str, str] | None = None
    for field_name, value in h3_entries:
        if field_name == "cn_section_1_2":
            if current_buyer:
                buyers.append(current_buyer)
            current_buyer = {field_name: value}
            continue
        if current_buyer is not None:
            if field_name in buyer_fields:
                current_buyer.setdefault(field_name, value)
                continue
            buyers.append(current_buyer)
            current_buyer = None
    if current_buyer:
        buyers.append(current_buyer)

    parts: list[dict[str, str]] = []
    current_part: dict[str, str] | None = None
    for field_name, value in h3_entries:
        if field_name == "cn_section_4_5_1":
            if current_part:
                parts.append(current_part)
            current_part = {field_name: value}
            continue
        if current_part is not None:
            if field_name in part_fields:
                current_part.setdefault(field_name, value)
                continue
            parts.append(current_part)
            current_part = None
    if current_part:
        parts.append(current_part)

    field_names = set(model_cls.model_fields.keys())
    payload: dict[str, object] = {}
    scalar_fields = field_names - {"buyers", "parts"}
    for field_name, value in h3_entries:
        if field_name in scalar_fields and field_name not in payload:
            payload[field_name] = value
    if "buyers" in field_names and buyers:
        payload["buyers"] = buyers
    if "parts" in field_names and parts:
        payload["parts"] = parts

    try:
        return model_cls(**payload)
    except ValidationError:
        return None


def _stringify_section_dict(payload: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in payload.items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def _extract_contract_notice_parts(
    soup: BeautifulSoup,
) -> tuple[list[ContractNoticePart] | None, list[dict[str, str]] | None]:
    """Extract per-part criteria + CPV blocks from ContractNotice SEKCJA IV.

    This path is section-first: chunk sections by part, validate/normalize via
    ContractNoticePartRaw, and then apply existing production parsing rules
    for derived outputs.
    """
    h3s = soup.find_all("h3")
    if not h3s:
        return None

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


# --- Type-specific value extraction ---


def _extract_values_contract_performing(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractPerformingNotice: fields 4.4 (contract value), 5.5 (total paid)."""
    value_contract_reported_execution = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    value_paid_total = _parse_pln_value(_span_value(_find_h3(soup, "5.5.")))
    currency = _extract_currency(soup, "5.4.7.")

    if value_contract_reported_execution is None and value_paid_total is None:
        return None
    return ExtractedValues(
        value_contract_reported_execution=value_contract_reported_execution,
        value_paid_total=value_paid_total,
        currency=currency,
    )


def _extract_values_tender_result(soup: BeautifulSoup) -> ExtractedValues | None:
    """TenderResultNotice: fields 8.2, 6.2, 6.3, 6.4, 4.3 (first lot)."""
    lots = _extract_tender_result_lots(soup)
    if lots:
        if len(lots) > 1:
            # Multi-lot notices are represented in htmlExtracted.lots.
            # Keep notice-level values null to avoid flattening ambiguity.
            return None
        lot = lots[0]
        value_awarded_contract = lot.value_awarded_contract
        value_estimated_procurement = lot.value_estimated_procurement
        value_bid_lowest = lot.value_bid_lowest
        value_bid_highest = lot.value_bid_highest
        value_winning_offer = lot.value_winning_offer
    else:
        value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.3.")))
        value_bid_lowest = _parse_pln_value(_span_value(_find_h3(soup, "6.2.")))
        value_bid_highest = _parse_pln_value(_span_value(_find_h3(soup, "6.3.")))
        value_winning_offer = _parse_pln_value(_span_value(_find_h3(soup, "6.4.")))

    if all(
        v is None
        for v in (
            value_awarded_contract,
            value_estimated_procurement,
            value_bid_lowest,
            value_bid_highest,
            value_winning_offer,
        )
    ):
        return None
    return ExtractedValues(
        value_awarded_contract=value_awarded_contract,
        value_estimated_procurement=value_estimated_procurement,
        value_bid_lowest=value_bid_lowest,
        value_bid_highest=value_bid_highest,
        value_winning_offer=value_winning_offer,
    )


def _extract_lot_id_from_text(text: str) -> str | None:
    """Extract human lot label from header text where possible."""
    patterns = [
        r"(?:CzÄ™Ĺ›Ä‡|Czesc)\s*(?:nr)?\s*([A-Za-z0-9._/-]+)",
        r"Lot\s*(?:nr)?\s*([A-Za-z0-9._/-]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _extract_winner_from_chunk(chunk: list[Tag]) -> str | None:
    for h3 in chunk:
        text = h3.get_text(separator=" ", strip=True)
        if "nazwa" in text.lower() and ("wykonaw" in text.lower() or "udzielono" in text.lower()):
            val = _span_value(h3) or _text_after_h3(h3)
            if val:
                return val
    return None


def _extract_tender_result_lots(soup: BeautifulSoup) -> list[TenderResultLot] | None:
    """Extract per-lot TenderResult values from repeated 4.3/6.2/6.3/6.4/8.2 sections."""
    h3s = soup.find_all("h3")
    chunks: list[list[Tag]] = []
    current: list[Tag] = []

    for h3 in h3s:
        current.append(h3)
        if _field_num(h3) == "8.2.":
            chunks.append(current)
            current = []

    # If there were no 8.2 delimiters, fall back to a single chunk.
    if not chunks and current:
        chunks = [current]

    lots: list[TenderResultLot] = []
    for idx, chunk in enumerate(chunks, start=1):
        by_field: dict[str, Tag] = {}
        for h3 in chunk:
            field = _field_num(h3)
            if field in {"4.3.", "6.2.", "6.3.", "6.4.", "8.2."}:
                by_field[field] = h3

        value_awarded_contract = _parse_pln_value(_span_value(by_field.get("8.2.")))
        value_estimated_procurement = _parse_pln_value(_span_value(by_field.get("4.3.")))
        value_bid_lowest = _parse_pln_value(_span_value(by_field.get("6.2.")))
        value_bid_highest = _parse_pln_value(_span_value(by_field.get("6.3.")))
        value_winning_offer = _parse_pln_value(_span_value(by_field.get("6.4.")))

        if all(
            v is None
            for v in (
                value_awarded_contract,
                value_estimated_procurement,
                value_bid_lowest,
                value_bid_highest,
                value_winning_offer,
            )
        ):
            continue

        lot_id = None
        for h3 in chunk:
            lot_id = _extract_lot_id_from_text(h3.get_text(separator=" ", strip=True))
            if lot_id:
                break

        lots.append(
            TenderResultLot(
                lot_id=lot_id or str(idx),
                value_awarded_contract=value_awarded_contract,
                value_estimated_procurement=value_estimated_procurement,
                value_bid_lowest=value_bid_lowest,
                value_bid_highest=value_bid_highest,
                value_winning_offer=value_winning_offer,
                winner=_extract_winner_from_chunk(chunk),
            )
        )

    return lots or None


def _extract_tender_result_parts(soup: BeautifulSoup) -> list[TenderResultPart] | None:
    """Extract TenderResultNotice part metadata from SEKCJA IV fields."""
    h3s = soup.find_all("h3")
    if not h3s:
        return None

    part_headers: list[tuple[int, str]] = []
    for idx, h3 in enumerate(h3s):
        text = h3.get_text(separator=" ", strip=True)
        part_id = _extract_part_id_from_header(text)
        if part_id:
            part_headers.append((idx, part_id))

    chunks: list[tuple[str | None, list[Tag]]] = []
    if part_headers:
        for i, (start_idx, part_id) in enumerate(part_headers):
            end_idx = part_headers[i + 1][0] if i + 1 < len(part_headers) else len(h3s)
            chunks.append((part_id, h3s[start_idx:end_idx]))
    else:
        chunks.append((None, h3s))

    parts: list[TenderResultPart] = []
    for seq, (_parsed_part_id, chunk) in enumerate(chunks, start=1):
        opis = None
        main_cpv = None
        secondary_cpv: list[str] = []
        value_estimated_procurement = None

        for h3 in chunk:
            text = h3.get_text(separator=" ", strip=True)
            if "4.5.1.)" in text:
                opis = _span_value(h3) or _text_after_h3(h3)
                if opis is None:
                    p = h3.find_next_sibling("p")
                    if p is not None:
                        opis = p.get_text(separator=" ", strip=True) or None
            elif "4.5.3.)" in text:
                raw = _span_value(h3) or _text_after_h3(h3)
                if raw:
                    parsed = parse_cpv_codes(raw)
                    main_cpv = parsed[0] if parsed else raw
            elif "4.5.4.)" in text:
                raw = _span_value(h3) or _text_after_h3(h3)
                if raw:
                    parsed = parse_cpv_codes(raw)
                    if parsed:
                        secondary_cpv.extend(parsed)
                    else:
                        secondary_cpv.append(raw)
            elif "4.3.)" in text:
                value = _parse_pln_value(_span_value(h3) or _text_after_h3(h3))
                if value is not None:
                    value_estimated_procurement = value

        if all(v is None for v in (opis, main_cpv, value_estimated_procurement)) and not secondary_cpv:
            continue

        parts.append(
            TenderResultPart(
                # Keep deterministic numbering by appearance in SEKCJA IV.
                part_id=str(seq),
                opis=opis,
                mainCPV=main_cpv,
                secondaryCPV=list(dict.fromkeys(secondary_cpv)) or None,
                value_estimated_procurement=value_estimated_procurement,
            )
        )

    return parts or None


def _extract_status_lots_from_procedure_result(procedure_result: str | None) -> list[TenderResultLot] | None:
    """Create synthetic lot rows for cancelled/unresolved outcomes."""
    if not procedure_result:
        return None
    tokens = [token.strip() for token in procedure_result.split(";") if token.strip()]
    if not tokens:
        return None
    normalized = [token.lower() for token in tokens]
    if not any(token in {"uniewaznienie", "nierozstrzygnieto"} for token in normalized):
        return None
    return [
        TenderResultLot(
            lot_id=str(i),
            winner=status,
        )
        for i, status in enumerate(tokens, start=1)
    ]


def _extract_values_contract_notice(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractNotice: fields 4.1.5 (total value), 4.1.6 (net of VAT)."""
    value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.1.5.")))
    if value_estimated_procurement is None:
        value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.1.6.")))
    if value_estimated_procurement is None:
        return None
    return ExtractedValues(value_estimated_procurement=value_estimated_procurement)


def _extract_values_agreement_update(soup: BeautifulSoup) -> ExtractedValues | None:
    """AgreementUpdateNotice: field 4.4 (agreement value)."""
    value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    if value_awarded_contract is None:
        return None
    return ExtractedValues(value_awarded_contract=value_awarded_contract)


def _extract_values_agreement_intention(soup: BeautifulSoup) -> ExtractedValues | None:
    """AgreementIntentionNotice: field 3.5 (procurement value)."""
    value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "3.5.")))
    if value_estimated_procurement is None:
        return None
    return ExtractedValues(value_estimated_procurement=value_estimated_procurement)


def _extract_agreement_intention_fields(
    soup: BeautifulSoup,
) -> tuple[str | None, float | None, str | None]:
    """AgreementIntentionNotice: 5.1.2 street, 3.5 value, 3.1 consultation info."""
    ai_street_512 = _span_value(_find_h3(soup, "5.1.2."))
    if ai_street_512 is None:
        ai_street_512 = _span_value(_find_h3_by_label(soup, ["ulica"]))

    value_estimated_procurement_ai_35 = _parse_pln_value(_span_value(_find_h3(soup, "3.5.")))
    ai_prior_market_consultation_31 = _span_value(_find_h3(soup, "3.1.")) or _text_after_h3(
        _find_h3(soup, "3.1.")
    )
    return ai_street_512, value_estimated_procurement_ai_35, ai_prior_market_consultation_31


def _extract_contract_performing_party_fields(
    soup: BeautifulSoup,
) -> tuple[
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    float | None,
]:
    """ContractPerformingNotice: repeated contractor fields + contract value.

    Fields:
    - 4.3.1.) Nazwa wykonawcy
    - 4.3.2.) Krajowy Numer Identyfikacyjny -> contractor_id_raw/parsed/type
    - 4.3.4.) Miejscowość
    - 4.3.6.) Województwo
    - 4.3.7.) Kraj
    - 4.4.) Wartość umowy
    """
    def _collect(field_num: str) -> list[str] | None:
        out: list[str] = []
        for h3 in _find_all_h3(soup, field_num):
            value = _span_value(h3) or _text_after_h3(h3)
            if value:
                out.append(value)
        if not out:
            return None
        # Preserve order, remove duplicates.
        return list(dict.fromkeys(out))

    names = _collect("4.3.1.")
    national_ids = _collect("4.3.2.")
    cities = _collect("4.3.4.")
    provinces = _collect("4.3.6.")
    countries = _collect("4.3.7.")
    contractor_id_raw: list[str] = []
    contractor_id_parsed: list[str] = []
    contractor_id_type: list[str] = []

    if national_ids:
        for idx, national_id in enumerate(national_ids):
            country = countries[idx] if countries and idx < len(countries) else None
            raw, parsed, id_type = _classify_contractor_id(country, national_id)
            if raw:
                contractor_id_raw.append(raw)
            if parsed:
                contractor_id_parsed.append(parsed)
            if id_type:
                contractor_id_type.append(id_type)

    def _uniq(values: list[str]) -> list[str] | None:
        if not values:
            return None
        return list(dict.fromkeys(values))

    value_contract_reported_execution_44 = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    return (
        names,
        _uniq(contractor_id_raw),
        _uniq(contractor_id_parsed),
        _uniq(contractor_id_type),
        cities,
        provinces,
        countries,
        value_contract_reported_execution_44,
    )


def _extract_values_small_contract(soup: BeautifulSoup) -> ExtractedValues | None:
    """SmallContractNotice: field 3.4 (value, no PLN suffix), 3.5 (currency)."""
    value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "3.4.")))
    if value_awarded_contract is None:
        return None
    currency = _extract_currency(soup, "3.5.")
    return ExtractedValues(value_awarded_contract=value_awarded_contract, currency=currency)


def _extract_competition_notice_fields(
    soup: BeautifulSoup,
) -> tuple[int | None, float | None, float | None, str | None, str | None]:
    """CompetitionNotice: core competition fields + submission deadline variants."""
    comp_num_awarded_63 = None
    raw_num_awarded = _span_value(_find_h3(soup, "6.3.")) or _text_after_h3(_find_h3(soup, "6.3."))
    if raw_num_awarded is not None:
        match = re.search(r"\d+", raw_num_awarded)
        if match is not None:
            try:
                comp_num_awarded_63 = int(match.group(0))
            except ValueError:
                comp_num_awarded_63 = None

    value_competition_prizes_64 = _parse_pln_value(
        _span_value(_find_h3(soup, "6.4.")) or _text_after_h3(_find_h3(soup, "6.4."))
    )
    value_competition_followon_order_651 = _parse_pln_value(
        _span_value(_find_h3(soup, "6.5.1.")) or _text_after_h3(_find_h3(soup, "6.5.1."))
    )
    comp_requirements_72 = _span_value(_find_h3(soup, "7.2.")) or _text_after_h3(_find_h3(soup, "7.2."))
    comp_submission_deadline = (
        _span_value(_find_h3(soup, "3.6."))
        or _text_after_h3(_find_h3(soup, "3.6."))
        or _span_value(_find_h3(soup, "3.5."))
        or _text_after_h3(_find_h3(soup, "3.5."))
    )

    return (
        comp_num_awarded_63,
        value_competition_prizes_64,
        value_competition_followon_order_651,
        comp_requirements_72,
        comp_submission_deadline,
    )


def _extract_competition_result_fields(soup: BeautifulSoup) -> str | None:
    """CompetitionResultNotice: 5.3 approval date of result/cancellation."""
    raw = _span_value(_find_h3(soup, "5.3.")) or _text_after_h3(_find_h3(soup, "5.3."))
    if not raw:
        return None
    match = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    return match.group(1) if match else None


_VALUE_EXTRACTORS = {
    "ContractPerformingNotice": _extract_values_contract_performing,
    "TenderResultNotice": _extract_values_tender_result,
    "ContractNotice": _extract_values_contract_notice,
    "AgreementUpdateNotice": _extract_values_agreement_update,
    "AgreementIntentionNotice": _extract_values_agreement_intention,
    "SmallContractNotice": _extract_values_small_contract,
}


# --- Type-specific detail extraction (non-value) ---


def _extract_details_tender_result(soup: BeautifulSoup) -> TenderResultEnrichment | None:
    """TenderResultNotice: fields 7.1 (joint bidders), 7.2 (enterprise size)."""
    joint_bidders = _parse_tak_nie(_span_value(_find_h3(soup, "7.1.")))
    contractor_size = _span_value(_find_h3(soup, "7.2."))

    if joint_bidders is None and contractor_size is None:
        return None
    return TenderResultEnrichment(
        joint_bidders=joint_bidders,
        contractor_size=contractor_size,
    )


def _extract_details_contract_performing(soup: BeautifulSoup) -> ContractExecution | None:
    """ContractPerformingNotice: execution details resolved by label + numeric fallback."""
    h3_contract_date = _find_h3_by_label(soup, ["data zawarcia umowy"]) or _find_h3(soup, "4.1.")
    contract_date = _span_value(h3_contract_date) or _text_after_h3(h3_contract_date)

    h3_execution_period = _find_h3_by_label(soup, ["okres realizacji"]) or _find_h3(soup, "4.2.")
    execution_period = _span_value(h3_execution_period) or _text_after_h3(h3_execution_period)

    h3_contract_executed = _find_h3_by_label(soup, ["czy umowa", "wykonana"]) or _find_h3(soup, "5.1.")
    contract_executed = _parse_tak_nie(_span_value(h3_contract_executed) or _text_after_h3(h3_contract_executed))

    h3_execution_end = _find_h3_by_label(soup, ["termin wykonania umowy"]) or _find_h3(soup, "5.2.")
    execution_end_date = _span_value(h3_execution_end) or _text_after_h3(h3_execution_end)

    h3_on_time = None
    for candidate in soup.find_all("h3"):
        raw = candidate.get_text(separator=" ", strip=True).lower()
        if "pierwotnie" in raw and "termin" in raw:
            h3_on_time = candidate
            break
    if h3_on_time is None:
        h3_on_time = _find_h3_by_label(soup, ["w terminie"]) or _find_h3(soup, "5.3.")
    executed_on_time = _parse_tak_nie(_span_value(h3_on_time) or _text_after_h3(h3_on_time))

    h3_changes = _find_h3_by_label(soup, ["liczba zmian"]) or _find_h3(soup, "5.4.1.")
    raw_changes = _span_value(h3_changes) or _text_after_h3(h3_changes)
    num_changes: int | None = None
    if raw_changes is not None:
        try:
            num_changes = int(raw_changes.strip())
        except ValueError:
            pass

    h3_proper = _find_h3_by_label(soup, ["wykonana naleĹĽycie"]) or _find_h3(soup, "5.6.")
    executed_properly = _parse_tak_nie(_span_value(h3_proper) or _text_after_h3(h3_proper))

    if all(
        v is None
        for v in (
            contract_date,
            execution_period,
            contract_executed,
            execution_end_date,
            executed_on_time,
            num_changes,
            executed_properly,
        )
    ):
        return None
    return ContractExecution(
        contract_date=contract_date,
        execution_period=execution_period,
        contract_executed=contract_executed,
        execution_end_date=execution_end_date,
        executed_on_time=executed_on_time,
        num_changes=num_changes,
        executed_properly=executed_properly,
    )


def _extract_details_notice_update(soup: BeautifulSoup) -> NoticeChange | None:
    """NoticeUpdateNotice: fields 3.2, 3.3, 3.4/3.4.1 (repeating)."""
    changed_notice_number = _span_value(_find_h3(soup, "3.2."))
    changed_notice_version = _span_value(_find_h3(soup, "3.3."))

    # Collect 3.4/3.4.1 pairs by walking h3 tags in document order
    # (mirrors the _extract_criteria pattern for 4.3.5/4.3.6 pairs)
    changes: list[ChangeEntry] = []
    all_h3s = soup.find_all("h3")
    i = 0
    while i < len(all_h3s):
        text = all_h3s[i].get_text()
        if "3.4.)" in text and "3.4.1.)" not in text:
            changed_section = _span_value(all_h3s[i]) or _text_after_h3(all_h3s[i])
            change_desc = None
            if i + 1 < len(all_h3s) and "3.4.1.)" in all_h3s[i + 1].get_text():
                change_desc = _collect_p_text(all_h3s[i + 1])
                i += 1  # skip the 3.4.1 we just consumed
            if changed_section or change_desc:
                changes.append(
                    ChangeEntry(
                        changed_section=changed_section,
                        change_description=change_desc,
                    )
                )
        i += 1

    if changed_notice_number is None and changed_notice_version is None and not changes:
        return None
    return NoticeChange(
        changed_notice_number=changed_notice_number,
        changed_notice_version=changed_notice_version,
        changes=changes or None,
    )


_DETAIL_EXTRACTORS: dict[str, tuple[str, object]] = {
    "TenderResultNotice": ("tender_result_enrichment", _extract_details_tender_result),
    "ContractPerformingNotice": ("contract_execution", _extract_details_contract_performing),
    "NoticeUpdateNotice": ("notice_change", _extract_details_notice_update),
}


# --- CPV code parsing ---


def parse_cpv_codes(cpv_raw: str) -> list[str]:
    """Parse cpvCode string into canonical CPV codes only.

    Input:  "45000000-7 (Roboty budowlane),90620000-9 (Uslugi odsniezania)"
    Output: ["45000000-7", "90620000-9"]
    """
    # Keep only canonical code tokens; ignore human-readable descriptions.
    matches = re.findall(r"\b(\d{8}-\d)\b", cpv_raw)
    if not matches:
        return []
    # Preserve order and deduplicate.
    return list(dict.fromkeys(matches))


# --- Main parse entry point ---


def parse_html(
    html: str,
    notice_type: str | None = None,
    procedure_result: str | None = None,
) -> HtmlExtracted:
    """Parse a single BZP notice HTML into extracted fields.

    Args:
        html: Raw HTML body of the notice.
        notice_type: The noticeType value (e.g. "ContractPerformingNotice").
            Required for type-aware value extraction. If None, value
            extraction falls back to TenderResultNotice field 8.2 only
            (legacy behavior).
    """
    soup = BeautifulSoup(html, "lxml")

    sections_model = _build_notice_sections_model(soup, notice_type=notice_type)
    core_model = sections_model if isinstance(sections_model, ContractNoticeCoreRaw) else None
    ogloszenie_dotyczy = (
        core_model.cn_section_2_1
        if core_model and core_model.cn_section_2_1
        else _extract_ogloszenie_dotyczy(soup)
    )
    address = _extract_address(soup, notice_type=notice_type)
    opis = None
    kryteria = None
    criteria_aspects_4310 = None
    criteria_aspects_4310_flag = None
    if notice_type in (None, "ContractNotice"):
        opis = _extract_description(soup)
        kryteria = _extract_criteria(soup)
        criteria_aspects_4310, criteria_aspects_4310_flag = _extract_criteria_aspects_4310(soup)
    contract_notice_parts = None
    contract_notice_parts_sections = None
    if notice_type == "ContractNotice":
        contract_notice_parts, contract_notice_parts_sections = _extract_contract_notice_parts(soup)
    if notice_type == "ContractNotice" and contract_notice_parts:
        first_part = contract_notice_parts[0]
        opis = first_part.opis or opis
        kryteria = first_part.kryteria_oceny or kryteria
        criteria_aspects_4310 = first_part.criteria_aspects_4310 or criteria_aspects_4310
        if criteria_aspects_4310_flag is None:
            criteria_aspects_4310_flag = first_part.criteria_aspects_4310_flag
    lots = _extract_tender_result_lots(soup) if notice_type == "TenderResultNotice" else None
    tender_result_parts = (
        _extract_tender_result_parts(soup) if notice_type == "TenderResultNotice" else None
    )
    if notice_type == "TenderResultNotice" and not lots:
        lots = _extract_status_lots_from_procedure_result(procedure_result)
    ai_street_512 = None
    value_estimated_procurement_ai_35 = None
    ai_prior_market_consultation_31 = None
    cpn_contractor_names_431 = None
    contractor_id_raw = None
    contractor_id_parsed = None
    contractor_id_type = None
    cpn_contractor_cities_434 = None
    cpn_contractor_provinces_436 = None
    cpn_contractor_countries_437 = None
    value_contract_reported_execution_44 = None
    value_paid_total_55 = None
    comp_num_awarded_63 = None
    value_competition_prizes_64 = None
    value_competition_followon_order_651 = None
    comp_requirements_72 = None
    comp_submission_deadline = None
    comp_result_approval_date_53 = None
    cn_partial_offers_allowed_418 = None
    cn_offers_scope_4110 = None
    if notice_type == "AgreementIntentionNotice":
        (
            ai_street_512,
            value_estimated_procurement_ai_35,
            ai_prior_market_consultation_31,
        ) = _extract_agreement_intention_fields(soup)
    if notice_type == "ContractPerformingNotice":
        (
            cpn_contractor_names_431,
            contractor_id_raw,
            contractor_id_parsed,
            contractor_id_type,
            cpn_contractor_cities_434,
            cpn_contractor_provinces_436,
            cpn_contractor_countries_437,
            value_contract_reported_execution_44,
        ) = _extract_contract_performing_party_fields(soup)
        value_paid_total_55 = _parse_pln_value(_span_value(_find_h3(soup, "5.5.")))
    if notice_type == "CompetitionNotice":
        (
            comp_num_awarded_63,
            value_competition_prizes_64,
            value_competition_followon_order_651,
            comp_requirements_72,
            comp_submission_deadline,
        ) = _extract_competition_notice_fields(soup)
    if notice_type == "CompetitionResultNotice":
        comp_result_approval_date_53 = _extract_competition_result_fields(soup)
    if notice_type == "ContractNotice":
        raw_418 = core_model.cn_section_4_1_8 if core_model else None
        raw_4110 = core_model.cn_section_4_1_10 if core_model else None
        cn_partial_offers_allowed_418 = (
            _parse_tak_nie(raw_418) if raw_418 is not None else _extract_cn_partial_offers_allowed_418(soup)
        )
        cn_offers_scope_4110 = _map_cn_offers_scope(raw_4110) if raw_4110 is not None else _extract_cn_offers_scope_4110(soup)

    contract_notice_core_sections = None
    if notice_type == "ContractNotice" and core_model is not None:
        contract_notice_core_sections = _stringify_section_dict(core_model.model_dump(exclude_none=True))

    # Type-aware value extraction
    values = None
    if notice_type and notice_type in _VALUE_EXTRACTORS:
        values = _VALUE_EXTRACTORS[notice_type](soup)
    elif notice_type is None:
        # Legacy fallback: extract only field 8.2 (TenderResultNotice)
        val = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        if val is not None:
            values = ExtractedValues(value_awarded_contract=val)

    # Type-aware detail extraction (non-value)
    details: dict[str, object] = {}
    if notice_type and notice_type in _DETAIL_EXTRACTORS:
        field_name, extractor = _DETAIL_EXTRACTORS[notice_type]
        details[field_name] = extractor(soup)

    return HtmlExtracted(
        ogloszenie_dotyczy=ogloszenie_dotyczy,
        **address,
        opis=opis,
        kryteria_oceny=kryteria,
        criteria_aspects_4310=criteria_aspects_4310,
        criteria_aspects_4310_flag=criteria_aspects_4310_flag,
        contract_notice_parts=contract_notice_parts,
        contract_notice_core_sections=contract_notice_core_sections,
        contract_notice_parts_sections=contract_notice_parts_sections,
        values=values,
        lots=lots,
        tender_result_parts=tender_result_parts,
        ai_street_512=ai_street_512,
        value_estimated_procurement_ai_35=value_estimated_procurement_ai_35,
        ai_prior_market_consultation_31=ai_prior_market_consultation_31,
        cpn_contractor_names_431=cpn_contractor_names_431,
        contractor_id_raw=contractor_id_raw,
        contractor_id_parsed=contractor_id_parsed,
        contractor_id_type=contractor_id_type,
        cpn_contractor_cities_434=cpn_contractor_cities_434,
        cpn_contractor_provinces_436=cpn_contractor_provinces_436,
        cpn_contractor_countries_437=cpn_contractor_countries_437,
        value_contract_reported_execution_44=value_contract_reported_execution_44,
        value_paid_total_55=value_paid_total_55,
        comp_num_awarded_63=comp_num_awarded_63,
        value_competition_prizes_64=value_competition_prizes_64,
        value_competition_followon_order_651=value_competition_followon_order_651,
        comp_requirements_72=comp_requirements_72,
        comp_submission_deadline=comp_submission_deadline,
        comp_result_approval_date_53=comp_result_approval_date_53,
        cn_partial_offers_allowed_418=cn_partial_offers_allowed_418,
        cn_offers_scope_4110=cn_offers_scope_4110,
        **details,
    )


def parse_html_address(html: str, notice_type: str | None = None) -> dict[str, str | None]:
    """Parse only common address fields from notice HTML.

    This lightweight parser is used by the Silver envelope path when
    full type-specific extraction is not needed.
    """
    soup = BeautifulSoup(html, "lxml")
    return _extract_address(soup, notice_type=notice_type)


def parse_html_address_light(html: str, notice_type: str | None = None) -> dict[str, str | None]:
    """Fast targeted extraction of common address fields without BeautifulSoup."""
    mapping = _ADDRESS_FIELD_NUMS_BY_TYPE.get(notice_type) or _ADDRESS_FIELD_NUMS_BY_TYPE[None]

    def _first_value(field_nums: tuple[str, ...]) -> str | None:
        for field_num in field_nums:
            value = _extract_h3_field_fast(html, field_num)
            if value:
                return value
        return None

    ulica = _first_value(mapping["ulica"])
    kod_pocztowy = _first_value(mapping["kod_pocztowy"])
    nuts3_raw = _first_value(mapping["nuts3"])

    nuts3_code = None
    nuts3_name = None
    if nuts3_raw and " - " in nuts3_raw:
        nuts3_code, nuts3_name = nuts3_raw.split(" - ", 1)
        nuts3_code = nuts3_code.strip() or None
        nuts3_name = nuts3_name.strip() or None

    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "nuts3_code": nuts3_code,
        "nuts3_name": nuts3_name,
    }


def _extract_h3_field_fast(html: str, field_num: str) -> str | None:
    marker_re = _field_marker_re(field_num)
    match = marker_re.search(html)
    if match is None:
        return None
    idx = match.start()
    start = html.rfind("<h3", 0, idx)
    end = html.find("</h3>", idx)
    if start < 0 or end < 0:
        return None
    snippet = html[start : end + 5]

    span = _SPAN_NORMAL_RE.search(snippet)
    if span:
        text = unescape(_TAG_RE.sub(" ", span.group(1)))
        text = re.sub(r"\s+", " ", text).strip()
        return text or None

    rel_idx = max(0, idx - start)
    tail = snippet[rel_idx + len(field_num) + 1 :]
    text = unescape(_TAG_RE.sub(" ", tail))
    text = re.sub(r"\s+", " ", text).strip(" :\u00a0\t\r\n")
    return text or None


def parse_html_agreement_intention_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for AgreementIntentionNotice."""
    # Prefer buyer address from section I (1.4/1.5), then fallback to section V contractor fields.
    ulica = (
        _extract_h3_field_fast(html, "1.4.1.")
        or _extract_h3_field_fast(html, "1.5.1.")
        or _extract_h3_field_fast(html, "5.1.2.")
    )
    kod_pocztowy = (
        _extract_h3_field_fast(html, "1.4.3.")
        or _extract_h3_field_fast(html, "1.5.3.")
        or _extract_h3_field_fast(html, "5.1.4.")
    )
    ai_street_512 = _extract_h3_field_fast(html, "5.1.2.")
    value_estimated_procurement_ai_35 = _parse_pln_value(_extract_h3_field_fast(html, "3.5."))
    ai_prior_market_consultation_31 = _extract_h3_field_fast(html, "3.1.")
    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "ai_street_512": ai_street_512,
        "value_estimated_procurement_ai_35": value_estimated_procurement_ai_35,
        "ai_prior_market_consultation_31": ai_prior_market_consultation_31,
    }


def parse_html_competition_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for CompetitionNotice/CompetitionResultNotice."""
    ulica = _extract_h3_field_fast(html, "1.5.1.")
    kod_pocztowy = _extract_h3_field_fast(html, "1.5.3.")
    raw_num_awarded = _extract_h3_field_fast(html, "6.3.")
    comp_num_awarded_63 = None
    if raw_num_awarded:
        match = re.search(r"\d+", raw_num_awarded)
        if match is not None:
            try:
                comp_num_awarded_63 = int(match.group(0))
            except ValueError:
                comp_num_awarded_63 = None
    value_competition_prizes_64 = _parse_pln_value(_extract_h3_field_fast(html, "6.4."))
    value_competition_followon_order_651 = _parse_pln_value(_extract_h3_field_fast(html, "6.5.1."))
    comp_requirements_72 = _extract_h3_field_fast(html, "7.2.")
    comp_submission_deadline = _extract_h3_field_fast(html, "3.6.") or _extract_h3_field_fast(html, "3.5.")
    comp_result_approval_date_53 = None
    raw_result_approval = _extract_h3_field_fast(html, "5.3.")
    if raw_result_approval:
        match = re.search(r"(\d{4}-\d{2}-\d{2})", raw_result_approval)
        if match:
            comp_result_approval_date_53 = match.group(1)
    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "comp_num_awarded_63": comp_num_awarded_63,
        "value_competition_prizes_64": value_competition_prizes_64,
        "value_competition_followon_order_651": value_competition_followon_order_651,
        "comp_requirements_72": comp_requirements_72,
        "comp_submission_deadline": comp_submission_deadline,
        "comp_result_approval_date_53": comp_result_approval_date_53,
    }


def parse_html_contract_performing_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for ContractPerformingNotice."""
    # Keep envelope address tied to section I buyer fields only.
    ulica = _extract_h3_field_fast(html, "1.5.1.") or _extract_h3_field_fast(html, "1.4.1.")
    kod_pocztowy = _extract_h3_field_fast(html, "1.5.3.") or _extract_h3_field_fast(html, "1.4.3.")
    if not ulica or not kod_pocztowy:
        # Defensive fallback for rare HTML variants where fast regex misses
        # section-I fields (layout drift, extra wrappers, malformed tags).
        soup = BeautifulSoup(html, "lxml")
        address = _extract_address(soup, notice_type="ContractPerformingNotice")
        if not ulica:
            ulica = address.get("ulica")
        if not kod_pocztowy:
            kod_pocztowy = address.get("kod_pocztowy")

    ids: list[str] = []
    names: list[str] = []
    cities: list[str] = []
    provinces: list[str] = []
    countries: list[str] = []

    for field_num, bucket in (
        ("4.3.1.", names),
        ("4.3.2.", ids),
        ("4.3.4.", cities),
        ("4.3.6.", provinces),
        ("4.3.7.", countries),
    ):
        marker_re = _field_marker_re(field_num)
        for match in marker_re.finditer(html):
            idx = match.start()
            h3_start = html.rfind("<h3", 0, idx)
            h3_end = html.find("</h3>", idx)
            if h3_start < 0 or h3_end < 0:
                continue
            snippet = html[h3_start : h3_end + 5]
            span = _SPAN_NORMAL_RE.search(snippet)
            if span:
                text = unescape(_TAG_RE.sub(" ", span.group(1)))
                text = re.sub(r"\s+", " ", text).strip()
                if text:
                    bucket.append(text)

    def _uniq(values: list[str]) -> list[str] | None:
        if not values:
            return None
        return list(dict.fromkeys(values))

    contractor_id_raw: list[str] = []
    contractor_id_parsed: list[str] = []
    contractor_id_type: list[str] = []
    for idx, national_id in enumerate(ids):
        country = countries[idx] if idx < len(countries) else None
        raw, parsed, id_type = _classify_contractor_id(country, national_id)
        if raw:
            contractor_id_raw.append(raw)
        if parsed:
            contractor_id_parsed.append(parsed)
        if id_type:
            contractor_id_type.append(id_type)

    execution_period_42 = _extract_h3_field_fast(html, "4.2.")
    execution_period_norm = _normalize_label_text(execution_period_42 or "")
    if not execution_period_42 or execution_period_norm.startswith("okres realizacji"):
        # Some CPN variants keep 4.2 value as plain text right after </h3>.
        # Fast extraction may return only the heading label, so fallback to soup.
        soup = BeautifulSoup(html, "lxml")
        h3_execution = _find_h3(soup, "4.2.") or _find_h3_by_label(
            soup,
            ["okres realizacji", "zamowienia"],
        )
        p_values = _collect_p_values(h3_execution)
        execution_period_42 = _span_value(h3_execution) or _text_after_h3(h3_execution) or (p_values[0] if p_values else None)

    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "cpn_contractor_names_431": _uniq(names),
        "contractor_id_raw": _uniq(contractor_id_raw),
        "contractor_id_parsed": _uniq(contractor_id_parsed),
        "contractor_id_type": _uniq(contractor_id_type),
        "cpn_contractor_cities_434": _uniq(cities),
        "cpn_contractor_provinces_436": _uniq(provinces),
        "cpn_contractor_countries_437": _uniq(countries),
        "cpn_contract_date_41": _extract_h3_field_fast(html, "4.1."),
        "cpn_contract_planned_execution_date_raw": execution_period_42,
        "cpn_execution_end_date_52": _extract_h3_field_fast(html, "5.2."),
        "executed_in_time": _parse_tak_nie(_extract_h3_field_fast(html, "5.3.")),
        "proper_execution": _parse_tak_nie(_extract_h3_field_fast(html, "5.6.")),
        "value_contract_reported_execution_44": _parse_pln_value(_extract_h3_field_fast(html, "4.4.")),
        "value_paid_total_55": _parse_pln_value(_extract_h3_field_fast(html, "5.5.")),
    }

