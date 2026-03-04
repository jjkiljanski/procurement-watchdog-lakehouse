"""Common value parsing helpers shared across notice-specific parsers."""

from __future__ import annotations

import re
from datetime import date

from bs4 import BeautifulSoup

from procurement.silver.parser_utils import _find_h3, _normalize_label_text, _span_value

_PLN_NUM_RE = re.compile(r"([\d\s\xa0,.]+?)\s*(?:\xa0)?\s*(?:PLN|EUR|USD|GBP|CHF)?$")


def _parse_pln_value(raw: str | None) -> float | None:
    """Parse a monetary value string into a float."""
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None

    match = _PLN_NUM_RE.match(raw)
    if match is None:
        match = re.match(r"([\d\s\xa0,.]+)", raw)
        if match is None:
            return None

    num_str = match.group(1).strip()
    num_str = num_str.replace("\xa0", "").replace(" ", "")
    if "." in num_str and "," in num_str:
        num_str = num_str.replace(".", "")
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


def _parse_criterion_weight(raw: str | None) -> int | None:
    """Parse criterion weight from strings like '60', '60,00', '40.00', '100 %'."""
    if raw is None:
        return None
    cleaned = raw.strip().replace("\xa0", " ")
    if not cleaned:
        return None

    match = re.search(r"([0-9][0-9\s.,]*)", cleaned)
    if match is None:
        return None

    num = match.group(1).replace(" ", "")
    if "." in num and "," in num:
        num = num.replace(".", "")
    num = num.replace(",", ".")

    try:
        return int(round(float(num)))
    except ValueError:
        return None


def parse_cpv_codes(cpv_raw: str) -> list[str]:
    """Parse cpvCode string into canonical CPV codes only."""
    matches = re.findall(r"\b(\d{8}-\d)\b", cpv_raw)
    if not matches:
        return []
    return list(dict.fromkeys(matches))


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


def classify_polish_national_id(raw_id: str) -> tuple[str | None, str]:
    """Classify Polish national ID into parsed value and type."""
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


def classify_national_id_by_country(
    country: str | None,
    raw_id: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Return (raw, parsed, type) for country-aware national ID parsing."""
    if raw_id is None:
        return None, None, None
    raw = raw_id.strip()
    if not raw:
        return None, None, None
    if not _is_poland_country(country):
        return raw, raw, "foreign"
    parsed, id_type = classify_polish_national_id(raw)
    return raw, parsed, id_type


def _classify_contractor_id(
    country: str | None,
    raw_id: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Return (raw, parsed, type) for contractor ID."""
    return classify_national_id_by_country(country, raw_id)


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


def parse_date_from_text(raw: str | None) -> str | None:
    """Extract ISO date string (YYYY-MM-DD) from raw text."""
    if not raw:
        return None
    m = re.search(r"(\d{4}-\d{2}-\d{2})", raw)
    return m.group(1) if m else None


def parse_int_from_text(raw: str | None) -> int | None:
    """Extract first integer from raw text."""
    if not raw:
        return None
    m = re.search(r"\d+", raw)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None
