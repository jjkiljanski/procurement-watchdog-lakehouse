"""Common value parsing helpers shared across notice-specific parsers."""

from __future__ import annotations

import re
import unicodedata
import calendar
from datetime import date, timedelta

from bs4 import BeautifulSoup

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


def _normalize_label_text(text: str) -> str:
    """Normalize text for robust label matching across encoding variants."""
    lowered = text.casefold()
    replacements = {
        "Ä…": "a", "Ä‡": "c", "Ä™": "e", "Ĺ‚": "l", "Ĺ„": "n",
        "Ăł": "o", "Ĺ›": "s", "Ĺş": "z", "ĹĽ": "z",
        "Ă„â€¦": "a", "Ă„â€ˇ": "c", "Ă„â„˘": "e", "Äąâ€š": "l",
        "Äąâ€ž": "n", "Ä‚Ĺ‚": "o", "Äąâ€ş": "s", "ÄąÂş": "z", "ÄąÂĽ": "z",
    }
    for src, dst in replacements.items():
        lowered = lowered.replace(src, dst)
    lowered = unicodedata.normalize("NFKD", lowered)
    lowered = "".join(ch for ch in lowered if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", lowered).strip()


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


def _add_calendar_months(d: date, months: int) -> date:
    """Add N calendar months to a date, clamping overflow (e.g. Jan 31 + 1M → Feb 28)."""
    total = d.month + months
    year = d.year + (total - 1) // 12
    month = (total - 1) % 12 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


# Matches Polish duration tokens: "238 dni", "12 miesiące", "3 tygodnie", "2 lata",
# and fixed end-date: "do 2024-12-13"
_DURATION_RE = re.compile(
    r"do\s+(\d{4}-\d{2}-\d{2})"                   # fixed end-date
    r"|(\d+)\s+d(?:ni|nia|zień|zien)"              # days
    r"|(\d+)\s+tyg(?:odn|odni|odnie|odniu|odn)?"  # weeks
    r"|(\d+)\s+miesi(?:ąc|ac|ę|e|ę|ecy|ąca)"      # months
    r"|(\d+)\s+(?:lat|lata|roku?)",                # years
    re.IGNORECASE | re.UNICODE,
)


def _parse_raw_duration(
    raw: str,
) -> tuple[str | None, int | str | None]:
    """Parse a Polish duration/end-date string into (kind, value).

    Returns:
        ('end_date', 'YYYY-MM-DD') for "do YYYY-MM-DD"
        ('days',  N)   for "N dni"
        ('weeks', N)   for "N tygodnie"
        ('months', N)  for "N miesiące"
        ('years', N)   for "N lat"
        (None, None)   when unrecognised
    """
    m = _DURATION_RE.search(raw.strip())
    if not m:
        return None, None
    end_date, days, weeks, months, years = m.groups()
    if end_date:
        return "end_date", end_date
    if days:
        return "days", int(days)
    if weeks:
        return "weeks", int(weeks)
    if months:
        return "months", int(months)
    if years:
        return "years", int(years)
    return None, None


def compute_duration_days(
    start_date_iso: str | None,
    raw_duration: str | None,
) -> int | None:
    """Return the contract duration in calendar days.

    Requires the start date so that months and years can be resolved to
    exact day counts, and "do YYYY-MM-DD" end-dates can be differenced.

    Parameters
    ----------
    start_date_iso:
        The contract signing date as an ISO string (section_4_1 after parsing).
    raw_duration:
        The raw Polish duration string from section_4_2.
    """
    if not start_date_iso or not raw_duration:
        return None
    kind, value = _parse_raw_duration(raw_duration)
    if kind is None:
        return None
    try:
        start = date.fromisoformat(start_date_iso)
        if kind == "days":
            return int(value)
        if kind == "weeks":
            return int(value) * 7
        if kind == "months":
            return (_add_calendar_months(start, int(value)) - start).days
        if kind == "years":
            return (_add_calendar_months(start, int(value) * 12) - start).days
        if kind == "end_date":
            return (date.fromisoformat(value) - start).days
    except (ValueError, TypeError):
        return None
    return None


def compute_contract_end_date(
    start_date_iso: str | None,
    raw_duration: str | None,
) -> str | None:
    """Return the contract end date as an ISO string.

    Parameters
    ----------
    start_date_iso:
        The contract signing date as an ISO string (section_4_1 after parsing).
    raw_duration:
        The raw Polish duration string from section_4_2.
    """
    if not start_date_iso or not raw_duration:
        return None
    kind, value = _parse_raw_duration(raw_duration)
    if kind is None:
        return None
    try:
        start = date.fromisoformat(start_date_iso)
        if kind == "days":
            return (start + timedelta(days=int(value))).isoformat()
        if kind == "weeks":
            return (start + timedelta(days=int(value) * 7)).isoformat()
        if kind == "months":
            return _add_calendar_months(start, int(value)).isoformat()
        if kind == "years":
            return _add_calendar_months(start, int(value) * 12).isoformat()
        if kind == "end_date":
            return str(value)
    except (ValueError, TypeError):
        return None
    return None


def parse_nuts3_code(raw: str | None) -> str | None:
    """Extract the NUTS-3 code from 'PL21A - Oświęcimski' → 'PL21A'."""
    if not raw or " - " not in raw:
        return None
    return raw.split(" - ", 1)[0].strip() or None


def parse_nuts3_name(raw: str | None) -> str | None:
    """Extract the NUTS-3 region name from 'PL21A - Oświęcimski' → 'Oświęcimski'."""
    if not raw or " - " not in raw:
        return None
    return raw.split(" - ", 1)[1].strip() or None


def parse_national_id_value(raw: str | None) -> str | None:
    """Extract the normalised digits from a Polish national ID string.

    Accepts prefixed formats like 'REGON 276258032' or 'REGON: 000515885'
    and returns only the canonical digit string, or ``None`` when unrecognised.
    """
    if not raw:
        return None
    parsed, id_type = classify_polish_national_id(raw.strip())
    return parsed


def parse_national_id_type(raw: str | None) -> str | None:
    """Return the type label (NIP / REGON / PESEL / foreign) for a national ID string.

    Returns ``None`` when the value cannot be classified.
    """
    if not raw:
        return None
    _, id_type = classify_polish_national_id(raw.strip())
    return id_type if id_type != "not_recognized" else None


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
