"""Common value parsing helpers shared across notice-specific parsers."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from procurement.silver.html_common import _find_h3, _span_value

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

