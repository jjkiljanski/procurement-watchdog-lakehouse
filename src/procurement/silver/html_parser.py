"""Extract structured fields from BZP notice HTML.

Each notice type has a different HTML template with different field numbers.
This parser dispatches value extraction by notice type. See
docs/html_structure.md for the full field reference.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from procurement.silver.models import EvalCriterion, ExtractedValues, HtmlExtracted

# Regex for parsing "1234,56 PLN" style values from span text
_PLN_NUM_RE = re.compile(r"([\d\s\xa0,.]+?)\s*(?:\xa0)?\s*(?:PLN|EUR|USD|GBP|CHF)?$")


def _find_h3(soup: BeautifulSoup, field_num: str) -> Tag | None:
    """Find the first <h3> whose text starts with a given field number."""
    for h3 in soup.find_all("h3"):
        if f"{field_num})" in h3.get_text():
            return h3
    return None


def _find_all_h3(soup: BeautifulSoup, field_num: str) -> list[Tag]:
    """Find all <h3> tags matching a given field number."""
    results = []
    for h3 in soup.find_all("h3"):
        if f"{field_num})" in h3.get_text():
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


# --- Address extraction (shared across types) ---


def _extract_address(soup: BeautifulSoup) -> dict:
    """Extract street, postal code, NUTS3 from SEKCJA I."""
    ulica = _span_value(_find_h3(soup, "1.5.1."))
    kod_pocztowy = _span_value(_find_h3(soup, "1.5.3."))

    nuts3_code = None
    nuts3_name = None
    nuts3_raw = _span_value(_find_h3(soup, "1.5.6."))
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

    i = 0
    while i < len(h3s):
        text = h3s[i].get_text()
        if "4.3.5.)" in text:
            name = _span_value(h3s[i])
            # Next h3 should be 4.3.6 with weight
            weight = None
            if i + 1 < len(h3s) and "4.3.6.)" in h3s[i + 1].get_text():
                raw = _span_value(h3s[i + 1])
                if raw is not None:
                    try:
                        weight = int(raw)
                    except ValueError:
                        pass
            if name and weight is not None:
                criteria.append(EvalCriterion(name=name, weight=weight))
        i += 1

    return criteria or None


# --- Type-specific value extraction ---


def _extract_values_contract_performing(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractPerformingNotice: fields 4.4 (contract value), 5.5 (total paid)."""
    contract_value = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    total_paid = _parse_pln_value(_span_value(_find_h3(soup, "5.5.")))
    currency = _extract_currency(soup, "5.4.7.")

    if contract_value is None and total_paid is None:
        return None
    return ExtractedValues(
        contract_value=contract_value,
        total_paid=total_paid,
        currency=currency,
    )


def _extract_values_tender_result(soup: BeautifulSoup) -> ExtractedValues | None:
    """TenderResultNotice: fields 8.2, 6.2, 6.3, 6.4, 4.3 (first lot)."""
    contract_value = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
    estimated_value = _parse_pln_value(_span_value(_find_h3(soup, "4.3.")))
    lowest_bid = _parse_pln_value(_span_value(_find_h3(soup, "6.2.")))
    highest_bid = _parse_pln_value(_span_value(_find_h3(soup, "6.3.")))
    winning_bid = _parse_pln_value(_span_value(_find_h3(soup, "6.4.")))

    if all(v is None for v in (contract_value, estimated_value, lowest_bid, highest_bid, winning_bid)):
        return None
    return ExtractedValues(
        contract_value=contract_value,
        estimated_value=estimated_value,
        lowest_bid=lowest_bid,
        highest_bid=highest_bid,
        winning_bid=winning_bid,
    )


def _extract_values_contract_notice(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractNotice: fields 4.1.5 (total value), 4.1.6 (net of VAT)."""
    estimated_value = _parse_pln_value(_span_value(_find_h3(soup, "4.1.5.")))
    if estimated_value is None:
        estimated_value = _parse_pln_value(_span_value(_find_h3(soup, "4.1.6.")))
    if estimated_value is None:
        return None
    return ExtractedValues(estimated_value=estimated_value)


def _extract_values_agreement_update(soup: BeautifulSoup) -> ExtractedValues | None:
    """AgreementUpdateNotice: field 4.4 (agreement value)."""
    contract_value = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    if contract_value is None:
        return None
    return ExtractedValues(contract_value=contract_value)


def _extract_values_agreement_intention(soup: BeautifulSoup) -> ExtractedValues | None:
    """AgreementIntentionNotice: field 3.5 (procurement value)."""
    estimated_value = _parse_pln_value(_span_value(_find_h3(soup, "3.5.")))
    if estimated_value is None:
        return None
    return ExtractedValues(estimated_value=estimated_value)


def _extract_values_small_contract(soup: BeautifulSoup) -> ExtractedValues | None:
    """SmallContractNotice: field 3.4 (value, no PLN suffix), 3.5 (currency)."""
    contract_value = _parse_pln_value(_span_value(_find_h3(soup, "3.4.")))
    if contract_value is None:
        return None
    currency = _extract_currency(soup, "3.5.")
    return ExtractedValues(contract_value=contract_value, currency=currency)


_VALUE_EXTRACTORS = {
    "ContractPerformingNotice": _extract_values_contract_performing,
    "TenderResultNotice": _extract_values_tender_result,
    "ContractNotice": _extract_values_contract_notice,
    "AgreementUpdateNotice": _extract_values_agreement_update,
    "AgreementIntentionNotice": _extract_values_agreement_intention,
    "SmallContractNotice": _extract_values_small_contract,
}


# --- CPV code parsing ---


def parse_cpv_codes(cpv_raw: str) -> list[str]:
    """Parse cpvCode string into a list of individual CPV entries.

    Input:  "45000000-7 (Roboty budowlane),90620000-9 (Usługi odśnieżania)"
    Output: ["45000000-7 (Roboty budowlane)", "90620000-9 (Usługi odśnieżania)"]
    """
    # Split on comma followed by a digit (start of next CPV code)
    return [part.strip() for part in re.split(r",(?=\d)", cpv_raw) if part.strip()]


# --- Main parse entry point ---


def parse_html(html: str, notice_type: str | None = None) -> HtmlExtracted:
    """Parse a single BZP notice HTML into extracted fields.

    Args:
        html: Raw HTML body of the notice.
        notice_type: The noticeType value (e.g. "ContractPerformingNotice").
            Required for type-aware value extraction. If None, value
            extraction falls back to TenderResultNotice field 8.2 only
            (legacy behavior).
    """
    soup = BeautifulSoup(html, "lxml")

    address = _extract_address(soup)
    opis = _extract_description(soup)
    kryteria = _extract_criteria(soup)

    # Type-aware value extraction
    values = None
    if notice_type and notice_type in _VALUE_EXTRACTORS:
        values = _VALUE_EXTRACTORS[notice_type](soup)
    elif notice_type is None:
        # Legacy fallback: extract only field 8.2 (TenderResultNotice)
        val = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        if val is not None:
            values = ExtractedValues(contract_value=val)

    return HtmlExtracted(
        **address,
        opis=opis,
        kryteria_oceny=kryteria,
        values=values,
    )
