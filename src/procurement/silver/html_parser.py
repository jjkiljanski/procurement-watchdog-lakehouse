"""Extract structured fields from BZP notice HTML."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from procurement.silver.models import EvalCriterion, HtmlExtracted


def _find_h3(soup: BeautifulSoup, field_num: str) -> Tag | None:
    """Find the first <h3> whose text starts with a given field number."""
    for h3 in soup.find_all("h3"):
        if f"{field_num})" in h3.get_text():
            return h3
    return None


def _span_value(h3: Tag | None) -> str | None:
    """Extract text from <span class='normal'> inside an h3."""
    if h3 is None:
        return None
    span = h3.find("span", class_="normal")
    if span is None:
        return None
    text = span.get_text().strip()
    return text or None


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


def _extract_contract_value(soup: BeautifulSoup) -> float | None:
    """Extract contract value in PLN from field 8.2 (TenderResultNotice)."""
    h3 = _find_h3(soup, "8.2.")
    raw = _span_value(h3)
    if raw is None:
        return None
    # Format: "465163,88 PLN" or "465163,88PLN"
    match = re.match(r"([\d\s]+(?:,\d+)?)", raw.replace("\xa0", ""))
    if match is None:
        return None
    num_str = match.group(1).replace(" ", "").replace(",", ".")
    try:
        return float(num_str)
    except ValueError:
        return None


def parse_cpv_codes(cpv_raw: str) -> list[str]:
    """Parse cpvCode string into a list of individual CPV entries.

    Input:  "45000000-7 (Roboty budowlane),90620000-9 (Usługi odśnieżania)"
    Output: ["45000000-7 (Roboty budowlane)", "90620000-9 (Usługi odśnieżania)"]
    """
    # Split on comma followed by a digit (start of next CPV code)
    return [part.strip() for part in re.split(r",(?=\d)", cpv_raw) if part.strip()]


def parse_html(html: str) -> HtmlExtracted:
    """Parse a single BZP notice HTML into extracted fields."""
    soup = BeautifulSoup(html, "lxml")

    address = _extract_address(soup)
    opis = _extract_description(soup)
    kryteria = _extract_criteria(soup)
    wartosc = _extract_contract_value(soup)

    return HtmlExtracted(
        **address,
        opis=opis,
        kryteria_oceny=kryteria,
        wartosc_umowy_pln=wartosc,
    )
