"""Gold-layer common extraction helpers shared across notice types.

Functions here encode the knowledge of *which* HTML sections map to
which business fields (address, ogłoszenie dotyczy, etc.) for each
notice type. They operate on BeautifulSoup objects; will later be
adapted to work from Silver section-column tables instead.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.section_pipeline.parser_utils import (
    _collect_p_text,
    _find_h3,
    _find_h3_by_label,
    _span_value,
    _text_after_h3,
)


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
    """Extract the 'Ogłoszenie dotyczy' field by label text.

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
