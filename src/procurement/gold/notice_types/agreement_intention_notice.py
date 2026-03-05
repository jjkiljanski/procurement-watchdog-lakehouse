"""Gold-layer extraction helpers for AgreementIntentionNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.html_parsing.utils import (
    _extract_h3_field_fast,
    _find_h3,
    _find_h3_by_label,
    _span_value,
    _text_after_h3,
)
from procurement.silver.field_parsers.common import (
    _parse_pln_value,
)
from procurement.silver.legacy.models import ExtractedValues


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


def parse_html_agreement_intention_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for AgreementIntentionNotice.

    Section mapping knowledge:
    - 1.4.1 / 1.5.1: ulica (buyer address, legacy templates use 1.4.x)
    - 1.4.3 / 1.5.3: kod_pocztowy
    - 5.1.2: ai_street_512 (contractor street)
    - 5.1.4: ai_zip (contractor zip — used for fallback kod_pocztowy)
    - 3.1: ai_prior_market_consultation
    - 3.5: value_estimated_procurement
    """
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
