"""Gold-layer extraction helpers for AgreementUpdateNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.html_parsing.utils import _find_h3, _span_value
from procurement.silver.field_parsers.common import _parse_pln_value
from procurement.silver.legacy.models import ExtractedValues


def _extract_values_agreement_update(soup: BeautifulSoup) -> ExtractedValues | None:
    """AgreementUpdateNotice: field 4.4 (agreement value)."""
    value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    if value_awarded_contract is None:
        return None
    return ExtractedValues(value_awarded_contract=value_awarded_contract)
