"""Gold-layer extraction helpers for SmallContractNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

from bs4 import BeautifulSoup

from procurement.silver.section_pipeline.parser_utils import _find_h3, _span_value
from procurement.silver.section_value_parsers.common import (
    _extract_currency,
    _parse_pln_value,
)
from procurement.silver.legacy.models import ExtractedValues


def _extract_values_small_contract(soup: BeautifulSoup) -> ExtractedValues | None:
    """SmallContractNotice: field 3.4 (value, no PLN suffix), 3.5 (currency)."""
    value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "3.4.")))
    if value_awarded_contract is None:
        return None
    currency = _extract_currency(soup, "3.5.")
    return ExtractedValues(value_awarded_contract=value_awarded_contract, currency=currency)
