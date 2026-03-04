"""Gold-layer extraction helpers for CompetitionNotice and CompetitionResultNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from procurement.silver.html_parsing.utils import (
    _extract_h3_field_fast,
    _find_h3,
    _span_value,
    _text_after_h3,
)
from procurement.silver.field_parsers.common import _parse_pln_value


def _extract_competition_notice_fields(
    soup: BeautifulSoup,
) -> tuple[int | None, float | None, float | None, str | None, str | None]:
    """CompetitionNotice: core competition fields + submission deadline variants.

    Section mapping knowledge:
    - 6.3: number of awarded works
    - 6.4: total prizes value
    - 6.5.1: follow-on order value
    - 7.2: requirements
    - 3.6 / 3.5: submission deadline
    """
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


def parse_html_competition_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for CompetitionNotice/CompetitionResultNotice.

    Section mapping knowledge:
    - 1.5.1: ulica (buyer address)
    - 1.5.3: kod_pocztowy
    - 6.3: number of awarded works
    - 6.4: total prizes value
    - 6.5.1: follow-on order value
    - 7.2: requirements
    - 3.6 / 3.5: submission deadline
    - 5.3: result approval date (CompetitionResultNotice)
    """
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
