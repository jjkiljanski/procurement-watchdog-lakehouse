"""Gold-layer extraction helpers for ContractPerformingNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

import re
from html import unescape

from bs4 import BeautifulSoup

from procurement.silver.section_pipeline.parser_utils import (
    _collect_p_values,
    _extract_h3_field_fast,
    _field_marker_re,
    _find_all_h3,
    _find_h3,
    _find_h3_by_label,
    _normalize_label_text,
    _span_value,
    _text_after_h3,
)
from procurement.silver.section_value_parsers.common import (
    _classify_contractor_id,
    _extract_currency,
    _parse_pln_value,
    _parse_tak_nie,
)
from procurement.silver.legacy.models import (
    ContractExecution,
    ExtractedValues,
)
from procurement.gold.notice_types.common import _extract_address

_SPAN_NORMAL_RE = re.compile(
    r"<span[^>]*class=['\"]normal['\"][^>]*>(.*?)</span>",
    re.IGNORECASE | re.DOTALL,
)
_TAG_RE = re.compile(r"<[^>]+>")


def _extract_values_contract_performing(soup: BeautifulSoup) -> ExtractedValues | None:
    """ContractPerformingNotice: fields 4.4 (contract value), 5.5 (total paid)."""
    value_contract_reported_execution = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    value_paid_total = _parse_pln_value(_span_value(_find_h3(soup, "5.5.")))
    currency = _extract_currency(soup, "5.4.7.")

    if value_contract_reported_execution is None and value_paid_total is None:
        return None
    return ExtractedValues(
        value_contract_reported_execution=value_contract_reported_execution,
        value_paid_total=value_paid_total,
        currency=currency,
    )


def _extract_contract_performing_party_fields(
    soup: BeautifulSoup,
) -> tuple[
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    list[str] | None,
    float | None,
]:
    """ContractPerformingNotice: repeated contractor fields + contract value.

    Fields:
    - 4.3.1.) Nazwa wykonawcy
    - 4.3.2.) Krajowy Numer Identyfikacyjny -> contractor_id_raw/parsed/type
    - 4.3.4.) Miejscowość
    - 4.3.6.) Województwo
    - 4.3.7.) Kraj
    - 4.4.) Wartość umowy
    """
    def _collect(field_num: str) -> list[str] | None:
        out: list[str] = []
        for h3 in _find_all_h3(soup, field_num):
            value = _span_value(h3) or _text_after_h3(h3)
            if value:
                out.append(value)
        if not out:
            return None
        # Preserve order, remove duplicates.
        return list(dict.fromkeys(out))

    names = _collect("4.3.1.")
    national_ids = _collect("4.3.2.")
    cities = _collect("4.3.4.")
    provinces = _collect("4.3.6.")
    countries = _collect("4.3.7.")
    contractor_id_raw: list[str] = []
    contractor_id_parsed: list[str] = []
    contractor_id_type: list[str] = []

    if national_ids:
        for idx, national_id in enumerate(national_ids):
            country = countries[idx] if countries and idx < len(countries) else None
            raw, parsed, id_type = _classify_contractor_id(country, national_id)
            if raw:
                contractor_id_raw.append(raw)
            if parsed:
                contractor_id_parsed.append(parsed)
            if id_type:
                contractor_id_type.append(id_type)

    def _uniq(values: list[str]) -> list[str] | None:
        if not values:
            return None
        return list(dict.fromkeys(values))

    value_contract_reported_execution_44 = _parse_pln_value(_span_value(_find_h3(soup, "4.4.")))
    return (
        names,
        _uniq(contractor_id_raw),
        _uniq(contractor_id_parsed),
        _uniq(contractor_id_type),
        cities,
        provinces,
        countries,
        value_contract_reported_execution_44,
    )


def _extract_details_contract_performing(soup: BeautifulSoup) -> ContractExecution | None:
    """ContractPerformingNotice: execution details resolved by label + numeric fallback."""
    h3_contract_date = _find_h3_by_label(soup, ["data zawarcia umowy"]) or _find_h3(soup, "4.1.")
    contract_date = _span_value(h3_contract_date) or _text_after_h3(h3_contract_date)

    h3_execution_period = _find_h3_by_label(soup, ["okres realizacji"]) or _find_h3(soup, "4.2.")
    execution_period = _span_value(h3_execution_period) or _text_after_h3(h3_execution_period)

    h3_contract_executed = _find_h3_by_label(soup, ["czy umowa", "wykonana"]) or _find_h3(soup, "5.1.")
    contract_executed = _parse_tak_nie(_span_value(h3_contract_executed) or _text_after_h3(h3_contract_executed))

    h3_execution_end = _find_h3_by_label(soup, ["termin wykonania umowy"]) or _find_h3(soup, "5.2.")
    execution_end_date = _span_value(h3_execution_end) or _text_after_h3(h3_execution_end)

    h3_on_time = None
    for candidate in soup.find_all("h3"):
        raw = candidate.get_text(separator=" ", strip=True).lower()
        if "pierwotnie" in raw and "termin" in raw:
            h3_on_time = candidate
            break
    if h3_on_time is None:
        h3_on_time = _find_h3_by_label(soup, ["w terminie"]) or _find_h3(soup, "5.3.")
    executed_on_time = _parse_tak_nie(_span_value(h3_on_time) or _text_after_h3(h3_on_time))

    h3_changes = _find_h3_by_label(soup, ["liczba zmian"]) or _find_h3(soup, "5.4.1.")
    raw_changes = _span_value(h3_changes) or _text_after_h3(h3_changes)
    num_changes: int | None = None
    if raw_changes is not None:
        try:
            num_changes = int(raw_changes.strip())
        except ValueError:
            pass

    h3_proper = _find_h3_by_label(soup, ["wykonana naleĹĽycie"]) or _find_h3(soup, "5.6.")
    executed_properly = _parse_tak_nie(_span_value(h3_proper) or _text_after_h3(h3_proper))

    if all(
        v is None
        for v in (
            contract_date,
            execution_period,
            contract_executed,
            execution_end_date,
            executed_on_time,
            num_changes,
            executed_properly,
        )
    ):
        return None
    return ContractExecution(
        contract_date=contract_date,
        execution_period=execution_period,
        contract_executed=contract_executed,
        execution_end_date=execution_end_date,
        executed_on_time=executed_on_time,
        num_changes=num_changes,
        executed_properly=executed_properly,
    )


def parse_html_contract_performing_light(html: str) -> dict[str, object]:
    """Fast targeted extraction for ContractPerformingNotice.

    Section mapping knowledge:
    - 1.5.1 / 1.4.1: ulica (buyer address)
    - 1.5.3 / 1.4.3: kod_pocztowy
    - 4.3.1: contractor names
    - 4.3.2: contractor national IDs
    - 4.3.4: contractor cities
    - 4.3.6: contractor provinces
    - 4.3.7: contractor countries
    - 4.1: contract date
    - 4.2: execution period
    - 4.4: contract value
    - 5.2: execution end date
    - 5.3: executed in time (Tak/Nie)
    - 5.5: value paid total
    - 5.6: proper execution (Tak/Nie)
    """
    # Keep envelope address tied to section I buyer fields only.
    ulica = _extract_h3_field_fast(html, "1.5.1.") or _extract_h3_field_fast(html, "1.4.1.")
    kod_pocztowy = _extract_h3_field_fast(html, "1.5.3.") or _extract_h3_field_fast(html, "1.4.3.")
    if not ulica or not kod_pocztowy:
        # Defensive fallback for rare HTML variants where fast regex misses
        # section-I fields (layout drift, extra wrappers, malformed tags).
        soup = BeautifulSoup(html, "lxml")
        address = _extract_address(soup, notice_type="ContractPerformingNotice")
        if not ulica:
            ulica = address.get("ulica")
        if not kod_pocztowy:
            kod_pocztowy = address.get("kod_pocztowy")

    ids: list[str] = []
    names: list[str] = []
    cities: list[str] = []
    provinces: list[str] = []
    countries: list[str] = []

    for field_num, bucket in (
        ("4.3.1.", names),
        ("4.3.2.", ids),
        ("4.3.4.", cities),
        ("4.3.6.", provinces),
        ("4.3.7.", countries),
    ):
        marker_re = _field_marker_re(field_num)
        for match in marker_re.finditer(html):
            idx = match.start()
            h3_start = html.rfind("<h3", 0, idx)
            h3_end = html.find("</h3>", idx)
            if h3_start < 0 or h3_end < 0:
                continue
            snippet = html[h3_start : h3_end + 5]
            span = _SPAN_NORMAL_RE.search(snippet)
            if span:
                text = unescape(_TAG_RE.sub(" ", span.group(1)))
                text = re.sub(r"\s+", " ", text).strip()
                if text:
                    bucket.append(text)

    def _uniq(values: list[str]) -> list[str] | None:
        if not values:
            return None
        return list(dict.fromkeys(values))

    contractor_id_raw: list[str] = []
    contractor_id_parsed: list[str] = []
    contractor_id_type: list[str] = []
    for idx, national_id in enumerate(ids):
        country = countries[idx] if idx < len(countries) else None
        raw, parsed, id_type = _classify_contractor_id(country, national_id)
        if raw:
            contractor_id_raw.append(raw)
        if parsed:
            contractor_id_parsed.append(parsed)
        if id_type:
            contractor_id_type.append(id_type)

    execution_period_42 = _extract_h3_field_fast(html, "4.2.")
    execution_period_norm = _normalize_label_text(execution_period_42 or "")
    if not execution_period_42 or execution_period_norm.startswith("okres realizacji"):
        # Some CPN variants keep 4.2 value as plain text right after </h3>.
        # Fast extraction may return only the heading label, so fallback to soup.
        soup = BeautifulSoup(html, "lxml")
        h3_execution = _find_h3(soup, "4.2.") or _find_h3_by_label(
            soup,
            ["okres realizacji", "zamowienia"],
        )
        p_values = _collect_p_values(h3_execution)
        execution_period_42 = _span_value(h3_execution) or _text_after_h3(h3_execution) or (p_values[0] if p_values else None)

    return {
        "ulica": ulica,
        "kod_pocztowy": kod_pocztowy,
        "cpn_contractor_names_431": _uniq(names),
        "contractor_id_raw": _uniq(contractor_id_raw),
        "contractor_id_parsed": _uniq(contractor_id_parsed),
        "contractor_id_type": _uniq(contractor_id_type),
        "cpn_contractor_cities_434": _uniq(cities),
        "cpn_contractor_provinces_436": _uniq(provinces),
        "cpn_contractor_countries_437": _uniq(countries),
        "cpn_contract_date_41": _extract_h3_field_fast(html, "4.1."),
        "cpn_contract_planned_execution_date_raw": execution_period_42,
        "cpn_execution_end_date_52": _extract_h3_field_fast(html, "5.2."),
        "executed_in_time": _parse_tak_nie(_extract_h3_field_fast(html, "5.3.")),
        "proper_execution": _parse_tak_nie(_extract_h3_field_fast(html, "5.6.")),
        "value_contract_reported_execution_44": _parse_pln_value(_extract_h3_field_fast(html, "4.4.")),
        "value_paid_total_55": _parse_pln_value(_extract_h3_field_fast(html, "5.5.")),
    }
