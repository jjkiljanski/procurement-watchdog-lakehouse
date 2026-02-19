"""Extract structured fields from BZP notice HTML.

Each notice type has a different HTML template with different field numbers.
This parser dispatches value extraction by notice type. See
docs/html_structure.md for the full field reference.
"""

from __future__ import annotations

import re
import unicodedata

from bs4 import BeautifulSoup, Tag

from procurement.silver.models import (
    ChangeEntry,
    ContractExecution,
    ContractNoticePart,
    EvalCriterion,
    ExtractedValues,
    HtmlExtracted,
    NoticeChange,
    TenderResultLot,
    TenderResultEnrichment,
)

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


def _find_h3_by_label(soup: BeautifulSoup, patterns: list[str]) -> Tag | None:
    """Find h3 by case-insensitive label fragments in text content."""
    lowered_patterns = [_normalize_label_text(p) for p in patterns]
    for h3 in soup.find_all("h3"):
        text = _normalize_label_text(h3.get_text(separator=" ", strip=True))
        if all(pattern in text for pattern in lowered_patterns):
            return h3
    return None


def _normalize_label_text(text: str) -> str:
    """Normalize text for robust label matching across encoding variants."""
    lowered = text.casefold()
    # Common mojibake fragments seen in current fixtures/docs.
    replacements = {
        "Ä…": "a",
        "Ä‡": "c",
        "Ä™": "e",
        "Ĺ‚": "l",
        "Ĺ„": "n",
        "Ăł": "o",
        "Ĺ›": "s",
        "Ĺş": "z",
        "ĹĽ": "z",
        "Ă„â€¦": "a",
        "Ă„â€ˇ": "c",
        "Ă„â„˘": "e",
        "Äąâ€š": "l",
        "Äąâ€ž": "n",
        "Ä‚Ĺ‚": "o",
        "Äąâ€ş": "s",
        "ÄąÂş": "z",
        "ÄąÂĽ": "z",
    }
    for src, dst in replacements.items():
        lowered = lowered.replace(src, dst)
    lowered = unicodedata.normalize("NFKD", lowered)
    lowered = "".join(ch for ch in lowered if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", lowered).strip()


def _field_num(h3: Tag | None) -> str | None:
    """Extract a field number prefix from an h3 (e.g. 6.2.)."""
    if h3 is None:
        return None
    text = h3.get_text(separator=" ", strip=True)
    m = re.search(r"(\d+\.\d+(?:\.\d+)?\.)\)", text)
    if m is None:
        return None
    return m.group(1)


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


def _text_after_h3(h3: Tag | None) -> str | None:
    """Extract plain text that follows an <h3> as a sibling text node.

    Handles the pattern where the value is NOT inside a <span> but is a
    bare text node after the h3 (e.g. field 4.2 or 3.4).
    """
    if h3 is None:
        return None
    from bs4 import NavigableString

    sibling = h3.next_sibling
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            text = sibling.strip()
            if text:
                return text
        elif hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "br":
                sibling = sibling.next_sibling
                continue
            break
        sibling = sibling.next_sibling
    return None


def _collect_p_text(h3: Tag) -> str | None:
    """Collect text from sibling <p> tags after an h3 until the next h3/h2.

    Used for change descriptions (3.4.1) that span multiple <p> elements.
    """
    parts: list[str] = []
    sibling = h3.next_sibling
    while sibling is not None:
        if hasattr(sibling, "name"):
            if sibling.name in ("h3", "h2"):
                break
            if sibling.name == "p":
                text = sibling.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)
        sibling = sibling.next_sibling
    return "\n".join(parts) if parts else None


# --- Address extraction (shared across types) ---


_ADDRESS_FIELD_NUMS_BY_TYPE: dict[str | None, dict[str, tuple[str, ...]]] = {
    None: {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    # Most templates keep address fields in section I, but some have variants.
    "ContractNotice": {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    "TenderResultNotice": {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    "NoticeUpdateNotice": {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    "AgreementUpdateNotice": {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    "AgreementIntentionNotice": {
        "ulica": ("1.5.1.",),
        "kod_pocztowy": ("1.5.3.",),
        "nuts3": ("1.5.6.",),
    },
    # Seen variants in execution notices with section IV labels.
    "ContractPerformingNotice": {
        "ulica": ("1.5.1.", "4.1."),
        "kod_pocztowy": ("1.5.3.", "4.3."),
        "nuts3": ("1.5.6.", "4.6."),
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
    """Extract the 'OgĹ‚oszenie dotyczy' field by label text.

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


def _extract_criteria_aspects_4310(soup: BeautifulSoup) -> tuple[str | None, bool | None]:
    """Extract ContractNotice field 4.3.10 text + Tak/Nie flag."""
    h3 = _find_h3(soup, "4.3.10.")
    raw = _span_value(h3) or _text_after_h3(h3)
    return raw, _parse_tak_nie(raw)


def _extract_part_id_from_header(text: str) -> str | None:
    """Extract part identifier from headers like 'Czesc/Czesc nr/Czesc 1'."""
    lowered = text.lower()
    if "4.3." in lowered:
        return None
    if len(lowered) > 64:
        return None
    if not lowered.strip().startswith("cz"):
        return None

    match = re.search(r"(?:nr\s*)?([a-z0-9._/-]+)\s*$", lowered, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None


def _extract_contract_notice_parts(soup: BeautifulSoup) -> list[ContractNoticePart] | None:
    """Extract per-part criteria blocks from ContractNotice SEKCJA IV."""
    h3s = soup.find_all("h3")
    if not h3s:
        return None

    part_headers: list[tuple[int, str]] = []
    for idx, h3 in enumerate(h3s):
        text = h3.get_text(separator=" ", strip=True)
        part_id = _extract_part_id_from_header(text)
        if part_id:
            part_headers.append((idx, part_id))

    if not part_headers:
        return None

    parts: list[ContractNoticePart] = []
    for i, (start_idx, part_id) in enumerate(part_headers):
        end_idx = part_headers[i + 1][0] if i + 1 < len(part_headers) else len(h3s)
        chunk = h3s[start_idx:end_idx]

        criteria: list[EvalCriterion] = []
        j = 0
        while j < len(chunk):
            text = chunk[j].get_text()
            if "4.3.5.)" in text:
                name = _span_value(chunk[j])
                weight = None
                if j + 1 < len(chunk) and "4.3.6.)" in chunk[j + 1].get_text():
                    raw_weight = _span_value(chunk[j + 1])
                    if raw_weight is not None:
                        try:
                            weight = int(raw_weight)
                        except ValueError:
                            pass
                if name and weight is not None:
                    criteria.append(EvalCriterion(name=name, weight=weight))
            j += 1

        aspects_raw = None
        aspects_flag = None
        for h3 in chunk:
            if "4.3.10.)" in h3.get_text():
                aspects_raw = _span_value(h3) or _text_after_h3(h3)
                aspects_flag = _parse_tak_nie(aspects_raw)
                break

        parts.append(
            ContractNoticePart(
                part_id=part_id,
                kryteria_oceny=criteria or None,
                criteria_aspects_4310=aspects_raw,
                criteria_aspects_4310_flag=aspects_flag,
            )
        )

    return parts or None


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
    lots = _extract_tender_result_lots(soup)
    if lots:
        if len(lots) > 1:
            # Multi-lot notices are represented in htmlExtracted.lots.
            # Keep notice-level values null to avoid flattening ambiguity.
            return None
        lot = lots[0]
        contract_value = lot.contract_value
        estimated_value = lot.estimated_value
        lowest_bid = lot.lowest_bid
        highest_bid = lot.highest_bid
        winning_bid = lot.winning_bid
    else:
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


def _extract_lot_id_from_text(text: str) -> str | None:
    """Extract human lot label from header text where possible."""
    patterns = [
        r"(?:CzÄ™Ĺ›Ä‡|Czesc)\s*(?:nr)?\s*([A-Za-z0-9._/-]+)",
        r"Lot\s*(?:nr)?\s*([A-Za-z0-9._/-]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _extract_winner_from_chunk(chunk: list[Tag]) -> str | None:
    for h3 in chunk:
        text = h3.get_text(separator=" ", strip=True)
        if "nazwa" in text.lower() and ("wykonaw" in text.lower() or "udzielono" in text.lower()):
            val = _span_value(h3) or _text_after_h3(h3)
            if val:
                return val
    return None


def _extract_tender_result_lots(soup: BeautifulSoup) -> list[TenderResultLot] | None:
    """Extract per-lot TenderResult values from repeated 4.3/6.2/6.3/6.4/8.2 sections."""
    h3s = soup.find_all("h3")
    chunks: list[list[Tag]] = []
    current: list[Tag] = []

    for h3 in h3s:
        current.append(h3)
        if _field_num(h3) == "8.2.":
            chunks.append(current)
            current = []

    # If there were no 8.2 delimiters, fall back to a single chunk.
    if not chunks and current:
        chunks = [current]

    lots: list[TenderResultLot] = []
    for idx, chunk in enumerate(chunks, start=1):
        by_field: dict[str, Tag] = {}
        for h3 in chunk:
            field = _field_num(h3)
            if field in {"4.3.", "6.2.", "6.3.", "6.4.", "8.2."}:
                by_field[field] = h3

        contract_value = _parse_pln_value(_span_value(by_field.get("8.2.")))
        estimated_value = _parse_pln_value(_span_value(by_field.get("4.3.")))
        lowest_bid = _parse_pln_value(_span_value(by_field.get("6.2.")))
        highest_bid = _parse_pln_value(_span_value(by_field.get("6.3.")))
        winning_bid = _parse_pln_value(_span_value(by_field.get("6.4.")))

        if all(v is None for v in (contract_value, estimated_value, lowest_bid, highest_bid, winning_bid)):
            continue

        lot_id = None
        for h3 in chunk:
            lot_id = _extract_lot_id_from_text(h3.get_text(separator=" ", strip=True))
            if lot_id:
                break

        lots.append(
            TenderResultLot(
                lot_id=lot_id or str(idx),
                contract_value=contract_value,
                estimated_value=estimated_value,
                lowest_bid=lowest_bid,
                highest_bid=highest_bid,
                winning_bid=winning_bid,
                winner=_extract_winner_from_chunk(chunk),
            )
        )

    return lots or None


def _extract_status_lots_from_procedure_result(procedure_result: str | None) -> list[TenderResultLot] | None:
    """Create synthetic lot rows for cancelled/unresolved outcomes."""
    if not procedure_result:
        return None
    tokens = [token.strip() for token in procedure_result.split(";") if token.strip()]
    if not tokens:
        return None
    normalized = [token.lower() for token in tokens]
    if not any(token in {"uniewaznienie", "nierozstrzygnieto"} for token in normalized):
        return None
    return [
        TenderResultLot(
            lot_id=str(i),
            winner=status,
        )
        for i, status in enumerate(tokens, start=1)
    ]


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


def _extract_agreement_intention_fields(
    soup: BeautifulSoup,
) -> tuple[str | None, float | None, str | None]:
    """AgreementIntentionNotice: 5.1.2 street, 3.5 value, 3.1 consultation info."""
    ai_street_512 = _span_value(_find_h3(soup, "5.1.2."))
    if ai_street_512 is None:
        ai_street_512 = _span_value(_find_h3_by_label(soup, ["ulica"]))

    ai_contract_value_35 = _parse_pln_value(_span_value(_find_h3(soup, "3.5.")))
    ai_prior_market_consultation_31 = _span_value(_find_h3(soup, "3.1.")) or _text_after_h3(
        _find_h3(soup, "3.1.")
    )
    return ai_street_512, ai_contract_value_35, ai_prior_market_consultation_31


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


# --- Type-specific detail extraction (non-value) ---


def _extract_details_tender_result(soup: BeautifulSoup) -> TenderResultEnrichment | None:
    """TenderResultNotice: fields 7.1 (joint bidders), 7.2 (enterprise size)."""
    joint_bidders = _parse_tak_nie(_span_value(_find_h3(soup, "7.1.")))
    contractor_size = _span_value(_find_h3(soup, "7.2."))

    if joint_bidders is None and contractor_size is None:
        return None
    return TenderResultEnrichment(
        joint_bidders=joint_bidders,
        contractor_size=contractor_size,
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


def _extract_details_notice_update(soup: BeautifulSoup) -> NoticeChange | None:
    """NoticeUpdateNotice: fields 3.2, 3.3, 3.4/3.4.1 (repeating)."""
    changed_notice_number = _span_value(_find_h3(soup, "3.2."))
    changed_notice_version = _span_value(_find_h3(soup, "3.3."))

    # Collect 3.4/3.4.1 pairs by walking h3 tags in document order
    # (mirrors the _extract_criteria pattern for 4.3.5/4.3.6 pairs)
    changes: list[ChangeEntry] = []
    all_h3s = soup.find_all("h3")
    i = 0
    while i < len(all_h3s):
        text = all_h3s[i].get_text()
        if "3.4.)" in text and "3.4.1.)" not in text:
            changed_section = _span_value(all_h3s[i]) or _text_after_h3(all_h3s[i])
            change_desc = None
            if i + 1 < len(all_h3s) and "3.4.1.)" in all_h3s[i + 1].get_text():
                change_desc = _collect_p_text(all_h3s[i + 1])
                i += 1  # skip the 3.4.1 we just consumed
            if changed_section or change_desc:
                changes.append(
                    ChangeEntry(
                        changed_section=changed_section,
                        change_description=change_desc,
                    )
                )
        i += 1

    if changed_notice_number is None and changed_notice_version is None and not changes:
        return None
    return NoticeChange(
        changed_notice_number=changed_notice_number,
        changed_notice_version=changed_notice_version,
        changes=changes or None,
    )


_DETAIL_EXTRACTORS: dict[str, tuple[str, object]] = {
    "TenderResultNotice": ("tender_result_enrichment", _extract_details_tender_result),
    "ContractPerformingNotice": ("contract_execution", _extract_details_contract_performing),
    "NoticeUpdateNotice": ("notice_change", _extract_details_notice_update),
}


# --- CPV code parsing ---


def parse_cpv_codes(cpv_raw: str) -> list[str]:
    """Parse cpvCode string into a list of individual CPV entries.

    Input:  "45000000-7 (Roboty budowlane),90620000-9 (UsĹ‚ugi odĹ›nieĹĽania)"
    Output: ["45000000-7 (Roboty budowlane)", "90620000-9 (UsĹ‚ugi odĹ›nieĹĽania)"]
    """
    # Split on comma followed by a digit (start of next CPV code)
    return [part.strip() for part in re.split(r",(?=\d)", cpv_raw) if part.strip()]


# --- Main parse entry point ---


def parse_html(
    html: str,
    notice_type: str | None = None,
    procedure_result: str | None = None,
) -> HtmlExtracted:
    """Parse a single BZP notice HTML into extracted fields.

    Args:
        html: Raw HTML body of the notice.
        notice_type: The noticeType value (e.g. "ContractPerformingNotice").
            Required for type-aware value extraction. If None, value
            extraction falls back to TenderResultNotice field 8.2 only
            (legacy behavior).
    """
    soup = BeautifulSoup(html, "lxml")

    ogloszenie_dotyczy = _extract_ogloszenie_dotyczy(soup)
    address = _extract_address(soup, notice_type=notice_type)
    opis = _extract_description(soup)
    kryteria = _extract_criteria(soup)
    criteria_aspects_4310, criteria_aspects_4310_flag = _extract_criteria_aspects_4310(soup)
    contract_notice_parts = (
        _extract_contract_notice_parts(soup) if notice_type == "ContractNotice" else None
    )
    lots = _extract_tender_result_lots(soup) if notice_type == "TenderResultNotice" else None
    if notice_type == "TenderResultNotice" and not lots:
        lots = _extract_status_lots_from_procedure_result(procedure_result)
    ai_street_512 = None
    ai_contract_value_35 = None
    ai_prior_market_consultation_31 = None
    if notice_type == "AgreementIntentionNotice":
        (
            ai_street_512,
            ai_contract_value_35,
            ai_prior_market_consultation_31,
        ) = _extract_agreement_intention_fields(soup)

    # Type-aware value extraction
    values = None
    if notice_type and notice_type in _VALUE_EXTRACTORS:
        values = _VALUE_EXTRACTORS[notice_type](soup)
    elif notice_type is None:
        # Legacy fallback: extract only field 8.2 (TenderResultNotice)
        val = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        if val is not None:
            values = ExtractedValues(contract_value=val)

    # Type-aware detail extraction (non-value)
    details: dict[str, object] = {}
    if notice_type and notice_type in _DETAIL_EXTRACTORS:
        field_name, extractor = _DETAIL_EXTRACTORS[notice_type]
        details[field_name] = extractor(soup)

    return HtmlExtracted(
        ogloszenie_dotyczy=ogloszenie_dotyczy,
        **address,
        opis=opis,
        kryteria_oceny=kryteria,
        criteria_aspects_4310=criteria_aspects_4310,
        criteria_aspects_4310_flag=criteria_aspects_4310_flag,
        contract_notice_parts=contract_notice_parts,
        values=values,
        lots=lots,
        ai_street_512=ai_street_512,
        ai_contract_value_35=ai_contract_value_35,
        ai_prior_market_consultation_31=ai_prior_market_consultation_31,
        **details,
    )

