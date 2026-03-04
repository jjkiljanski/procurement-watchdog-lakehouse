"""Gold-layer extraction helpers for TenderResultNotice.

TODO: adapt to section-column-based logic once Silver section tables are
      the primary input.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

from procurement.silver.parser_utils import (
    _field_num,
    _find_h3,
    _span_value,
    _text_after_h3,
)
from procurement.silver.html_value_parsers.common_values import (
    _parse_pln_value,
    _parse_tak_nie,
    parse_cpv_codes,
)
from procurement.silver.models import (
    ExtractedValues,
    TenderResultEnrichment,
    TenderResultLot,
    TenderResultPart,
)


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

        value_awarded_contract = _parse_pln_value(_span_value(by_field.get("8.2.")))
        value_estimated_procurement = _parse_pln_value(_span_value(by_field.get("4.3.")))
        value_bid_lowest = _parse_pln_value(_span_value(by_field.get("6.2.")))
        value_bid_highest = _parse_pln_value(_span_value(by_field.get("6.3.")))
        value_winning_offer = _parse_pln_value(_span_value(by_field.get("6.4.")))

        if all(
            v is None
            for v in (
                value_awarded_contract,
                value_estimated_procurement,
                value_bid_lowest,
                value_bid_highest,
                value_winning_offer,
            )
        ):
            continue

        lot_id = None
        for h3 in chunk:
            lot_id = _extract_lot_id_from_text(h3.get_text(separator=" ", strip=True))
            if lot_id:
                break

        lots.append(
            TenderResultLot(
                lot_id=lot_id or str(idx),
                value_awarded_contract=value_awarded_contract,
                value_estimated_procurement=value_estimated_procurement,
                value_bid_lowest=value_bid_lowest,
                value_bid_highest=value_bid_highest,
                value_winning_offer=value_winning_offer,
                winner=_extract_winner_from_chunk(chunk),
            )
        )

    return lots or None


def _extract_tender_result_parts(soup: BeautifulSoup) -> list[TenderResultPart] | None:
    """Extract TenderResultNotice part metadata from SEKCJA IV fields."""
    h3s = soup.find_all("h3")
    if not h3s:
        return None

    part_headers: list[tuple[int, str]] = []
    for idx, h3 in enumerate(h3s):
        text = h3.get_text(separator=" ", strip=True)
        part_id = _extract_part_id_from_header(text)
        if part_id:
            part_headers.append((idx, part_id))

    chunks: list[tuple[str | None, list[Tag]]] = []
    if part_headers:
        for i, (start_idx, part_id) in enumerate(part_headers):
            end_idx = part_headers[i + 1][0] if i + 1 < len(part_headers) else len(h3s)
            chunks.append((part_id, h3s[start_idx:end_idx]))
    else:
        chunks.append((None, h3s))

    parts: list[TenderResultPart] = []
    for seq, (_parsed_part_id, chunk) in enumerate(chunks, start=1):
        opis = None
        main_cpv = None
        secondary_cpv: list[str] = []
        value_estimated_procurement = None

        for h3 in chunk:
            text = h3.get_text(separator=" ", strip=True)
            if "4.5.1.)" in text:
                opis = _span_value(h3) or _text_after_h3(h3)
                if opis is None:
                    p = h3.find_next_sibling("p")
                    if p is not None:
                        opis = p.get_text(separator=" ", strip=True) or None
            elif "4.5.3.)" in text:
                raw = _span_value(h3) or _text_after_h3(h3)
                if raw:
                    parsed = parse_cpv_codes(raw)
                    main_cpv = parsed[0] if parsed else raw
            elif "4.5.4.)" in text:
                raw = _span_value(h3) or _text_after_h3(h3)
                if raw:
                    parsed = parse_cpv_codes(raw)
                    if parsed:
                        secondary_cpv.extend(parsed)
                    else:
                        secondary_cpv.append(raw)
            elif "4.3.)" in text:
                value = _parse_pln_value(_span_value(h3) or _text_after_h3(h3))
                if value is not None:
                    value_estimated_procurement = value

        if all(v is None for v in (opis, main_cpv, value_estimated_procurement)) and not secondary_cpv:
            continue

        parts.append(
            TenderResultPart(
                # Keep deterministic numbering by appearance in SEKCJA IV.
                part_id=str(seq),
                opis=opis,
                mainCPV=main_cpv,
                secondaryCPV=list(dict.fromkeys(secondary_cpv)) or None,
                value_estimated_procurement=value_estimated_procurement,
            )
        )

    return parts or None


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


def _extract_values_tender_result(soup: BeautifulSoup) -> ExtractedValues | None:
    """TenderResultNotice: fields 8.2, 6.2, 6.3, 6.4, 4.3 (first lot)."""
    lots = _extract_tender_result_lots(soup)
    if lots:
        if len(lots) > 1:
            # Multi-lot notices are represented in htmlExtracted.lots.
            # Keep notice-level values null to avoid flattening ambiguity.
            return None
        lot = lots[0]
        value_awarded_contract = lot.value_awarded_contract
        value_estimated_procurement = lot.value_estimated_procurement
        value_bid_lowest = lot.value_bid_lowest
        value_bid_highest = lot.value_bid_highest
        value_winning_offer = lot.value_winning_offer
    else:
        value_awarded_contract = _parse_pln_value(_span_value(_find_h3(soup, "8.2.")))
        value_estimated_procurement = _parse_pln_value(_span_value(_find_h3(soup, "4.3.")))
        value_bid_lowest = _parse_pln_value(_span_value(_find_h3(soup, "6.2.")))
        value_bid_highest = _parse_pln_value(_span_value(_find_h3(soup, "6.3.")))
        value_winning_offer = _parse_pln_value(_span_value(_find_h3(soup, "6.4.")))

    if all(
        v is None
        for v in (
            value_awarded_contract,
            value_estimated_procurement,
            value_bid_lowest,
            value_bid_highest,
            value_winning_offer,
        )
    ):
        return None
    return ExtractedValues(
        value_awarded_contract=value_awarded_contract,
        value_estimated_procurement=value_estimated_procurement,
        value_bid_lowest=value_bid_lowest,
        value_bid_highest=value_bid_highest,
        value_winning_offer=value_winning_offer,
    )


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
