"""Unit tests for section_pipeline/raw_section_extractor.py.

Requires only BeautifulSoup (bs4) — no Spark, no file I/O.
HTML fixtures are inline strings kept minimal to test exactly one thing.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from procurement.silver.section_pipeline.raw_section_extractor import (
    _collect_p_values,
    _span_value,
    _text_after_h3,
    build_notice_sections_model,
    extract_contract_notice_section_number,
    extract_contract_notice_section_value,
    section_number_key,
    section_to_field_name,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _h3(html_fragment: str) -> "Tag":
    """Parse a fragment and return its first h3 element."""
    return _soup(html_fragment).find("h3")


# ===========================================================================
# extract_contract_notice_section_number
# ===========================================================================


class TestExtractSectionNumber:
    def test_two_level_with_trailing_dot(self):
        assert extract_contract_notice_section_number("4.2.) Opis") == "4.2"

    def test_two_level_no_trailing_dot(self):
        assert extract_contract_notice_section_number("4.2) Opis") == "4.2"

    def test_three_level_with_trailing_dot(self):
        assert extract_contract_notice_section_number("4.2.2.) Krótki opis") == "4.2.2"

    def test_three_level_no_trailing_dot(self):
        assert extract_contract_notice_section_number("1.5.1) Ulica") == "1.5.1"

    def test_section_at_start_of_string(self):
        assert extract_contract_notice_section_number("1.1.) Rola") == "1.1"

    def test_embedded_in_full_h3_text(self):
        # As it appears in get_text() output — section number followed by label
        assert extract_contract_notice_section_number(
            "4.2.2.) Krótki opis przedmiotu zamówienia"
        ) == "4.2.2"

    def test_no_section_number_returns_none(self):
        assert extract_contract_notice_section_number("Kryterium 1") is None

    def test_plain_text_no_number_returns_none(self):
        assert extract_contract_notice_section_number("Brak numeru sekcji") is None

    def test_section_number_not_preceded_by_digit(self):
        # "14.2.)" → the regex's lookbehind prevents matching ".2." inside "14.2."
        # but "14" at start has nothing before it → should match "14.2"
        assert extract_contract_notice_section_number("14.2.) Coś") == "14.2"

    def test_section_number_with_surrounding_whitespace(self):
        assert extract_contract_notice_section_number("  2.7.)  Wartość") == "2.7"


# ===========================================================================
# section_to_field_name
# ===========================================================================


class TestSectionToFieldName:
    def test_two_level(self):
        assert section_to_field_name("2.7") == "section_2_7"

    def test_three_level(self):
        assert section_to_field_name("1.5.1") == "section_1_5_1"

    def test_two_level_large_numbers(self):
        assert section_to_field_name("14.2") == "section_14_2"


# ===========================================================================
# section_number_key
# ===========================================================================


class TestSectionNumberKey:
    def test_two_level(self):
        assert section_number_key("2.7") == (2, 7)

    def test_three_level(self):
        assert section_number_key("1.5.1") == (1, 5, 1)

    def test_ordering_semantics(self):
        # Keys are used for ordering; (1,5,1) < (1,5,2) < (1,6,0)
        assert section_number_key("1.5.1") < section_number_key("1.5.2")
        assert section_number_key("1.5.2") < section_number_key("1.6.0")


# ===========================================================================
# _span_value
# ===========================================================================


class TestSpanValue:
    def test_returns_span_text(self):
        h3 = _h3('<h3>4.2.) Label <span class="normal">Some Value</span></h3>')
        assert _span_value(h3) == "Some Value"

    def test_strips_whitespace(self):
        h3 = _h3('<h3><span class="normal">  Trimmed  </span></h3>')
        assert _span_value(h3) == "Trimmed"

    def test_empty_span_returns_none(self):
        h3 = _h3('<h3><span class="normal">   </span></h3>')
        assert _span_value(h3) is None

    def test_no_span_returns_none(self):
        h3 = _h3("<h3>No span here</h3>")
        assert _span_value(h3) is None

    def test_wrong_span_class_returns_none(self):
        h3 = _h3('<h3><span class="other">ignored</span></h3>')
        assert _span_value(h3) is None

    def test_none_input_returns_none(self):
        assert _span_value(None) is None


# ===========================================================================
# _text_after_h3
# ===========================================================================


class TestTextAfterH3:
    def test_plain_text_sibling(self):
        # NavigableString directly after h3
        soup = _soup("<div><h3>Header</h3>Direct text content<p>para</p></div>")
        h3 = soup.find("h3")
        result = _text_after_h3(h3)
        assert result == "Direct text content"

    def test_skips_br_and_reads_text(self):
        soup = _soup("<div><h3>Header</h3><br/>After br</div>")
        h3 = soup.find("h3")
        result = _text_after_h3(h3)
        assert result == "After br"

    def test_stops_at_next_h3(self):
        soup = _soup("<div><h3>First</h3><h3>Second — should not be returned</h3></div>")
        h3 = soup.find("h3")
        assert _text_after_h3(h3) is None

    def test_stops_at_h2(self):
        soup = _soup("<div><h3>Header</h3><h2>Section title</h2></div>")
        h3 = soup.find("h3")
        assert _text_after_h3(h3) is None

    def test_stops_at_p_element(self):
        # <p> elements are NOT text-after; they break the scan
        soup = _soup("<div><h3>Header</h3><p>Para — not text-after</p></div>")
        h3 = soup.find("h3")
        assert _text_after_h3(h3) is None

    def test_none_input_returns_none(self):
        assert _text_after_h3(None) is None

    def test_empty_text_nodes_skipped(self):
        # Whitespace-only NavigableStrings (e.g. newlines) should not match
        soup = _soup("<div><h3>Header</h3>\n   \n<h3>Next</h3></div>")
        h3 = soup.find("h3")
        assert _text_after_h3(h3) is None


# ===========================================================================
# _collect_p_values
# ===========================================================================


class TestCollectPValues:
    def test_collects_single_p(self):
        soup = _soup("<div><h3>Header</h3><p>First paragraph</p></div>")
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["First paragraph"]

    def test_collects_multiple_p(self):
        soup = _soup(
            "<div><h3>Header</h3><p>Para one</p><p>Para two</p><p>Para three</p></div>"
        )
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["Para one", "Para two", "Para three"]

    def test_stops_at_next_h3(self):
        soup = _soup(
            "<div><h3>H1</h3><p>Para</p><h3>H2</h3><p>Not collected</p></div>"
        )
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["Para"]

    def test_stops_at_h2(self):
        soup = _soup("<div><h3>Header</h3><p>Included</p><h2>Section</h2><p>Excluded</p></div>")
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["Included"]

    def test_skips_non_p_elements(self):
        soup = _soup("<div><h3>Header</h3><span>skipped</span><p>Collected</p></div>")
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["Collected"]

    def test_empty_p_skipped(self):
        soup = _soup("<div><h3>Header</h3><p></p><p>Real content</p></div>")
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == ["Real content"]

    def test_none_input_returns_empty(self):
        assert _collect_p_values(None) == []

    def test_no_p_siblings_returns_empty(self):
        soup = _soup("<div><h3>Header</h3><span>no para</span></div>")
        h3 = soup.find("h3")
        assert _collect_p_values(h3) == []


# ===========================================================================
# extract_contract_notice_section_value  (cascade: span → text-after → p)
# ===========================================================================


class TestExtractSectionValue:
    def test_prefers_span_over_text_and_p(self):
        soup = _soup(
            '<div><h3>1.1.) Label <span class="normal">Span wins</span></h3>'
            "plain text"
            "<p>Para also present</p></div>"
        )
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) == "Span wins"

    def test_falls_back_to_text_after_when_no_span(self):
        soup = _soup("<div><h3>1.1.) Label</h3>Text after h3<p>Para</p></div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) == "Text after h3"

    def test_falls_back_to_p_when_no_span_or_text(self):
        soup = _soup("<div><h3>1.1.) Label</h3><p>Only in para</p></div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) == "Only in para"

    def test_multiple_p_joined_with_space(self):
        soup = _soup("<div><h3>1.1.) Label</h3><p>First</p><p>Second</p></div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) == "First Second"

    def test_returns_none_when_no_value_source(self):
        soup = _soup("<div><h3>1.1.) Label with no value</h3><h3>Next</h3></div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) is None


# ===========================================================================
# build_notice_sections_model
# ===========================================================================

# ---------------------------------------------------------------------------
# Minimal profile fixtures — deliberately not loaded from real JSON files
# so Tier 2 tests remain independent of Tier 3 (profile integrity).
# ---------------------------------------------------------------------------

_CORE_PROFILE = {
    "1.1": {"col_name": "section_1_1", "data_model": "core"},
    "1.2": {"col_name": "section_1_2", "data_model": "core"},
}

_CLIENT_PROFILE = {
    "1.2": {"col_name": "section_1_2", "data_model": "client"},
    "1.3": {"col_name": "section_1_3", "data_model": "client"},
}

_PART_CORE_PROFILE = {
    "4.1": {"col_name": "section_4_1", "data_model": "part.core"},
    "4.2": {"col_name": "section_4_2", "data_model": "part.core"},
}

_PART_SUB_PROFILE = {
    "4.1": {"col_name": "section_4_1", "data_model": "part.core"},
    "6.1": {"col_name": "section_6_1", "data_model": "part.part"},
}


def _make_soup(body_html: str) -> BeautifulSoup:
    return BeautifulSoup(f"<html><body>{body_html}</body></html>", "html.parser")


class TestBuildNoticeSectionsModel:

    # --- Core sections -------------------------------------------------------

    def test_single_core_section(self):
        soup = _make_soup(
            '<h3>1.1.) Rola <span class="normal">Samodzielnie</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert result["core"]["section_1_1"] == "Samodzielnie"

    def test_multiple_core_sections(self):
        soup = _make_soup(
            '<h3>1.1.) Rola <span class="normal">Val A</span></h3>'
            '<h3>1.2.) Nazwa <span class="normal">Val B</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert result["core"]["section_1_1"] == "Val A"
        assert result["core"]["section_1_2"] == "Val B"

    def test_section_not_in_profile_is_skipped(self):
        soup = _make_soup(
            '<h3>1.1.) Known <span class="normal">Known</span></h3>'
            '<h3>9.9.) Unknown <span class="normal">Ignored</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert "section_9_9" not in result["core"]
        assert result["core"]["section_1_1"] == "Known"

    def test_section_with_no_value_is_skipped(self):
        soup = _make_soup(
            '<h3>1.1.) Rola <span class="normal">Present</span></h3>'
            "<h3>1.2.) Empty — no value source</h3>"
        )
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert "section_1_2" not in result["core"]

    def test_duplicate_core_section_raises(self):
        soup = _make_soup(
            '<h3>1.1.) First <span class="normal">Val 1</span></h3>'
            '<h3>1.1.) Duplicate <span class="normal">Val 2</span></h3>'
        )
        with pytest.raises(ValueError, match="Duplicate core section"):
            build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})

    def test_empty_html_returns_empty_core(self):
        soup = _make_soup("")
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert result == {"core": {}}

    def test_unknown_notice_type_skips_all_sections(self):
        soup = _make_soup(
            '<h3>1.1.) Label <span class="normal">Value</span></h3>'
        )
        result = build_notice_sections_model(soup, "UnknownType", {"ContractNotice": _CORE_PROFILE})
        assert result == {"core": {}}

    def test_none_notice_dicts_skips_all_sections(self):
        soup = _make_soup(
            '<h3>1.1.) Label <span class="normal">Value</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", None)
        assert result == {"core": {}}

    # --- Single-level non-core model (client) --------------------------------

    def test_single_client_entry(self):
        soup = _make_soup(
            '<h3>1.2.) Nazwa <span class="normal">Szkoła</span></h3>'
            '<h3>1.3.) Oddział <span class="normal">Dział ZP</span></h3>'
        )
        result = build_notice_sections_model(soup, "CN", {"CN": _CLIENT_PROFILE})
        assert "client" in result
        assert len(result["client"]) == 1
        core = result["client"][0]["core"]
        assert core["section_1_2"] == "Szkoła"
        assert core["section_1_3"] == "Dział ZP"

    def test_multiple_client_entries(self):
        # Second client starts when section number resets (goes backwards)
        soup = _make_soup(
            '<h3>1.2.) Client 1 name <span class="normal">Buyer A</span></h3>'
            '<h3>1.3.) Client 1 dept <span class="normal">Dept A</span></h3>'
            '<h3>1.2.) Client 2 name <span class="normal">Buyer B</span></h3>'
            '<h3>1.3.) Client 2 dept <span class="normal">Dept B</span></h3>'
        )
        result = build_notice_sections_model(soup, "CN", {"CN": _CLIENT_PROFILE})
        assert len(result["client"]) == 2
        assert result["client"][0]["core"]["section_1_2"] == "Buyer A"
        assert result["client"][1]["core"]["section_1_2"] == "Buyer B"

    # --- Two-level model (part.core) -----------------------------------------

    def test_single_part_with_part_core_sections(self):
        soup = _make_soup(
            '<h3>4.1.) Part name <span class="normal">Część 1</span></h3>'
            '<h3>4.2.) Part value <span class="normal">42 000 PLN</span></h3>'
        )
        result = build_notice_sections_model(soup, "CN", {"CN": _PART_CORE_PROFILE})
        assert len(result["part"]) == 1
        core = result["part"][0]["core"]
        assert core["section_4_1"] == "Część 1"
        assert core["section_4_2"] == "42 000 PLN"

    def test_multiple_parts_split_on_section_number_reset(self):
        soup = _make_soup(
            '<h3>4.1.) Part 1 <span class="normal">P1</span></h3>'
            '<h3>4.2.) Part 1 val <span class="normal">V1</span></h3>'
            '<h3>4.1.) Part 2 <span class="normal">P2</span></h3>'
            '<h3>4.2.) Part 2 val <span class="normal">V2</span></h3>'
        )
        result = build_notice_sections_model(soup, "CN", {"CN": _PART_CORE_PROFILE})
        assert len(result["part"]) == 2
        assert result["part"][0]["core"]["section_4_1"] == "P1"
        assert result["part"][1]["core"]["section_4_1"] == "P2"

    # --- Sub-list within a part (part.part) ----------------------------------

    def test_part_with_nested_sub_items(self):
        # One part with part.core fields + two part.part sub-items
        soup = _make_soup(
            '<h3>4.1.) Part name <span class="normal">Część 1</span></h3>'
            '<h3>6.1.) Sub item 1 <span class="normal">Sub A</span></h3>'
            '<h3>6.1.) Sub item 2 <span class="normal">Sub B</span></h3>'
        )
        result = build_notice_sections_model(soup, "CN", {"CN": _PART_SUB_PROFILE})
        assert len(result["part"]) == 1
        part0 = result["part"][0]
        assert part0["core"]["section_4_1"] == "Część 1"
        assert len(part0["part"]) == 2
        assert part0["part"][0]["section_6_1"] == "Sub A"
        assert part0["part"][1]["section_6_1"] == "Sub B"

    # --- Value extraction fallbacks ------------------------------------------

    def test_p_tag_value_extracted_for_core_section(self):
        profile = {"4.2.2": {"col_name": "section_4_2_2", "data_model": "core"}}
        soup = _make_soup(
            "<h3>4.2.2.) Opis</h3>"
            "<p>Long description from a p tag</p>"
        )
        result = build_notice_sections_model(soup, "CN", {"CN": profile})
        assert result["core"]["section_4_2_2"] == "Long description from a p tag"

    def test_text_after_h3_extracted_for_core_section(self):
        profile = {"4.2.2": {"col_name": "section_4_2_2", "data_model": "core"}}
        soup = _make_soup("<h3>4.2.2.) Opis</h3>Inline text value<p>unrelated</p>")
        # Note: <p> breaks _text_after_h3 scan so "unrelated" won't be picked up
        # text_after returns "Inline text value"
        result = build_notice_sections_model(soup, "CN", {"CN": profile})
        assert result["core"]["section_4_2_2"] == "Inline text value"
