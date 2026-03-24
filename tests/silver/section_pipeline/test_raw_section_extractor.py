"""Unit tests for section_pipeline/raw_section_extractor.py.

Requires only BeautifulSoup (bs4) — no Spark, no file I/O.
HTML fixtures are inline strings kept minimal to test exactly one thing.
"""

from __future__ import annotations

import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent / "src"))

from procurement.silver.section_pipeline.raw_section_extractor import (
    _collect_full_block,
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

    def test_text_only_when_no_p_siblings(self):
        soup = _soup("<div><h3>1.1.) Label</h3>Text after h3<h3>Next</h3></div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3) == "Text after h3"

    def test_combines_text_and_p_siblings_with_newline(self):
        # 3.4.1-style: label text node followed by before/after <p> elements
        soup = _soup(
            "<div><h3>3.4.1.) Opis</h3>"
            "8.3.  Termin otwarcia ofert"
            "<p>Przed zmianą: 2025-01-07 09:30</p>"
            "<p>Po zmianie: 2025-01-21 09:30</p></div>"
        )
        h3 = soup.find("h3")
        result = extract_contract_notice_section_value(h3)
        assert result == (
            "8.3.  Termin otwarcia ofert\n"
            "Przed zmianą: 2025-01-07 09:30\n"
            "Po zmianie: 2025-01-21 09:30"
        )

    def test_combines_text_and_single_p(self):
        # Addition-only change: only "Przed zmianą:" paragraph, no after
        soup = _soup(
            "<div><h3>3.4.1.) Opis</h3>"
            "5.4.  Warunki udziału"
            "<p>Przed zmianą: stara treść</p></div>"
        )
        h3 = soup.find("h3")
        result = extract_contract_notice_section_value(h3)
        assert result == "5.4.  Warunki udziału\nPrzed zmianą: stara treść"

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
# _collect_full_block
# ===========================================================================


class TestCollectFullBlock:
    # Realistic NoticeUpdateConcession 3.4.1 layout
    _NUC_HTML = (
        "<div>"
        "<h3>3.4.1) Opis</h3>"
        " "
        "<h3>5.9.2.</h3>"
        " "
        '<p class="mb-0">Termin składania ofert</p>'
        " "
        "<p>Przed zmianą: 2025-02-21 09:00</p>"
        " "
        "<p>Po zmianie: 2025-02-25 09:00</p>"
        "</div>"
    )

    def test_collects_inner_h3_and_p_elements(self):
        soup = _soup(self._NUC_HTML)
        h3 = soup.find("h3")
        result = _collect_full_block(h3)
        assert result == (
            "5.9.2.\n"
            "Termin składania ofert\n"
            "Przed zmianą: 2025-02-21 09:00\n"
            "Po zmianie: 2025-02-25 09:00"
        )

    def test_stops_at_next_structural_h3(self):
        # Second 3.4.1 block must NOT bleed into the first block's result
        soup = _soup(
            "<div>"
            "<h3>3.4.1) Opis</h3>"
            "<h3>5.9.2.</h3>"
            "<p>Przed zmianą: old</p>"
            "<p>Po zmianie: new</p>"
            "<h3>3.4.1) Opis</h3>"   # next structural h3 — boundary
            "<h3>5.15.</h3>"
            "<p>Przed zmianą: other old</p>"
            "</div>"
        )
        h3 = soup.find("h3")
        result = _collect_full_block(h3)
        assert "other old" not in (result or "")
        assert result == "5.9.2.\nPrzed zmianą: old\nPo zmianie: new"

    def test_stops_at_h2(self):
        soup = _soup(
            "<div><h3>3.4.1) Opis</h3><h3>5.1.</h3><p>Val</p><h2>SEKCJA</h2><p>excluded</p></div>"
        )
        h3 = soup.find("h3")
        assert _collect_full_block(h3) == "5.1.\nVal"

    def test_returns_none_when_no_content(self):
        soup = _soup("<div><h3>3.4.1) Opis</h3><h3>3.4.1) Next structural</h3></div>")
        h3 = soup.find("h3")
        assert _collect_full_block(h3) is None

    def test_skips_whitespace_only_text_nodes(self):
        soup = _soup("<div><h3>3.4.1) Opis</h3>  \n  <h3>5.1.</h3>  <p>Val</p></div>")
        h3 = soup.find("h3")
        assert _collect_full_block(h3) == "5.1.\nVal"


# ===========================================================================
# extract_contract_notice_section_value — section_value_mode dispatch
# ===========================================================================


class TestExtractSectionValueModeDispatch:
    def test_full_block_mode_uses_collect_full_block(self):
        soup = _soup(
            "<div><h3>3.4.1) Opis</h3>"
            "<h3>5.9.2.</h3>"
            '<p class="mb-0">Termin składania ofert</p>'
            "<p>Przed zmianą: 2025-02-21 09:00</p>"
            "<p>Po zmianie: 2025-02-25 09:00</p></div>"
        )
        h3 = soup.find("h3")
        cfg = {"section_value_mode": "full_block", "col_name": "section_3_4_1", "data_model": "part.part"}
        result = extract_contract_notice_section_value(h3, cfg)
        assert result == (
            "5.9.2.\n"
            "Termin składania ofert\n"
            "Przed zmianą: 2025-02-21 09:00\n"
            "Po zmianie: 2025-02-25 09:00"
        )

    def test_standard_mode_explicit(self):
        soup = _soup("<div><h3>1.1.) Label</h3>Value text</div>")
        h3 = soup.find("h3")
        cfg = {"section_value_mode": "standard", "col_name": "section_1_1", "data_model": "core"}
        assert extract_contract_notice_section_value(h3, cfg) == "Value text"

    def test_no_mode_key_defaults_to_standard(self):
        soup = _soup("<div><h3>1.1.) Label</h3>Value text</div>")
        h3 = soup.find("h3")
        cfg = {"col_name": "section_1_1", "data_model": "core"}
        assert extract_contract_notice_section_value(h3, cfg) == "Value text"

    def test_none_cfg_defaults_to_standard(self):
        soup = _soup("<div><h3>1.1.) Label</h3>Value text</div>")
        h3 = soup.find("h3")
        assert extract_contract_notice_section_value(h3, None) == "Value text"

    def test_full_block_mode_passed_through_build_notice_sections_model(self):
        profile = {
            "3.4.1": {
                "col_name": "section_3_4_1",
                "data_model": "part.part",
                "section_value_mode": "full_block",
            }
        }
        soup = _make_soup(
            "<h3>3.4.1) Opis</h3>"
            "<h3>5.9.2.</h3>"
            "<p>Przed zmianą: old val</p>"
            "<p>Po zmianie: new val</p>"
        )
        result = build_notice_sections_model(soup, "NUC", {"NUC": profile})
        part_items = result.get("part", [{}])[0].get("part", [])
        assert len(part_items) == 1
        assert part_items[0]["section_3_4_1"] == "5.9.2.\nPrzed zmianą: old val\nPo zmianie: new val"


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

    def test_duplicate_core_section_quarantined(self):
        # Duplicate core section must NOT raise; first value is kept and
        # _parse_errors is populated so the orchestrator can route the row to quarantine.
        soup = _make_soup(
            '<h3>1.1.) First <span class="normal">Val 1</span></h3>'
            '<h3>1.1.) Duplicate <span class="normal">Val 2</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        # First occurrence wins
        assert result["core"]["section_1_1"] == "Val 1"
        # Error recorded for quarantine routing
        assert "_parse_errors" in result
        assert any("duplicate_core_section" in e for e in result["_parse_errors"])
        assert any("1.1" in e for e in result["_parse_errors"])

    def test_empty_html_returns_empty_core(self):
        soup = _make_soup("")
        result = build_notice_sections_model(soup, "ContractNotice", {"ContractNotice": _CORE_PROFILE})
        assert result == {"core": {}}

    def test_unknown_notice_type_skips_all_sections(self):
        soup = _make_soup(
            '<h3>1.1.) Label <span class="normal">Value</span></h3>'
        )
        result = build_notice_sections_model(soup, "UnknownType", {"ContractNotice": _CORE_PROFILE})
        assert result["core"] == {}
        # All sections are unknown when notice type has no registered profile
        assert "1.1" in result.get("_unknown_sections", [])

    def test_none_notice_dicts_skips_all_sections(self):
        soup = _make_soup(
            '<h3>1.1.) Label <span class="normal">Value</span></h3>'
        )
        result = build_notice_sections_model(soup, "ContractNotice", None)
        assert result["core"] == {}
        # All sections are unknown when no notice dicts are provided
        assert "1.1" in result.get("_unknown_sections", [])

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
        soup = _make_soup("<h3>4.2.2.) Opis</h3>Inline text value<p>para content</p>")
        # text node + <p> sibling → combined with newline
        result = build_notice_sections_model(soup, "CN", {"CN": profile})
        assert result["core"]["section_4_2_2"] == "Inline text value\npara content"


class TestBuildNoticeUnknownSections:
    """build_notice_sections_model tracks section numbers absent from the profile."""

    _PROFILE = {
        "1.1": {"col_name": "section_1_1", "data_model": "core", "section_header": "Field A"},
    }

    def _html(self, *sections: tuple[str, str]) -> str:
        parts = [f'<h3>{num}) <span class="normal">{val}</span></h3>' for num, val in sections]
        return "\n".join(parts)

    def test_no_unknown_sections_no_key(self):
        soup = BeautifulSoup(self._html(("1.1", "alpha")), "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert "_unknown_sections" not in result

    def test_single_unknown_section_added(self):
        soup = BeautifulSoup(self._html(("1.1", "alpha"), ("9.9", "mystery")), "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert "_unknown_sections" in result
        assert "9.9" in result["_unknown_sections"]

    def test_known_section_not_in_unknown_list(self):
        soup = BeautifulSoup(self._html(("1.1", "alpha"), ("9.9", "mystery")), "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert "1.1" not in result["_unknown_sections"]

    def test_multiple_unknown_sections_sorted(self):
        soup = BeautifulSoup(
            self._html(("9.2", "b"), ("1.1", "alpha"), ("9.1", "a")), "html.parser"
        )
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert result["_unknown_sections"] == ["9.1", "9.2"]

    def test_duplicate_unknown_section_appears_once(self):
        soup = BeautifulSoup(self._html(("9.9", "a"), ("9.9", "b")), "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert result["_unknown_sections"].count("9.9") == 1

    def test_empty_profile_all_sections_unknown(self):
        soup = BeautifulSoup(self._html(("1.1", "alpha"), ("2.2", "beta")), "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": {}})
        assert set(result["_unknown_sections"]) == {"1.1", "2.2"}

    def test_unknown_notice_type_all_sections_unknown(self):
        soup = BeautifulSoup(self._html(("1.1", "alpha")), "html.parser")
        result = build_notice_sections_model(soup, "UnknownType", {"TestNotice": self._PROFILE})
        assert "1.1" in result["_unknown_sections"]

    def test_section_without_value_not_counted(self):
        """A section present in HTML but with no parseable value is silently ignored."""
        html = '<h3>9.9) </h3>'  # no span.normal, no text
        soup = BeautifulSoup(html, "html.parser")
        result = build_notice_sections_model(soup, "TestNotice", {"TestNotice": self._PROFILE})
        assert "_unknown_sections" not in result
