"""Tests for BZP silver layer HTML parser."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.html_parser import parse_cpv_codes, parse_html
from procurement.silver.models import BzpNoticeSilver, EvalCriterion, HtmlExtracted

# --- Minimal HTML templates ---

MINIMAL_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJĄCY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">ul. Testowa 42</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">00-001</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL911 - Miasto Warszawa</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA IV – PRZEDMIOT ZAMÓWIENIA</h2>
<h3 class="mb-0">4.2.2.) Krótki opis przedmiotu zamówienia</h3>
<p class="mb-0">Dostawa sprzętu komputerowego dla szkoły.</p>
<h3 class="mb-0">Kryterium 1</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60</span></h3>
<h3 class="mb-0">Kryterium 2</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Gwarancja</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40</span></h3>
</main></body></html>"""

TENDER_RESULT_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJĄCY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">al. Wolności 10</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">30-500</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL213 - Miasto Kraków</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VIII – PROCEDURA</h2>
<h3 class="mb-0">8.2.) Wartość umowy/umowy ramowej: <span class="normal">465163,88 PLN</span></h3>
</main></body></html>"""

EMPTY_HTML = "<html><head></head><body></body></html>"


# --- Address extraction ---


class TestAddressExtraction:
    def test_street(self):
        r = parse_html(MINIMAL_HTML)
        assert r.ulica == "ul. Testowa 42"

    def test_postal_code(self):
        r = parse_html(MINIMAL_HTML)
        assert r.kod_pocztowy == "00-001"

    def test_nuts3_code_and_name(self):
        r = parse_html(MINIMAL_HTML)
        assert r.nuts3_code == "PL911"
        assert r.nuts3_name == "Miasto Warszawa"

    def test_missing_address_returns_none(self):
        r = parse_html(EMPTY_HTML)
        assert r.ulica is None
        assert r.kod_pocztowy is None
        assert r.nuts3_code is None


# --- Description extraction ---


class TestDescriptionExtraction:
    def test_description_extracted(self):
        r = parse_html(MINIMAL_HTML)
        assert "Dostawa sprzętu komputerowego" in r.opis

    def test_missing_description(self):
        r = parse_html(EMPTY_HTML)
        assert r.opis is None


# --- Bid criteria extraction ---


class TestCriteriaExtraction:
    def test_criteria_extracted(self):
        r = parse_html(MINIMAL_HTML)
        assert r.kryteria_oceny is not None
        assert len(r.kryteria_oceny) == 2

    def test_criteria_names_and_weights(self):
        r = parse_html(MINIMAL_HTML)
        names = {c.name for c in r.kryteria_oceny}
        assert names == {"Cena", "Gwarancja"}
        weights = {c.name: c.weight for c in r.kryteria_oceny}
        assert weights["Cena"] == 60
        assert weights["Gwarancja"] == 40

    def test_missing_criteria(self):
        r = parse_html(EMPTY_HTML)
        assert r.kryteria_oceny is None


# --- Contract value extraction ---


class TestContractValueExtraction:
    def test_value_extracted(self):
        r = parse_html(TENDER_RESULT_HTML)
        assert r.wartosc_umowy_pln == pytest.approx(465163.88)

    def test_missing_value(self):
        r = parse_html(MINIMAL_HTML)
        assert r.wartosc_umowy_pln is None


# --- CPV code parsing ---


class TestParseCpvCodes:
    def test_single_code(self):
        result = parse_cpv_codes("79710000-4 (Usługi ochroniarskie)")
        assert result == ["79710000-4 (Usługi ochroniarskie)"]

    def test_multiple_codes(self):
        raw = "45000000-7 (Roboty budowlane),90620000-9 (Usługi odśnieżania)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2
        assert result[0] == "45000000-7 (Roboty budowlane)"
        assert result[1] == "90620000-9 (Usługi odśnieżania)"

    def test_codes_with_commas_in_description(self):
        # Comma inside parenthetical description should NOT split
        raw = "45000000-7 (Roboty budowlane),71322000-1 (Usługi inżynierii projektowej w zakresie inżynierii lądowej i wodnej)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2


# --- General ---


class TestParseHtml:
    def test_returns_html_extracted_model(self):
        r = parse_html(MINIMAL_HTML)
        assert isinstance(r, HtmlExtracted)

    def test_empty_html_no_crash(self):
        r = parse_html(EMPTY_HTML)
        assert isinstance(r, HtmlExtracted)
        assert r.ulica is None
