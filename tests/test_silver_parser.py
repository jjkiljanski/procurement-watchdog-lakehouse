"""Tests for BZP silver layer HTML parser."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.html_parser import (
    _parse_pln_value,
    parse_cpv_codes,
    parse_html,
)
from procurement.silver.models import BzpNoticeSilver, EvalCriterion, HtmlExtracted

# --- Minimal HTML templates ---

MINIMAL_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJÄ„CY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">ul. Testowa 42</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">00-001</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL911 - Miasto Warszawa</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA</h2>
<h3 class="mb-0">4.2.2.) KrĂłtki opis przedmiotu zamĂłwienia</h3>
<p class="mb-0">Dostawa sprzÄ™tu komputerowego dla szkoĹ‚y.</p>
<h3 class="mb-0">Kryterium 1</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60</span></h3>
<h3 class="mb-0">Kryterium 2</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Gwarancja</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40</span></h3>
</main></body></html>"""

TENDER_RESULT_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJÄ„CY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">al. WolnoĹ›ci 10</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">30-500</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL213 - Miasto KrakĂłw</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA</h2>
<h3 class="mb-0">4.3.) WartoĹ›Ä‡ zamĂłwienia: <span class="normal">500000,00                PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VI OFERTY</h2>
<h3 class="mb-0">6.2.) Cena lub koszt oferty z najniĹĽszÄ… cenÄ…: <span class="normal">400000 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena lub koszt oferty z najwyĹĽszÄ… cenÄ…: <span class="normal">550000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">465163,88 PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VIII â€“ UMOWA</h2>
<h3 class="mb-0">8.2.) WartoĹ›Ä‡ umowy/umowy ramowej: <span class="normal">465163,88 PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV â€“ PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE</h2>
<h3 class="mb-0">4.4.) WartoĹ›Ä‡ umowy: <span class="normal">24280,56                        PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA V PRZEBIEG REALIZACJI UMOWY</h2>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">PLN</span></h3>
<h3 class="mb-0">5.5.) ĹÄ…czna wartoĹ›Ä‡ wynagrodzenia: <span class="normal">20000,00                PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_EUR_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.4.) WartoĹ›Ä‡ umowy: <span class="normal">39127,53                EUR</span></h3>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">EUR</span></h3>
<h3 class="mb-0">5.5.) ĹÄ…czna wartoĹ›Ä‡ wynagrodzenia: <span class="normal">39127,53                EUR</span></h3>
</main></body></html>"""

AGREEMENT_UPDATE_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.4.) WartoĹ›Ä‡ umowy/umowy ramowej: <span class="normal">996945,00                PLN</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.5.) WartoĹ›Ä‡ zamĂłwienia: <span class="normal">2509756,10                    \xa0PLN</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.5.) ĹÄ…czna wartoĹ›Ä‡: <span class="normal">35946524,88                    PLN</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_VAT_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.6.) WartoĹ›Ä‡ zamĂłwienia (bez VAT): <span class="normal">570513,92                PLN</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_PARTS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">Czesc nr 1</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Termin realizacji</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40</span></h3>
<h3 class="mb-0">4.3.10.) Zamawiajacy okresla aspekty spoleczne, srodowiskowe lub innowacyjne, zada etykiet lub stosuje rachunek kosztow cyklu zycia w odniesieniu do kryterium oceny ofert: <span class="normal">Tak</span></h3>
<h3 class="mb-0">Czesc nr 2</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">80</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Jakosc</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">20</span></h3>
<h3 class="mb-0">4.3.10.) Zamawiajacy okresla aspekty spoleczne, srodowiskowe lub innowacyjne, zada etykiet lub stosuje rachunek kosztow cyklu zycia w odniesieniu do kryterium oceny ofert: <span class="normal">Nie</span></h3>
</main></body></html>"""

OGLOSZENIE_DOTYCZY_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA II - INFORMACJE PODSTAWOWE</h2>
<h3 class="mb-0">2.1.) OgĹ‚oszenie dotyczy:</h3>
<p class="mb-0">ZamĂłwienia publicznego</p>
</main></body></html>"""

NON_TARGET_21_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA II - INFORMACJE PODSTAWOWE</h2>
<h3 class="mb-0">2.1.) Numer ogloszenia: <span class="normal">08de00d4-2cc0-5e06-d903-3900014f790f</span></h3>
<h3 class="mb-0">2.2.) Numer ogloszenia w BZP: <span class="normal">2025/BZP 00123456/01</span></h3>
</main></body></html>"""

NON_TARGET_21_ZMIANY_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA II - INFORMACJE PODSTAWOWE</h2>
<h3 class="mb-0">2.1.) Ogloszenie dotyczy zmiany:</h3>
<p class="mb-0">Numeru ogloszenia w BZP</p>
</main></body></html>"""

SMALL_CONTRACT_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.4.) WartoĹ›Ä‡: <span class="normal">25399,50</span></h3>
<h3 class="mb-0">3.5.) Kod waluty: <span class="normal">PLN</span></h3>
</main></body></html>"""

TENDER_RESULT_ENRICHMENT_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJÄ„CY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">al. WolnoĹ›ci 10</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">30-500</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL213 - Miasto KrakĂłw</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VII WYKONAWCA, KTĂ“REMU UDZIELONO ZAMĂ“WIENIA</h2>
<h3 class="mb-0">7.1.) Czy zamĂłwienie zostaĹ‚o udzielone wykonawcom wspĂłlnie ubiegajÄ…cym siÄ™ o udzielenie zamĂłwienia: <span class="normal">Nie</span></h3>
<h3 class="mb-0">7.2.) WielkoĹ›Ä‡ przedsiÄ™biorstwa wykonawcy: <span class="normal">MaĹ‚y przedsiÄ™biorca</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VIII UMOWA</h2>
<h3 class="mb-0">8.2.) WartoĹ›Ä‡ umowy/umowy ramowej: <span class="normal">465163,88 PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_DETAILS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV â€“ PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE</h2>
<h3 class="mb-0">4.1.) Data zawarcia umowy: <span class="normal">2025-03-15</span></h3>
<h3 class="mb-0">4.2.) Okres realizacji zamĂłwienia: </h3>
56 dni
<h3 class="mb-0">4.4.) WartoĹ›Ä‡ umowy: <span class="normal">24280,56                        PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA V PRZEBIEG REALIZACJI UMOWY</h2>
<h3 class="mb-0">5.1.) Czy umowa zostaĹ‚a wykonana: <span class="normal">Tak</span></h3>
<h3 class="mb-0">5.2.) Termin wykonania umowy: <span class="normal">2025-05-10</span></h3>
<h3 class="mb-0">5.3.) Czy umowÄ™ wykonano w pierwotnie okreĹ›lonym terminie: <span class="normal">Tak</span></h3>
<h3 class="mb-0">5.4.1.) Liczba zmian: <span class="normal">0</span></h3>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">PLN</span></h3>
<h3 class="mb-0">5.5.) ĹÄ…czna wartoĹ›Ä‡ wynagrodzenia: <span class="normal">20000,00                PLN</span></h3>
<h3 class="mb-0">5.6.) Czy umowa zostaĹ‚a wykonana naleĹĽycie: <span class="normal">Tak</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_LABEL_BASED_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.) Ulica: <span class="normal">ul. Kwiatowa 1</span></h3>
<h3 class="mb-0">4.2.) MiejscowoĹ›Ä‡: <span class="normal">Warszawa</span></h3>
<h3 class="mb-0">Data zawarcia umowy: <span class="normal">2025-01-01</span></h3>
<h3 class="mb-0">Okres realizacji umowy: <span class="normal">12 tygodni</span></h3>
<h3 class="mb-0">Czy umowa zostaĹ‚a wykonana: <span class="normal">Tak</span></h3>
<h3 class="mb-0">Termin wykonania umowy: <span class="normal">2025-04-01</span></h3>
<h3 class="mb-0">Czy umowÄ™ wykonano w pierwotnie okreĹ›lonym terminie: <span class="normal">Nie</span></h3>
<h3 class="mb-0">Liczba zmian: <span class="normal">2</span></h3>
<h3 class="mb-0">Czy umowa zostaĹ‚a wykonana naleĹĽycie: <span class="normal">Tak</span></h3>
</main></body></html>"""

NOTICE_UPDATE_SINGLE_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA III ZMIANA OGĹOSZENIA</h2>
<h3 class="mb-0">3.2.) Numer zmienianego ogĹ‚oszenia w BZP: <span class="normal">2025/BZP 00512345/01</span></h3>
<h3 class="mb-0">3.3.) Identyfikator ostatniej wersji zmienianego ogĹ‚oszenia: <span class="normal">01</span></h3>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogĹ‚oszenia: </h3>
SEKCJA VIII - PROCEDURA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, ktĂłry naleĹĽy dodaÄ‡ lub zmieniÄ‡ w ogĹ‚oszeniu: </h3>
<p class="mb-0">Przed zmianÄ…:</p>
<p class="mb-0">Termin skĹ‚adania ofert: 2025-11-15 10:00</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Termin skĹ‚adania ofert: 2025-11-22 10:00</p>
</main></body></html>"""

NOTICE_UPDATE_MULTI_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA III ZMIANA OGĹOSZENIA</h2>
<h3 class="mb-0">3.2.) Numer zmienianego ogĹ‚oszenia w BZP: <span class="normal">2025/BZP 00512345/01</span></h3>
<h3 class="mb-0">3.3.) Identyfikator ostatniej wersji zmienianego ogĹ‚oszenia: <span class="normal">02</span></h3>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogĹ‚oszenia: </h3>
SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, ktĂłry naleĹĽy dodaÄ‡ lub zmieniÄ‡ w ogĹ‚oszeniu: </h3>
<p class="mb-0">Przed zmianÄ…:</p>
<p class="mb-0">Opis A stary</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Opis A nowy</p>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogĹ‚oszenia: </h3>
SEKCJA VIII - PROCEDURA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, ktĂłry naleĹĽy dodaÄ‡ lub zmieniÄ‡ w ogĹ‚oszeniu: </h3>
<p class="mb-0">Przed zmianÄ…:</p>
<p class="mb-0">Termin: 2025-11-15</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Termin: 2025-11-22</p>
</main></body></html>"""

TENDER_RESULT_MULTI_LOT_HTML = """\
<html><head></head><body><main>
<h2>SEKCJA IV</h2>
<h3 class="mb-0">CzÄ™Ĺ›Ä‡ nr 1</h3>
<h3 class="mb-0">4.3.) WartoĹ›Ä‡ zamĂłwienia: <span class="normal">100000,00 PLN</span></h3>
<h3 class="mb-0">6.2.) Cena oferty najniĹĽszej: <span class="normal">90000,00 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena oferty najwyĹĽszej: <span class="normal">120000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">95000,00 PLN</span></h3>
<h3 class="mb-0">8.2.) WartoĹ›Ä‡ umowy: <span class="normal">95000,00 PLN</span></h3>
<h3 class="mb-0">CzÄ™Ĺ›Ä‡ nr 2</h3>
<h3 class="mb-0">4.3.) WartoĹ›Ä‡ zamĂłwienia: <span class="normal">200000,00 PLN</span></h3>
<h3 class="mb-0">6.2.) Cena oferty najniĹĽszej: <span class="normal">180000,00 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena oferty najwyĹĽszej: <span class="normal">230000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">190000,00 PLN</span></h3>
<h3 class="mb-0">8.2.) WartoĹ›Ä‡ umowy: <span class="normal">190000,00 PLN</span></h3>
</main></body></html>"""

EMPTY_HTML = "<html><head></head><body></body></html>"


# --- Number format parsing ---


class TestParsePlnValue:
    def test_comma_decimal(self):
        assert _parse_pln_value("465163,88 PLN") == pytest.approx(465163.88)

    def test_integer(self):
        assert _parse_pln_value("295590 PLN") == pytest.approx(295590.0)

    def test_bare_number(self):
        assert _parse_pln_value("25399,50") == pytest.approx(25399.50)

    def test_space_thousands(self):
        assert _parse_pln_value("1 000 000,00 PLN") == pytest.approx(1000000.0)

    def test_dot_thousands_comma_decimal(self):
        assert _parse_pln_value("130.000,00 PLN") == pytest.approx(130000.0)

    def test_nbsp_before_pln(self):
        assert _parse_pln_value("2509756,10\xa0PLN") == pytest.approx(2509756.10)

    def test_trailing_spaces(self):
        assert _parse_pln_value("24280,56                        PLN") == pytest.approx(24280.56)

    def test_eur(self):
        assert _parse_pln_value("39127,53                EUR") == pytest.approx(39127.53)

    def test_none(self):
        assert _parse_pln_value(None) is None

    def test_empty(self):
        assert _parse_pln_value("") is None


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
        assert "Dostawa sprzÄ™tu komputerowego" in r.opis

    def test_missing_description(self):
        r = parse_html(EMPTY_HTML)
        assert r.opis is None


# --- Field 2.1 extraction ---


class TestOgloszenieDotyczyExtraction:
    def test_extracts_ogloszenie_dotyczy(self):
        r = parse_html(OGLOSZENIE_DOTYCZY_HTML)
        assert r.ogloszenie_dotyczy == "ZamĂłwienia publicznego"

    def test_missing_ogloszenie_dotyczy_returns_none(self):
        r = parse_html(EMPTY_HTML)
        assert r.ogloszenie_dotyczy is None

    def test_non_target_field_21_does_not_match(self):
        r = parse_html(NON_TARGET_21_HTML)
        assert r.ogloszenie_dotyczy is None

    def test_non_target_dotyczy_zmiany_does_not_match(self):
        r = parse_html(NON_TARGET_21_ZMIANY_HTML)
        assert r.ogloszenie_dotyczy is None


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


# --- Value extraction: TenderResultNotice ---


class TestTenderResultValues:
    def test_contract_value(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(465163.88)

    def test_estimated_value(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.estimated_value == pytest.approx(500000.0)

    def test_bid_values(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.lowest_bid == pytest.approx(400000.0)
        assert r.values.highest_bid == pytest.approx(550000.0)
        assert r.values.winning_bid == pytest.approx(465163.88)

    def test_default_currency(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.currency == "PLN"

    def test_legacy_fallback_without_notice_type(self):
        r = parse_html(TENDER_RESULT_HTML)
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(465163.88)

    def test_multi_lot_extraction(self):
        r = parse_html(TENDER_RESULT_MULTI_LOT_HTML, notice_type="TenderResultNotice")
        assert r.lots is not None
        assert len(r.lots) == 2
        assert r.lots[0].lot_id == "1"
        assert r.lots[1].lot_id == "2"
        assert r.lots[0].winning_bid == pytest.approx(95000.0)
        assert r.lots[1].winning_bid == pytest.approx(190000.0)
        assert r.values is None

    def test_cancellation_creates_status_lots(self):
        r = parse_html(EMPTY_HTML, notice_type="TenderResultNotice", procedure_result="uniewaznienie;nieRozstrzygnieto")
        assert r.lots is not None
        assert len(r.lots) == 2
        assert r.lots[0].winner == "uniewaznienie"
        assert r.lots[1].winner == "nieRozstrzygnieto"


# --- Value extraction: ContractPerformingNotice ---


class TestContractPerformingValues:
    def test_contract_value(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(24280.56)

    def test_total_paid(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values.total_paid == pytest.approx(20000.0)

    def test_currency_pln(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values.currency == "PLN"

    def test_currency_eur(self):
        r = parse_html(CONTRACT_PERFORMING_EUR_HTML, notice_type="ContractPerformingNotice")
        assert r.values.currency == "EUR"
        assert r.values.contract_value == pytest.approx(39127.53)


# --- Value extraction: ContractNotice ---


class TestContractNoticeValues:
    def test_estimated_value_from_415(self):
        r = parse_html(CONTRACT_NOTICE_HTML, notice_type="ContractNotice")
        assert r.values is not None
        assert r.values.estimated_value == pytest.approx(35946524.88)

    def test_fallback_to_416(self):
        r = parse_html(CONTRACT_NOTICE_VAT_HTML, notice_type="ContractNotice")
        assert r.values is not None
        assert r.values.estimated_value == pytest.approx(570513.92)

    def test_no_value_returns_none(self):
        r = parse_html(MINIMAL_HTML, notice_type="ContractNotice")
        assert r.values is None

    def test_extracts_4310_top_level(self):
        r = parse_html(CONTRACT_NOTICE_PARTS_HTML, notice_type="ContractNotice")
        assert r.criteria_aspects_4310 in {"Tak", "Nie"}
        assert r.criteria_aspects_4310_flag in {True, False}

    def test_extracts_contract_notice_parts(self):
        r = parse_html(CONTRACT_NOTICE_PARTS_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        assert len(r.contract_notice_parts) == 2

        part1 = r.contract_notice_parts[0]
        part2 = r.contract_notice_parts[1]
        assert part1.part_id == "1"
        assert part2.part_id == "2"
        assert part1.criteria_aspects_4310 == "Tak"
        assert part1.criteria_aspects_4310_flag is True
        assert part2.criteria_aspects_4310 == "Nie"
        assert part2.criteria_aspects_4310_flag is False

        p1_weights = {c.name: c.weight for c in (part1.kryteria_oceny or [])}
        p2_weights = {c.name: c.weight for c in (part2.kryteria_oceny or [])}
        assert p1_weights["Cena"] == 60
        assert p1_weights["Termin realizacji"] == 40
        assert p2_weights["Cena"] == 80
        assert p2_weights["Jakosc"] == 20


# --- Value extraction: AgreementUpdateNotice ---


class TestAgreementUpdateValues:
    def test_contract_value(self):
        r = parse_html(AGREEMENT_UPDATE_HTML, notice_type="AgreementUpdateNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(996945.0)


# --- Value extraction: AgreementIntentionNotice ---


class TestAgreementIntentionValues:
    def test_estimated_value(self):
        r = parse_html(AGREEMENT_INTENTION_HTML, notice_type="AgreementIntentionNotice")
        assert r.values is not None
        assert r.values.estimated_value == pytest.approx(2509756.10)


# --- Value extraction: SmallContractNotice ---


class TestSmallContractValues:
    def test_contract_value_bare_number(self):
        r = parse_html(SMALL_CONTRACT_HTML, notice_type="SmallContractNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(25399.50)

    def test_currency_from_separate_field(self):
        r = parse_html(SMALL_CONTRACT_HTML, notice_type="SmallContractNotice")
        assert r.values.currency == "PLN"


# --- Value extraction: unsupported types ---


class TestUnsupportedTypeValues:
    def test_notice_update_no_values(self):
        r = parse_html(EMPTY_HTML, notice_type="NoticeUpdateNotice")
        assert r.values is None

    def test_empty_html_no_values(self):
        r = parse_html(EMPTY_HTML)
        assert r.values is None


# --- CPV code parsing ---


class TestParseCpvCodes:
    def test_single_code(self):
        result = parse_cpv_codes("79710000-4 (UsĹ‚ugi ochroniarskie)")
        assert result == ["79710000-4 (UsĹ‚ugi ochroniarskie)"]

    def test_multiple_codes(self):
        raw = "45000000-7 (Roboty budowlane),90620000-9 (UsĹ‚ugi odĹ›nieĹĽania)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2
        assert result[0] == "45000000-7 (Roboty budowlane)"
        assert result[1] == "90620000-9 (UsĹ‚ugi odĹ›nieĹĽania)"

    def test_codes_with_commas_in_description(self):
        # Comma inside parenthetical description should NOT split
        raw = "45000000-7 (Roboty budowlane),71322000-1 (UsĹ‚ugi inĹĽynierii projektowej w zakresie inĹĽynierii lÄ…dowej i wodnej)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2


# --- General ---


# --- Detail extraction: TenderResultEnrichment ---


class TestTenderResultEnrichment:
    def test_joint_bidders_false(self):
        r = parse_html(TENDER_RESULT_ENRICHMENT_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_enrichment is not None
        assert r.tender_result_enrichment.joint_bidders is False

    def test_contractor_size(self):
        r = parse_html(TENDER_RESULT_ENRICHMENT_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_enrichment.contractor_size == "MaĹ‚y przedsiÄ™biorca"

    def test_values_still_extracted(self):
        r = parse_html(TENDER_RESULT_ENRICHMENT_HTML, notice_type="TenderResultNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(465163.88)

    def test_no_enrichment_for_other_types(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.tender_result_enrichment is None

    def test_missing_enrichment_returns_none(self):
        r = parse_html(EMPTY_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_enrichment is None


# --- Detail extraction: ContractExecution ---


class TestContractExecution:
    def test_contract_date(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution is not None
        assert r.contract_execution.contract_date == "2025-03-15"

    def test_execution_period_plain_text(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.execution_period == "56 dni"

    def test_contract_executed_true(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.contract_executed is True

    def test_execution_end_date(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.execution_end_date == "2025-05-10"

    def test_executed_on_time_true(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.executed_on_time is True

    def test_num_changes_zero(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.num_changes == 0

    def test_executed_properly_true(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution.executed_properly is True

    def test_values_still_extracted(self):
        r = parse_html(CONTRACT_PERFORMING_DETAILS_HTML, notice_type="ContractPerformingNotice")
        assert r.values is not None
        assert r.values.contract_value == pytest.approx(24280.56)

    def test_no_execution_for_other_types(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.contract_execution is None

    def test_missing_execution_returns_none(self):
        r = parse_html(EMPTY_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution is None

    def test_label_based_execution_extraction(self):
        r = parse_html(CONTRACT_PERFORMING_LABEL_BASED_HTML, notice_type="ContractPerformingNotice")
        assert r.contract_execution is not None
        assert r.contract_execution.contract_date == "2025-01-01"
        assert r.contract_execution.execution_period == "12 tygodni"
        assert r.contract_execution.executed_on_time is False
        assert r.contract_execution.num_changes == 2


# --- Detail extraction: NoticeChange ---


class TestNoticeChange:
    def test_changed_notice_number(self):
        r = parse_html(NOTICE_UPDATE_SINGLE_HTML, notice_type="NoticeUpdateNotice")
        assert r.notice_change is not None
        assert r.notice_change.changed_notice_number == "2025/BZP 00512345/01"

    def test_changed_notice_version(self):
        r = parse_html(NOTICE_UPDATE_SINGLE_HTML, notice_type="NoticeUpdateNotice")
        assert r.notice_change.changed_notice_version == "01"

    def test_single_change_section(self):
        r = parse_html(NOTICE_UPDATE_SINGLE_HTML, notice_type="NoticeUpdateNotice")
        assert r.notice_change.changes is not None
        assert len(r.notice_change.changes) == 1
        assert "SEKCJA VIII" in r.notice_change.changes[0].changed_section

    def test_single_change_description(self):
        r = parse_html(NOTICE_UPDATE_SINGLE_HTML, notice_type="NoticeUpdateNotice")
        desc = r.notice_change.changes[0].change_description
        assert "Przed zmianÄ…:" in desc
        assert "Po zmianie:" in desc
        assert "2025-11-22" in desc

    def test_multiple_changes(self):
        r = parse_html(NOTICE_UPDATE_MULTI_HTML, notice_type="NoticeUpdateNotice")
        assert r.notice_change.changes is not None
        assert len(r.notice_change.changes) == 2

    def test_multiple_changes_sections(self):
        r = parse_html(NOTICE_UPDATE_MULTI_HTML, notice_type="NoticeUpdateNotice")
        sections = [c.changed_section for c in r.notice_change.changes]
        assert any("SEKCJA IV" in s for s in sections)
        assert any("SEKCJA VIII" in s for s in sections)

    def test_no_change_for_other_types(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.notice_change is None

    def test_missing_change_returns_none(self):
        r = parse_html(EMPTY_HTML, notice_type="NoticeUpdateNotice")
        assert r.notice_change is None


# --- Helper: _parse_tak_nie ---


class TestParseTakNie:
    def test_tak(self):
        from procurement.silver.html_parser import _parse_tak_nie

        assert _parse_tak_nie("Tak") is True

    def test_nie(self):
        from procurement.silver.html_parser import _parse_tak_nie

        assert _parse_tak_nie("Nie") is False

    def test_none(self):
        from procurement.silver.html_parser import _parse_tak_nie

        assert _parse_tak_nie(None) is None

    def test_whitespace(self):
        from procurement.silver.html_parser import _parse_tak_nie

        assert _parse_tak_nie("  Tak  ") is True

    def test_unexpected_value(self):
        from procurement.silver.html_parser import _parse_tak_nie

        assert _parse_tak_nie("Maybe") is None


# --- General ---


class TestParseHtml:
    def test_returns_html_extracted_model(self):
        r = parse_html(MINIMAL_HTML)
        assert isinstance(r, HtmlExtracted)

    def test_empty_html_no_crash(self):
        r = parse_html(EMPTY_HTML)
        assert isinstance(r, HtmlExtracted)
        assert r.ulica is None


class TestSilverDerivedHelpers:
    def test_execution_duration_parsing(self):
        from procurement.silver.spark_transforms import _extract_execution_duration_days

        assert _extract_execution_duration_days("56 dni") == 56
        assert _extract_execution_duration_days("12 tygodni") == 84
        assert _extract_execution_duration_days("2 miesiÄ…ce") == 60

    def test_criteria_weight_extraction(self):
        from procurement.silver.spark_transforms import _criteria_summary

        num, price, non_price = _criteria_summary(
            [
                {"name": "Cena", "weight": 60},
                {"name": "Gwarancja", "weight": 40},
            ]
        )
        assert num == 2
        assert price == 60
        assert non_price == 40

    def test_update_delta_heuristics(self):
        from procurement.silver.spark_transforms import _classify_notice_change

        deadline_changed, criteria_changed, scope_changed = _classify_notice_change(
            [
                {
                    "changed_section": "SEKCJA VIII - PROCEDURA",
                    "change_description": "Termin skladania ofert przesunieto na 2025-11-22",
                }
            ]
        )
        assert deadline_changed is True
        assert criteria_changed is False
        assert scope_changed is False

