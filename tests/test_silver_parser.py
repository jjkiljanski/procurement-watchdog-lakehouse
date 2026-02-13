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
<h2 class="bg-light p-3 mt-4">SEKCJA IV – PRZEDMIOT ZAMÓWIENIA</h2>
<h3 class="mb-0">4.3.) Wartość zamówienia: <span class="normal">500000,00                PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VI OFERTY</h2>
<h3 class="mb-0">6.2.) Cena lub koszt oferty z najniższą ceną: <span class="normal">400000 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena lub koszt oferty z najwyższą ceną: <span class="normal">550000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">465163,88 PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VIII – UMOWA</h2>
<h3 class="mb-0">8.2.) Wartość umowy/umowy ramowej: <span class="normal">465163,88 PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV – PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE</h2>
<h3 class="mb-0">4.4.) Wartość umowy: <span class="normal">24280,56                        PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA V PRZEBIEG REALIZACJI UMOWY</h2>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">PLN</span></h3>
<h3 class="mb-0">5.5.) Łączna wartość wynagrodzenia: <span class="normal">20000,00                PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_EUR_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.4.) Wartość umowy: <span class="normal">39127,53                EUR</span></h3>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">EUR</span></h3>
<h3 class="mb-0">5.5.) Łączna wartość wynagrodzenia: <span class="normal">39127,53                EUR</span></h3>
</main></body></html>"""

AGREEMENT_UPDATE_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.4.) Wartość umowy/umowy ramowej: <span class="normal">996945,00                PLN</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.5.) Wartość zamówienia: <span class="normal">2509756,10                    \xa0PLN</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.5.) Łączna wartość: <span class="normal">35946524,88                    PLN</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_VAT_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.6.) Wartość zamówienia (bez VAT): <span class="normal">570513,92                PLN</span></h3>
</main></body></html>"""

SMALL_CONTRACT_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.4.) Wartość: <span class="normal">25399,50</span></h3>
<h3 class="mb-0">3.5.) Kod waluty: <span class="normal">PLN</span></h3>
</main></body></html>"""

TENDER_RESULT_ENRICHMENT_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA I - ZAMAWIAJĄCY</h2>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">al. Wolności 10</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">30-500</span></h3>
<h3 class="mb-0">1.5.6.) Lokalizacja NUTS 3: <span class="normal">PL213 - Miasto Kraków</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VII WYKONAWCA, KTÓREMU UDZIELONO ZAMÓWIENIA</h2>
<h3 class="mb-0">7.1.) Czy zamówienie zostało udzielone wykonawcom wspólnie ubiegającym się o udzielenie zamówienia: <span class="normal">Nie</span></h3>
<h3 class="mb-0">7.2.) Wielkość przedsiębiorstwa wykonawcy: <span class="normal">Mały przedsiębiorca</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA VIII UMOWA</h2>
<h3 class="mb-0">8.2.) Wartość umowy/umowy ramowej: <span class="normal">465163,88 PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_DETAILS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV – PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE</h2>
<h3 class="mb-0">4.1.) Data zawarcia umowy: <span class="normal">2025-03-15</span></h3>
<h3 class="mb-0">4.2.) Okres realizacji zamówienia: </h3>
56 dni
<h3 class="mb-0">4.4.) Wartość umowy: <span class="normal">24280,56                        PLN</span></h3>
<h2 class="bg-light p-3 mt-4">SEKCJA V PRZEBIEG REALIZACJI UMOWY</h2>
<h3 class="mb-0">5.1.) Czy umowa została wykonana: <span class="normal">Tak</span></h3>
<h3 class="mb-0">5.2.) Termin wykonania umowy: <span class="normal">2025-05-10</span></h3>
<h3 class="mb-0">5.3.) Czy umowę wykonano w pierwotnie określonym terminie: <span class="normal">Tak</span></h3>
<h3 class="mb-0">5.4.1.) Liczba zmian: <span class="normal">0</span></h3>
<h3 class="mb-0">5.4.7.) Kod waluty: <span class="normal">PLN</span></h3>
<h3 class="mb-0">5.5.) Łączna wartość wynagrodzenia: <span class="normal">20000,00                PLN</span></h3>
<h3 class="mb-0">5.6.) Czy umowa została wykonana należycie: <span class="normal">Tak</span></h3>
</main></body></html>"""

NOTICE_UPDATE_SINGLE_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA III ZMIANA OGŁOSZENIA</h2>
<h3 class="mb-0">3.2.) Numer zmienianego ogłoszenia w BZP: <span class="normal">2025/BZP 00512345/01</span></h3>
<h3 class="mb-0">3.3.) Identyfikator ostatniej wersji zmienianego ogłoszenia: <span class="normal">01</span></h3>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogłoszenia: </h3>
SEKCJA VIII - PROCEDURA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, który należy dodać lub zmienić w ogłoszeniu: </h3>
<p class="mb-0">Przed zmianą:</p>
<p class="mb-0">Termin składania ofert: 2025-11-15 10:00</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Termin składania ofert: 2025-11-22 10:00</p>
</main></body></html>"""

NOTICE_UPDATE_MULTI_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA III ZMIANA OGŁOSZENIA</h2>
<h3 class="mb-0">3.2.) Numer zmienianego ogłoszenia w BZP: <span class="normal">2025/BZP 00512345/01</span></h3>
<h3 class="mb-0">3.3.) Identyfikator ostatniej wersji zmienianego ogłoszenia: <span class="normal">02</span></h3>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogłoszenia: </h3>
SEKCJA IV – PRZEDMIOT ZAMÓWIENIA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, który należy dodać lub zmienić w ogłoszeniu: </h3>
<p class="mb-0">Przed zmianą:</p>
<p class="mb-0">Opis A stary</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Opis A nowy</p>
<h3 class="mb-0">3.4.) Identyfikator sekcji zmienianego ogłoszenia: </h3>
SEKCJA VIII - PROCEDURA
<h3 class="mb-0">3.4.1.) Opis zmiany, w tym tekst, który należy dodać lub zmienić w ogłoszeniu: </h3>
<p class="mb-0">Przed zmianą:</p>
<p class="mb-0">Termin: 2025-11-15</p>
<p class="mb-0">Po zmianie:</p>
<p class="mb-0">Termin: 2025-11-22</p>
</main></body></html>"""

TENDER_RESULT_MULTI_LOT_HTML = """\
<html><head></head><body><main>
<h2>SEKCJA IV</h2>
<h3 class="mb-0">Część nr 1</h3>
<h3 class="mb-0">4.3.) Wartość zamówienia: <span class="normal">100000,00 PLN</span></h3>
<h3 class="mb-0">6.2.) Cena oferty najniższej: <span class="normal">90000,00 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena oferty najwyższej: <span class="normal">120000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">95000,00 PLN</span></h3>
<h3 class="mb-0">8.2.) Wartość umowy: <span class="normal">95000,00 PLN</span></h3>
<h3 class="mb-0">Część nr 2</h3>
<h3 class="mb-0">4.3.) Wartość zamówienia: <span class="normal">200000,00 PLN</span></h3>
<h3 class="mb-0">6.2.) Cena oferty najniższej: <span class="normal">180000,00 PLN</span></h3>
<h3 class="mb-0">6.3.) Cena oferty najwyższej: <span class="normal">230000,00 PLN</span></h3>
<h3 class="mb-0">6.4.) Cena oferty wykonawcy: <span class="normal">190000,00 PLN</span></h3>
<h3 class="mb-0">8.2.) Wartość umowy: <span class="normal">190000,00 PLN</span></h3>
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


# --- Detail extraction: TenderResultEnrichment ---


class TestTenderResultEnrichment:
    def test_joint_bidders_false(self):
        r = parse_html(TENDER_RESULT_ENRICHMENT_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_enrichment is not None
        assert r.tender_result_enrichment.joint_bidders is False

    def test_contractor_size(self):
        r = parse_html(TENDER_RESULT_ENRICHMENT_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_enrichment.contractor_size == "Mały przedsiębiorca"

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
        assert "Przed zmianą:" in desc
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
