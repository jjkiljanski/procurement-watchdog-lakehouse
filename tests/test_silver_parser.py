"""Tests for BZP silver layer HTML parser."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.silver.html_parser import (
    _parse_pln_value,
    normalize_tender_result_contractors,
    parse_html_agreement_intention_light,
    parse_html_competition_light,
    parse_cpv_codes,
    parse_html,
    parse_html_contract_performing_light,
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

ADDRESS_14_ONLY_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">1.4.1.) Ulica: <span class="normal">ul. Test 14</span></h3>
<h3 class="mb-0">1.4.3.) Kod pocztowy: <span class="normal">11-222</span></h3>
<h3 class="mb-0">1.4.6.) Lokalizacja NUTS 3: <span class="normal">PL111 - Test NUTS</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.5.) WartoĹ›Ä‡ zamĂłwienia: <span class="normal">2509756,10                    \xa0PLN</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_DETAILS_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.1.) Przed wszczeciem postepowania przeprowadzono konsultacje rynkowe: <span class="normal">Tak</span></h3>
<h3 class="mb-0">3.5.) WartoĹ›Ä‡ zamowienia: <span class="normal">123456,78 PLN</span></h3>
<h3 class="mb-0">5.1.2.) Ulica: <span class="normal">ul. Rynek 5</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_ADDRESS_14_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">1.4.1.) Ulica: <span class="normal">ul. Dluga 10</span></h3>
<h3 class="mb-0">1.4.3.) Kod pocztowy: <span class="normal">12-345</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_ADDRESS_15_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">1.5.1.) Ulica: <span class="normal">ul. Tysiaclecia 5</span></h3>
<h3 class="mb-0">1.5.3.) Kod pocztowy: <span class="normal">97-500</span></h3>
</main></body></html>"""

AGREEMENT_INTENTION_ADDRESS_512_514_ONLY_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">5.1.2.) Ulica: <span class="normal">ul. Malicka 42</span></h3>
<h3 class="mb-0">5.1.4.) Kod pocztowy: <span class="normal">42-290</span></h3>
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
<h3 class="mb-0">4.2.6.) Główny kod CPV: <span class="normal">45000000-7 (Roboty budowlane)</span></h3>
<h3 class="mb-0">4.2.7.) Dodatkowy kod CPV: <span class="normal">45100000-8 (Przygotowanie terenu)</span></h3>
<h3 class="mb-0">4.2.10.) Okres realizacji zamówienia albo umowy ramowej: <span class="normal">do 2025-02-28</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Termin realizacji</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40</span></h3>
<h3 class="mb-0">4.3.10.) Zamawiajacy okresla aspekty spoleczne, srodowiskowe lub innowacyjne, zada etykiet lub stosuje rachunek kosztow cyklu zycia w odniesieniu do kryterium oceny ofert: <span class="normal">Tak</span></h3>
<h3 class="mb-0">Czesc nr 2</h3>
<h3 class="mb-0">4.2.6.) Główny kod CPV: <span class="normal">71000000-8 (Usługi architektoniczne)</span></h3>
<h3 class="mb-0">4.2.7.) Dodatkowy kod CPV: <span class="normal">71200000-0 (Usługi architektoniczne i podobne)</span></h3>
<h3 class="mb-0">4.2.10.) Okres realizacji zamówienia albo umowy ramowej: <span class="normal">12 miesięcy</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">80</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Jakosc</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">20</span></h3>
<h3 class="mb-0">4.3.10.) Zamawiajacy okresla aspekty spoleczne, srodowiskowe lub innowacyjne, zada etykiet lub stosuje rachunek kosztow cyklu zycia w odniesieniu do kryterium oceny ofert: <span class="normal">Nie</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_PARTS_WITH_OPIS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">Czesc nr 1</h3>
<h3 class="mb-0">4.2.2.) KrĂłtki opis przedmiotu zamĂłwienia</h3>
<p class="mb-0">Opis czesci pierwszej.</p>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60</span></h3>
<h3 class="mb-0">4.3.10.) ... <span class="normal">Tak</span></h3>
<h3 class="mb-0">Czesc nr 2</h3>
<h3 class="mb-0">4.2.2.) KrĂłtki opis przedmiotu zamĂłwienia</h3>
<p class="mb-0">Opis czesci drugiej.</p>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">80</span></h3>
<h3 class="mb-0">4.3.10.) ... <span class="normal">Nie</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_PARTS_DECIMAL_WEIGHTS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">Czesc nr 1</h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60,00</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Termin realizacji</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40,00</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_SECONDARY_CPV_PARAGRAPHS_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">Czesc nr 1</h3>
<h3 class="mb-0">4.2.6.) Główny kod CPV: </h3>
<p class="mb-0">45233140-2 - Roboty drogowe</p>
<h3 class="mb-0">4.2.7.) Dodatkowy kod CPV: </h3>
<p class="mb-0">45100000-8 - Przygotowanie terenu pod budowę</p>
<p class="mb-0">34922100-7 - Oznakowanie drogowe</p>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">100</span></h3>
</main></body></html>"""

CONTRACT_NOTICE_SINGLE_PART_NO_HEADER_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">4.2.2.) Krótki opis przedmiotu zamówienia</h3>
<p class="mb-0">Opis jednej części.</p>
<h3 class="mb-0">4.2.6.) Główny kod CPV: <span class="normal">45200000-9 (Roboty budowlane)</span></h3>
<h3 class="mb-0">4.2.7.) Dodatkowy kod CPV: <span class="normal">45110000-1 (Roboty ziemne)</span></h3>
<h3 class="mb-0">4.2.10.) Okres realizacji zamówienia albo umowy ramowej: <span class="normal">do 2025-04-01</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">70</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Termin</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">30</span></h3>
<h3 class="mb-0">4.3.10.) ... <span class="normal">Tak</span></h3>
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

COMPETITION_NOTICE_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA VI - WARUNKI KONKURSU</h2>
<h3 class="mb-0">6.3.) Liczba prac konkursowych, ktore zostana nagrodzone: <span class="normal">3</span></h3>
<h3 class="mb-0">6.4.) Wartosc nagrod pienieznych lub rzeczowych: <span class="normal">50000,00 PLN</span></h3>
<h3 class="mb-0">6.5.1.) Wartosc zamowienia: <span class="normal">120000,00 PLN</span></h3>
<h3 class="mb-0">7.2.) Czy ustanowiono wymagania srodowiskowe lub spoleczne: <span class="normal">Tak</span></h3>
</main></body></html>"""

COMPETITION_NOTICE_NO_PRIZES_HTML = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA VI - WARUNKI KONKURSU</h2>
<h3 class="mb-0">6.3.) Liczba prac konkursowych, ktore zostana nagrodzone: <span class="normal">1</span></h3>
<h3 class="mb-0">6.5.1.) Wartosc zamowienia: <span class="normal">80000,00 PLN</span></h3>
</main></body></html>"""

COMPETITION_NOTICE_SUBMISSION_36_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.6.) Termin skladania wnioskow o dopuszczenie do udzialu w konkursie: <span class="normal">2025-06-10 15:00</span></h3>
</main></body></html>"""

COMPETITION_NOTICE_SUBMISSION_35_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">3.5.) Termin skladania opracowan studialnych: <span class="normal">2025-05-20 10:00</span></h3>
</main></body></html>"""

COMPETITION_RESULT_NOTICE_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">5.3.) Data zatwierdzenia rozstrzygniecia konkursu/uniewaznienia konkursu: <span class="normal">2025-06-30</span></h3>
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

CONTRACT_PERFORMING_MULTI_CONTRACTOR_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">4.3.1.) Nazwa (firma) wykonawcy, któremu udzielono zamówienia: <span class="normal">ABC Sp. z o.o.</span></h3>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">NIP: 639-10-05-245</span></h3>
<h3 class="mb-0">4.3.4.) Miejscowość: <span class="normal">Warszawa</span></h3>
<h3 class="mb-0">4.3.6.) Województwo: <span class="normal">mazowieckie</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
<h3 class="mb-0">4.3.1.) Nazwa (firma) wykonawcy, któremu udzielono zamówienia: <span class="normal">XYZ S.A.</span></h3>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">DE-ABC-987654</span></h3>
<h3 class="mb-0">4.3.4.) Miejscowość: <span class="normal">Krakow</span></h3>
<h3 class="mb-0">4.3.6.) Województwo: <span class="normal">malopolskie</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Niemcy</span></h3>
<h3 class="mb-0">4.4.) Wartość umowy: <span class="normal">12345,67 PLN</span></h3>
</main></body></html>"""

CONTRACT_PERFORMING_WITH_144_COLLISION_HTML = """\
<html><head></head><body><main>
<h3 class="mb-0">1.4.4.) Wojewodztwo: <span class="normal">wielkopolskie</span></h3>
<h3 class="mb-0">4.4.) Wartosc umowy: <span class="normal">624212,60 PLN</span></h3>
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

TENDER_RESULT_PART_DETAILS_HTML = """\
<html><head></head><body><main>
<h2>SEKCJA IV</h2>
<h3 class="mb-0">Czesc nr 1</h3>
<h3 class="mb-0">4.5.1.) Krótki opis przedmiotu zamówienia: <span class="normal">Opis 1</span></h3>
<h3 class="mb-0">4.5.3.) Główny kod CPV: <span class="normal">45000000-7 (Roboty budowlane)</span></h3>
<h3 class="mb-0">4.5.4.) Dodatkowy kod CPV: <span class="normal">45100000-8 (Przygotowanie terenu)</span></h3>
<h3 class="mb-0">4.3.) Wartość zamówienia: <span class="normal">100000,00 PLN</span></h3>
<h3 class="mb-0">Czesc nr 2</h3>
<h3 class="mb-0">4.5.1.) Krótki opis przedmiotu zamówienia: <span class="normal">Opis 2</span></h3>
<h3 class="mb-0">4.5.3.) Główny kod CPV: <span class="normal">71000000-8 (Usługi architektoniczne)</span></h3>
<h3 class="mb-0">4.5.4.) Dodatkowy kod CPV: <span class="normal">71200000-0 (Usługi architektoniczne i podobne)</span></h3>
<h3 class="mb-0">4.3.) Wartość zamówienia: <span class="normal">200000,00 PLN</span></h3>
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

    def test_label_based_street_for_contract_performing(self):
        r = parse_html(CONTRACT_PERFORMING_LABEL_BASED_HTML, notice_type="ContractPerformingNotice")
        assert r.ulica == "ul. Kwiatowa 1"

    def test_label_based_postal_missing_returns_none(self):
        r = parse_html(CONTRACT_PERFORMING_LABEL_BASED_HTML, notice_type="ContractPerformingNotice")
        assert r.kod_pocztowy is None

    def test_agreement_intention_address_uses_14xx_fields(self):
        r = parse_html(AGREEMENT_INTENTION_ADDRESS_14_HTML, notice_type="AgreementIntentionNotice")
        assert r.ulica == "ul. Dluga 10"
        assert r.kod_pocztowy == "12-345"

    def test_agreement_intention_address_uses_15xx_fields(self):
        r = parse_html(AGREEMENT_INTENTION_ADDRESS_15_HTML, notice_type="AgreementIntentionNotice")
        assert r.ulica == "ul. Tysiaclecia 5"
        assert r.kod_pocztowy == "97-500"

    def test_agreement_intention_light_address_uses_15xx_fields(self):
        r = parse_html_agreement_intention_light(AGREEMENT_INTENTION_ADDRESS_15_HTML)
        assert r["ulica"] == "ul. Tysiaclecia 5"
        assert r["kod_pocztowy"] == "97-500"

    def test_agreement_intention_light_address_uses_514_fallback(self):
        r = parse_html_agreement_intention_light(AGREEMENT_INTENTION_ADDRESS_512_514_ONLY_HTML)
        assert r["ulica"] == "ul. Malicka 42"
        assert r["kod_pocztowy"] == "42-290"

    def test_agreement_update_address_uses_14xx_fields(self):
        r = parse_html(ADDRESS_14_ONLY_HTML, notice_type="AgreementUpdateNotice")
        assert r.ulica == "ul. Test 14"
        assert r.kod_pocztowy == "11-222"

    def test_concession_address_uses_14xx_fields(self):
        r = parse_html(ADDRESS_14_ONLY_HTML, notice_type="ConcessionNotice")
        assert r.ulica == "ul. Test 14"
        assert r.kod_pocztowy == "11-222"

    def test_small_contract_address_uses_14xx_fields(self):
        r = parse_html(ADDRESS_14_ONLY_HTML, notice_type="SmallContractNotice")
        assert r.ulica == "ul. Test 14"
        assert r.kod_pocztowy == "11-222"


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
        names = {c.str for c in r.kryteria_oceny}
        assert names == {"Cena", "Gwarancja"}
        weights = {c.str: c.weight for c in r.kryteria_oceny}
        assert weights["Cena"] == 60
        assert weights["Gwarancja"] == 40

    def test_missing_criteria(self):
        r = parse_html(EMPTY_HTML)
        assert r.kryteria_oceny is None

    def test_decimal_weights(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Cena</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">60,00</span></h3>
<h3 class="mb-0">4.3.5.) Nazwa kryterium: <span class="normal">Jakosc</span></h3>
<h3 class="mb-0">4.3.6.) Waga: <span class="normal">40,00</span></h3>
</main></body></html>"""
        r = parse_html(html)
        weights = {c.str: c.weight for c in (r.kryteria_oceny or [])}
        assert weights["Cena"] == 60
        assert weights["Jakosc"] == 40


# --- Value extraction: TenderResultNotice ---


class TestTenderResultValues:
    def test_contract_value(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values is not None
        assert r.values.value_awarded_contract == pytest.approx(465163.88)

    def test_estimated_value(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.value_estimated_procurement == pytest.approx(500000.0)

    def test_bid_values(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.value_bid_lowest == pytest.approx(400000.0)
        assert r.values.value_bid_highest == pytest.approx(550000.0)
        assert r.values.value_winning_offer == pytest.approx(465163.88)

    def test_default_currency(self):
        r = parse_html(TENDER_RESULT_HTML, notice_type="TenderResultNotice")
        assert r.values.currency == "PLN"

    def test_legacy_fallback_without_notice_type(self):
        r = parse_html(TENDER_RESULT_HTML)
        assert r.values is not None
        assert r.values.value_awarded_contract == pytest.approx(465163.88)

    def test_multi_lot_extraction(self):
        r = parse_html(TENDER_RESULT_MULTI_LOT_HTML, notice_type="TenderResultNotice")
        assert r.lots is not None
        assert len(r.lots) == 2
        assert r.lots[0].lot_id == "1"
        assert r.lots[1].lot_id == "2"
        assert r.lots[0].value_winning_offer == pytest.approx(95000.0)
        assert r.lots[1].value_winning_offer == pytest.approx(190000.0)
        assert r.values is None

    def test_cancellation_creates_status_lots(self):
        r = parse_html(EMPTY_HTML, notice_type="TenderResultNotice", procedure_result="uniewaznienie;nieRozstrzygnieto")
        assert r.lots is not None
        assert len(r.lots) == 2
        assert r.lots[0].winner == "uniewaznienie"
        assert r.lots[1].winner == "nieRozstrzygnieto"

    def test_tender_result_parts_extracted(self):
        r = parse_html(TENDER_RESULT_PART_DETAILS_HTML, notice_type="TenderResultNotice")
        assert r.tender_result_parts is not None
        assert len(r.tender_result_parts) == 2
        p1 = r.tender_result_parts[0]
        p2 = r.tender_result_parts[1]
        assert p1.part_id == "1"
        assert p1.opis == "Opis 1"
        assert p1.mainCPV.startswith("45000000-7")
        assert p1.secondaryCPV and p1.secondaryCPV[0].startswith("45100000-8")
        assert p1.value_estimated_procurement == pytest.approx(100000.0)
        assert p2.part_id == "2"
        assert p2.value_estimated_procurement == pytest.approx(200000.0)

    def test_tender_result_parts_are_numbered_sequentially(self):
        html = """\
<html><head></head><body><main>
<h2 class="bg-light p-3 mt-4">SEKCJA IV - PRZEDMIOT ZAMOWIENIA</h2>
<h3 class="mb-0">Czesc nr A</h3>
<h3 class="mb-0">4.5.1.) Krotki opis przedmiotu zamowienia: <span class="normal">Opis A</span></h3>
<h3 class="mb-0">4.5.3.) Glowny kod CPV: <span class="normal">45000000-7</span></h3>
<h3 class="mb-0">4.3.) Wartosc zamowienia: <span class="normal">100000,00 PLN</span></h3>
<h3 class="mb-0">Czesc nr B</h3>
<h3 class="mb-0">4.5.1.) Krotki opis przedmiotu zamowienia: <span class="normal">Opis B</span></h3>
<h3 class="mb-0">4.5.3.) Glowny kod CPV: <span class="normal">71000000-8</span></h3>
<h3 class="mb-0">4.3.) Wartosc zamowienia: <span class="normal">200000,00 PLN</span></h3>
</main></body></html>"""
        r = parse_html(html, notice_type="TenderResultNotice")
        assert r.tender_result_parts is not None
        assert len(r.tender_result_parts) == 2
        assert [p.part_id for p in r.tender_result_parts] == ["1", "2"]


# --- Value extraction: ContractPerformingNotice ---


class TestContractPerformingValues:
    def test_contract_value(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values is not None
        assert r.values.value_contract_reported_execution == pytest.approx(24280.56)

    def test_total_paid(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values.value_paid_total == pytest.approx(20000.0)

    def test_currency_pln(self):
        r = parse_html(CONTRACT_PERFORMING_HTML, notice_type="ContractPerformingNotice")
        assert r.values.currency == "PLN"

    def test_currency_eur(self):
        r = parse_html(CONTRACT_PERFORMING_EUR_HTML, notice_type="ContractPerformingNotice")
        assert r.values.currency == "EUR"
        assert r.values.value_contract_reported_execution == pytest.approx(39127.53)


# --- Value extraction: ContractNotice ---


class TestContractNoticeValues:
    def test_estimated_value_from_415(self):
        r = parse_html(CONTRACT_NOTICE_HTML, notice_type="ContractNotice")
        assert r.values is not None
        assert r.values.value_estimated_procurement == pytest.approx(35946524.88)

    def test_fallback_to_416(self):
        r = parse_html(CONTRACT_NOTICE_VAT_HTML, notice_type="ContractNotice")
        assert r.values is not None
        assert r.values.value_estimated_procurement == pytest.approx(570513.92)

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

        p1_weights = {c.str: c.weight for c in (part1.kryteria_oceny or [])}
        p2_weights = {c.str: c.weight for c in (part2.kryteria_oceny or [])}
        assert p1_weights["Cena"] == 60
        assert p1_weights["Termin realizacji"] == 40
        assert p2_weights["Cena"] == 80
        assert p2_weights["Jakosc"] == 20
        assert [c.no for c in (part1.kryteria_oceny or [])] == [1, 2]
        assert [c.no for c in (part2.kryteria_oceny or [])] == [1, 2]
        assert part1.mainCPV == "45000000-7"
        assert part2.mainCPV == "71000000-8"
        assert part1.secondaryCPV == ["45100000-8"]
        assert part2.secondaryCPV == ["71200000-0"]

    def test_extracts_part_descriptions(self):
        r = parse_html(CONTRACT_NOTICE_PARTS_WITH_OPIS_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        assert r.contract_notice_parts[0].opis == "Opis czesci pierwszej."
        assert r.contract_notice_parts[1].opis == "Opis czesci drugiej."

    def test_extracts_contract_notice_parts_decimal_weights(self):
        r = parse_html(CONTRACT_NOTICE_PARTS_DECIMAL_WEIGHTS_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        weights = {c.str: c.weight for c in (r.contract_notice_parts[0].kryteria_oceny or [])}
        assert weights["Cena"] == 60
        assert weights["Termin realizacji"] == 40

    def test_single_part_without_explicit_part_header(self):
        r = parse_html(CONTRACT_NOTICE_SINGLE_PART_NO_HEADER_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        assert len(r.contract_notice_parts) == 1
        part = r.contract_notice_parts[0]
        assert part.part_id is None
        assert part.mainCPV == "45200000-9"
        assert part.secondaryCPV == ["45110000-1"]
        assert part.opis == "Opis jednej części."
        assert [c.no for c in (part.kryteria_oceny or [])] == [1, 2]

    def test_extracts_secondary_cpv_from_paragraph_lines(self):
        r = parse_html(CONTRACT_NOTICE_SECONDARY_CPV_PARAGRAPHS_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        assert len(r.contract_notice_parts) == 1
        part = r.contract_notice_parts[0]
        assert part.mainCPV == "45233140-2"
        assert part.secondaryCPV == ["45100000-8", "34922100-7"]

    def test_extracts_contract_planned_execution_date_from_4210(self):
        r = parse_html(CONTRACT_NOTICE_PARTS_HTML, notice_type="ContractNotice")
        assert r.contract_notice_parts is not None
        assert [p.contract_planned_execution_date for p in r.contract_notice_parts] == ["do 2025-02-28", "12 miesięcy"]


# --- Value extraction: AgreementUpdateNotice ---


class TestAgreementUpdateValues:
    def test_contract_value(self):
        r = parse_html(AGREEMENT_UPDATE_HTML, notice_type="AgreementUpdateNotice")
        assert r.values is not None
        assert r.values.value_awarded_contract == pytest.approx(996945.0)


# --- Value extraction: AgreementIntentionNotice ---


class TestAgreementIntentionValues:
    def test_estimated_value(self):
        r = parse_html(AGREEMENT_INTENTION_HTML, notice_type="AgreementIntentionNotice")
        assert r.values is not None
        assert r.values.value_estimated_procurement == pytest.approx(2509756.10)

    def test_ai_details_extracted(self):
        r = parse_html(AGREEMENT_INTENTION_DETAILS_HTML, notice_type="AgreementIntentionNotice")
        assert r.ai_street_512 == "ul. Rynek 5"
        assert r.value_estimated_procurement_ai_35 == pytest.approx(123456.78)
        assert r.ai_prior_market_consultation_31 == "Tak"


# --- Value extraction: SmallContractNotice ---


class TestSmallContractValues:
    def test_contract_value_bare_number(self):
        r = parse_html(SMALL_CONTRACT_HTML, notice_type="SmallContractNotice")
        assert r.values is not None
        assert r.values.value_awarded_contract == pytest.approx(25399.50)

    def test_currency_from_separate_field(self):
        r = parse_html(SMALL_CONTRACT_HTML, notice_type="SmallContractNotice")
        assert r.values.currency == "PLN"


class TestCompetitionNoticeValues:
    def test_extracts_competition_specific_fields(self):
        r = parse_html(COMPETITION_NOTICE_HTML, notice_type="CompetitionNotice")
        assert r.comp_num_awarded_63 == 3
        assert r.value_competition_prizes_64 == pytest.approx(50000.0)
        assert r.value_competition_followon_order_651 == pytest.approx(120000.0)
        assert r.comp_requirements_72 == "Tak"

    def test_missing_prizes_value_is_none(self):
        r = parse_html(COMPETITION_NOTICE_NO_PRIZES_HTML, notice_type="CompetitionNotice")
        assert r.comp_num_awarded_63 == 1
        assert r.value_competition_prizes_64 is None
        assert r.value_competition_followon_order_651 == pytest.approx(80000.0)

    def test_submission_deadline_from_36(self):
        r = parse_html(COMPETITION_NOTICE_SUBMISSION_36_HTML, notice_type="CompetitionNotice")
        assert r.comp_submission_deadline == "2025-06-10 15:00"

    def test_submission_deadline_from_35(self):
        r = parse_html(COMPETITION_NOTICE_SUBMISSION_35_HTML, notice_type="CompetitionNotice")
        assert r.comp_submission_deadline == "2025-05-20 10:00"

    def test_submission_deadline_light_parser_variants(self):
        r36 = parse_html_competition_light(COMPETITION_NOTICE_SUBMISSION_36_HTML)
        r35 = parse_html_competition_light(COMPETITION_NOTICE_SUBMISSION_35_HTML)
        assert r36["comp_submission_deadline"] == "2025-06-10 15:00"
        assert r35["comp_submission_deadline"] == "2025-05-20 10:00"


class TestCompetitionResultNoticeValues:
    def test_result_approval_date_53(self):
        r = parse_html(COMPETITION_RESULT_NOTICE_HTML, notice_type="CompetitionResultNotice")
        assert r.comp_result_approval_date_53 == "2025-06-30"

    def test_result_approval_date_53_light_parser(self):
        r = parse_html_competition_light(COMPETITION_RESULT_NOTICE_HTML)
        assert r["comp_result_approval_date_53"] == "2025-06-30"


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
        assert result == ["79710000-4"]

    def test_multiple_codes(self):
        raw = "45000000-7 (Roboty budowlane),90620000-9 (UsĹ‚ugi odĹ›nieĹĽania)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2
        assert result[0] == "45000000-7"
        assert result[1] == "90620000-9"

    def test_codes_with_commas_in_description(self):
        # Comma inside parenthetical description should NOT split
        raw = "45000000-7 (Roboty budowlane),71322000-1 (UsĹ‚ugi inĹĽynierii projektowej w zakresie inĹĽynierii lÄ…dowej i wodnej)"
        result = parse_cpv_codes(raw)
        assert len(result) == 2
        assert result == ["45000000-7", "71322000-1"]


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
        assert r.values.value_awarded_contract == pytest.approx(465163.88)

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
        assert r.values.value_contract_reported_execution == pytest.approx(24280.56)

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

    def test_extracts_multi_contractor_html_fields(self):
        r = parse_html(
            CONTRACT_PERFORMING_MULTI_CONTRACTOR_HTML,
            notice_type="ContractPerformingNotice",
        )
        assert r.cpn_contractor_names_431 == ["ABC Sp. z o.o.", "XYZ S.A."]
        assert r.contractor_id_raw == ["NIP: 639-10-05-245", "DE-ABC-987654"]
        assert r.contractor_id_parsed == ["6391005245", "DE-ABC-987654"]
        assert r.contractor_id_type == ["NIP", "foreign"]
        assert r.cpn_contractor_cities_434 == ["Warszawa", "Krakow"]
        assert r.cpn_contractor_provinces_436 == ["mazowieckie", "malopolskie"]
        assert r.cpn_contractor_countries_437 == ["Polska", "Niemcy"]
        assert r.value_contract_reported_execution_44 == pytest.approx(12345.67)

    def test_contract_value_not_confused_with_144(self):
        r = parse_html(
            CONTRACT_PERFORMING_WITH_144_COLLISION_HTML,
            notice_type="ContractPerformingNotice",
        )
        assert r.value_contract_reported_execution_44 == pytest.approx(624212.60)

    def test_contract_value_not_confused_with_144_light(self):
        r = parse_html_contract_performing_light(CONTRACT_PERFORMING_WITH_144_COLLISION_HTML)
        assert r["value_contract_reported_execution_44"] == pytest.approx(624212.60)

    def test_extracts_multi_contractor_html_fields_light(self):
        r = parse_html_contract_performing_light(CONTRACT_PERFORMING_MULTI_CONTRACTOR_HTML)
        assert r["cpn_contractor_names_431"] == ["ABC Sp. z o.o.", "XYZ S.A."]
        assert r["contractor_id_raw"] == ["NIP: 639-10-05-245", "DE-ABC-987654"]
        assert r["contractor_id_parsed"] == ["6391005245", "DE-ABC-987654"]
        assert r["contractor_id_type"] == ["NIP", "foreign"]
        assert r["cpn_contractor_cities_434"] == ["Warszawa", "Krakow"]
        assert r["cpn_contractor_provinces_436"] == ["mazowieckie", "malopolskie"]
        assert r["cpn_contractor_countries_437"] == ["Polska", "Niemcy"]

    def test_contract_performing_light_does_not_use_41_43_for_address(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">4.1.) Data zawarcia umowy: <span class="normal">2025-01-01</span></h3>
<h3 class="mb-0">4.3.) Dane wykonawcy, z ktorym zawarto umowe:</h3>
</main></body></html>"""
        r = parse_html_contract_performing_light(html)
        assert r["ulica"] is None
        assert r["kod_pocztowy"] is None

    def test_contract_performing_light_extracts_section_i_address_fields(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">1.4.1.) Ulica: <span class="normal">ul. Testowa 10</span></h3>
<h3 class="mb-0">1.4.3.) Kod pocztowy: <span class="normal">12-345</span></h3>
<h3 class="mb-0">4.3.) Dane wykonawcy, z ktorym zawarto umowe:</h3>
</main></body></html>"""
        r = parse_html_contract_performing_light(html)
        assert r["ulica"] == "ul. Testowa 10"
        assert r["kod_pocztowy"] == "12-345"

    def test_contractor_id_classification_poland_regon_pesel_and_nonrecognized(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">REGON: 471 325 473</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">44051401458</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">ID-ABCD</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
</main></body></html>"""
        r = parse_html_contract_performing_light(html)
        assert r["contractor_id_raw"] == ["REGON: 471 325 473", "44051401458", "ID-ABCD"]
        assert r["contractor_id_parsed"] == ["471325473", "44051401458"]
        assert r["contractor_id_type"] == ["REGON", "PESEL", "not_recognized"]

    def test_contractor_id_pesel_example_is_typed_as_pesel(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">57300200091</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
</main></body></html>"""
        r = parse_html_contract_performing_light(html)
        assert r["contractor_id_raw"] == ["57300200091"]
        assert r["contractor_id_parsed"] == ["57300200091"]
        assert r["contractor_id_type"] == ["PESEL"]

    def test_contractor_id_prefers_nip_over_regon_when_both_present(self):
        html = """\
<html><head></head><body><main>
<h3 class="mb-0">4.3.2.) Krajowy Numer Identyfikacyjny: <span class="normal">NIP 7393843471 REGON 281393800</span></h3>
<h3 class="mb-0">4.3.7.) Kraj: <span class="normal">Polska</span></h3>
</main></body></html>"""
        r = parse_html_contract_performing_light(html)
        assert r["contractor_id_raw"] == ["NIP 7393843471 REGON 281393800"]
        assert r["contractor_id_parsed"] == ["7393843471"]
        assert r["contractor_id_type"] == ["NIP"]

    def test_contract_performing_light_extracts_execution_booleans(self):
        r = parse_html_contract_performing_light(CONTRACT_PERFORMING_DETAILS_HTML)
        assert r["executed_in_time"] is True
        assert r["proper_execution"] is True

    def test_contract_performing_light_extracts_execution_period_value_not_label(self):
        r = parse_html_contract_performing_light(CONTRACT_PERFORMING_DETAILS_HTML)
        assert r["cpn_contract_planned_execution_date_raw"] == "56 dni"

    def test_contract_performing_light_safe_wrapper_returns_address(self):
        from procurement.silver.spark_transforms import _parse_html_cpn_light_safe

        html = """\
<html><head></head><body><main>
<h3 class="mb-0">1.4.1.) Ulica: <span class="normal">ul. Testowa 10</span></h3>
<h3 class="mb-0">1.4.3.) Kod pocztowy: <span class="normal">12-345</span></h3>
<h3 class="mb-0">4.3.) Dane wykonawcy, z ktorym zawarto umowe:</h3>
</main></body></html>"""
        r = _parse_html_cpn_light_safe(html)
        assert r is not None
        assert r["ulica"] == "ul. Testowa 10"
        assert r["kod_pocztowy"] == "12-345"


class TestTenderResultContractorIdNormalization:
    def test_normalizes_trn_contractor_national_id(self):
        contractors = [
            {
                "contractorName": "ABC Sp. z o.o.",
                "contractorCountry": "PL",
                "contractorNationalId": "NIP 7393843471 REGON 281393800",
            },
            {
                "contractorName": "XYZ GmbH",
                "contractorCountry": "DE",
                "contractorNationalId": "DE-ABC-987654",
            },
        ]
        out = normalize_tender_result_contractors(contractors)
        assert out is not None
        assert out[0]["contractorNationalId_raw"] == "NIP 7393843471 REGON 281393800"
        assert out[0]["contractorNationalId_parsed"] == "7393843471"
        assert out[0]["contractorNationalId_type"] == "NIP"
        assert "contractorNationalId" not in out[0]
        assert out[1]["contractorNationalId_raw"] == "DE-ABC-987654"
        assert out[1]["contractorNationalId_parsed"] == "DE-ABC-987654"
        assert out[1]["contractorNationalId_type"] == "foreign"


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

    def test_parse_cpn_planned_execution_date_direct_do_date(self):
        from procurement.silver.spark_transforms import _parse_cpn_contract_planned_execution_date

        assert _parse_cpn_contract_planned_execution_date("do 2025-02-28", "2024-10-04") == "2025-02-28"

    def test_parse_cpn_planned_execution_date_months_from_contract_date(self):
        from procurement.silver.spark_transforms import _parse_cpn_contract_planned_execution_date

        assert _parse_cpn_contract_planned_execution_date("36 miesiące", "2022-03-21") == "2025-03-21"

    def test_parse_cpn_planned_execution_date_days_from_contract_date(self):
        from procurement.silver.spark_transforms import _parse_cpn_contract_planned_execution_date

        assert _parse_cpn_contract_planned_execution_date("30 dni", "2025-02-10") == "2025-03-12"

    def test_parse_cpn_planned_execution_date_range_uses_end_date(self):
        from procurement.silver.spark_transforms import _parse_cpn_contract_planned_execution_date

        assert (
            _parse_cpn_contract_planned_execution_date(
                "od 2024-10-04 do 2024-12-20",
                "2024-10-01",
            )
            == "2024-12-20"
        )

    def test_parse_cn_planned_execution_dates(self):
        from procurement.silver.spark_transforms import _parse_contract_notice_planned_execution_dates

        parsed = _parse_contract_notice_planned_execution_dates(
            [
                "do 2025-02-28",
                "36 miesiące",
                "30 dni",
                "od 2024-10-04 do 2024-12-20",
            ],
            "2022-03-21T10:00:00Z",
        )
        assert parsed == [
            "2025-02-28",
            "2025-03-21",
            "2022-04-20",
            "2024-12-20",
        ]

    def test_parse_organization_national_id_poland_nip(self):
        from procurement.silver.spark_transforms import _parse_organization_national_id_safe

        assert _parse_organization_national_id_safe("Polska", "NIP: 739-384-34-71") == "7393843471"

    def test_parse_organization_national_id_foreign(self):
        from procurement.silver.spark_transforms import _parse_organization_national_id_safe

        assert _parse_organization_national_id_safe("Niemcy", "DE-ABC-987654") == "DE-ABC-987654"

    def test_parse_organization_national_id_not_recognized(self):
        from procurement.silver.spark_transforms import _parse_organization_national_id_safe

        assert _parse_organization_national_id_safe("Polska", "ID-ABCD") is None

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




