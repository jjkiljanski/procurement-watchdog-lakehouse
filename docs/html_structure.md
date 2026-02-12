# BZP HTML Structure Reference

Analysis of the HTML body (`htmlBody`) embedded in each BZP notice.
Based on ~10,000 records from October 2025.

See also: `docs/data_profile.md` for overall dataset statistics.

## General HTML anatomy

Every notice HTML follows the same DOM pattern:

```
<html>
  <body>
    <h2>SEKCJA I - ZAMAWIAJĄCY</h2>
    <h3>1.1.) ... <span class="normal">value</span></h3>
    <h3>1.2.) ... <span class="normal">value</span></h3>
    ...
    <h2>SEKCJA II – INFORMACJE PODSTAWOWE</h2>
    <h3>2.1.) ... <span class="normal">value</span></h3>
    ...
  </body>
</html>
```

**Key structural rules:**
- Sections are delimited by `<h2>` tags (SEKCJA I, II, III, ...)
- Fields are in `<h3>` tags with a numbered prefix like `4.4.)`
- Field values live inside `<span class="normal">` within the `<h3>`
- Some fields have sibling `<p>` elements for longer text (e.g. descriptions)
- Multi-lot notices repeat per-lot fields (e.g. field 8.2 appears once per lot)

## Critical caveat: field numbers are reused across notice types

The **same field number means completely different things** in different
notice types. A parser MUST filter by `noticeType` before extracting fields.

| Field # | ContractPerformingNotice | TenderResultNotice | ContractNotice |
|---|---|---|---|
| 4.4. | **Wartość umowy** (contract value) | *(not present)* | Rodzaj zamówienia (order type) |
| 8.2. | *(not present)* | **Wartość umowy** (contract value) | Miejsce składania ofert (submission place) |
| 6.4. | *(not present)* | **Cena oferty zwycięskiej** (winning bid) | Zamawiający wymaga wadium (deposit required) |
| 4.3. | *(not present)* | **Wartość zamówienia** (total value) | Kryteria oceny ofert (evaluation criteria) |
| 3.5. | *(not present)* | *(not present)* | Informacje o środkach komunikacji (comms info) |

## Sections per notice type

| Notice type | Count | Sections | # Fields |
|---|---|---|---|
| ContractNotice | 25,917 (26.6%) | I–VIII | 89 |
| TenderResultNotice | 23,675 (24.3%) | I–VIII | 63 |
| ContractPerformingNotice | 32,940 (33.8%) | I–VI | 48 |
| NoticeUpdateNotice | 12,132 (12.5%) | I–III | 21 |
| AgreementUpdateNotice | 1,247 (1.3%) | I–V | — |
| AgreementIntentionNotice | 850 (0.9%) | I–V | — |
| SmallContractNotice | 549 (0.6%) | I–VI | ~20 |

### Section names by type

**ContractPerformingNotice:**
1. SEKCJA I - ZAMAWIAJĄCY
2. SEKCJA II – INFORMACJE PODSTAWOWE
3. SEKCJA III – PODSTAWOWE INFORMACJE O POSTĘPOWANIU
4. SEKCJA IV – PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE
5. SEKCJA V – PRZEBIEG REALIZACJI UMOWY
6. SEKCJA VI – INFORMACJE DODATKOWE

**ContractNotice:**
1. SEKCJA I - ZAMAWIAJĄCY
2. SEKCJA II – INFORMACJE PODSTAWOWE
3. SEKCJA III – UDOSTĘPNIANIE DOKUMENTÓW ZAMÓWIENIA I KOMUNIKACJA
4. SEKCJA IV – PRZEDMIOT ZAMÓWIENIA
5. SEKCJA V - KWALIFIKACJA WYKONAWCÓW
6. SEKCJA VI - WARUNKI ZAMÓWIENIA
7. SEKCJA VII - PROJEKTOWANE POSTANOWIENIA UMOWY
8. SEKCJA VIII – PROCEDURA

**TenderResultNotice:**
1. SEKCJA I - ZAMAWIAJĄCY
2. SEKCJA II – INFORMACJE PODSTAWOWE
3. SEKCJA III – TRYB UDZIELENIA ZAMÓWIENIA
4. SEKCJA IV – PRZEDMIOT ZAMÓWIENIA
5. SEKCJA V – ZAKOŃCZENIE POSTĘPOWANIA
6. SEKCJA VI – OFERTY
7. SEKCJA VII – WYKONAWCA, KTÓREMU UDZIELONO ZAMÓWIENIA
8. SEKCJA VIII – UMOWA

**NoticeUpdateNotice:**
1. SEKCJA I - ZAMAWIAJĄCY
2. SEKCJA II – INFORMACJE PODSTAWOWE
3. SEKCJA III – ZMIANA OGŁOSZENIA

## Monetary value fields

### Overview: where values appear

| Notice type | Share | Value fields | Extraction rate | Notes |
|---|---|---|---|---|
| ContractPerformingNotice | 33.8% | 4.4, 5.5 | **100%** | Always present, always PLN |
| TenderResultNotice | 24.3% | 8.2, 6.2, 6.3, 6.4, 4.5.5, 4.3 | **86%** | Per-lot fields repeat |
| AgreementUpdateNotice | 1.3% | 4.4 | **100%** | Always present |
| SmallContractNotice | 0.6% | 3.4 | **39%** | No PLN suffix; currency in 3.5 |
| AgreementIntentionNotice | 0.9% | 3.5 | **23%** | Sparse |
| ContractNotice | 26.6% | 4.1.5, 4.1.6, 4.2.5 | **13%** | Most don't publish value |
| NoticeUpdateNotice | 12.5% | *(none)* | **0%** | PLN only in free text |

**Current parser** (`html_parser.py`) extracts only field 8.2 from TenderResultNotice.
By adding type-aware extraction, we could go from **~20% overall** to **~62% overall**.

### Detailed field reference

#### ContractPerformingNotice (100% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **4.4.** | Wartość umowy | 1 per notice (always) | Original contract value in PLN |
| **5.5.** | Łączna wartość wynagrodzenia wypłacona | 1 per notice (always) | Total remuneration actually paid |
| 5.4.7. | Kod waluty | 1 per notice | Currency code (almost always "PLN") |

Both 4.4 and 5.5 have the format: `<span class="normal">24280,56 PLN</span>`

**Semantic note:** Field 4.4 is the contractual value; field 5.5 is what was
actually paid. These can differ (e.g. 5.5 = 0.00 PLN means contract not yet
executed; 5.5 < 4.4 means partial execution).

#### TenderResultNotice (86% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **8.2.** | Wartość umowy/umowy ramowej | 1 per lot | Contract/framework agreement value |
| **6.2.** | Cena oferty z najniższą ceną | 1 per lot | Lowest bid price |
| **6.3.** | Cena oferty z najwyższą ceną | 1 per lot | Highest bid price |
| **6.4.** | Cena oferty wykonawcy | 1 per lot | Winning contractor's bid price |
| 4.5.5. | Wartość części | 1 per lot | Estimated lot value (multi-lot only) |
| 4.3. | Wartość zamówienia / Łączna wartość | 1 per notice | Total procurement value |

Format: `<span class="normal">89298,00 PLN</span>`

**Multi-lot structure:** For multi-lot procurements, fields 8.2/6.2/6.3/6.4
repeat once per lot. Observed range: 1–15 lots per notice.

**Lot count distribution** (n=500):

| Lots | Notices |
|---|---|
| 0 (no 8.2 field) | 95 (19%) |
| 1 | 320 (64%) |
| 2 | 36 (7%) |
| 3 | 15 (3%) |
| 4+ | 34 (7%) |

Notices with 0 lots (19%) may still have value in fields 4.3 or 4.5.5.

**Fallback chain:** 8.2 → 6.4 → 4.3 → 4.5.5 (for getting *some* value)

#### ContractNotice (13% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 4.1.5. | Łączna wartość / Wartość zamówienia | 1 per notice | Total value (when disclosed) |
| 4.1.6. | Wartość zamówienia (bez VAT) | 1 per notice | Value net of VAT (rare) |
| 4.2.5. | Wartość części | 1 per lot | Lot estimated value (multi-lot, rare) |

Format: `<span class="normal">184430,40 PLN</span>`

**Low extraction rate explanation:** Most contracting authorities do not
disclose the estimated procurement value in the published notice. Only ~13%
include it. This is expected — disclosure is optional at this stage.

#### AgreementUpdateNotice (100% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **4.4.** | Wartość umowy/umowy ramowej | 1 per notice (always) | Agreement value |
| 5.5.2. | Kod waluty | 1 per notice | Currency code |

#### AgreementIntentionNotice (23% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 3.5. | Wartość zamówienia | 1 per notice | Procurement value |

Format includes a non-breaking space: `<span class="normal">2509756,10 \xa0PLN</span>`

#### SmallContractNotice (39% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 3.4. | Wartość | 1 per notice | Contract value (**no PLN suffix**) |
| 3.5. | Kod waluty | 1 per notice | Currency code (separate field) |

**Unique pattern:** Unlike all other types, SmallContractNotice stores the
value as a bare number without "PLN": `<span class="normal">25399,50</span>`.
The currency is in the separate field 3.5.

#### NoticeUpdateNotice (0% extraction)

No structured value fields. When "PLN" appears in the HTML (3.6% of notices),
it is within free-text amendment descriptions, not parseable structured fields.

## Number format patterns

Across ~13,700 PLN values extracted from structured spans:

| Pattern | Count | Share | Example |
|---|---|---|---|
| Comma decimal, no thousands sep | 11,506 | 83.8% | `89298,00 PLN` |
| Integer (no decimal) | 2,178 | 15.9% | `295590 PLN` |
| Space as thousands separator | 136 | 1.0% | `1 000 000,00 PLN` |
| Dot as thousands + comma decimal | 50 | 0.4% | `130.000,00 PLN` |

**Parsing strategy:**
1. Remove spaces and non-breaking spaces (thousand separators)
2. If both `.` and `,` present: remove `.` (thousands), convert `,` → `.`
3. If only `,`: convert `,` → `.`
4. Parse as float

Regex: `([\d\s\xa0,.]+?)\s*PLN`

## Currency

- **99.9%+ of structured value fields use PLN**
- Rare exceptions found: EUR (field 6.2/8.2 in TenderResultNotice),
  USD (field 8.2), EUR (fields 4.4/5.5 in ContractPerformingNotice)
- The "Kod waluty" field (5.4.7 in ContractPerformingNotice, 5.5.2 in
  AgreementUpdateNotice, 3.5 in SmallContractNotice) explicitly declares
  the currency
- **Recommendation:** Parse the currency code field when available; default
  to PLN when absent

## Extraction improvement roadmap

### Current state (parser v1)

Only extracts from TenderResultNotice field 8.2. Overall value coverage:
~20% of all records (only 81% of the 24% that are TenderResultNotice).

### Proposed improvements

| Priority | Notice type | Fields | Impact |
|---|---|---|---|
| **P0** | ContractPerformingNotice | 4.4, 5.5 | +33.8% coverage (100% rate) |
| **P1** | TenderResultNotice | 6.2, 6.3, 6.4, 4.3 | More value dimensions |
| **P2** | AgreementUpdateNotice | 4.4 | +1.3% coverage (100% rate) |
| **P3** | ContractNotice | 4.1.5, 4.2.5 | +3.5% coverage (13% rate) |
| **P4** | AgreementIntentionNotice | 3.5 | +0.2% coverage (23% rate) |
| **P5** | SmallContractNotice | 3.4 | +0.2% coverage (39% rate) |

**P0 alone would nearly double the value extraction coverage** since
ContractPerformingNotice is the largest notice type (33.8%) and has 100%
extraction rate.

### Multi-value output schema

To support multi-lot notices and multiple value types, the silver layer
should move from a single `wartosc_umowy_pln: float` to a richer structure:

```
values: {
    contract_value: float | null       # 4.4 (CPN/AUN) or 8.2 (TRN)
    total_paid: float | null           # 5.5 (CPN only)
    estimated_value: float | null      # 4.3/4.1.5 (total) or 4.2.5/4.5.5 (lot)
    lowest_bid: float | null           # 6.2 (TRN only)
    highest_bid: float | null          # 6.3 (TRN only)
    winning_bid: float | null          # 6.4 (TRN only)
    currency: str                      # from Kod waluty or default "PLN"
}
```

For multi-lot TenderResultNotice, the lot-level values (8.2, 6.2, 6.3, 6.4)
would be arrays, while the procurement-level value (4.3) remains a scalar.

## Other extractable fields (non-value)

Currently extracted: address (1.5.x), description (4.2.2), criteria (4.3.5/6),
contract value (8.2). Additional fields worth considering:

| Field | Type | Present in | Description |
|---|---|---|---|
| 4.2.2. | text | ContractNotice | Procurement description (currently extracted) |
| 4.3.5./4.3.6. | struct | ContractNotice | Evaluation criteria name + weight |
| 5.1. | text | TenderResultNotice | Procedure outcome (awarded/annulled) |
| 7.1./7.2./7.3. | text | TenderResultNotice | Winning contractor details |
| 4.1./4.2./4.3. | text | ContractPerformingNotice | Contract details |
| 5.1.–5.4. | text | ContractPerformingNotice | Execution progress details |
| 3.2. | text | NoticeUpdateNotice | What changed in the notice |
