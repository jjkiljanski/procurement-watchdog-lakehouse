# BZP HTML Structure Reference

Analysis of the HTML body (`htmlBody`) embedded in each BZP notice.
Primary structure notes were built on ~10,000 records from October 2025,
with parser status notes updated to current Silver behavior.

See also: `docs/data_profile.md` for overall dataset statistics.

## General HTML anatomy

Every notice HTML follows the same DOM pattern:

```
<html>
  <body>
    <h2>SEKCJA I - ZAMAWIAJÄ„CY</h2>
    <h3>1.1.) ... <span class="normal">value</span></h3>
    <h3>1.2.) ... <span class="normal">value</span></h3>
    ...
    <h2>SEKCJA II â€“ INFORMACJE PODSTAWOWE</h2>
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
| 4.4. | **WartoĹ›Ä‡ umowy** (contract value) | *(not present)* | Rodzaj zamĂłwienia (order type) |
| 8.2. | *(not present)* | **WartoĹ›Ä‡ umowy** (contract value) | Miejsce skĹ‚adania ofert (submission place) |
| 6.4. | *(not present)* | **Cena oferty zwyciÄ™skiej** (winning bid) | ZamawiajÄ…cy wymaga wadium (deposit required) |
| 4.3. | *(not present)* | **WartoĹ›Ä‡ zamĂłwienia** (total value) | Kryteria oceny ofert (evaluation criteria) |
| 3.5. | *(not present)* | *(not present)* | Informacje o Ĺ›rodkach komunikacji (comms info) |

## Sections per notice type

| Notice type | Count | Sections | # Fields |
|---|---|---|---|
| ContractNotice | 25,917 (26.6%) | Iâ€“VIII | 89 |
| TenderResultNotice | 23,675 (24.3%) | Iâ€“VIII | 63 |
| ContractPerformingNotice | 32,940 (33.8%) | Iâ€“VI | 48 |
| NoticeUpdateNotice | 12,132 (12.5%) | Iâ€“III | 21 |
| AgreementUpdateNotice | 1,247 (1.3%) | Iâ€“V | â€” |
| AgreementIntentionNotice | 850 (0.9%) | Iâ€“V | â€” |
| SmallContractNotice | 549 (0.6%) | Iâ€“VI | ~20 |

### Section names by type

**ContractPerformingNotice:**
1. SEKCJA I - ZAMAWIAJÄ„CY
2. SEKCJA II â€“ INFORMACJE PODSTAWOWE
3. SEKCJA III â€“ PODSTAWOWE INFORMACJE O POSTÄPOWANIU
4. SEKCJA IV â€“ PODSTAWOWE INFORMACJE O ZAWARTEJ UMOWIE
5. SEKCJA V â€“ PRZEBIEG REALIZACJI UMOWY
6. SEKCJA VI â€“ INFORMACJE DODATKOWE

**ContractNotice:**
1. SEKCJA I - ZAMAWIAJÄ„CY
2. SEKCJA II â€“ INFORMACJE PODSTAWOWE
3. SEKCJA III â€“ UDOSTÄPNIANIE DOKUMENTĂ“W ZAMĂ“WIENIA I KOMUNIKACJA
4. SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA
5. SEKCJA V - KWALIFIKACJA WYKONAWCĂ“W
6. SEKCJA VI - WARUNKI ZAMĂ“WIENIA
7. SEKCJA VII - PROJEKTOWANE POSTANOWIENIA UMOWY
8. SEKCJA VIII â€“ PROCEDURA

**TenderResultNotice:**
1. SEKCJA I - ZAMAWIAJÄ„CY
2. SEKCJA II â€“ INFORMACJE PODSTAWOWE
3. SEKCJA III â€“ TRYB UDZIELENIA ZAMĂ“WIENIA
4. SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA
5. SEKCJA V â€“ ZAKOĹCZENIE POSTÄPOWANIA
6. SEKCJA VI â€“ OFERTY
7. SEKCJA VII â€“ WYKONAWCA, KTĂ“REMU UDZIELONO ZAMĂ“WIENIA
8. SEKCJA VIII â€“ UMOWA

**NoticeUpdateNotice:**
1. SEKCJA I - ZAMAWIAJÄ„CY
2. SEKCJA II â€“ INFORMACJE PODSTAWOWE
3. SEKCJA III â€“ ZMIANA OGĹOSZENIA

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

**Current parser** (`html_parser.py`) uses type-aware extraction for all 6 notice
types with value fields. Overall value coverage: **~62% of all records**.

### Detailed field reference

#### ContractPerformingNotice (100% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **4.4.** | WartoĹ›Ä‡ umowy | 1 per notice (always) | Original contract value in PLN |
| **5.5.** | ĹÄ…czna wartoĹ›Ä‡ wynagrodzenia wypĹ‚acona | 1 per notice (always) | Total remuneration actually paid |
| 5.4.7. | Kod waluty | 1 per notice | Currency code (almost always "PLN") |

Both 4.4 and 5.5 have the format: `<span class="normal">24280,56 PLN</span>`

**Semantic note:** Field 4.4 is the contractual value; field 5.5 is what was
actually paid. These can differ (e.g. 5.5 = 0.00 PLN means contract not yet
executed; 5.5 < 4.4 means partial execution).

#### TenderResultNotice (86% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **8.2.** | WartoĹ›Ä‡ umowy/umowy ramowej | 1 per lot | Contract/framework agreement value |
| **6.2.** | Cena oferty z najniĹĽszÄ… cenÄ… | 1 per lot | Lowest bid price |
| **6.3.** | Cena oferty z najwyĹĽszÄ… cenÄ… | 1 per lot | Highest bid price |
| **6.4.** | Cena oferty wykonawcy | 1 per lot | Winning contractor's bid price |
| 4.5.5. | WartoĹ›Ä‡ czÄ™Ĺ›ci | 1 per lot | Estimated lot value (multi-lot only) |
| 4.3. | WartoĹ›Ä‡ zamĂłwienia / ĹÄ…czna wartoĹ›Ä‡ | 1 per notice | Total procurement value |

Format: `<span class="normal">89298,00 PLN</span>`

**Multi-lot structure:** For multi-lot procurements, fields 8.2/6.2/6.3/6.4
repeat once per lot. Observed range: 1â€“15 lots per notice.

**Lot count distribution** (n=500):

| Lots | Notices |
|---|---|
| 0 (no 8.2 field) | 95 (19%) |
| 1 | 320 (64%) |
| 2 | 36 (7%) |
| 3 | 15 (3%) |
| 4+ | 34 (7%) |

Notices with 0 lots (19%) may still have value in fields 4.3 or 4.5.5.

Current Silver behavior also emits synthetic lot entries when a
TenderResultNotice has no numeric lot values but `procedureResult`
contains lot outcomes (e.g. `uniewaznienie`, `nieRozstrzygnieto`).

**Fallback chain:** 8.2 â†’ 6.4 â†’ 4.3 â†’ 4.5.5 (for getting *some* value)

#### ContractNotice (13% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 4.1.5. | ĹÄ…czna wartoĹ›Ä‡ / WartoĹ›Ä‡ zamĂłwienia | 1 per notice | Total value (when disclosed) |
| 4.1.6. | WartoĹ›Ä‡ zamĂłwienia (bez VAT) | 1 per notice | Value net of VAT (rare) |
| 4.2.5. | WartoĹ›Ä‡ czÄ™Ĺ›ci | 1 per lot | Lot estimated value (multi-lot, rare) |

Format: `<span class="normal">184430,40 PLN</span>`

**Low extraction rate explanation:** Most contracting authorities do not
disclose the estimated procurement value in the published notice. Only ~13%
include it. This is expected â€” disclosure is optional at this stage.

#### AgreementUpdateNotice (100% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| **4.4.** | WartoĹ›Ä‡ umowy/umowy ramowej | 1 per notice (always) | Agreement value |
| 5.5.2. | Kod waluty | 1 per notice | Currency code |

#### AgreementIntentionNotice (23% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 3.5. | WartoĹ›Ä‡ zamĂłwienia | 1 per notice | Procurement value |

Format includes a non-breaking space: `<span class="normal">2509756,10 \xa0PLN</span>`

#### SmallContractNotice (39% extraction)

| Field | Label | Occurrences | Description |
|---|---|---|---|
| 3.4. | WartoĹ›Ä‡ | 1 per notice | Contract value (**no PLN suffix**) |
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
2. If both `.` and `,` present: remove `.` (thousands), convert `,` â†’ `.`
3. If only `,`: convert `,` â†’ `.`
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

### Current state (parser v2 â€” type-aware)

The parser performs type-aware extraction for both **monetary values** and
**detail enrichment fields**. The silver layer uses a nested `values` struct:

```
values: {
    contract_value: float | null       # 4.4 (CPN/AUN) or 8.2 (TRN) or 3.4 (SCN)
    total_paid: float | null           # 5.5 (CPN only)
    estimated_value: float | null      # 4.3/4.1.5 (CN) or 3.5 (AIN)
    lowest_bid: float | null           # 6.2 (TRN only)
    highest_bid: float | null          # 6.3 (TRN only)
    winning_bid: float | null          # 6.4 (TRN only)
    currency: str                      # from Kod waluty or default "PLN"
}
```

Plus three type-specific detail sub-structs:

| Sub-struct | Notice type | Fields |
|---|---|---|
| `tender_result_enrichment` | TenderResultNotice | joint_bidders (7.1), contractor_size (7.2) |
| `contract_execution` | ContractPerformingNotice | contract_date (4.1), execution_period (4.2), contract_executed (5.1), execution_end_date (5.2), executed_on_time (5.3), num_changes (5.4.1), executed_properly (5.6) |
| `notice_change` | NoticeUpdateNotice | changed_notice_number (3.2), changed_notice_version (3.3), changes[] (3.4 + 3.4.1) |

### Completed improvements

| Priority | Notice type | Fields | Status |
|---|---|---|---|
| **P0** | ContractPerformingNotice | 4.4, 5.5 | Done (100% extraction) |
| **P1** | TenderResultNotice | 6.2, 6.3, 6.4, 4.3 | Done (~86% extraction) |
| **P2** | AgreementUpdateNotice | 4.4 | Done (100% extraction) |
| **P3** | ContractNotice | 4.1.5, 4.2.5 | Done (13% extraction â€” most don't disclose) |
| **P4** | AgreementIntentionNotice | 3.5 | Done (23% extraction) |
| **P5** | SmallContractNotice | 3.4 | Done (33% extraction) |

### Remaining improvements

| Issue | Description |
|---|---|
| Multi-lot arrays | For multi-lot TenderResultNotice, lot-level values (8.2, 6.2, 6.3, 6.4) could be arrays instead of single values |
| Duration analysis | contract_date format consistency should be monitored for date-diff fallback quality |

## Other extractable fields (non-value)

Currently extracted: `ogloszenie_dotyczy` (2.1), address (1.5.x), description (4.2.2), criteria (4.3.5/6),
monetary values (type-aware), and the detail enrichment fields listed below.

### ContractNotice part-level extraction (new)

For `ContractNotice`, Silver now emits additional fields to avoid flattening
multi-part criteria into one notice-level list:

| Field | Type | Scope | Description |
|---|---|---|---|
| `criteria_aspects_4310` | string | notice | Raw value of field `4.3.10.)` ("Tak"/"Nie"/other text). |
| `criteria_aspects_4310_flag` | bool | notice | Parsed boolean from `4.3.10.)` when value is exactly `Tak`/`Nie`. |
| `contract_notice_parts[]` | list | per part | Per-part extraction for multi-part notices (`Część ...`). |
| `contract_notice_parts[].part_id` | string | per part | Parsed part identifier from part header. |
| `contract_notice_parts[].kryteria_oceny[]` | list | per part | Criteria name/weight pairs (`4.3.5`/`4.3.6`) scoped to the part. |
| `contract_notice_parts[].criteria_aspects_4310` | string | per part | Part-level value of field `4.3.10.)` when present in part block. |
| `contract_notice_parts[].criteria_aspects_4310_flag` | bool | per part | Parsed boolean (`Tak`/`Nie`) for part-level `4.3.10.)`. |

Notes:
- Notice-level `kryteria_oceny` remains for backward compatibility.
- For multi-part notices, consumers should prefer `contract_notice_parts[]`.
- Gold currently uses Silver outputs only; no HTML re-parse in Gold.

### ContractNotice field 4.3.10 (`criteria_aspects_4310`) snapshot

In the current October 2025 silver snapshot (`31` daily files, `116,075` rows):

- Notice-level `criteria_aspects_4310` has only two observed values: `Nie`, `Tak`.
- Counts (notice-level): `Nie=30,723`, `Tak=638`.
- Counts (part-level): `Nie=39,669`, `Tak=847`.
- `contract_notice_parts[].criteria_aspects_4310` is present for all parsed parts
  in this snapshot.

Reference outputs generated from silver:
- `E:\git_projects\procurement-watchdog-api-exploration\data\reports\silver_criteria_aspects_4310_counts.json`
- `E:\git_projects\procurement-watchdog-api-exploration\data\reports\silver_criteria_names_4310_tak_without_ekol_srodowisk.json`

### Implemented detail fields

| Field | Type | Notice type | Sub-struct | Extraction rate |
|---|---|---|---|---|
| 7.1. | bool | TenderResultNotice | tender_result_enrichment.joint_bidders | 81.8% |
| 7.2. | str | TenderResultNotice | tender_result_enrichment.contractor_size | 60.9% |
| 4.1. | str | ContractPerformingNotice | contract_execution.contract_date | 100% |
| 4.2. | str | ContractPerformingNotice | contract_execution.execution_period | 100% (label-based extraction + fallback) |
| 5.1. | bool | ContractPerformingNotice | contract_execution.contract_executed | 100% |
| 5.2. | str | ContractPerformingNotice | contract_execution.execution_end_date | 96.6% |
| 5.3. | bool | ContractPerformingNotice | contract_execution.executed_on_time | 99.8% |
| 5.4.1. | int | ContractPerformingNotice | contract_execution.num_changes | 100% |
| 5.6. | bool | ContractPerformingNotice | contract_execution.executed_properly | 100% |
| 3.2. | str | NoticeUpdateNotice | notice_change.changed_notice_number | 100% |
| 3.3. | str | NoticeUpdateNotice | notice_change.changed_notice_version | 100% |
| 3.4.+3.4.1. | list | NoticeUpdateNotice | notice_change.changes[] | 99.6% |

### Fields not yet extracted (skipped as redundant with JSON)

| Field | Type | Present in | Description | Reason skipped |
|---|---|---|---|---|
| 5.1. | text | TenderResultNotice | Procedure outcome | Already in `procedureResult` JSON field |
| 7.3./7.4./7.5. | text | TenderResultNotice | Contractor name/address | Already in `contractors` JSON field |
| 4.3.1./4.3.2. | text | ContractPerformingNotice | Contractor name/address | Already in `contractors` JSON field |

