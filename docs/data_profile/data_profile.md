# BZP Data Profile

Primary baseline in this document is 27 days of data from October 2025
(97,349 records). Parsing/field notes are updated to reflect current
Silver extraction behavior.

## Volume

| Metric | Value |
|---|---|
| Total records (27 days) | 97,349 |
| Records/weekday | ~4,570 (mean) |
| Records/weekend | ~1,316 (mean); Sundays ~2,300â€“2,800, Saturdays ~100 |
| Records per file | single JSON array per day |

Weekend volumes are **not** negligible â€” Sundays carry ~2,500 notices
(likely delayed batch publications from Friday/Saturday).

### Publication hours

Peak publishing window is **6 AM â€“ 1 PM** (>80% of daily volume).
Hour 10â€“12 is the absolute peak (~13,000â€“14,400 per hour across 27 days).
Negligible volume between midnight and 4 AM.

## Notice types (10 observed / 16 defined)

| noticeType | Count | Share | Description |
|---|---|---|---|
| ContractPerformingNotice | 32,940 | 33.8% | Contract execution reports |
| ContractNotice | 25,917 | 26.6% | New tenders (call for bids) |
| TenderResultNotice | 23,675 | 24.3% | Tender award decisions |
| NoticeUpdateNotice | 12,132 | 12.5% | Amendments to published notices |
| AgreementUpdateNotice | 1,247 | 1.3% | Agreement amendments |
| AgreementIntentionNotice | 850 | 0.9% | Intent to award directly |
| SmallContractNotice | 549 | 0.6% | Small-value contracts |
| CircumstancesFulfillmentNotice | 34 | <0.1% | Rare |
| CompetitionNotice | 4 | <0.1% | Rare |
| ConcessionNotice | 1 | <0.1% | Rare |

Not yet observed: CompetitionResultNotice, ConcessionIntentionAgreementNotice,
NoticeUpdateConcession, ConcessionAgreementNotice, ConcessionUpdateAgreementNotice.

## Field completeness

### Always present (0% null)

objectId, noticeType, noticeNumber, bzpNumber, publicationDate,
isTenderAmountBelowEU, orderObject, cpvCode, clientType,
organizationName, organizationCity, organizationCountry,
organizationNationalId, organizationId, htmlBody

### Nearly always present

| Field | Null % | Notes |
|---|---|---|
| organizationProvince | 0.0% | All 16 Polish voivodeships represented |
| tenderType | 0.6% | Null only for SmallContractNotice (549 records) |
| tenderId | 0.6% | Null only for SmallContractNotice (OCDS-format UUID) |
| clientType | 0.1% | 41 unique values (hierarchical codes) |

### Conditionally present (depends on noticeType)

| Field | Null % | Present in |
|---|---|---|
| orderType | 12.5% | Most types; null for NoticeUpdateNotice, SmallContractNotice, CompetitionNotice, ConcessionNotice |
| contractors | 39.7% | Post-award types only (see below) |
| submittingOffersDate | 73.1% | ContractNotice + ConcessionNotice (bid deadline) |
| procedureResult | 75.7% | TenderResultNotice only (100% there) |

## Field interdependencies

### Complete field presence by notice type

```
                                orderType  tenderType  tenderId  submittingOffersDate  procedureResult  contractors
AgreementIntentionNotice            100%       100%      100%           0%                 0%            100%
AgreementUpdateNotice               100%       100%      100%           0%                 0%            100%
CircumstancesFulfillmentNotice      100%       100%      100%           0%                 0%            100%
CompetitionNotice                     0%       100%      100%           0%                 0%              0%
ConcessionNotice                      0%       100%      100%         100%                 0%              0%
ContractNotice                      100%       100%      100%         100%                 0%              0%
ContractPerformingNotice            100%       100%      100%           0%                 0%            100%
NoticeUpdateNotice                    0%       100%      100%           0%                 0%              0%
SmallContractNotice                  93%         0%        0%          52%                 0%              0%
TenderResultNotice                  100%       100%      100%           0%               100%            100%
```

### contractors (list of ContractorDto)

**Rule:** `contractors is not null` iff notice type is one of
{TenderResultNotice, ContractPerformingNotice, AgreementIntentionNotice,
AgreementUpdateNotice, CircumstancesFulfillmentNotice}.

Contractor field completeness (within records that have contractors):

| Field | Present % |
|---|---|
| contractorName | 58.4% |
| contractorCity | 58.4% |
| contractorCountry | 58.4% |
| contractorNationalId | 57.0% |
| contractorProvince | 50.8% |

Mean contractors per record: 1.4 (median 1, max 98).
90% of records have exactly 1 contractor.

### submittingOffersDate

Present in ContractNotice (100%) and ConcessionNotice (100%).
Also partially present in SmallContractNotice (52%).
Null for all post-award and update types.

### procedureResult

Present **only** in TenderResultNotice (100% there, 0% everywhere else).

The field is semicolon-delimited with **per-lot outcomes**. Three possible
values per lot:
- `zawarcieUmowy` â€” contract awarded
- `uniewaznienie` â€” annulled
- `nieRozstrzygnieto` â€” unresolved

Examples:
- `zawarcieUmowy` â€” single lot, awarded (60.5%)
- `uniewaznienie` â€” single lot, annulled (14.8%)
- `zawarcieUmowy;zawarcieUmowy` â€” two lots, both awarded (6.0%)

591 unique combinations observed. Some contain empty segments (e.g.
`;;;zawarcieUmowy;;`) indicating lots without a recorded outcome.

In current Silver logic, `procedureResult` is also used as a fallback lot
signal for TenderResult notices with no numeric lot values. Outcomes like
`uniewaznienie` / `nieRozstrzygnieto` are emitted as synthetic status lots.

### SmallContractNotice â€” the oddball

SmallContractNotice (0.6% of data) is unique:
- No `tenderType` (0%)
- No `tenderId` (0%)
- `orderType` present in 93% (not 100% like other types with orderType)
- `submittingOffersDate` present in 52%

### orderType distribution

| orderType | Count | Share |
|---|---|---|
| Delivery | 40,838 | 47.9% |
| Works | 23,207 | 27.2% |
| Services | 21,127 | 24.8% |
| (null) | 12,177 | â€” |

Null orderType is exclusive to: NoticeUpdateNotice (12,132),
SmallContractNotice (40), CompetitionNotice (4), ConcessionNotice (1).

## Code fields & dictionaries

Official BZP dictionary files are stored in `refs/bzp_api/`.

| API field | Dictionary file | Unique values | Notes |
|---|---|---|---|
| clientType | SL.MO.013.json | 41 | Hierarchical tree (e.g., "1.1.2" â†’ "jednostka samorzÄ…du terytorialnego") |
| orderType | ENUM.002.json | 3 | Flat: Delivery/Services/Works |
| orderType (competitions) | SL.MO.042.json | 2 | jednoetapowy/dwuetapowy |
| tenderType (contracts) | ENUM.018.json | ~80 | Deeply nested; verbose legal references (art. 275, 297, etc.) |
| tenderType (frameworks) | ENUM.019.json | ~30 | Similarly verbose |
| tenderType (competitions) | ENUM.017.json | 2 | Flat |
| organizationProvince | SL.MT.007.json | 16 | Province codes â†’ names (e.g., "PL02" â†’ "dolnoĹ›lÄ…skie") |

### clientType top values

| Code | Count | Description (from SL.MO.013) |
|---|---|---|
| 1.1.2 | 40,744 | jednostka samorzÄ…du terytorialnego |
| 1.1.12 | 12,967 | samodzielny publiczny zakĹ‚ad opieki zdrowotnej |
| 1.4 | 9,683 | osoba prawna, o ktĂłrej mowa w art. 4 pkt 3 ustawy (podmiot prawa publicznego) |
| 1.1.5 | 8,778 | jednostka budĹĽetowa |
| 1.5 | 5,587 | inny zamawiajÄ…cy |
| 1.1.13 | 5,358 | uczelnia publiczna |
| 1.1.1.1 | 4,623 | organ administracji rzÄ…dowej (centralnej lub terenowej) |

### tenderType top values

| Code | Count |
|---|---|
| 1.1.1 | 68,199 (70%) |
| 1.1.2 | 13,880 (14%) |
| 2.1 | 10,764 (11%) |
| 1.4.2 | 991 |
| 1.4.1.7 | 825 |

46 unique tenderType values observed. Top 3 codes cover 95% of records.

## HTML structure

> **Detailed reference:** See [`docs/data_profile/html_structure.md`](html_structure.md) for
> a comprehensive analysis of value fields, number formats, multi-lot patterns,
> and an extraction improvement roadmap.

### Sections by notice type

Each notice type has a fundamentally different HTML template:

| Notice type | Sections | Fields | Key sections |
|---|---|---|---|
| ContractNotice | 8 (Iâ€“VIII) | 89 | ZAMAWIAJÄ„CY, PRZEDMIOT ZAMĂ“WIENIA, PROCEDURA |
| TenderResultNotice | 8 (Iâ€“VIII) | 63 | ZAMAWIAJÄ„CY, PRZEDMIOT, ZAKOĹCZENIE, OFERTY, WYKONAWCA, UMOWA |
| ContractPerformingNotice | 6 (Iâ€“VI) | 48 | ZAMAWIAJÄ„CY, POSTÄPOWANIE, UMOWA, REALIZACJA |
| NoticeUpdateNotice | 3 (Iâ€“III) | 21 | ZAMAWIAJÄ„CY, INFO PODSTAWOWE, ZMIANA OGĹOSZENIA |
| AgreementUpdateNotice | 5 (Iâ€“V) | â€” | ZAMAWIAJÄ„CY, POSTÄPOWANIE, UMOWA, ZMIANA UMOWY |
| AgreementIntentionNotice | 5 (Iâ€“V) | â€” | ZAMAWIAJÄ„CY, PRZEDMIOT, TRYB, ZAWARCIE UMOWY |

### Current HTML extraction quality

The parser (`html_parser.py`) performs **type-aware extraction** â€”
address/description/criteria for all types, monetary values dispatched
per notice type, and detail enrichment for TRN/CPN/NUN.

#### ContractNotice 4.3.10 and part-level criteria (new)

Silver now emits ContractNotice-specific fields that preserve part-level
structure in multi-part notices:

| Field | Type | Meaning |
|---|---|---|
| `htmlExtracted.criteria_aspects_4310` | string | Raw value of `4.3.10.)` at notice level |
| `htmlExtracted.criteria_aspects_4310_flag` | bool | Parsed `Tak`/`Nie` flag from `4.3.10.)` |
| `htmlExtracted.contract_notice_parts[]` | list | Part-level blocks for `Część ...` sections |
| `htmlExtracted.contract_notice_parts[].kryteria_oceny[]` | list | Criteria name/weight by part |
| `htmlExtracted.contract_notice_parts[].criteria_aspects_4310` | string | Part-level `4.3.10.)` raw value |
| `htmlExtracted.contract_notice_parts[].criteria_aspects_4310_flag` | bool | Part-level `Tak`/`Nie` flag |

Recommended usage:
- For single-part notices, notice-level criteria are usually sufficient.
- For multi-part notices, prefer `contract_notice_parts[]` to avoid mixing
  criteria/weights across parts.

#### Common fields (address, description, criteria)

| Field | ContractNotice | TenderResultNotice | Other types |
|---|---|---|---|
| ulica, kod_pocztowy | 100% | 100% | 0% |
| nuts3_code, nuts3_name | 100% | 100% | 0% |
| opis | 100% | 0% | 0% |
| kryteria_oceny | 86% | 0% | 0% |

**Exception:** AgreementIntentionNotice (0.9%) and CompetitionNotice
extract address fields (100%) despite being minor types.

#### Monetary values (type-aware, `values.*` struct)

| Field | Notice type | Extraction rate |
|---|---|---|
| contract_value | TenderResultNotice (8.2) | 88% |
| contract_value | ContractPerformingNotice (4.4) | 100% |
| contract_value | AgreementUpdateNotice (4.4) | 100% |
| contract_value | SmallContractNotice (3.4) | 33% |
| estimated_value | ContractNotice (4.1.5) | 13% |
| estimated_value | AgreementIntentionNotice (3.5) | 23% |
| total_paid | ContractPerformingNotice (5.5) | 100% |
| lowest_bid / highest_bid / winning_bid | TenderResultNotice (6.2/6.3/6.4) | ~86% |

#### Detail enrichment (type-specific sub-structs)

| Sub-struct | Notice type | Key fields | Extraction rate |
|---|---|---|---|
| tender_result_enrichment | TenderResultNotice | joint_bidders, contractor_size | 82% / 61% |
| contract_execution | ContractPerformingNotice | 7 fields (dates, booleans, num_changes) | 97â€“100% |
| notice_change | NoticeUpdateNotice | changed_notice_number, changes[] | 100% / 99.6% |

## Fetch behavior (current)

Daily fetch now applies two safety steps before writing bronze JSON:

1. Keep only notices whose `publicationDate` falls on the requested day.
2. Deduplicate remaining notices by `objectId` (first occurrence kept).

This reduces cross-day spillover and duplicate-object rows at ingestion time.

## TenderResultNotice â€” Contractor enrichment

From HTML fields 7.1 (joint bidders) and 7.2 (enterprise size). These
are HTML-only â€” the JSON `contractors` field does not carry them.

| Field | Extracted | Rate | Notes |
|---|---|---|---|
| contractor_size | 5,576 / 9,150 | 60.9% | Only for awarded contracts |
| joint_bidders | 7,489 / 9,150 | 81.8% | Tak/Nie boolean |

### Enterprise size distribution (n=5,576 awarded)

| Size | Count | Share |
|---|---|---|
| Mikro przedsiÄ™biorca | 2,448 | 43.9% |
| MaĹ‚y przedsiÄ™biorca | 2,093 | 37.5% |
| Ĺšredni przedsiÄ™biorca | 1,035 | 18.6% |

No "DuĹĽy przedsiÄ™biorca" (large enterprise) observed â€” above-EU tenders
(where large enterprises dominate) go through TED, not BZP.

### Joint bidders (consortium)

3.4% of awarded tenders (251 / 7,489) involved a consortium.

## ContractPerformingNotice â€” Execution details

From HTML sections IV and V. Contract execution reports are the largest
notice type (33.5%) and carry rich structured fields.

### Field extraction rates (n=12,352)

| Field | Extracted | Rate |
|---|---|---|
| contract_date (4.1) | 12,352 | 100.0% |
| execution_period (4.2) | 12,352 | 100.0% |
| contract_executed (5.1) | 12,352 | 100.0% |
| execution_end_date (5.2) | 11,929 | 96.6% |
| executed_on_time (5.3) | 12,325 | 99.8% |
| num_changes (5.4.1) | 12,352 | 100.0% |
| executed_properly (5.6) | 12,352 | 100.0% |

Execution parsing was updated to resolve fields by label text
(`Data zawarcia umowy`, `Okres realizacji`, etc.) with numeric fallback.
This removed the common city-name misread in `execution_period`.

### Boolean field distributions

| Field | True | False | None |
|---|---|---|---|
| contract_executed | 12,255 (99.2%) | 97 (0.8%) | â€” |
| executed_on_time | 10,560 (85.6%) | 1,765 (14.3%) | 27 |
| executed_properly | 12,105 (98.0%) | 247 (2.0%) | â€” |

**Key insight:** 14.3% of contracts are **not** executed on time, and 2%
are executed improperly. Cross-tabulation shows that late + improper
execution (117 cases) is more common than on-time + improper (62 cases).

### Contract amendments (num_changes)

| Changes | Count | Share |
|---|---|---|
| 0 | 9,940 | 80.5% |
| 1 | 1,698 | 13.7% |
| 2 | 404 | 3.3% |
| 3+ | 310 | 2.5% |

Mean 0.30, max 13 amendments per contract.

## NoticeUpdateNotice â€” Amendment details

From HTML section III. Notice updates (11.8%) describe which sections
of the original notice were changed.

### Field extraction rates (n=4,363)

| Field | Extracted | Rate |
|---|---|---|
| changed_notice_number (3.2) | 4,363 | 100.0% |
| changed_notice_version (3.3) | 4,363 | 100.0% |
| changes[] (3.4 + 3.4.1) | 4,346 | 99.6% |

All `changed_notice_version` values are "01" (no versioning beyond first).

### Number of changed sections per notice

| Sections | Count | Share |
|---|---|---|
| 1 | 3,394 | 78.1% |
| 2 | 748 | 17.2% |
| 3 | 164 | 3.8% |
| 4+ | 40 | 0.9% |

Mean 1.28, max 6 sections changed per notice.

### Most frequently changed sections

| Section | Count | Share |
|---|---|---|
| SEKCJA VIII - PROCEDURA | 3,476 | 62.6% |
| SEKCJA IV â€“ PRZEDMIOT ZAMĂ“WIENIA | 632 | 11.4% |
| SEKCJA V - KWALIFIKACJA WYKONAWCĂ“W | 430 | 7.7% |
| SEKCJA V - PRZEBIEG REALIZACJI UMOWY | 193 | 3.5% |
| SEKCJA IX - POZOSTAĹE INFORMACJE | 147 | 2.6% |

PROCEDURA (VIII) dominates â€” likely deadline extensions and procedural
adjustments. Change description length: mean 908 chars, median 59 chars,
max ~20K (highly skewed by a few large amendments).

## CPV codes

- Mean codes per notice: 2.7 (median 1, max 120)
- Format: `XXXXXXXX-X (Description)`, comma-separated when multiple

Top CPV divisions (2-digit):

| Division | Description | Count |
|---|---|---|
| 45 | Construction works | 114,689 |
| 33 | Medical supplies | 20,009 |
| 71 | Architecture/engineering | 14,215 |
| 39 | Furniture | 10,882 |
| 30 | Office/computing | 9,153 |
| 15 | Food | 9,026 |
| 90 | Environment | 8,646 |
| 34 | Transport equipment | 7,629 |

Construction (45xx) dominates with >40% of all CPV code occurrences.

## Contract values

From TenderResultNotice HTML (field 8.2):

| Metric | PLN |
|---|---|
| Extracted | 19,317 / 23,675 (81.6%) |
| Mean | 1,171,667 |
| Median (P50) | 231,855 |
| P25 | 98,790 |
| P75 | 479,700 |
| P95 | 2,148,895 |
| P99 | 7,825,359 |
| Max | 5,342,169,325 |

Highly right-skewed. Half of contracts are under 232K PLN.
Top 1% exceeds 7.8M PLN.

## Organizations

| Metric | Value |
|---|---|
| Unique organizations | 7,880 |
| Unique cities | 2,453 |
| Unique NIP (tax IDs) | 7,870 |

### Notices by province

| Code | Province | Count | Share |
|---|---|---|---|
| PL14 | Mazowieckie | 17,145 | 17.6% |
| PL24 | ĹšlÄ…skie | 9,502 | 9.8% |
| PL12 | MaĹ‚opolskie | 8,855 | 9.1% |
| PL30 | Wielkopolskie | 7,685 | 7.9% |
| PL02 | DolnoĹ›lÄ…skie | 6,989 | 7.2% |
| PL06 | Lubelskie | 5,930 | 6.1% |
| PL18 | Podkarpackie | 5,900 | 6.1% |
| PL22 | Pomorskie | 5,673 | 5.8% |
| PL10 | ĹĂłdzkie | 5,445 | 5.6% |
| PL04 | Kujawsko-Pomorskie | 4,975 | 5.1% |

Heaviest publishers: Wody Polskie (water management, 800 notices),
large hospitals (Sosnowiec, Siedlce, KrakĂłw), universities (AGH, PW).

## isTenderAmountBelowEU

88.3% of records are below the EU threshold (`True`).

Notable exceptions:
- ContractPerformingNotice: 33.4% are above-EU (much higher than average)
- CircumstancesFulfillmentNotice: 52.9% above-EU
- TenderResultNotice: 99.9% below-EU (likely because above-EU tenders
  go through TED/eSender instead of BZP)

## Notice linking (bzpNumber)

- 51,379 unique bzpNumbers across 97,349 records
- Each bzpNumber has either 1 or 2 notices (never more in this dataset)
- 45,970 bzpNumbers have exactly 2 records; 5,409 have exactly 1
- **All multi-record bzpNumbers share the same noticeType** (no cross-type linking)
- Linking across notice types likely uses `tenderId` instead

## Open questions

- [x] ~~What do clientType codes represent?~~ â†’ SL.MO.013 dictionary
- [x] ~~What is the relationship between tenderType and notice types?~~ â†’ 46 unique values, top 3 cover 95%
- [x] ~~What HTML fields exist in ContractPerformingNotice?~~ â†’ 48 fields; execution details now extracted (sections IVâ€“V)
- [x] ~~What HTML fields exist in NoticeUpdateNotice?~~ â†’ 21 fields; amendment details now extracted (section III)
- [x] ~~Fix `execution_period` parser bug (extracts city name instead of period text)~~ -> fixed with label-based extraction + fallback
- [ ] How stable are the HTML templates across time (months/years)?
- [x] ~~Should code fields be resolved to descriptions in silver vs gold layer?~~ â†’ Resolved in silver (provinceName, clientTypeName)
- [ ] What is the meaning of the two records per bzpNumber pattern?
- [ ] Can `tenderId` be used to link ContractNotice â†’ TenderResultNotice â†’ ContractPerformingNotice?

