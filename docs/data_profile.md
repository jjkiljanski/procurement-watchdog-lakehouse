# BZP Data Profile

Based on 27 days of data from October 2025 (97,349 records).
This document is updated as we learn more about the dataset.

## Volume

| Metric | Value |
|---|---|
| Total records (27 days) | 97,349 |
| Records/weekday | ~4,570 (mean) |
| Records/weekend | ~1,316 (mean); Sundays ~2,300–2,800, Saturdays ~100 |
| Records per file | single JSON array per day |

Weekend volumes are **not** negligible — Sundays carry ~2,500 notices
(likely delayed batch publications from Friday/Saturday).

### Publication hours

Peak publishing window is **6 AM – 1 PM** (>80% of daily volume).
Hour 10–12 is the absolute peak (~13,000–14,400 per hour across 27 days).
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
- `zawarcieUmowy` — contract awarded
- `uniewaznienie` — annulled
- `nieRozstrzygnieto` — unresolved

Examples:
- `zawarcieUmowy` — single lot, awarded (60.5%)
- `uniewaznienie` — single lot, annulled (14.8%)
- `zawarcieUmowy;zawarcieUmowy` — two lots, both awarded (6.0%)

591 unique combinations observed. Some contain empty segments (e.g.
`;;;zawarcieUmowy;;`) indicating lots without a recorded outcome.

### SmallContractNotice — the oddball

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
| (null) | 12,177 | — |

Null orderType is exclusive to: NoticeUpdateNotice (12,132),
SmallContractNotice (40), CompetitionNotice (4), ConcessionNotice (1).

## Code fields & dictionaries

Official BZP dictionary files are stored in `refs/bzp_api/`.

| API field | Dictionary file | Unique values | Notes |
|---|---|---|---|
| clientType | SL.MO.013.json | 41 | Hierarchical tree (e.g., "1.1.2" → "jednostka samorządu terytorialnego") |
| orderType | ENUM.002.json | 3 | Flat: Delivery/Services/Works |
| orderType (competitions) | SL.MO.042.json | 2 | jednoetapowy/dwuetapowy |
| tenderType (contracts) | ENUM.018.json | ~80 | Deeply nested; verbose legal references (art. 275, 297, etc.) |
| tenderType (frameworks) | ENUM.019.json | ~30 | Similarly verbose |
| tenderType (competitions) | ENUM.017.json | 2 | Flat |
| organizationProvince | SL.MT.007.json | 16 | Province codes → names (e.g., "PL02" → "dolnośląskie") |

### clientType top values

| Code | Count | Description (from SL.MO.013) |
|---|---|---|
| 1.1.2 | 40,744 | jednostka samorządu terytorialnego |
| 1.1.12 | 12,967 | samodzielny publiczny zakład opieki zdrowotnej |
| 1.4 | 9,683 | osoba prawna, o której mowa w art. 4 pkt 3 ustawy (podmiot prawa publicznego) |
| 1.1.5 | 8,778 | jednostka budżetowa |
| 1.5 | 5,587 | inny zamawiający |
| 1.1.13 | 5,358 | uczelnia publiczna |
| 1.1.1.1 | 4,623 | organ administracji rządowej (centralnej lub terenowej) |

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

### Sections by notice type

Each notice type has a fundamentally different HTML template:

| Notice type | Sections | Fields | Key sections |
|---|---|---|---|
| ContractNotice | 8 (I–VIII) | 89 | ZAMAWIAJĄCY, PRZEDMIOT ZAMÓWIENIA, PROCEDURA |
| TenderResultNotice | 8 (I–VIII) | 63 | ZAMAWIAJĄCY, PRZEDMIOT, ZAKOŃCZENIE, OFERTY, WYKONAWCA, UMOWA |
| ContractPerformingNotice | 6 (I–VI) | 48 | ZAMAWIAJĄCY, POSTĘPOWANIE, UMOWA, REALIZACJA |
| NoticeUpdateNotice | 3 (I–III) | 21 | ZAMAWIAJĄCY, INFO PODSTAWOWE, ZMIANA OGŁOSZENIA |
| AgreementUpdateNotice | 5 (I–V) | — | ZAMAWIAJĄCY, POSTĘPOWANIE, UMOWA, ZMIANA UMOWY |
| AgreementIntentionNotice | 5 (I–V) | — | ZAMAWIAJĄCY, PRZEDMIOT, TRYB, ZAWARCIE UMOWY |

### Current HTML extraction quality

The current parser (`html_parser.py`) targets ContractNotice and
TenderResultNotice only. Extraction rates per notice type:

| Field | ContractNotice | TenderResultNotice | Other types |
|---|---|---|---|
| ulica, kod_pocztowy | 100% | 100% | 0% |
| nuts3_code, nuts3_name | 100% | 100% | 0% |
| opis | 100% | 0% | 0% |
| kryteria_oceny | 89% | 0% | 0% |
| wartosc_umowy_pln | 2% | 79% | 0% |

**Exception:** AgreementIntentionNotice (0.9%) and CompetitionNotice
extract address fields (100%) despite being minor types.

**Coverage gap:** ContractPerformingNotice (33.8% of data) is completely
unparsed — it has 48 HTML fields including contract execution details
that could be valuable.

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
| PL24 | Śląskie | 9,502 | 9.8% |
| PL12 | Małopolskie | 8,855 | 9.1% |
| PL30 | Wielkopolskie | 7,685 | 7.9% |
| PL02 | Dolnośląskie | 6,989 | 7.2% |
| PL06 | Lubelskie | 5,930 | 6.1% |
| PL18 | Podkarpackie | 5,900 | 6.1% |
| PL22 | Pomorskie | 5,673 | 5.8% |
| PL10 | Łódzkie | 5,445 | 5.6% |
| PL04 | Kujawsko-Pomorskie | 4,975 | 5.1% |

Heaviest publishers: Wody Polskie (water management, 800 notices),
large hospitals (Sosnowiec, Siedlce, Kraków), universities (AGH, PW).

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

- [x] ~~What do clientType codes represent?~~ → SL.MO.013 dictionary
- [x] ~~What is the relationship between tenderType and notice types?~~ → 46 unique values, top 3 cover 95%
- [ ] What HTML fields exist in ContractPerformingNotice (33.8% of data)?
- [ ] What HTML fields exist in NoticeUpdateNotice (12.5% of data)?
- [ ] How stable are the HTML templates across time (months/years)?
- [ ] Should code fields be resolved to descriptions in silver vs gold layer?
- [ ] What is the meaning of the two records per bzpNumber pattern?
- [ ] Can `tenderId` be used to link ContractNotice → TenderResultNotice → ContractPerformingNotice?
