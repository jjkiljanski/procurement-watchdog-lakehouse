# ContractNotice: Current Parsing Map

This document describes the **current** ContractNotice parsing implemented in code, based on:
- `src/procurement/silver/html_parser.py`
- `src/procurement/silver/spark_transforms.py`
- `src/procurement/silver/notice_types/definitions.py`

## Output layers

ContractNotice parsing currently feeds two Silver outputs:

1. `common_envelope` (shared columns across notice types)
- `street`, `postal_code` are parsed from notice-type-specific address locations in HTML.

2. `notice_type_tables/noticeType=ContractNotice`
- ContractNotice-specific columns listed below.

## Section -> Field Mapping

| HTML section | Parser extraction | Silver target column(s) | Oct 2025 presence | Notes |
|---|---|---|---|---|
| `2.1.) Ogłoszenie dotyczy` | `_extract_ogloszenie_dotyczy` -> `htmlExtracted.ogloszenie_dotyczy` | `cn_notice_concerns` | `100.0%` | Stored as text. |
| `4.1.5.) Łączna wartość` | `_extract_values_contract_notice` | `htmlExtracted.values.value_estimated_procurement` | `9.9%` | Fallback to `4.1.6` if missing. Not currently materialized as a dedicated ContractNotice specific column. |
| `4.1.6.) Wartość zamówienia (bez VAT)` | `_extract_values_contract_notice` | `htmlExtracted.values.value_estimated_procurement` | `1.9%` | Used only as fallback for 4.1.5. |
| `4.1.8.) Możliwe jest składanie ofert częściowych` | `_extract_cn_partial_offers_allowed_418` | `cn_partial_offers_allowed_418` | `100.0%` | `Tak/Nie -> true/false`, missing -> `null`. |
| `4.1.10.) Ofertę można składać ...` | `_extract_cn_offers_scope_4110` | `cn_offers_scope_4110` | `28.4%` | Normalized to: `wszystkie` / `kilka` / `jedna` / `null`. |
| `4.2.2.) Krótki opis przedmiotu zamówienia` | `_extract_contract_notice_parts` (`opis`) | `cn_description_by_part` | `100.0%` | List by part (single-part notice = list of length 1). |
| `4.2.6.) Główny kod CPV` | `_extract_contract_notice_parts` (`mainCPV`) | `cpvMainCode` | `100.0%` | List by part; CPV digits are normalized by existing CPV parser logic. |
| `4.2.7.) Dodatkowy kod CPV` | `_extract_contract_notice_parts` (`secondaryCPV`) | `cpvSecondaryCode` | `57.6%` | List of lists by part; may be empty for a part. |
| `4.2.10.) Okres realizacji ...` | `_extract_contract_notice_parts` (`contract_planned_execution_date`) | `contract_planned_execution_date`, `contract_planned_execution_date_parsed` | `100.0%` | Raw list + parsed list (date parsing UDF). |
| `4.3.5.) Nazwa kryterium` + `4.3.6.) Waga` | `_extract_contract_notice_parts` (`kryteria_oceny`) | `criteria`, `cn_award_criteria_by_part`, `numCriteria`, `priceWeight`, `nonPriceWeightSum` | `4.3.5: 100.0%`, `4.3.6: 98.2%` | Criteria parsed per part; derived aggregates computed in Spark transforms. |
| `4.3.10.) ... aspekty społeczne/środowiskowe...` | `_extract_contract_notice_parts` (`criteria_aspects_4310`, `criteria_aspects_4310_flag`) | `cn_criteria_aspects_4310`, `cn_criteria_aspects_4310_flag` | `100.0%` | Text list + parsed boolean list by part. |

## Part model used internally

ContractNotice section IV is normalized to per-part records (`cn_parts_normalized`) before final columns are derived.
If explicit part headers are missing, parser falls back to a single synthetic part from top-level extracted fields.

## Address fields (shared envelope)

Address fields are parsed outside ContractNotice-specific table and written to common envelope:
- `street`
- `postal_code`

These use notice-type-aware address extraction (not generic one-label matching).

## Not currently parsed as dedicated ContractNotice fields

Examples of sections currently not materialized as their own ContractNotice columns:
- `4.1.9.) Liczba części` (used analytically in ad-hoc checks, not in current Silver schema)
- `4.1.11+)` related policy fields (no dedicated ContractNotice columns yet)

## Source of truth

When this document and code diverge, code is authoritative:
- `src/procurement/silver/html_parser.py`
- `src/procurement/silver/spark_transforms.py`
- `src/procurement/silver/notice_types/definitions.py`

## Migration Plan: Production ContractNotice -> Section-Split Model

Goal: replace current production ContractNotice parsing with section-first extraction and validation via:
- `src/procurement/silver/notice_types/contract_notice_split_models.py`

while preserving all currently delivered business columns and their parsing semantics.

### Scope and constraints

1. Keep output compatibility for current ContractNotice consumer columns:
- `cn_notice_concerns`
- `cn_partial_offers_allowed_418`
- `cn_offers_scope_4110`
- `cn_award_criteria_by_part`
- `cn_criteria_aspects_4310`
- `cn_criteria_aspects_4310_flag`
- `cn_description_by_part`
- `cpvMainCode`
- `cpvSecondaryCode`
- `numCriteria`
- `priceWeight`
- `nonPriceWeightSum`
- `contract_planned_execution_date`
- `contract_planned_execution_date_parsed`

2. New parser flow must be section-driven:
- split HTML into section dictionary (`D.D` / `D.D.D` keys),
- construct `ContractNoticeCoreRaw` + `ContractNoticePartRaw`,
- run existing production parsing logic on top of model-backed raw section fields.

3. If previous logic computed a transformed value from a single extracted section:
- keep transformation code,
- source input from `cn_section_*` instead of ad-hoc soup queries.

4. If previous logic built derived columns from multiple sections:
- keep existing derivation behavior unchanged,
- only replace extraction source (model-backed section fields).

### Execution phases

#### Phase 1: Section extractor integration (no semantic change)

1. Add/extend a ContractNotice section splitter in `html_parser.py`:
- parse all section headers into normalized keys (`1.1`, `4.3.10`, etc.),
- capture section value text as raw string,
- detect part blocks for multi-part notices and map repeating section ranges into part records.

2. Build model instances:
- `ContractNoticeCoreRaw` from notice-level section dict,
- `ContractNoticePartRaw` list from part section dicts.

3. Persist model-backed raw section payload in `htmlExtracted` for ContractNotice path (temporary migration bridge).

Acceptance:
- all ContractNotice rows produce either valid model payload or explicit validation/quarantine reason,
- no change yet in final ContractNotice table columns.

#### Phase 2: Rewire existing ContractNotice derived columns

1. Replace direct ContractNotice soup extraction calls with model-backed inputs:
- `cn_notice_concerns` <- section `2.1`,
- `cn_partial_offers_allowed_418` <- section `4.1.8`,
- `cn_offers_scope_4110` <- section `4.1.10`,
- section IV part outputs (`description`, `criteria`, `cpv`, `4.3.10`) from `ContractNoticePartRaw`.

2. Keep all current transformation logic for:
- criteria parsing,
- price/non-price weight aggregation,
- CPV normalization,
- planned execution date parsing.

3. Add explicit new section-derived raw columns only when needed for traceability; naming rule:
- `cn_section_<d>_<d>[_<d>]_raw` for direct raw carryover,
- parsed variants as `..._<suffix>` (e.g., `_flag`, `_parsed`) matching existing naming style.

Acceptance:
- row-level parity for existing ContractNotice output columns on sampled days,
- no regression in silver validation for ContractNotice.

#### Phase 3: Spark transform alignment

1. Update `spark_transforms.py` ContractNotice branch to consume new section-model payload.
2. Keep deterministic/idempotent behavior and schema stability.
3. Remove old temporary bridge fields once all required columns are sourced from section-model flow.

Acceptance:
- output schema unchanged for existing columns,
- job runtime not worse than current baseline beyond acceptable migration overhead.

#### Phase 4: Validation and tests

1. Extend tests:
- unit tests for section split (single-part, multi-part),
- tests for mapped derived columns parity (`cn_*`, cpv, criteria, dates).

2. Add targeted regression tests for known fragile fields:
- `4.1.8`, `4.1.10`,
- `4.3.10`,
- part criteria + weights + cpv secondary.

3. Validate on real sample window (at least first 5 days October 2025) before promoting to production branch.

Acceptance:
- tests green,
- no unexpected ContractNotice quarantine spike,
- sampled values match historical expected behavior.

### Mapping: old extraction -> new model source

- `2.1` -> `cn_section_2_1` -> `cn_notice_concerns`
- `4.1.8` -> `cn_section_4_1_8` -> `cn_partial_offers_allowed_418`
- `4.1.10` -> `cn_section_4_1_10` -> `cn_offers_scope_4110`
- `4.2.2` -> `part.cn_section_4_2_2` -> `cn_description_by_part`
- `4.2.6` -> `part.cn_section_4_2_6` -> `cpvMainCode`
- `4.2.7` -> `part.cn_section_4_2_7` -> `cpvSecondaryCode`
- `4.2.10` -> `part.cn_section_4_2_10` -> `contract_planned_execution_date` (+ parsed)
- `4.3.5/4.3.6` -> part criteria fields -> `cn_award_criteria_by_part`, `numCriteria`, `priceWeight`, `nonPriceWeightSum`
- `4.3.10` -> `part.cn_section_4_3_10` -> `cn_criteria_aspects_4310` + `_flag`

### Out of scope in this step

- Full production cutover in one commit.
- Extending business semantics beyond current production columns.
- Refactoring other notice types.
