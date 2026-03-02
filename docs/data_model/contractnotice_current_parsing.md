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

| HTML section | Parser extraction | Silver target column(s) | Notes |
|---|---|---|---|
| `2.1.) Ogłoszenie dotyczy` | `_extract_ogloszenie_dotyczy` -> `htmlExtracted.ogloszenie_dotyczy` | `cn_notice_concerns` | Stored as text. |
| `4.1.5.) Łączna wartość` | `_extract_values_contract_notice` | `htmlExtracted.values.value_estimated_procurement` | Fallback to `4.1.6` if missing. Not currently materialized as a dedicated ContractNotice specific column. |
| `4.1.6.) Wartość zamówienia (bez VAT)` | `_extract_values_contract_notice` | `htmlExtracted.values.value_estimated_procurement` | Used only as fallback for 4.1.5. |
| `4.1.8.) Możliwe jest składanie ofert częściowych` | `_extract_cn_partial_offers_allowed_418` | `cn_partial_offers_allowed_418` | `Tak/Nie -> true/false`, missing -> `null`. |
| `4.1.10.) Ofertę można składać ...` | `_extract_cn_offers_scope_4110` | `cn_offers_scope_4110` | Normalized to: `wszystkie` / `kilka` / `jedna` / `null`. |
| `4.2.2.) Krótki opis przedmiotu zamówienia` | `_extract_contract_notice_parts` (`opis`) | `cn_description_by_part` | List by part (single-part notice = list of length 1). |
| `4.2.6.) Główny kod CPV` | `_extract_contract_notice_parts` (`mainCPV`) | `cpvMainCode` | List by part; CPV digits are normalized by existing CPV parser logic. |
| `4.2.7.) Dodatkowy kod CPV` | `_extract_contract_notice_parts` (`secondaryCPV`) | `cpvSecondaryCode` | List of lists by part; may be empty for a part. |
| `4.2.10.) Okres realizacji ...` | `_extract_contract_notice_parts` (`contract_planned_execution_date`) | `contract_planned_execution_date`, `contract_planned_execution_date_parsed` | Raw list + parsed list (date parsing UDF). |
| `4.3.5.) Nazwa kryterium` + `4.3.6.) Waga` | `_extract_contract_notice_parts` (`kryteria_oceny`) | `criteria`, `cn_award_criteria_by_part`, `numCriteria`, `priceWeight`, `nonPriceWeightSum` | Criteria parsed per part; derived aggregates computed in Spark transforms. |
| `4.3.10.) ... aspekty społeczne/środowiskowe...` | `_extract_contract_notice_parts` (`criteria_aspects_4310`, `criteria_aspects_4310_flag`) | `cn_criteria_aspects_4310`, `cn_criteria_aspects_4310_flag` | Text list + parsed boolean list by part. |

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
