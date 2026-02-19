# Silver NoticeType-Split Model (Implemented)

This file name is kept for history, but the content below describes the model that is currently implemented.

## Status

- Notice ingest split is implemented in `scripts/build_silver.py`.
- Case lifecycle projection is implemented in `scripts/build_case_derived_facts.py`.
- Gold consumes notice-level Silver (`common_envelope` + `notice_type_tables`) in `scripts/build_gold.py`.
- Run-stats reads daily Silver notice outputs in `scripts/build_run_stats.py`.

## Physical layout

Silver notice ingest is written in two datasets:

- `data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/`
- `data/silver/notice_type_tables/noticeType=<TOKEN>/publicationDateDay=YYYY-MM-DD/`

Case lifecycle projection is written separately:

- `data/silver/case_derived_facts/asOfDate=YYYY-MM-DD/`

Notes:

- The Silver build processes input in sorted `noticeType` batches.
- Notice type folder token is normalized (`normalized_notice_type_token`).
- Null `noticeType` is stored under `noticeType=__NULL__`.

## Common envelope contract

The envelope contains shared identity and buyer/context fields.
Current contract (see `ENVELOPE_COLUMNS` in `scripts/build_silver.py`):

- `objectId`
- `noticeType`
- `noticeNumber`
- `bzpNumber`
- `publicationDate`
- `publicationDateDay`
- `isTenderAmountBelowEU`
- `orderObject`
- `clientType`
- `clientTypeName`
- `orderType`
- `tenderType`
- `submittingOffersDate`
- `organizationName`
- `organizationCity`
- `organizationProvince`
- `provinceName`
- `organizationCountry`
- `organizationNationalId`
- `organizationId`
- `tenderId`
- `caseId`
- `noticeStage`
- `hasTenderResult`
- `hasContractExecution`
- `organizationNameNormalized`

Important:

- `cpvCodes` is intentionally not in the envelope.

## NoticeType-specific contract

Specific tables use definitions from:

- `src/procurement/silver/notice_types/definitions.py`

Current base specific column set:

- `objectId`
- `noticeType`
- `noticeNumber`
- `bzpNumber`
- `publicationDate`
- `publicationDateDay`
- `tenderId`
- `caseId`
- `cpvCode`
- `cpvCodes`
- `procedureResult`
- `procedureResultParsed`
- `contractors`
- `numCriteria`
- `priceWeight`
- `nonPriceWeightSum`
- `contractorNameNormalized`
- `htmlExtracted`

Important:

- `organizationId` and `organizationName` are not duplicated into specific tables.
- process/lifecycle metrics are not duplicated into specific tables.
- Gold gets buyer identity fields from envelope and merges by `objectId`.

## Case-Derived Facts contract

`case_derived_facts` stores process/tender-level state keyed by case:

- `caseId`
- `buyer_id`
- `first_publicationDate`
- `last_publicationDate`
- `num_notices`
- `num_updates`
- `has_init`
- `has_result`
- `has_execution`
- `time_to_award_days`
- `award_to_completion_days`
- `deadline_changed_count`
- `criteria_changed_count`
- `scope_changed_count`
- `execution_delayed_any`
- `execution_risk_any`
- `paid_ratio_max`
- `paid_ratio_median`
- `bidding_window_days_median`
- `execution_duration_days_median`
- `asOfDate`

Build modes for `build_case_derived_facts.py`:

- `full`: reads all Silver notices up to `target_date` and rebuilds snapshot.
- `incremental`: reads latest day to find touched `caseId`s, recomputes those cases from Silver history up to `target_date`, then merges with previous snapshot.

## Supported notice types in definitions

Explicit definitions currently exist for:

- `ContractNotice`
- `TenderResultNotice`
- `ContractPerformingNotice`
- `NoticeUpdateNotice`
- `AgreementIntentionNotice`
- `AgreementNotice`
- `AgreementUpdateNotice`
- `CircumstancesFulfillmentNotice`
- `None` (null notice type fallback)

Unknown types fall back to the base specific column set.

## Gold input model

Gold reads split notice-level Silver and reconstructs analytical inputs by:

1. Reading envelope rows for the target scope.
2. Reading specific rows for the same scope.
3. Left-joining specific to envelope by `objectId`.
4. Coalescing shared fields (`organizationId`, `clientType`, etc.) from envelope when missing in specific.

This join is implemented in `scripts/build_gold.py` (`_read_silver_split_layout`).

## Determinism and reruns

- Writes are deterministic from a given daily Bronze JSON.
- Writes are idempotent for touched partitions (dynamic partition overwrite).
- Envelope and specific partitions are overwritten per processed day, not appended blindly.
