# Silver Layer

Purpose:

- Convert raw records into a conformed analytical schema.
- Enrich with parsed HTML-derived fields and normalized identifiers.
- Provide deterministic, joinable daily parquet for downstream Gold and run-stats.

Inputs:

- Preferred: `data/bronze/notices/noticeType=*/publicationDateDay=YYYY-MM-DD/`
- Fallback: `data/raw/bzp_YYYY-MM-DD.json` (legacy compatibility)

Primary build entrypoint:

- `scripts/build_silver.py`
- `scripts/build_case_derived_facts.py` (separate lifecycle projection job)

Operational modes:

- Daily: build one day from Bronze and run `case_derived_facts` in `incremental` mode.
- Backfill: process many days from Bronze, then run `case_derived_facts` `full` for initial snapshot.

Outputs:

- `data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/`
- `data/silver/notice_type_tables/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`
- `data/silver/case_derived_facts/asOfDate=YYYY-MM-DD/` (built by `build_case_derived_facts.py`)
- `data/silver/_quarantine/notice_rows/publicationDateDay=YYYY-MM-DD/` (rows failing Silver row-level validation)

Processing model:

- Input is processed in sorted `noticeType` batches.
- Each batch is transformed with `build_silver_for_notice_type(...)`.
- Shared columns go to `common_envelope`.
- Notice-specific payload goes to `notice_type_tables`.
- `noticeType` folder tokens are normalized; null maps to `__NULL__`.
- Process/lifecycle case metrics are built in a second Spark job and written to `case_derived_facts`.

Core transformation module:

- `src/procurement/silver/spark_transforms.py`

Key semantics:

- `caseId` as canonical case key (`tenderId` fallback to `noticeNumber`).
- `noticeStage` classification (`INIT`, `UPDATE`, `RESULT`, `EXECUTION`).
- `htmlExtracted` nested struct for parsed values/lots/execution/change fields.
- derived operational fields (`biddingWindowDays`, `priceWeight`, `paidRatio`, change flags, execution risk flags).
- `cpvCodes` is kept in noticeType-specific tables, not in the envelope.
- `submittingOffersDate` is kept in specific tables (`ContractNotice`, `ConcessionNotice`), not in the envelope.
- `street` and `postal_code` are promoted to envelope columns for cross-type joins.
- `organizationId` and `organizationName` are kept in envelope and are not duplicated into specific tables.
- notice-type tables intentionally avoid process/lifecycle fields that are mostly null outside relevant notice classes.
- `hasTenderResult` and `hasContractExecution` are not materialized in Silver; use `noticeType` semantics directly.
- `procedureResult` and `procedureResultParsed` are emitted only for `TenderResultNotice` specific tables.
- `AgreementIntentionNotice` specific table emits focused columns:
- `ai_street_512`, `ai_contract_value_35`, `ai_prior_market_consultation_31` (without `htmlExtracted`,
- and without criteria/weight/contractor-normalization fields that are null for this type).
- `AgreementUpdateNotice` specific table drops `numCriteria`, `priceWeight`, `nonPriceWeightSum`, `contractorNameNormalized`, and `htmlExtracted`.
- `ContractNotice` specific table drops `contractors`, `contractorNameNormalized`, and `htmlExtracted`, and emits:
- `cn_notice_concerns`, `cn_award_criteria_by_part`, `cn_criteria_aspects_4310`,
- `cn_criteria_aspects_4310_flag`, `cn_description_by_part`.
- `ContractPerformingNotice` specific table drops `htmlExtracted`, `numCriteria`, `priceWeight`, `nonPriceWeightSum`,
- `contractorNameNormalized`,
- and emits contractor HTML fallback fields:
- `cpn_contractor_national_ids_432`, `cpn_contractor_cities_434`, `cpn_contractor_provinces_436`, `cpn_contract_value_44`.
- `NoticeUpdateNotice` specific table drops `cpvCodes`, `contractors`, criteria/weight fields and `htmlExtracted`,
- and emits flattened change columns: `changed_notice_number`, `changed_notice_version`, `changes`.
- `TenderResultNotice` specific table drops `numCriteria`, `priceWeight`, `nonPriceWeightSum`,
- `contractorNameNormalized`, and `htmlExtracted`, and emits:
- `trn_notice_concerns`, `trn_parts` (part-level `opis`, `mainCPV`, `secondaryCPV`, `expected_value`).
- Spark validation runs after each day write (`build_silver.py`, `build_silver_backfill.py`) and reports:
- `street` non-null/non-empty coverage,
- `postal_code` format validity (`XX-XXX`) for present values.
- Additional batch-level checks: required key nulls (`objectId`, `organizationId`, `caseId`),
- publication date parseability + day/partition consistency, duplicate `objectId`,
- `noticeStage` consistency by `noticeType`, CPV code format, negative `biddingWindowDays`,
- invalid `submittingOffersDate`, and missing `procedureResultParsed` for `TenderResultNotice`.
- Rows failing row-level checks are skipped from main Silver outputs and written to `_quarantine`
- with `__validation_errors` and `validation_notice_type` for triage.
- `CircumstancesFulfillmentNotice` specific table drops `numCriteria`, `priceWeight`,
- `nonPriceWeightSum`, `contractorNameNormalized`, and `htmlExtracted`.
- `SmallContractNotice` specific table drops `contractors`, `numCriteria`, `priceWeight`,
- `nonPriceWeightSum`, `contractorNameNormalized`, and `htmlExtracted`.
- `CompetitionNotice` specific table drops `contractors`, `numCriteria`, `priceWeight`,
- `nonPriceWeightSum`, `contractorNameNormalized`, and `htmlExtracted`, and emits:
- `comp_num_awarded_63`, `comp_prizes_value_64`, `comp_order_value_651`, `comp_requirements_72`.
- `ConcessionNotice` specific table drops `contractors`, `numCriteria`, `priceWeight`,
- `nonPriceWeightSum`, `contractorNameNormalized`, and `htmlExtracted`.
- `case_derived_facts` is case-grain lifecycle state with two modes:
- `full`: rebuild from all Silver notices up to `asOfDate`.
- `incremental`: recompute only cases touched by latest daily notices and merge with prior snapshot.

See also:

- `docs/deployment/OPERATING_MODES.md`

Reporting:

- lightweight daily run stats are generated by `scripts/build_run_stats.py` and written to
  `data/reports/run_stats/run_stats_YYYY-MM-DD.{json,md}`.
