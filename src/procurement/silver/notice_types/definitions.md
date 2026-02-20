# Silver Notice-Type Definitions (Observed)

Source of truth for field structure: `src/procurement/silver/notice_types/definitions.py`.
Examples and observed types below were read from real Silver parquets in `E:/git_projects/procurement-watchdog-api-exploration/data/silver`.
Generated at: 2026-02-20T21:25:29.278334Z

## Base Specific Columns (Exact Order)

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
- `contractors`
- `numCriteria`
- `priceWeight`
- `nonPriceWeightSum`
- `contractorNameNormalized`
- `htmlExtracted`

## AgreementIntentionNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de0d44-c3ea-6f45-342c-650001973068` | yes |
| `noticeType` | Notice class/type. | `string` | `AgreementIntentionNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00480477/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00480477` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-17T06:16:52.8929489Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-17` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-e213f724-877a-4483-9fa6-7d4106c3dcbb` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-e213f724-877a-4483-9fa6-7d4106c3dcbb` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['50232100-1', '09300000-2']` | yes |
| `contractors` | Contractor list from metadata (array of structs/maps). | `array<map<string,string>>` | `[{'contractorNationalId': '85262912', 'contractorName': 'ENEA Oświetlenie sp. z o.o', 'contractorCountry': 'PL', 'contractorCity': 'Szczecin', 'contractorProvince': 'PL32'}]` | yes |
| `ai_street_512` | AgreementIntentionNotice field 5.1.2 street value. | `string` | `Ku Słońcu 34` | yes |
| `ai_contract_value_35` | AgreementIntentionNotice field 3.5 value. | `double` | `24390.24` | yes |
| `ai_prior_market_consultation_31` | AgreementIntentionNotice field 3.1 consultation flag/text. | `string` | `Nie` | yes |

## AgreementNotice

Path not found in current Silver data: `/ext/data/silver/notice_type_tables/noticeType=AgreementNotice`

### Fields (from definitions.py)

- `objectId`: Unique notice object identifier (source payload ID).
- `noticeType`: Notice class/type.
- `noticeNumber`: Notice number with version (BZP format).
- `bzpNumber`: Base BZP notice number (without version).
- `publicationDate`: Notice publication timestamp (UTC string).
- `publicationDateDay`: Publication date partition key.
- `tenderId`: Tender-level identifier from source.
- `caseId`: Case key used for cross-notice linking.
- `cpvCode`: Raw CPV source string (legacy; not materialized currently).
- `cpvCodes`: Parsed CPV code list (canonical code format).
- `contractors`: Contractor list from metadata (array of structs/maps).
- `numCriteria`: Number of evaluation criteria parsed (ContractNotice logic).
- `priceWeight`: Sum of price-related criterion weights.
- `nonPriceWeightSum`: Sum of non-price criterion weights.
- `contractorNameNormalized`: Normalized contractor name list for matching.
- `htmlExtracted`: Compact parsed HTML struct (when materialized for a type).

## AgreementUpdateNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de15d9-6b51-fa88-6f2a-fc0001f951b4` | yes |
| `noticeType` | Notice class/type. | `string` | `AgreementUpdateNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00498898/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00498898` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-28T04:21:08.5633087Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-28` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-7607d570-2116-4da5-bd95-f32e191ebff0` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-7607d570-2116-4da5-bd95-f32e191ebff0` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['45000000-7', '71320000-7', '71220000-6', '45240000-1', '45111200-0', '45112700-2', '45112000-5']` | yes |
| `contractors` | Contractor list from metadata (array of structs/maps). | `array<map<string,string>>` | `[{'contractorNationalId': '5833453314', 'contractorName': 'DREW-KOS Sp. z o.o.', 'contractorCountry': 'PL', 'contractorCity': 'Koszalin', 'contractorProvince': 'PL32'}]` | yes |

## CircumstancesFulfillmentNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de0c79-5199-2a69-342c-650001971f1b` | yes |
| `noticeType` | Notice class/type. | `string` | `CircumstancesFulfillmentNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00477705/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00477705` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-16T06:00:33.2646866Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-16` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-4d025651-4654-11ee-a60c-9ec5599dddc1` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-4d025651-4654-11ee-a60c-9ec5599dddc1` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['45233220-7']` | yes |
| `contractors` | Contractor list from metadata (array of structs/maps). | `array<map<string,string>>` | `[{'contractorNationalId': '7692220795', 'contractorName': 'Przediębiorstwo Komunikacji, Transportu i Usług Komunalnych Gminy Belchatów Sp. z o.o.', 'contractorCountry': 'PL', 'contractorCity': 'Bełchatów', 'contractorPro` | yes |

## CompetitionNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de17af-5600-de45-6f2a-fc00017393dc` | yes |
| `noticeType` | Notice class/type. | `string` | `CompetitionNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00506332/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00506332` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-30T12:24:56.2705173Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-30` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-8c401a90-2167-40ce-a4dd-ef6573103412` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-8c401a90-2167-40ce-a4dd-ef6573103412` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['71220000-6', '71420000-8', '71222000-0', '71248000-8']` | yes |
| `comp_num_awarded_63` | CompetitionNotice field 6.3 number of awarded works. | `int` | `5` | yes |
| `comp_prizes_value_64` | CompetitionNotice field 6.4 prizes value. | `double` | `68000.0` | yes |
| `comp_order_value_651` | CompetitionNotice field 6.5.1 order value. | `double` | `284552.84` | yes |
| `comp_requirements_72` | CompetitionNotice field 7.2 environmental/social requirement. | `string` | `Nie` | yes |

## ConcessionNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de00d7-526e-c5ec-d903-3900014f79ab` | yes |
| `noticeType` | Notice class/type. | `string` | `ConcessionNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00451385/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00451385` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-01T10:43:13.40208Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-01` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-c8dbcf00-56a4-45b7-81f2-37f29f6ff2da` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-c8dbcf00-56a4-45b7-81f2-37f29f6ff2da` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['65100000-4', '65111000-4', '65130000-3', '90000000-7']` | yes |
| `submittingOffersDate` | Offer submission deadline timestamp. | `string` | `2025-10-22T10:00:00Z` | yes |

## ContractNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de1783-ebe2-e1a4-6f2a-fc0001738add` | yes |
| `noticeType` | Notice class/type. | `string` | `ContractNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00504565/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00504565` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-30T07:14:09.8738733Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-30` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-80d5e84c-d300-42ce-ab34-aee05cd9954b` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-80d5e84c-d300-42ce-ab34-aee05cd9954b` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['90910000-9', '90911200-8', '90911300-9', '90919200-4', '98310000-9']` | yes |
| `numCriteria` | Number of evaluation criteria parsed (ContractNotice logic). | `int` | `2` | yes |
| `priceWeight` | Sum of price-related criterion weights. | `int` | `60` | yes |
| `nonPriceWeightSum` | Sum of non-price criterion weights. | `int` | `40` | yes |
| `cn_notice_concerns` | ContractNotice field 2.1 scope (what notice concerns). | `string` | `Zamówienia publicznego` | yes |
| `cn_award_criteria_by_part` | ContractNotice criteria-by-part map list (criterion -> weight). | `array<map<string,int>>` | `[{'Czas reakcji na zgłoszoną reklamację, tj. usuniecie zgłoszonych nieprawidłowości w wykonaniu usługi sprzątania': 40, 'Cena': 60}]` | yes |
| `cn_criteria_aspects_4310` | ContractNotice field 4.3.10 text by part. | `array<string>` | `['Nie']` | yes |
| `cn_criteria_aspects_4310_flag` | Parsed boolean from 4.3.10 by part. | `array<boolean>` | `[False]` | yes |
| `cn_description_by_part` | ContractNotice short description(s) by part. | `array<string>` | `['1.\tUsługa codziennego sprzątania budynku polega na wykonywaniu następujących czynności: 1)\tsprzątaniu pomieszczeń biurowych i korytarzy z użyciem środków chemicznych adekwatnych do czyszczonych powierzchni, w tym: a)` | yes |
| `submittingOffersDate` | Offer submission deadline timestamp. | `string` | `2025-11-13T09:00:00Z` | yes |

## ContractPerformingNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de0fc8-6cc3-b5a5-342c-6500019748ef` | yes |
| `noticeType` | Notice class/type. | `string` | `ContractPerformingNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00484610/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00484610` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-20T11:04:22.5710295Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-20` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-a30a2295-64e2-4466-8ef3-4cef6bcb1d15` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-a30a2295-64e2-4466-8ef3-4cef6bcb1d15` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['48611000-4']` | yes |
| `contractors` | Contractor list from metadata (array of structs/maps). | `array<map<string,string>>` | `[{'contractorNationalId': '012521511', 'contractorName': 'A.P.N. Promise S.A.', 'contractorCountry': 'PL', 'contractorCity': 'Warszawa', 'contractorProvince': 'PL14'}]` | yes |
| `cpn_contractor_national_ids_432` | ContractPerformingNotice field 4.3.2 values. | `array<string>` | `['012521511']` | yes |
| `cpn_contractor_cities_434` | ContractPerformingNotice field 4.3.4 values. | `array<string>` | `['Warszawa']` | yes |
| `cpn_contractor_provinces_436` | ContractPerformingNotice field 4.3.6 values. | `array<string>` | `['mazowieckie']` | yes |
| `cpn_contract_value_44` | ContractPerformingNotice field 4.4 contract value. | `double` | `117553.17` | yes |

## NoticeUpdateNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de105b-8919-39db-342c-650001974f60` | yes |
| `noticeType` | Notice class/type. | `string` | `NoticeUpdateNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00485781/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00485781` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-21T04:37:26.1275314Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-21` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-31a276b5-2622-47ae-8250-acb52d211298` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-31a276b5-2622-47ae-8250-acb52d211298` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `changed_notice_number` | NoticeUpdateNotice: referenced changed notice number. | `string` | `2025/BZP 00453384` | yes |
| `changed_notice_version` | NoticeUpdateNotice: referenced changed notice version. | `string` | `01` | yes |
| `changes` | NoticeUpdateNotice flattened change entries. | `array<struct<changed_section:string,change_description:string>>` | `[Row(changed_section='SEKCJA V - KWALIFIKACJA WYKONAWCÓW', change_description='Przed zmianą: Zamawiający żąda złożenia przedmiotowych środków dowodowych: 1.1. ISO 9001:2015 lub dokument równoważny wystawiony na producent` | yes |

## SmallContractNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de049e-ca25-5c8c-d903-3900019150f5` | yes |
| `noticeType` | Notice class/type. | `string` | `SmallContractNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00457688/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00457688` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-06T06:08:37.5840915Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-06` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `NULL` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `2025/BZP 00457688/01` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `[]` | yes |

## TenderResultNotice

| Field | Meaning | Observed dtype | Example value | Present in parquet |
|---|---|---|---|---|
| `objectId` | Unique notice object identifier (source payload ID). | `string` | `08de00e0-d365-14ae-d903-3900014f7bd5` | yes |
| `noticeType` | Notice class/type. | `string` | `TenderResultNotice` | yes |
| `noticeNumber` | Notice number with version (BZP format). | `string` | `2025/BZP 00451820/01` | yes |
| `bzpNumber` | Base BZP notice number (without version). | `string` | `2025/BZP 00451820` | yes |
| `publicationDate` | Notice publication timestamp (UTC string). | `string` | `2025-10-01T11:51:15.2383674Z` | yes |
| `publicationDateDay` | Publication date partition key. | `date` | `2025-10-01` | yes |
| `tenderId` | Tender-level identifier from source. | `string` | `ocds-148610-dcc1a728-f242-4593-b0e5-911ab98414ad` | yes |
| `caseId` | Case key used for cross-notice linking. | `string` | `ocds-148610-dcc1a728-f242-4593-b0e5-911ab98414ad` | yes |
| `cpvCode` | Raw CPV source string (legacy; not materialized currently). | `-` | `NULL` | no |
| `cpvCodes` | Parsed CPV code list (canonical code format). | `array<string>` | `['45000000-7']` | yes |
| `contractors` | Contractor list from metadata (array of structs/maps). | `array<map<string,string>>` | `[{'contractorNationalId': '8132743570', 'contractorName': 'P.P.H.U. ITALIA Migut Dariusz', 'contractorCountry': 'PL', 'contractorCity': 'Malawa', 'contractorProvince': None}]` | yes |
| `procedureResult` | TenderResultNotice procedure result text. | `string` | `zawarcieUmowy` | yes |
| `procedureResultParsed` | Parsed procedureResult list. | `array<string>` | `['zawarcieUmowy']` | yes |
| `trn_notice_concerns` | TenderResultNotice field 2.1 scope. | `string` | `Zamówienia publicznego` | yes |
| `trn_parts` | TenderResultNotice part-level parsed struct list. | `array<struct<part_id:string,opis:string,mainCPV:string,secondaryCPV:array<string>,expected_value:double>>` | `[Row(part_id=None, opis='1.\tPrzedmiotem zamówienia jest remont segmentów mieszkalnych w D.S. „HILTON”      . 2. Szczegółowy opis oraz sposób realizacji zamówienia zawiera Opis przedmiotu zamówienia (OPZ), stanowiący Zał` | yes |
