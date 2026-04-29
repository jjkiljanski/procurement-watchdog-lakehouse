# Silver Quarantine Logic

Silver's new pipeline path (`pipeline_orchestrator.py`) applies several validation layers in
sequence. Each layer either routes bad rows to a quarantine table or records errors inline on
the row itself. The table below summarises all cases.

| Case | Name | Fatal? | Row in silver? | Written to quarantine? |
|------|------|--------|----------------|------------------------|
| 0 | HTML structural error | yes | no | yes |
| 1 | Unknown section numbers | no | yes | yes (for monitoring) |
| 2 | Column parser failure | no | yes (column = None) | no |
| 3 | Pydantic contract violation | yes | no | yes |
| 4 | No registered profile | yes | no | yes |

---

## Case 0 — HTML structural error

**When**: `raw_html_sections_parser.py` encounters a structural problem it cannot recover from,
such as a duplicate core section that would overwrite already-parsed data. This indicates the
notice HTML deviates from the expected document structure.

**Effect**: The entire row is excluded from silver and written to quarantine with
`quarantine_case = "case_0_html_parse"`.

---

## Case 1 — Unknown section numbers

**When**: The parser finds a `<h3>` section number that has no entry in the notice-type profile
JSON (e.g. `contract_notice_profile.json`). The section is simply ignored during parsing.

**Effect**: The row is **kept** in silver unchanged — missing a section is not a data defect.
The row is **also written to quarantine** (with the unknown section numbers logged) so that
engineers can discover sections that should be added to the profile. This is a monitoring
signal, not a data quality error.

---

## Case 2 — Column parser failure (non-fatal)

**When**: A registered column parser (`parse_tak_nie`, `parse_pln_value`, `parse_date_from_text`,
etc.) raises any exception on a raw section value. Parsers are wrapped in fault-tolerant UDFs
(`_make_fault_tolerant_udf` in `spark_table_builder.py`) that catch all exceptions.

**Effect**:
- The column that failed gets `None` instead of a typed value.
- The error message is appended to the row's `parse_errors` column (`array<string>`).
- The row is **kept** in silver with `parse_errors` populated.
- Nothing is written to quarantine.

The `parse_errors` column is `None` when all parsers succeed and is non-empty only when at
least one column could not be parsed. Downstream consumers can filter on
`size(parse_errors) = 0` for fully-clean rows, or include rows with parse errors and treat
affected columns as missing data.

Typical causes: unexpected text values (`'nie dotyczy'` in a boolean column), malformed
numbers, or edge-case date formats not yet handled by the parser. These represent data
variation in source HTML, not bugs in the pipeline.

---

## Case 3 — Pydantic contract violation (programming-contract check)

**When**: After `apply_column_parsers` has run, the invariant should hold: every column is
either a correctly-typed value or `None`. `apply_pydantic_validation` checks this invariant
by running the Pydantic section model against each row as a JSON object.

If Pydantic raises a `ValidationError` here it means a parser returned a value of the wrong
type without raising an exception (e.g. returned a `str` where `int` was expected and didn't
raise `ParseError`). This is a **bug in the parser implementation**, not bad source data.

**Effect**: The offending rows are quarantined with `quarantine_case = "case_3_pydantic"` so
the programming error surfaces clearly rather than silently corrupting silver.

In practice this case **should never fire** when all parsers are correctly implemented.

---

## Case 4 — No registered profile

**When**: The notice type of the incoming record has no profile JSON registered in
`notice_schema_reader.py` (`NOTICE_TYPE_TO_PROFILE_KEY` mapping).

**Effect**: The row cannot be parsed at all and is excluded from silver.

---

## Quarantine table location

Apache Iceberg table at `iceberg/common/quarantine/` (locally: `data/iceberg/common/quarantine/`; GCP: `gs://{bucket}/iceberg/common/quarantine/`).

Partitioned by `(publicationDateDay, notice_type)`. Accessed via `silver.common.quarantine` in Spark.

## parse_errors column

The `parse_errors` column is present on all silver section tables produced by the new path.
Schema: `array<string>`. Value is `null` when no parser errors occurred on that row.

To find rows with any parse error:

```sql
SELECT * FROM silver_core WHERE size(parse_errors) > 0
```

To find clean rows only:

```sql
SELECT * FROM silver_core WHERE parse_errors IS NULL
```
