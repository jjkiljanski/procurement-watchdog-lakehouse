# Silver Layer — Pipeline Architecture

The Silver layer converts raw Bronze Parquet notices into structured, typed
tables partitioned by `noticeType` and `publicationDateDay`.

---

## Directory layout

```
silver/
├── pipeline_orchestrator.py   Entry point: run_silver_day_core()
├── common_envelope.py         build_envelope_df() + validate_envelope_schema()
│
├── section_pipeline/          ◄ PRIMARY: profile-driven section tables
│   ├── notice_schema_reader.py  Load & query notice_schemas/*_profile.json files
│   ├── raw_section_extractor.py HTML → raw section dict (BeautifulSoup + profile)
│   │                            Also contains low-level soup helpers (_find_h3, _span_value, …)
│   ├── spark_table_builder.py   Spark: build + explode section DataFrames; apply column parsers
│   ├── parser_registry.py       Registry of str→typed column-level parsers
│   └── final_schema_validator.py  Driver-side Pydantic schema check per section table
│
├── section_value_parsers/     Column-level value parsers (str → typed)
│   ├── common.py              Shared: parse_tak_nie, parse_pln_value,
│   │                          parse_date_from_text, parse_cpv_codes,
│   │                          classify_polish_national_id, _normalize_label_text, …
│   ├── types.py               ParsedValues type alias
│   └── <notice_type>.py       Per-type placeholders — implement here, then register
│                              in section_pipeline/parser_registry.py
│
├── notice_schemas/            Per-notice-type schemas
│   ├── __init__.py            normalized_notice_type_token()
│   ├── *_profile.json         Section number → {col_name, data_model, parser?}
│   └── *_models.py            Pydantic models (all fields str|None until typed)
│
└── legacy/                    Old HTML-parsing pipeline preserved for reference
```

---

## Two parallel pipelines

### 1. Section table pipeline  *(primary, new)*

Converts each HTML notice into one Parquet table **per data model** (core, part, client, …).
Every section becomes a column; values are raw strings until column parsers are configured.

```
Bronze Parquet
    │
    ▼  make_html_sections_udf()          [section_pipeline/spark_table_builder.py]
    │  BeautifulSoup + profile JSON  →  JSON { "core": {…}, "part": [{…}] }
    │
    ▼  build_section_tables()            [section_pipeline/spark_table_builder.py]
    │  from_json / posexplode  →  one DataFrame per data_model
    │
    ▼  apply_column_parsers()            [section_pipeline/spark_table_builder.py]
    │  UDF per column where profile has "parser": {"fn": "…"}
    │  (all parsers currently null → no-op; activated as section_value_parsers are implemented)
    │
    ▼  validate_section_models()         [section_pipeline/final_schema_validator.py]
    │  Driver-side: import *_models.py, warn on missing columns
    │
    ▼  Parquet write
       data/silver/notice_type_tables/
         noticeType=<TYPE>/
           data_model=<MODEL>/
             publicationDateDay=<DATE>/
```

**How to add a column parser:**
1. Implement the function in `section_value_parsers/common.py` (shared across types) or
   `section_value_parsers/<snake_type_name>.py` (type-specific).
2. Register it in `section_pipeline/parser_registry.py` → `COMMON_PARSERS`
   (or `NOTICE_TYPE_PARSERS["MyType"]` for a type-specific one).
3. Add `"parser": {"fn": "my_fn"}` to the relevant entry in
   `notice_schemas/<type>_profile.json`.
4. Update the corresponding Pydantic field in `notice_schemas/<type>_models.py`
   to match the function's return type.

### 2. Envelope pipeline  *(simplified, no HTML parsing)*

Produces the `common_envelope` table: all Bronze structured columns (except the
internal `recordHash` hash and the `htmlBody` blob) plus a small set of derived
columns that require only dictionary lookups or pure Spark expressions.
Defined in `common_envelope.py`.

```
Bronze Parquet
    │
    ▼  build_envelope_df()               [common_envelope.py]
    │  All Bronze structured cols + clientTypeName, provinceName,
    │  caseId (coalesce tenderId/objectId), noticeStage (from noticeType)
    │
    ▼  validate_envelope_schema()        [common_envelope.py]
    │  Driver-side column-presence check (warns on drift)
    │
    ▼  Parquet write
       data/silver/common_envelope/publicationDateDay=<DATE>/
```

The Pydantic model `CommonEnvelopeRow` in `common_envelope.py` documents the
expected schema and is used for driver-side validation.

---

## Section profile JSON schema

Each entry in `notice_schemas/<type>_profile.json`:

```json
"2.7": {
  "col_name":       "section_2_7",
  "section_header": "Czy dopuszcza się złożenie oferty częściowej",
  "data_model":     "core",
  "example_values": ["Tak", "Nie"],
  "parser":         null
}
```

| Key | Meaning |
|---|---|
| `col_name` | Spark / Parquet column name |
| `section_header` | Polish section title (informational) |
| `data_model` | `core` (one row/notice) · `part` · `client` · `part.part` (two-level) · … |
| `example_values` | Representative real values (informational) |
| `parser` | `null` or `{"fn": "parse_tak_nie"}` — activates a column-level UDF |

---

## How notice HTML is parsed based on schema definitions

The profile JSON for each notice type (`notice_schemas/<type>_profile.json`) is the complete instruction set for the parser. It maps every BZP section number to a column name, an entity type, and an optional typed parser. The HTML parser itself contains no notice-type-specific knowledge — all of it lives in the profile.

### What the profile defines

Every entry maps one numbered section (e.g. `"4.2.2"`) to:

- **`col_name`** — the Silver column name for this section's value
- **`data_model`** — the entity type this section belongs to: `core` (once per notice), `client` (once per contracting authority), `part` / `part.core` (once per contract part), `part.part` (once per sub-item within a part, e.g. an evaluation criterion)
- **`parser`** — optional typed parser function (`parse_tak_nie`, `parse_pln_value`, etc.) applied after extraction; `null` means the raw string is kept as-is

Sections not listed in the profile are silently skipped and recorded in `_unknown_sections`.

### Reading sections from HTML

BZP HTML is a flat sequence of `<h3>` headings. Each heading carries a section number in its text and a value either inline (inside a `<span class="normal">`) or in the text or `<p>` elements that immediately follow it. The parser scans all headings in document order, extracts the number and value from each, looks the number up in the profile, and stores the value under the `col_name` defined there.

### Splitting repeating entities

Notices with multiple parts, authorities, or criteria serialise all of them as one continuous flat sequence of headings — there are no explicit delimiters. The parser identifies entity boundaries by watching for section numbers that go backwards within a given `data_model` group: when a section number is lower than or equal to the last section number seen for that entity type, a new instance of that entity has started.

For example: if the last heading for a `part`-level section was `4.3.5` and the next `part`-level heading is `4.3.4`, the parser closes the current part and opens a new one. Each entity type (`part`, `client`, `part.part`, …) is tracked independently, so a reset in one does not affect the others. When a parent entity resets (e.g. a new part starts), all its child counters (e.g. criteria within that part) are reset too.

### Output

The parser produces a nested structure grouping values by entity type:

```
core    →  flat dict of section values (one per notice)
client  →  list of dicts, one per contracting authority
part    →  list of dicts, one per contract part;
           each may contain a nested sub-list (e.g. "part" for criteria rows)
```

`spark_table_builder.py` explodes this into one Spark DataFrame per entity type, where each row is one entity instance and each column corresponds to one profile entry.

---

## Data model hierarchy

| `data_model` value | Table shape | Example notice types |
|---|---|---|
| `core` | 1 row per notice | all |
| `client` | 1 row per buyer | ContractNotice, TenderResultNotice |
| `part` | 1 row per contract part | ContractNotice, TenderResultNotice, … |
| `part.core` | 1 row per part (core fields) | ContractNotice |
| `part.part` | 1 row per sub-item within a part | NoticeUpdateNotice |
| `criterion_procedure` | 1 row per evaluation criterion | CompetitionNotice, ConcessionNotice |
| `criterion_qualification` | 1 row per qualification criterion | ConcessionNotice |
| `change_matter` | 1 row per contract change | ContractPerformingNotice |

---

## Running the pipeline

### Development workflow (no rebuild on code changes)

Build the deps-only image **once** (or after adding/upgrading dependencies).
The `--build-arg DEV=1` flag installs only the Python dependencies without
baking in the source code:

```bash
docker build --build-arg DEV=1 -t procurement-silver:deps .
```

Run with source/scripts/refs mounted — code changes are picked up immediately,
no image rebuild needed:

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "<repo-root>/src:/app/src:ro" \
  -v "<repo-root>/scripts:/app/scripts:ro" \
  -v "<repo-root>/refs:/app/refs:ro" \
  -v "<bronze-root>:/data/bronze:ro" \
  -v "<silver-root>:/data/silver" \
  procurement-silver:deps \
  python scripts/pipeline/build_silver_day.py 2025-10-01 \
    --bronze-dir /data/bronze \
    --silver-dir /data/silver \
    --spark-master "local[*]"
```

### Production / CI workflow

Build the full self-contained image (bakes source in, no volumes needed):

```bash
docker build -t procurement-silver:latest .
```

```bash
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "<bronze-root>:/data/bronze:ro" \
  -v "<silver-root>:/data/silver" \
  procurement-silver:latest \
  python scripts/pipeline/build_silver_day.py 2025-10-01 \
    --bronze-dir /data/bronze \
    --silver-dir /data/silver \
    --spark-master "local[*]"
```
