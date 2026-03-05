# Silver Layer — Pipeline Architecture

The Silver layer converts raw Bronze Parquet notices into structured, typed
tables partitioned by `noticeType` and `publicationDateDay`.

---

## Directory layout

```
silver/
├── build_core.py              Entry point: run_silver_day_core()
├── spark_transforms.py        Envelope transforms (legacy HTML path, see below)
├── validation.py              Common envelope validation rules
├── models.py                  Shared Pydantic / dataclass models
│
├── section_pipeline/          ◄ PRIMARY: profile-driven section tables
│   ├── profile.py             Load & query *_sections_profile.json files
│   ├── html_extractor.py      HTML → sections model (BeautifulSoup + profile)
│   ├── spark.py               Spark: build + explode section DataFrames; apply column parsers
│   ├── column_parsers.py      Registry of str→typed column-level UDFs
│   └── validation.py          Driver-side Pydantic schema check per section table
│
├── html_parsing/              BeautifulSoup utilities + legacy HTML parser (envelope path)
│   ├── utils.py               Low-level soup helpers (_find_h3, _span_value, …)
│   └── parser.py              Legacy full-HTML parser; still used by spark_transforms.py
│
├── field_parsers/             Column-level value parsers
│   └── common.py              Shared parsers: parse_tak_nie, parse_pln_value,
│                              parse_date_from_text, parse_cpv_codes, national-ID classifiers, …
│
├── notice_sections/           Per-notice-type schemas
│   ├── *_sections_profile.json  Section number → {col_name, data_model, parser?}
│   ├── *_models.py              Auto-generated Pydantic models (all fields str|None for now)
│   └── definitions.py           Envelope column lists + normalized_notice_type_token()
│
└── legacy/                    Unused code preserved for reference (see legacy/README.md)
```

---

## Two parallel pipelines

### 1. Section table pipeline  *(primary, new)*

Converts each HTML notice into one Parquet table **per data model** (core, part, client, …).
Every section becomes a column; values are raw strings until column parsers are configured.

```
Bronze Parquet
    │
    ▼  make_html_sections_udf()          [section_pipeline/spark.py]
    │  BeautifulSoup + profile JSON  →  JSON { "core": {…}, "part": [{…}] }
    │
    ▼  build_section_tables()            [section_pipeline/spark.py]
    │  from_json / posexplode  →  one DataFrame per data_model
    │
    ▼  apply_column_parsers()            [section_pipeline/spark.py]
    │  UDF per column where profile has "parser": {"fn": "…"}
    │  (currently all null → no-op; activated when Gold types are introduced)
    │
    ▼  validate_section_models()         [section_pipeline/validation.py]
    │  Driver-side: import *_models.py, warn on missing columns
    │
    ▼  Parquet write
       data/silver/notice_type_tables/
         noticeType=<TYPE>/
           data_model=<MODEL>/
             publicationDateDay=<DATE>/
```

**How to add a column parser:**
1. Register the function in `section_pipeline/column_parsers.py` → `COMMON_PARSERS`
   (or `NOTICE_TYPE_PARSERS["MyType"]` for a type-specific one).
2. Add `"parser": {"fn": "my_fn"}` to the relevant entry in
   `notice_sections/<type>_sections_profile.json`.
3. Update the corresponding Pydantic field in `notice_sections/<type>_models.py`
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

Each entry in `notice_sections/<type>_sections_profile.json`:

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

## Data model hierarchy

| `data_model` value | Table shape | Example notice types |
|---|---|---|
| `core` | 1 row per notice | all |
| `client` | 1 row per buyer | ContractNotice, TenderResultNotice |
| `part` | 1 row per contract part | ContractNotice, TenderResultNotice, … |
| `part.part` | 1 row per sub-item within a part | NoticeUpdateNotice |
| `criterion_procedure` | 1 row per evaluation criterion | CompetitionNotice, ConcessionNotice |
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
