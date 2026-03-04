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

### 2. Envelope pipeline  *(legacy, still active)*

Produces the `common_envelope` table (shared columns used across all notice types)
and the `htmlExtracted` struct (soup-based typed fields per notice type).
Implemented in `spark_transforms.py` + `html_parsing/parser.py`.

```
Bronze Parquet
    │
    ▼  build_silver_for_notice_type()    [spark_transforms.py]
    │  Metadata columns + legacy soup extraction (parse_html UDF)
    │
    ▼  with_notice_validation_errors()   [validation.py]
    │
    ▼  Parquet write
       data/silver/common_envelope/publicationDateDay=<DATE>/
```

> **Roadmap**: once the section table pipeline covers all typed fields, the envelope
> pipeline will be simplified to metadata-only (no soup extraction) and
> `html_parsing/parser.py` will move to `legacy/`.

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

```bash
# Inside the procurement-silver:dev Docker image (Dockerfile.launcher)
MSYS_NO_PATHCONV=1 docker run --rm \
  -v "<bronze-root>:/data/bronze:ro" \
  -v "<silver-root>:/data/silver" \
  procurement-silver:dev \
  python scripts/pipeline/build_silver_day.py 2025-10-01 \
    --bronze-dir /data/bronze \
    --silver-dir /data/silver \
    --spark-master "local[*]"
```

Build the image (only needed after code changes):

```bash
docker build -f Dockerfile.launcher -t procurement-silver:dev .
```
