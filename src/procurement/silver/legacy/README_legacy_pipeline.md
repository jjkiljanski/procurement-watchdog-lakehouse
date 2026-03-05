# Legacy Silver Code

Files here are **preserved for reference only** — they are not imported by
any active pipeline code and are not guaranteed to run.

## Why they were kept

These modules represent two earlier design phases that were superseded by the
current profile-driven section pipeline (`section_pipeline/`).

## Contents

### `html_parsing/`

| File | Origin | Why legacy |
|---|---|---|
| `orchestrator.py` | `html_orchestrator.py` | Orchestrated the old two-step HTML parse (soup → sections model → typed values). Replaced by `section_pipeline/spark.py` + `section_pipeline/html_extractor.py`. |
| `sections_parser.py` | `html_sections_parser.py` | Intermediate section extraction layer; superseded by `section_pipeline/html_extractor.py` (profile-driven). |

### `field_parsers/`

| File | Origin | Why legacy |
|---|---|---|
| `registry.py` | `html_value_parsers/registry.py` | Dispatched to per-notice-type full-HTML parsers. The active registry is now `section_pipeline/column_parsers.py` (column-level, profile-driven). |
| `types.py` | `html_value_parsers/types.py` | `ParsedValues = dict[str, Any]` type alias used only by the legacy per-type stubs. |
| `__init__.py` | `html_value_parsers/__init__.py` | Re-exported `PARSER_REGISTRY` / `parse_notice_values`; no active caller. |
| `<notice_type>.py` (×14) | `html_value_parsers/<type>.py` | Stub parsers returning `{}`. Will be re-implemented as column-level parsers inside `field_parsers/common.py` and `section_pipeline/column_parsers.py` when Gold typing is introduced. |

### Docs

| File | Why legacy |
|---|---|
| `HTML_PARSING_MODULARIZATION.md` | Design notes from the HTML modularisation phase (now complete). |
| `MODEL_NOTICETYPE_SPLIT.md` | Design notes from the per-notice-type model split (now complete). |

## What replaced them

See `../PIPELINE.md` for the current silver pipeline architecture.
