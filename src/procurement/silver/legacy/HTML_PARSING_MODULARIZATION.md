# Silver HTML Parsing Modularization

This document describes the new modular parsing scaffold introduced to replace
the monolithic `html_parser.py` incrementally and safely.

## Goal

Separate HTML parsing into clear stages so each concern is isolated:

1. split HTML into section-based raw model,
2. run notice-type-specific value parsing,
3. orchestrate both steps through a stable entrypoint.

## Modules

### `parser_utils.py`

Shared low-level helpers for section handling (naming, section number parsing,
value extraction primitives).  
This module is the target place for reusable logic moved out of `html_parser.py`.

### `html_sections_parser.py`

Facade over the current profile-driven section splitter
(`raw_html_sections_parser.py`), exposing:

- `build_notice_sections_model(...)`
- section-number/value helpers

It provides a stable import surface for the new pipeline while internals evolve.

### `html_value_parsers/`

Notice-type-specific parsers (`ContractNotice`, `TenderResultNotice`, etc.).

- `registry.py` keeps `PARSER_REGISTRY` and dispatch helper `parse_notice_values(...)`.
- one file per notice type contains parser function scaffold:
  `parse_<notice_type>(sections_model, soup, procedure_result)`.

Current state: parser stubs intentionally return `{}` until migration of
production extraction logic is completed.

### `html_orchestrator.py`

New composition entrypoint:

1. builds BeautifulSoup,
2. builds section model,
3. dispatches notice parser by notice type,
4. returns:
   - `notice_type`
   - `sections_model`
   - `parsed_values`

Function:
- `parse_notice_html(...)`

## Migration Strategy

1. Keep existing production path in `html_parser.py` unchanged.
2. Move reusable helpers to `parser_utils.py`.
3. Migrate one notice type at a time from `html_parser.py` into
   `html_value_parsers/<notice>.py`.
4. Add tests per notice parser as migration progresses.
5. Switch production integration to `html_orchestrator.py` only after feature parity.

This avoids large risky rewrites and keeps behavior debuggable.
