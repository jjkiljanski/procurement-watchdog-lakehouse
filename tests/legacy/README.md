# Legacy tests

Tests for code that now lives in `src/procurement/silver/legacy/`.

These tests import modules that were part of the old monolithic HTML-parsing
silver pipeline and are kept here for reference while the legacy source code
is still being reviewed before final deletion.

Do not fix broken imports here — fix or replace the tests in the active
subtrees (`tests/silver/`, `tests/bronze/`, etc.) instead.
