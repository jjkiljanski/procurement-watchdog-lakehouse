# Documentation Index

This folder is organized by purpose:

- `docs/data_model/`: dataset structure, parsing profile, and extraction coverage.
- `docs/legal/`: legal/domain interpretation notes.
- `docs/deployment/`: operational model, runbooks, and deployment requirements.
- `docs/runbooks/`: operational procedures (daily/backfill, restart/recovery).

Repository scope note:

- This repo is the Spark lakehouse / preparation layer.
- The current dbt analytical business logic lives in `procurement-watchdog-analytics`:
  `https://github.com/jjkiljanski/procurement-watchdog-analytics`
- The `gold` package in this repo is retained mainly for future Spark-side enrichment work, not as the canonical home of current business marts.

Quick links:

- `docs/runbooks/OPERATING_MODES.md`
- `docs/runbooks/CASE_DERIVED_CONCURRENCY.md`
- `docs/deployment/ENV_REQUIREMENTS.md`
- `docs/deployment/RUNTIME_CONTRACTS.md`
- `CONTRIBUTING.md`
