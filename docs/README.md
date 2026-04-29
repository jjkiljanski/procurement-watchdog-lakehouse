# Documentation Index

This folder is organized by purpose:

- `docs/data_model/` — dataset structure, parsing profiles, and HTML extraction coverage.
- `docs/legal/` — legal and domain interpretation notes (notice types, contracting authority types, procurement procedures).
- `docs/deployment/` — environment requirements and runtime contracts.
- `docs/runbooks/` — operational procedures (local Docker setup, operating modes, quarantine handling).

Top-level reference documents:

- [`cloud_architecture.md`](cloud_architecture.md) — GCP service topology and data flow.
- [`iceberg.md`](iceberg.md) — Iceberg table layout and partitioning decisions.
- [`logging.md`](logging.md) — structured JSON logging conventions.
- [`observability.md`](observability.md) — pipeline run tracking and DQ metrics.
- [`dataproc_tuning.md`](dataproc_tuning.md) — Dataproc Serverless configuration and tuning notes.

Repository scope note:

- This repo is the ingestion and transformation layer (bronze → silver).
- Analytical business logic lives in `procurement-watchdog-analytics`:
  `https://github.com/jjkiljanski/procurement-watchdog-analytics`

Quick links:

- [`docs/runbooks/OPERATING_MODES.md`](runbooks/OPERATING_MODES.md)
- [`docs/runbooks/QUARANTINE.md`](runbooks/QUARANTINE.md)
- [`docs/deployment/ENV_REQUIREMENTS.md`](deployment/ENV_REQUIREMENTS.md)
- [`docs/deployment/RUNTIME_CONTRACTS.md`](deployment/RUNTIME_CONTRACTS.md)
- [`CONTRIBUTING.md`](../CONTRIBUTING.md)
