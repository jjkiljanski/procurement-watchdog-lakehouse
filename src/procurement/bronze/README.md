# Bronze Layer

Purpose:

- Preserve source payload fidelity.
- Validate incoming records with Pydantic.
- Keep a stable ingestion artifact for reproducibility.

Inputs:

- `data/raw/bzp_YYYY-MM-DD.json`

Primary build entrypoint:

- `scripts/build_bronze.py`

Outputs:

- `data/bronze/bzp_YYYY-MM-DD.json`
- `data/bronze/bzp_YYYY-MM-DD_errors.json` (only when validation failures exist)

Current guarantees:

- Append-like daily snapshots.
- Validation split into valid/error outputs.
- HTML body is hashed in bronze output for safe storage while preserving referential traceability.

