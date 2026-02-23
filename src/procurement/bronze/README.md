# Bronze Layer

Purpose:

- Keep `bronze_raw` payload fidelity for replay/audit.
- Validate incoming records with Pydantic models.
- Publish canonical Bronze Parquet for fast downstream Spark jobs.

Inputs:

- `data/bronze_raw/bzp_YYYY-MM-DD.json`

Primary build entrypoint:

- `scripts/build_bronze.py`

Operational intent:

- API fetch and Bronze conversion are decoupled:
- fetch step writes immutable daily payloads to `bronze_raw`,
- Bronze step validates and writes canonical Parquet.
- This split is important for reliable/high-throughput backfills.

Outputs:

- `data/bronze/notices/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/`
- `data/bronze/errors/bzp_YYYY-MM-DD_errors.json` (only when validation failures exist)
- `data/bronze/_index/seen_notice_ids/seen_notice_ids.sqlite` (cross-day duplicate index by `objectId`)

Bronze row payload notes:

- Each validated notice row includes `recordHash` (SHA-256 of canonical validated notice JSON).
- `recordHash` is deterministic and intended for future change-detection/upsert flows.
- Silver remains backward-compatible with legacy Bronze partitions that do not have `recordHash`.

Current guarantees:

- Deterministic and idempotent daily writes (partition overwrite for touched day/type).
- Validation split into valid/error outputs.
- Schema-stable canonical layer (`noticeType` + `publicationDateDay` partition contract).
- Cross-day duplicate suppression (`objectId`) before Bronze write, with same-day rerun allowance.

See also:

- `docs/deployment/OPERATING_MODES.md`
