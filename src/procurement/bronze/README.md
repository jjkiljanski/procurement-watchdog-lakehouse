# Bronze Layer

Purpose:

- Keep `bronze_raw` payload fidelity for replay/audit.
- Validate incoming records with Pydantic models.
- Publish canonical Bronze Parquet for fast downstream Spark jobs.

Inputs:

- `data/bronze_raw/bzp_YYYY-MM-DD.json`

Primary build entrypoint:

- `scripts/pipeline/build_bronze.py`

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

## Deduplication

The BZP API re-delivers notices across multiple daily fetches. Analysis of the full 2025
dataset (797,837 raw rows across 365 daily files) confirmed:

- **178,467 objectIds appeared exactly twice** in the raw JSON files.
- All duplicates were **exact-copy re-deliveries** — identical content, zero cases of
  a notice being corrected or updated between appearances.
- Every duplicate appeared in exactly two files (no objectId appeared 3+ times).

The current deduplication strategy in `build_bronze.py` is therefore correct for observed
data: a SQLite seen-index (`_index/seen_notice_ids/seen_notice_ids.sqlite`) records the
`first_target_date` for each `objectId`. On every subsequent run, any `objectId` already
in the index from a **different** target date is dropped before the Parquet write.
Same-day reruns are allowed through (idempotent).

### Known limitation — content changes not detected

The seen-index uses a **first-seen-wins** strategy without comparing `recordHash`. If the
API ever delivers a corrected version of a previously ingested notice (same `objectId`,
different content), the updated version will be **silently discarded** rather than
replacing the old record.

This has not occurred in the 2025 dataset. If it becomes a concern, the deduplication
logic should be upgraded to:

1. Compare the incoming `recordHash` against the hash stored at first ingestion.
2. If hashes differ, overwrite the Bronze partition for that `objectId` and mark it for
   Silver re-processing.
3. Update the seen-index entry with the new hash and date.

See also:

- `docs/runbooks/OPERATING_MODES.md`
