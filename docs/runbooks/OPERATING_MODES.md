# Operating Modes

This project has two operational modes with different optimization goals.

The pipeline can run **locally** (`RUNTIME_ENV=local`) or on **Google Cloud
Platform** (`RUNTIME_ENV=gcp`).  The mode is switched via environment
variables — no code changes required.  See `docs/cloud_architecture.md` for
GCP setup and `config/runtime_local.env` / `config/runtime_gcp.env.example`
for the full variable reference.

## Mode 1: Daily Incremental (CRON)

Goal: ingest yesterday's data and refresh downstream layers quickly and deterministically.

Suggested sequence for day `D`:

1. Fetch API notices for `D` into `bronze_raw`.
2. Convert `bronze_raw(D)` to canonical Bronze Parquet.
3. Build Silver for `D` from Bronze.
4. Update `silver/case_derived_facts` in `incremental` mode for `D`.

Example commands (local):

```bash
# Paths default to runtime-resolved values; RUNTIME_ENV=local is the default.
python scripts/pipeline/fetch_bzp_yesterday.py D
python scripts/pipeline/build_bronze.py D
python scripts/pipeline/build_silver_day.py D
python scripts/pipeline/build_silver_update_deltas.py D
python scripts/pipeline/build_case_derived_facts.py D --mode incremental
```

On GCP this is handled automatically by the `bzp_daily` Airflow DAG
(`dags/daily_dag.py`).  See `docs/cloud_architecture.md`.

## Mode 2: Massive Backfill / Historical Load

Goal: populate long history (months/years) with robust retries and high throughput.

Recommended split:

### Phase A: API-bound ingest (retry-friendly)

- Fetch many days to `bronze_raw` first.
- This phase is network/API bound and should be decoupled from Spark transforms.

### Phase B: Spark-bound transforms (long-lived jobs)

1. Convert `bronze_raw` range -> Bronze Parquet.
2. Build Silver range from Bronze in one long-lived Spark run with checkpoint state.
3. Build `case_derived_facts` (`full` once for initial snapshot, then `incremental` for new arrivals).

Rationale:

- Spark startup cost is significant when run per day.
- Backfill should prefer long-lived Spark runs or parallel day workers to amortize startup overhead.
- Separating API ingest from Spark compute improves failure isolation and retry behavior.

Recommended backfill command:

```bash
python scripts/pipeline/build_silver_backfill.py \
  --start-date 2025-10-01 \
  --end-date 2025-10-31 \
  --bronze-dir data/bronze \
  --silver-dir data/silver
```

Restart safety:

- `build_silver_backfill.py` writes an explicit state index (`data/silver/_state/silver_backfill_<start>_<end>.json` by default).
- Only days marked `completed` in state are skipped on resume.
- Any interrupted/non-completed day is fully cleaned and rebuilt, so partial writes are never treated as done.

Restart safety:

- `build_silver_backfill.py` writes an explicit state index
  (`{silver_dir}/_state/silver_backfill_<start>_<end>.json` by default).
- Only days marked `completed` in state are skipped on resume.

On GCP this is handled by the `bzp_backfill` Airflow DAG (`dags/backfill_dag.py`),
triggered manually via the Airflow UI.  See `docs/cloud_architecture.md`.

Lineage metadata:

- fetch → `data/obs/pipeline_runs/` (local) or skipped (GCP — TODO extend obs.py for GCS)
- bronze → `data/bronze/errors/bzp_YYYY-MM-DD_errors.json` (validation failures)
- bronze dedup → Spark query against existing Bronze Parquet (was SQLite — removed for GCS compatibility)
- silver → `data/obs/pipeline_runs/` (local)

## Reliability and Idempotency Notes

- `build_bronze.py`: deterministic for the same `bronze_raw` input; writes partitioned Bronze by `noticeType/publicationDateDay`.
- `build_bronze.py`: suppresses cross-day duplicate notices by `objectId` using persistent seen-index; same-day reruns are still allowed.
- `build_silver_day.py`: deterministic and idempotent for a target day; overwrites touched day partitions.
- `build_case_derived_facts.py`:
  - `full`: rebuilds snapshot as-of target day.
  - `incremental`: recomputes only touched cases and merges with nearest snapshot.

## Current Performance Guidance

- Bronze is not currently bottlenecked by Pydantic validation; most cost is Spark startup + write path.
- Silver bottleneck is primarily HTML parsing and transform materialization, especially for:
  - `ContractNotice`
  - `TenderResultNotice`
  - `ContractPerformingNotice`
- Silver uses notice-type batches and adaptive repartitioning for heavy parser types to improve intra-day parallelism.

## Notes

Key rule:
- API fetch and Spark processing are decoupled.
- Backfill should amortize Spark startup overhead.
