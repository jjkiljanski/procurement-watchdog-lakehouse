# Operating Modes

This project has two operational modes with different optimization goals.

## Mode 1: Daily Incremental (CRON)

Goal: ingest yesterday's data and refresh downstream layers quickly and deterministically.

Suggested sequence for day `D`:

1. Fetch API notices for `D` into `bronze_raw`.
2. Convert `bronze_raw(D)` to canonical Bronze Parquet.
3. Build Silver for `D` from Bronze.
4. Update `silver/case_derived_facts` in `incremental` mode for `D`.
5. Build Gold marts/signals for `D` (`--scope daily`).

Example commands:

```bash
python scripts/pipeline/fetch_bzp_yesterday.py D --output-dir data/bronze_raw
python scripts/pipeline/build_bronze.py D --bronze-raw-dir data/bronze_raw --bronze-dir data/bronze
python scripts/pipeline/build_silver.py D --input-layer bronze --bronze-dir data/bronze --silver-dir data/silver
python scripts/pipeline/build_case_derived_facts.py D --mode incremental --silver-dir data/silver
python scripts/pipeline/build_gold.py D --scope daily --silver-dir data/silver --gold-dir data/gold
```

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
4. Build Gold (daily or as-of snapshots, depending on use case).

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

Lineage metadata:

- fetch -> `data/bronze_raw/_meta/fetch_day=YYYY-MM-DD.json`
- bronze -> `data/bronze/_meta/day=YYYY-MM-DD.json`
- bronze dedup index -> `data/bronze/_index/seen_notice_ids/seen_notice_ids.sqlite`
- silver -> `data/silver/_meta/day=YYYY-MM-DD.json`

## Reliability and Idempotency Notes

- `build_bronze.py`: deterministic for the same `bronze_raw` input; writes partitioned Bronze by `noticeType/publicationDateDay`.
- `build_bronze.py`: suppresses cross-day duplicate notices by `objectId` using persistent seen-index; same-day reruns are still allowed.
- `build_silver.py`: deterministic and idempotent for a target day; overwrites touched day partitions.
- `build_case_derived_facts.py`:
  - `full`: rebuilds snapshot as-of target day.
  - `incremental`: recomputes only touched cases and merges with nearest snapshot.
- `build_gold.py`: overwrites target-date partitions for each output mart/signal dataset.

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
