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
4. Build notice-change deltas for `D` from Silver.

Example commands (local):

```bash
# Paths default to runtime-resolved values; RUNTIME_ENV=local is the default.
python scripts/pipeline/fetch_bzp_yesterday.py D
python scripts/pipeline/build_bronze.py D
python scripts/pipeline/build_silver_day.py D
python scripts/pipeline/build_silver_update_deltas.py D
```

Or use the convenience wrapper:

```bash
python scripts/ops/run_transforms_for_day.py D  # runs bronze → silver → deltas
```

On GCP this is handled automatically by the `bzp-daily` Cloud Workflow
(`workflows/daily.yaml`).  See `docs/cloud_architecture.md`.

## Mode 2: Massive Backfill / Historical Load

Goal: populate long history (months/years) with robust retries and high throughput.

Recommended split:

### Phase A: API-bound ingest (retry-friendly)

- Fetch many days to `bronze_raw` first using `fetch_bzp_range.py`.
- This phase is network/API bound and must be decoupled from Spark transforms.
- The BZP API has no published rate limit; `fetch_bzp_range.py` uses exponential
  backoff (via `src/procurement/fetch/bzp_api.py`) for all HTTP calls.
- Dates with an existing `fetch` processed-date manifest matching the current
  script hash are skipped automatically.

### Phase B: Spark-bound transforms (long-lived jobs)

1. Convert `bronze_raw` range → Bronze Parquet.
2. Build Silver range from Bronze.
3. Build notice-change deltas from Silver.

Rationale:

- Spark startup cost is significant when run per day.
- Separating API ingest from Spark compute improves failure isolation and retry behavior.
- On GCP, completing all downloads first avoids paying for Dataproc batches idling on HTTP calls.

Local backfill — Phase A (range download):

```bash
python scripts/pipeline/fetch_bzp_range.py 2025-10-01 2025-10-31
```

Skips dates that already have a matching `fetch` manifest.  Use `--force` to re-fetch all.

Local backfill — Phase B (single long-lived Spark session):

```bash
python scripts/pipeline/build_bronze_range.py \
  --start-date 2025-10-01 \
  --end-date 2025-10-31

python scripts/pipeline/build_silver_range.py \
  --start-date 2025-10-01 \
  --end-date 2025-10-31

python scripts/pipeline/build_silver_update_deltas.py \
  --start-date 2025-10-01 \
  --end-date 2025-10-31
```

The range scripts keep one SparkSession alive across all dates per stage,
amortising JVM startup overhead.  Per-(date, notice_type) manifests are written
after each batch so interrupted runs resume from where they left off.

GCP backfill restart safety (`bzp-backfill` Cloud Workflow):

- The fetch step executes `fetch_bzp_range.py` via the `bzp-downloader` Cloud Run Job.
  It checks the `fetch` manifest per date and skips already-downloaded dates.
- Each range script writes per-(date, notice_type) manifests to
  `gs://{LAKEHOUSE_BUCKET}/_processed/{layer}/{date}/{notice_type}.json` on success.
- The manifest contains a `script_hash` computed by `sha256_paths()` over the
  entry-point script **and** its stage-specific source package
  (`src/procurement/fetch/`, `bronze/`, or `silver/`).  A change to any
  `.py` file in the relevant package invalidates manifests for that stage
  only — unrelated stages are unaffected.  The hash is computed once at
  process start and reused for all per-date manifest comparisons in that run.
- Pass `force="true"` when triggering the workflow to reprocess all dates
  regardless of manifest state (e.g. after deploying updated scripts).

On GCP, triggered manually via `gcloud workflows run bzp-backfill`.
See `docs/cloud_architecture.md`.

## Observability

- **Local**: pipeline run metadata + data quality metrics written to `data/obs/` as Parquet.
- **GCP**: streamed to BigQuery dataset `BQ_OBS_DATASET` (default: `procurement_obs`).
  Tables `pipeline_runs`, `dq_metrics`, `quarantine_summary` are created automatically.
  Cloud Logging captures structured operational logs from Dataproc and Cloud Run
  automatically.

## Reliability and Idempotency Notes

- `build_bronze.py`: deterministic for the same `bronze_raw` input; writes partitioned Bronze
  by `noticeType/publicationDateDay`.  Cross-day duplicate `objectId`s are suppressed via
  a Spark query against existing Bronze Parquet.  Same-day reruns are idempotent.
- `build_silver_day.py`: deterministic and idempotent for a target day; overwrites touched
  day partitions.
- `build_silver_update_deltas.py`: overwrites the target date partition; idempotent.

## Current Performance Guidance

- Bronze: most cost is Spark startup + write path, not Pydantic validation.
- Silver bottleneck is HTML parsing and transform materialisation, especially for:
  - `ContractNotice`
  - `TenderResultNotice`
  - `ContractPerformingNotice`
- Silver uses notice-type batches and adaptive repartitioning for heavy parser types.

## Notes

Key rule:
- API fetch and Spark processing are always decoupled.
- Backfill should amortise Spark startup overhead (local: one long-lived session;
  GCP: Dataproc Serverless handles parallelism natively).
