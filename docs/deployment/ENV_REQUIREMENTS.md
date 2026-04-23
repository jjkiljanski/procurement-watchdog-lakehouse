# Environment Requirements

Minimum requirements for reliable deployment:

- Shared object storage for `bronze_raw`, `bronze`, `silver`.
- Scheduler/orchestrator for daily and backfill runs.
- Sufficient Spark resources for HTML parsing-heavy Silver batches.

Storage/runtime capabilities expected:

- atomic write/replace for small metadata files (state/manifest/pointer),
- stable path semantics for partitioned Parquet writes,
- retention policies for `bronze_raw` (short-lived; 7 days recommended) and
  temporary outputs.

Operational requirements:

- structured logs and pipeline run metadata persisted (local: `data/obs/`
  Parquet; GCP: BigQuery obs tables),
- retry policy for failed days/ranges,
- alerting on failed jobs or stalled backfills.
