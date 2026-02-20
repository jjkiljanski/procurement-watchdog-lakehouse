# Environment Requirements

Minimum requirements for reliable deployment:

- Shared object storage for `bronze_raw`, `bronze`, `silver`, `gold`.
- Scheduler/orchestrator for daily and backfill runs.
- Lock backend for single-writer datasets (at least `case_derived_facts`).
- Sufficient Spark resources for HTML parsing-heavy Silver batches.

Storage/runtime capabilities expected:

- atomic write/replace for small metadata files (state/manifest/pointer),
- stable path semantics for partitioned parquet writes,
- retention policies for `bronze_raw` and temporary outputs.

Operational requirements:

- structured logs and run manifests persisted,
- retry policy for failed days/ranges,
- alerting on failed jobs or stalled backfills.
