# Operating Model

Two modes are supported:

1. Daily incremental
- fetch `D-1` -> `bronze_raw`
- build `bronze` for `D-1`
- build `silver` for `D-1`
- update `case_derived_facts` incremental for `D-1`
- build `gold` daily for `D-1`

2. Massive backfill
- fetch a date range to `bronze_raw`
- run long-lived Spark transforms:
  - `bronze_raw -> bronze`
  - `bronze -> silver` (resumable backfill)
  - `case_derived_facts` rebuild/update
  - `gold` rebuild/as-of snapshots

Key rule:
- API fetch and Spark processing are decoupled.
- Backfill should amortize Spark startup overhead.
