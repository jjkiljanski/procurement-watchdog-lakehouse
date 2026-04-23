# Observability

This document describes how observability is implemented across the procurement pipeline
(fetch → bronze → silver).

---

## Design principles

- **Emit during the run.** Each pipeline step writes its own obs records. No separate scraping.
- **One dashboard script.** `build_obs.py` reads obs tables, computes silver snapshots,
  prints a Markdown dashboard, and enforces thresholds.
- **Backend-agnostic write interface.** `obs.py` routes to local Parquet or BigQuery based
  on `RUNTIME_ENV` — no caller changes needed.

---

## Write backends

| Runtime | Backend | Location |
|---|---|---|
| `local` | pyarrow Parquet, date-partitioned | `data/obs/` (or custom `obs_dir`) |
| `gcp` | BigQuery streaming inserts | dataset `BQ_OBS_DATASET` (default: `procurement_obs`) |

In GCP mode, tables and the dataset are created automatically on first write.
Cloud Logging captures structured operational logs from Dataproc and Cloud Run
automatically alongside these metrics.

---

## Obs tables

**Local**: all tables live under `data/obs/`, date-partitioned as `dt=YYYY-MM-DD/`.
**GCP**: BigQuery tables in the `BQ_OBS_DATASET` dataset.

### `pipeline_runs/`

One row per layer run. Written by each pipeline step on completion.

| column | type | description |
|---|---|---|
| `layer` | string | `fetch` / `bronze` / `silver` / `gold` |
| `target_date` | string | YYYY-MM-DD |
| `run_id` | string | unique per run |
| `started_at` | string | ISO 8601 UTC |
| `completed_at` | string | ISO 8601 UTC |
| `status` | string | `ok` / `empty` / `failed` |
| `git_commit` | string | short SHA from `$GIT_COMMIT` env var or `git rev-parse --short HEAD` |
| `script_hash` | string | SHA-256 of the entry-point script file |
| `written_at` | string | ISO 8601 UTC — when this row was written (see [Re-run semantics](#re-run-semantics)) |
| `count_*` | int | layer-specific counts (e.g. `count_raw_total`, `count_valid_total`) |
| `extra_json` | string | overflow JSON |

### `dq_metrics/`

Tall-format (one row per metric). Written by each pipeline step + `build_obs.py`.

| column | type | description |
|---|---|---|
| `layer` | string | `bronze` / `silver` / `gold` |
| `target_date` | string | YYYY-MM-DD |
| `notice_type` | string | notice type or `__all__` / `__obs_snapshot__` |
| `metric_name` | string | e.g. `valid_rate`, `nonnull_caseId` |
| `metric_value` | float | |
| `written_at` | string | ISO 8601 UTC |

### `quarantine_summary/`

One row per notice_type per day where quarantine rows were written. Written by silver.

| column | type | description |
|---|---|---|
| `target_date` | string | YYYY-MM-DD |
| `notice_type` | string | |
| `row_count` | int | number of quarantined rows |
| `written_at` | string | ISO 8601 UTC |

---

## Who writes what

| Step | Module | Writes |
|---|---|---|
| `fetch_bzp_yesterday.py` | `procurement.obs` | `pipeline_runs` (layer=fetch) |
| `build_bronze.py` | `procurement.obs` | `pipeline_runs` (layer=bronze) + `dq_metrics` (valid_rate, dedup_rate) |
| `build_silver.py` (legacy) | `procurement.obs` | `pipeline_runs` (layer=silver) + `dq_metrics` (envelope + per-batch) + `quarantine_summary` |
| `pipeline_orchestrator.py` (backfill) | `procurement.obs` | same as silver |
| `build_obs.py` | `procurement.obs` | `dq_metrics` (silver snapshot + gold stats) |

---

## Pipeline step: `build_obs.py`

Runs as the last step in `run_pipeline.py` (replaces the old `build_run_stats.py`).

1. Reads silver output for `target_date` → computes row counts, non-null rates, distinct
   counts → writes to `dq_metrics` with `notice_type=__obs_snapshot__`.
2. Reads gold output for `target_date` → computes row counts per dataset → writes to
   `dq_metrics` with `notice_type=<dataset_name>`.
3. Reads obs tables (last 14 days) → renders a Markdown dashboard to stdout.
4. Checks configured thresholds → exits non-zero on violation.

---

## Central writer: `src/procurement/obs.py`

All obs writes go through three functions:

```python
from procurement.obs import write_pipeline_run, write_dq_metrics, write_quarantine_summary
```

The module also exports shared utilities: `now_utc_iso`, `atomic_write_json`, `sha256_file`,
`git_commit_sha`.

---

## Logging

Structured JSON logging is configured in `src/procurement/logging.py`. All pipeline scripts
call `setup_logging()` which emits one JSON object per line to stderr + optional file.

Log files: `data/logs/pipeline_YYYY-MM-DD.log` (written by `run_pipeline.py`).

---

## Data quality validation (Silver)

Silver runs two DQ layers:

1. **Legacy path** (`build_silver.py`): row-level validation via
   `src/procurement/silver/legacy/validation.py`. Invalid rows go to
   `data/silver/_quarantine/notice_rows/`.

2. **New path** (`pipeline_orchestrator.py`): five validation cases:
   - Case 0 — HTML structural parse error → row excluded, written to quarantine
   - Case 1 — unknown section numbers → row kept in silver, also written to quarantine (monitoring signal)
   - Case 2 — column parser failure → **non-fatal**; column set to `None`, error appended to `parse_errors` column on row; nothing quarantined
   - Case 3 — Pydantic contract violation → row excluded, written to quarantine; indicates a parser implementation bug
   - Case 4 — no registered profile for notice type → row excluded

   Quarantine rows written to `data/silver/quarantine/noticeType=<TYPE>/`.

   All silver section rows carry a `parse_errors: array<string>` column (null when clean).

See `src/docs/runbooks/QUARANTINE.md` for full case descriptions and the `parse_errors` column contract.

---

## Re-run semantics

Obs tables are **append-only**. Re-running a pipeline day appends new rows rather than
replacing old ones. This is intentional:

- No delete-before-write means there is no gap window where a crashed re-run could leave
  the table empty.
- Every run is permanently recorded, giving a full audit history.

The `written_at` column is the dedup key. To get the latest run per `(layer, target_date)`:

```sql
-- DuckDB / any SQL with window functions
SELECT * FROM pipeline_runs
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY layer, target_date
    ORDER BY written_at DESC
) = 1
```

For `dq_metrics`: partition by `(layer, target_date, notice_type, metric_name)`.
For `quarantine_summary`: partition by `(target_date, notice_type)`.

---

## Architectural decisions

**Obs tables are append-only Parquet, not a transactional store.**
The alternative (delete-then-write per re-run) creates a gap window: if the pipeline
crashes after deleting the old record but before writing the new one, the day goes
unrecorded. Parquet has no transactions, so there is no safe way to close this window
without Delta Lake or Iceberg. The append-only + `written_at` pattern avoids the problem
entirely at zero cost — the worst case is a brief period of duplicates, which query-time
deduplication handles.

**Silver data uses Spark partition overwrite, not append.**
Appending versioned notice records would cause significant storage bloat during
experimentation (each re-run doubles the data for that day). Spark's partition overwrite
is safe enough here: Spark writes output to a `_temporary/` staging directory and commits
atomically on job success, so a crashed run leaves the old partition intact rather than
producing a partial write. This is a different risk profile from the raw pyarrow writes
used for obs tables, which have no staging step.

**`git_commit` is injected via `$GIT_COMMIT` env var when running in Docker.**
The container has no access to the `.git` directory and no `git` binary. The env var
is set at `docker run` time: `-e GIT_COMMIT=$(git rev-parse --short HEAD)`.
`git_commit_sha()` in `obs.py` falls back to running `git` directly when the env var
is absent (useful for local non-Docker runs).

**`script_hash` captures the SHA-256 of the entry-point script.**
This is a data provenance signal: the hash changes when the script logic changes, even
if the git commit is the same (e.g. dirty working tree). It complements `git_commit`
for debugging "why did this day produce different output than yesterday".

---

## Querying obs tables

With pandas / pyarrow (no Spark needed):

```python
import pyarrow.dataset as ds

runs = ds.dataset("data/obs/pipeline_runs", format="parquet", partitioning="hive").to_table().to_pandas()
dq   = ds.dataset("data/obs/dq_metrics",   format="parquet", partitioning="hive").to_table().to_pandas()
quar = ds.dataset("data/obs/quarantine_summary", format="parquet", partitioning="hive").to_table().to_pandas()
```

With Spark:

```python
runs = spark.read.parquet("data/obs/pipeline_runs")
dq   = spark.read.parquet("data/obs/dq_metrics")
quar = spark.read.parquet("data/obs/quarantine_summary")
```

---

## Configuring thresholds

Edit `THRESHOLDS` in `scripts/pipeline/build_obs.py`:

```python
THRESHOLDS: dict[tuple[str, str, str], tuple[str, float]] = {
    # (layer, notice_type, metric_name): (operator, threshold_value)
    ("bronze", "__all__", "valid_rate"): (">=", 0.95),
}
```

`build_obs.py` exits non-zero when any threshold is breached, causing `run_pipeline.py` to
mark the `obs` step as failed.
