# Observability

This document describes how observability is implemented across the procurement pipeline
(fetch → bronze → silver → gold).

---

## Design principles

- **No external dependencies.** Everything is standard Python + pandas + pyarrow.
- **Parquet as the observability store.** Obs data is queryable via Spark or pandas alongside
  the main pipeline data.
- **Emit during the run.** Each pipeline step writes its own obs records. No separate scraping.
- **One dashboard script.** `build_obs.py` reads obs tables, computes silver/gold snapshots,
  prints a Markdown dashboard, and enforces thresholds.

---

## Obs tables

All tables live under `data/obs/`, date-partitioned as `dt=YYYY-MM-DD/`.

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
| `git_commit` | string | short SHA |
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

### `quarantine_summary/`

One row per notice_type per day where quarantine rows were written. Written by silver.

| column | type | description |
|---|---|---|
| `target_date` | string | YYYY-MM-DD |
| `notice_type` | string | |
| `row_count` | int | number of quarantined rows |

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

2. **New path** (`pipeline_orchestrator.py`): four quarantine cases:
   - Case 0 — HTML structural parse error (duplicate core section)
   - Case 1 — unknown section numbers (not in profile JSON)
   - Case 2 — strict column parser failure (`ParseError`)
   - Case 3 — Pydantic row-level validation failure
   - Case 4 — no registered profile for notice type

   Quarantine rows written to `data/silver/quarantine/noticeType=<TYPE>/`.

See `docs/runbooks/SILVER_QUARANTINE.md` for case details.

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
