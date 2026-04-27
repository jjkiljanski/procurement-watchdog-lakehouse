# Pipeline Logging

## Format

Every pipeline log line is a single JSON object emitted to **stderr**:

```json
{"ts": "2025-10-01T03:00:12.345Z", "level": "INFO", "logger": "scripts.pipeline.fetch_bzp_yesterday", "msg": "fetch_bzp_yesterday done: date=2025-10-01 kept=842 dropped_by_day=0 dropped_dup=0", "stage": "fetch", "date": "2025-10-01", "status": "ok"}
```

Fields present in every record:

| Field    | Description                                |
|----------|--------------------------------------------|
| `ts`     | ISO-8601 UTC timestamp                     |
| `level`  | `DEBUG` / `INFO` / `WARNING` / `ERROR`     |
| `logger` | Python module name of the emitting logger  |
| `msg`    | Human-readable message                     |

Fields present when set by the pipeline code (used for GCP Cloud Logging filtering):

| Field         | Populated by                           | Values                                |
|---------------|----------------------------------------|---------------------------------------|
| `stage`       | All pipeline scripts                   | `fetch`, `bronze`, `silver`, `deltas` |
| `date`        | Key log calls (start / skip / done)    | `YYYY-MM-DD`                          |
| `notice_type` | Silver batch logs                      | e.g. `ContractNotice`                 |
| `status`      | Start / skip / success boundary logs   | `started`, `skipped`, `ok`, `empty`   |
| `elapsed_s`   | Reserved for timing logs               | float seconds                         |
| `exc`         | Any log call with an exception attached| Full traceback string                 |

## GCP Cloud Logging Queries

Logs are written to **stderr**, which Cloud Run and Dataproc Serverless automatically forward to Cloud Logging as `jsonPayload.*`.  Useful filter expressions:

```
# All logs for one stage
jsonPayload.stage="bronze"

# All logs for a specific date across every stage
jsonPayload.date="2025-10-01"

# Skipped dates during a backfill
jsonPayload.status="skipped"

# Silver failures for a specific notice type
jsonPayload.stage="silver" AND jsonPayload.notice_type="ContractNotice" AND severity=ERROR

# Full day pipeline trace
jsonPayload.date="2025-10-01" AND jsonPayload.status=("started" OR "ok" OR "skipped" OR "empty")
```

## Implementation

**`src/procurement/logging.py`** is the single source of truth:

- `JsonFormatter` — formats `LogRecord` objects as JSON; promotes the
  structured-field whitelist (`_STRUCTURED_FIELDS`) to top-level JSON keys.
- `setup_logging(level, log_file)` — configures the root logger (stderr + optional file).
- `get_stage_logger(name, stage)` — factory returning a `_StageAdapter` that
  auto-injects `stage=<stage>` into every record. Call-site `extra=` kwargs are
  merged (not overwritten) so per-record context coexists with the stage field.

Pipeline scripts use:

```python
from procurement.logging import get_stage_logger, setup_logging

setup_logging()
log = get_stage_logger(__name__, "bronze")  # stage injected into all records

log.info("build_bronze started: date=%s", target_date,
         extra={"date": target_date, "status": "started"})
```

## Per-stage Coverage

### `fetch` stage
Scripts: `fetch_bzp_yesterday.py`, `fetch_bzp_range.py`, `src/procurement/fetch/bzp_api.py`

Key log events:

| Event | Status field | Notes |
|-------|-------------|-------|
| Daily fetch started | `started` | date in msg and `date` field |
| Date skipped (range backfill) | `skipped` | manifest hash matched |
| Date fetched (range backfill) | `started` → `ok` | separate start + completion records |
| Fetch complete with counts | `ok` | kept / dropped_by_day / dropped_dup counts in msg |
| File written | — | path + record count + KB size in msg |

The fetch stage also writes an observability record to `pipeline_runs` table (via
`obs.write_pipeline_run`) on every successful fetch — both daily and range scripts.

### `bronze` stage
Scripts: `build_bronze.py`, `build_bronze_range.py`

Key log events:

| Event | Status field | Notes |
|-------|-------------|-------|
| Bronze started | `started` | |
| Date skipped (range) | `skipped` | |
| Bronze written | `ok` | raw / dedup / valid / invalid counts |
| No valid records | `empty` | warns before skipping the Parquet write |

### `silver` stage
Scripts: `build_silver_day.py`, `build_silver_range.py`, `src/procurement/silver/pipeline_orchestrator.py`

Key log events:

| Event | Status field | Notes |
|-------|-------------|-------|
| Silver day started | `started` | date field set |
| Per-batch complete | `ok` | notice_type + date fields set, rows + elapsed in msg |
| Section table written | — | notice_type field, table name + elapsed in msg |
| Silver day done | `ok` | |

### `deltas` stage
Script: `build_silver_update_deltas.py`

Key log events:

| Event | Status field | Notes |
|-------|-------------|-------|
| Single-day started | `started` | |
| No NUN data for day | `skipped` | |
| Day complete (batch mode) | `ok` | date + elapsed in msg |
| Run finished | — | total elapsed in msg |
