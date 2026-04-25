"""Central observability module for the procurement pipeline.

Write backends
--------------
Local (``RUNTIME_ENV=local`` or any mode where ``obs_dir`` is provided):
    Parquet files appended under ``obs_dir/`` (date-partitioned).
    Tables: pipeline_runs/, dq_metrics/, quarantine_summary/

GCP (``RUNTIME_ENV=gcp``, ``obs_dir=None``):
    Rows streamed to BigQuery using the ``google-cloud-bigquery`` client.
    Dataset: ``BQ_OBS_DATASET`` env var (default: ``procurement_obs``).
    Tables: pipeline_runs, dq_metrics, quarantine_summary.
    Tables and the dataset are created automatically on first write if absent.

The caller switches backends by passing ``obs_dir``:
- Pass a ``Path`` → local Parquet.
- Pass ``None``  → BigQuery when ``RUNTIME_ENV=gcp``, otherwise a no-op.

Utilities:
    now_utc_iso, atomic_write_json, sha256_file, sha256_paths, git_commit_sha
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Shared utilities
# ---------------------------------------------------------------------------

def now_utc_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def atomic_write_json(path: Path, payload: dict) -> None:
    """Write JSON atomically (write to .tmp then rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_paths(*items: Path) -> str:
    """SHA-256 of a set of files and/or directories.

    Files are hashed directly.  Directories are walked recursively — only
    ``.py`` files are included.  Paths are sorted before hashing so the
    result is deterministic regardless of filesystem enumeration order.

    Use this instead of ``sha256_file`` in pipeline scripts so that changes
    to supporting library code (not just the entry-point script) invalidate
    the processed-date manifest and trigger a rerun.
    """
    h = hashlib.sha256()
    collected: list[Path] = []
    for item in items:
        if item.is_dir():
            collected.extend(item.rglob("*.py"))
        elif item.exists():
            collected.append(item)
    for fp in sorted(set(collected)):
        h.update(fp.read_bytes())
    return h.hexdigest()


def git_commit_sha(cwd: Path | None = None) -> str | None:
    if sha := os.environ.get("GIT_COMMIT"):
        return sha
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(cwd) if cwd else None,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        return out.strip()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Local Parquet backend
# ---------------------------------------------------------------------------

_OBS_DIR = Path("data/obs")


def _append_parquet(table_dir: Path, rows: list[dict], partition_key: str) -> None:
    """Append rows to a date-partitioned Parquet table using pyarrow."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not rows:
        return
    part_dir = table_dir / partition_key
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%H%M%S_%f")
    pq.write_table(pa.Table.from_pylist(rows), part_dir / f"part-{ts}.parquet")


# ---------------------------------------------------------------------------
# BigQuery backend
# ---------------------------------------------------------------------------

# BQ table schemas (DDL).  The ``pipeline_runs`` table uses ``counts_json``
# instead of dynamic ``count_*`` columns so the schema stays stable even when
# new pipeline scripts add different count keys.
_BQ_SCHEMAS: dict[str, str] = {
    "pipeline_runs": """
        layer          STRING    NOT NULL,
        target_date    DATE      NOT NULL,
        run_id         STRING    NOT NULL,
        started_at     TIMESTAMP,
        completed_at   TIMESTAMP,
        written_at     TIMESTAMP,
        status         STRING,
        git_commit     STRING,
        script_hash    STRING,
        counts_json    STRING,
        extra_json     STRING
    """,
    "dq_metrics": """
        layer          STRING    NOT NULL,
        target_date    DATE      NOT NULL,
        notice_type    STRING,
        metric_name    STRING    NOT NULL,
        metric_value   FLOAT64,
        written_at     TIMESTAMP
    """,
    "quarantine_summary": """
        target_date    DATE      NOT NULL,
        notice_type    STRING    NOT NULL,
        row_count      INT64,
        written_at     TIMESTAMP
    """,
}

# Cache of (project, dataset, table) combos known to exist, to avoid
# repeated existence checks on every write.
_bq_tables_confirmed: set[str] = set()


def _bq_obs_dataset() -> str:
    return os.environ.get("BQ_OBS_DATASET", "procurement_obs")


def _bq_project() -> str:
    project = os.environ.get("GCP_PROJECT", "").strip()
    if not project:
        raise EnvironmentError(
            "GCP_PROJECT env var is required for BigQuery obs writes."
        )
    return project


def _ensure_bq_table(client: Any, project: str, dataset: str, table: str) -> None:
    """Create the BQ dataset + table if they do not exist (idempotent)."""
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound

    cache_key = f"{project}.{dataset}.{table}"
    if cache_key in _bq_tables_confirmed:
        return

    # Ensure dataset exists.
    dataset_ref = bigquery.DatasetReference(project, dataset)
    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = os.environ.get("DATAPROC_REGION", "US")
        client.create_dataset(ds, exists_ok=True)

    # Ensure table exists.
    table_ref = f"{project}.{dataset}.{table}"
    try:
        client.get_table(table_ref)
    except NotFound:
        schema_ddl = _BQ_SCHEMAS[table]
        client.query(
            f"CREATE TABLE IF NOT EXISTS `{table_ref}` ({schema_ddl})"
        ).result()

    _bq_tables_confirmed.add(cache_key)


def _bq_insert(table: str, rows: list[dict]) -> None:
    """Stream *rows* into the BigQuery obs table *table*.

    Called only when ``RUNTIME_ENV=gcp`` and ``obs_dir`` is ``None``.
    Errors are logged as warnings rather than crashing the pipeline — obs
    failures should never abort a pipeline run.
    """
    import logging

    if not rows:
        return

    log = logging.getLogger(__name__)
    try:
        from google.cloud import bigquery

        project = _bq_project()
        dataset = _bq_obs_dataset()
        client = bigquery.Client(project=project)
        _ensure_bq_table(client, project, dataset, table)

        table_ref = f"{project}.{dataset}.{table}"
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            log.warning("BigQuery obs insert errors for %s: %s", table, errors)
    except Exception as exc:
        log.warning("BigQuery obs write failed for table=%s: %s", table, exc)


def _use_bq() -> bool:
    """Return True when the GCP provider is active and obs_dir was not given."""
    return os.environ.get("RUNTIME_ENV", "local").strip().lower() == "gcp"


# ---------------------------------------------------------------------------
# Public write functions
# ---------------------------------------------------------------------------

def write_pipeline_run(
    *,
    layer: str,
    target_date: str,
    run_id: str,
    started_at: str,
    completed_at: str,
    status: str,
    counts: dict[str, int],
    git_commit: str | None = None,
    script_hash: str | None = None,
    extra: dict[str, Any] | None = None,
    obs_dir: Path | None = None,
) -> None:
    """Append one pipeline run record.

    Local: written to ``obs_dir/pipeline_runs/dt=YYYY-MM-DD/``.
    GCP:   streamed to BigQuery ``pipeline_runs`` table.
    """
    if obs_dir is not None:
        row = {
            "layer": layer,
            "target_date": target_date,
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "written_at": now_utc_iso(),
            "status": status,
            "git_commit": git_commit,
            "script_hash": script_hash,
            **{f"count_{k}": v for k, v in counts.items()},
            "extra_json": json.dumps(extra or {}, ensure_ascii=False),
        }
        _append_parquet(obs_dir / "pipeline_runs", [row], f"dt={target_date}")
        return

    if _use_bq():
        _bq_insert("pipeline_runs", [{
            "layer": layer,
            "target_date": target_date,
            "run_id": run_id,
            "started_at": started_at,
            "completed_at": completed_at,
            "written_at": now_utc_iso(),
            "status": status,
            "git_commit": git_commit,
            "script_hash": script_hash,
            "counts_json": json.dumps(counts, ensure_ascii=False),
            "extra_json": json.dumps(extra or {}, ensure_ascii=False),
        }])


def write_dq_metrics(
    *,
    layer: str,
    target_date: str,
    notice_type: str | None,
    metrics: dict[str, float | int],
    obs_dir: Path | None = None,
) -> None:
    """Append per-layer/notice_type data quality metrics as tall rows.

    Local: written to ``obs_dir/dq_metrics/dt=YYYY-MM-DD/``.
    GCP:   streamed to BigQuery ``dq_metrics`` table.
    """
    written_at = now_utc_iso()
    rows = [
        {
            "layer": layer,
            "target_date": target_date,
            "notice_type": notice_type or "__all__",
            "metric_name": k,
            "metric_value": float(v),
            "written_at": written_at,
        }
        for k, v in metrics.items()
    ]

    if obs_dir is not None:
        _append_parquet(obs_dir / "dq_metrics", rows, f"dt={target_date}")
        return

    if _use_bq():
        _bq_insert("dq_metrics", rows)


def write_quarantine_summary(
    *,
    target_date: str,
    notice_type: str,
    row_count: int,
    obs_dir: Path | None = None,
) -> None:
    """Append one quarantine summary record per notice_type per day.

    Local: written to ``obs_dir/quarantine_summary/dt=YYYY-MM-DD/``.
    GCP:   streamed to BigQuery ``quarantine_summary`` table.
    """
    row = {
        "target_date": target_date,
        "notice_type": notice_type,
        "row_count": row_count,
        "written_at": now_utc_iso(),
    }

    if obs_dir is not None:
        _append_parquet(obs_dir / "quarantine_summary", [row], f"dt={target_date}")
        return

    if _use_bq():
        _bq_insert("quarantine_summary", [row])
