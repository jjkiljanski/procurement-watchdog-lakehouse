"""Central observability module for the procurement pipeline.

Tables written (Parquet, date-partitioned under data/obs/ by default):
  pipeline_runs/      — one row per layer run (fetch / bronze / silver / gold)
  dq_metrics/         — tall-format data quality metrics per layer/notice_type
  quarantine_summary/ — quarantine row counts per notice_type/day

Utilities (previously in lineage.py, now canonical here):
  now_utc_iso, atomic_write_json, sha256_file, git_commit_sha
"""

from __future__ import annotations

import hashlib
import json
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


def git_commit_sha(cwd: Path | None = None) -> str | None:
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
# Obs table writers
# ---------------------------------------------------------------------------

_OBS_DIR = Path("data/obs")


def _append_parquet(table_dir: Path, rows: list[dict], partition_key: str) -> None:
    """Append rows to a date-partitioned Parquet table using pandas."""
    import pandas as pd

    if not rows:
        return
    part_dir = table_dir / partition_key
    part_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%H%M%S_%f")
    pd.DataFrame(rows).to_parquet(part_dir / f"part-{ts}.parquet", index=False)


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
    extra: dict[str, Any] | None = None,
    obs_dir: Path | None = None,
) -> None:
    """Append one pipeline run record to pipeline_runs/dt=YYYY-MM-DD/."""
    row = {
        "layer": layer,
        "target_date": target_date,
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": completed_at,
        "status": status,
        "git_commit": git_commit,
        **{f"count_{k}": v for k, v in counts.items()},
        "extra_json": json.dumps(extra or {}, ensure_ascii=False),
    }
    _append_parquet((obs_dir or _OBS_DIR) / "pipeline_runs", [row], f"dt={target_date}")


def write_dq_metrics(
    *,
    layer: str,
    target_date: str,
    notice_type: str | None,
    metrics: dict[str, float | int],
    obs_dir: Path | None = None,
) -> None:
    """Append per-layer/notice_type data quality metrics as tall rows."""
    rows = [
        {
            "layer": layer,
            "target_date": target_date,
            "notice_type": notice_type or "__all__",
            "metric_name": k,
            "metric_value": float(v),
        }
        for k, v in metrics.items()
    ]
    _append_parquet((obs_dir or _OBS_DIR) / "dq_metrics", rows, f"dt={target_date}")


def write_quarantine_summary(
    *,
    target_date: str,
    notice_type: str,
    row_count: int,
    obs_dir: Path | None = None,
) -> None:
    """Append one quarantine summary record per notice_type per day."""
    _append_parquet(
        (obs_dir or _OBS_DIR) / "quarantine_summary",
        [{"target_date": target_date, "notice_type": notice_type, "row_count": row_count}],
        f"dt={target_date}",
    )
