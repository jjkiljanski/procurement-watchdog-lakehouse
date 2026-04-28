"""Backfill Silver over date range (wrapper over core Silver build).

Path resolution
---------------
``bronze-dir`` and ``silver-dir`` default to the runtime-resolved paths:

- **local** (``RUNTIME_ENV=local``): ``{LOCAL_DATA_ROOT}/bronze/`` and
  ``{LOCAL_DATA_ROOT}/silver/``
- **GCP**   (``RUNTIME_ENV=gcp``):  ``gs://{LAKEHOUSE_BUCKET}/bronze/`` and
  ``gs://{LAKEHOUSE_BUCKET}/silver/``

Pass ``--bronze-dir`` / ``--silver-dir`` explicitly to override.

State file
----------
Backfill progress is saved to a JSON state file so interrupted runs can
resume.  Default location: ``{silver-dir}/_state/silver_backfill_{start}_{end}.json``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.lineage import atomic_write_json, now_utc_iso
from procurement.logging import setup_logging
from procurement.runtime import get_runtime
from procurement.silver.pipeline_orchestrator import CoreRunConfig, run_silver_day_core

setup_logging()
import logging

log = logging.getLogger(__name__)


def _date_list(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError(f"end-date {end_date} is before start-date {start_date}")
    out: list[str] = []
    d = start
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _default_state_path(silver_dir: str, start_date: str, end_date: str) -> Path:
    return Path(silver_dir) / "_state" / f"silver_backfill_{start_date}_{end_date}.json"


def _load_or_init_state(
    state_path: Path,
    days: list[str],
    start_date: str,
    end_date: str,
    reset_state: bool,
) -> dict:
    if reset_state and state_path.exists():
        state_path.unlink()
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("start_date") != start_date or state.get("end_date") != end_date:
            raise ValueError(
                f"State file range mismatch: {state.get('start_date')}..{state.get('end_date')} "
                f"!= {start_date}..{end_date}"
            )
        existing_days = state.get("days", {})
        for day in days:
            existing_days.setdefault(day, {"status": "pending", "attempts": 0})
        state["days"] = existing_days
        return state
    state = {
        "version": 1,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "start_date": start_date,
        "end_date": end_date,
        "days": {day: {"status": "pending", "attempts": 0} for day in days},
    }
    atomic_write_json(state_path, state)
    return state


def _save_state(state_path: Path, state: dict) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    atomic_write_json(state_path, state)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Silver over date range in one Spark job.")
    parser.add_argument("--start-date", required=True, help="Start day YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End day YYYY-MM-DD")
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help="Bronze root directory.  Defaults to the runtime-resolved 'bronze' path.",
    )
    parser.add_argument(
        "--silver-dir",
        default=None,
        help="Silver root directory.  Defaults to the runtime-resolved 'silver' path.",
    )
    parser.add_argument("--state-path", default="", help="Checkpoint state JSON path")
    parser.add_argument("--reset-state", action="store_true", help="Delete existing state and restart range")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue other days if one day fails (failed day remains resumable)",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Optional cap of days to process in one run (0 = all pending days)",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*], local[6]).  Defaults to SPARK_MASTER env var.",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = adaptive per batch)",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Force repartition count per batch (0 = adaptive)",
    )
    parser.add_argument(
        "--max-batch-workers",
        type=int,
        default=0,
        help="Max concurrent notice-type batches (0 = default tuned cap)",
    )
    parser.add_argument(
        "--max-section-write-workers",
        type=int,
        default=0,
        help="Max concurrent section-model writes inside one batch (0 = default tuned cap)",
    )
    parser.add_argument(
        "--lock-stale-minutes",
        type=int,
        default=240,
        help="Treat an existing day lock as stale after this many minutes",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    days = _date_list(args.start_date, args.end_date)

    rt = get_runtime()
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    silver_dir = args.silver_dir or rt.storage.resolve("silver")

    state_path = Path(args.state_path) if args.state_path else _default_state_path(
        silver_dir, args.start_date, args.end_date
    )

    state = _load_or_init_state(
        state_path=state_path,
        days=days,
        start_date=args.start_date,
        end_date=args.end_date,
        reset_state=args.reset_state,
    )
    pending_days = [d for d in days if state["days"][d].get("status") != "completed"]
    if args.max_days > 0:
        pending_days = pending_days[: args.max_days]
    if not pending_days:
        log.info("No pending days to process. Range already completed: %s..%s", args.start_date, args.end_date)
        return

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    spark = rt.spark.get_session("bzp-silver-backfill", **extra)
    obs_dir = rt.storage.obs_path()

    try:
        failed: list[str] = []
        run_started = now_utc_iso()
        log.info(
            "Backfill started range=%s..%s pending_days=%d",
            args.start_date,
            args.end_date,
            len(pending_days),
        )
        for day in pending_days:
            log.info("Backfill day start: %s", day)
            day_state = state["days"][day]
            day_state["status"] = "in_progress"
            day_state["attempts"] = int(day_state.get("attempts", 0)) + 1
            day_state["started_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            day_state.pop("error", None)
            _save_state(state_path, state)
            try:
                cfg = CoreRunConfig(
                    target_date=day,
                    bronze_dir=bronze_dir,
                    silver_dir=silver_dir,
                    input_layer="bronze",
                    shuffle_partitions=args.shuffle_partitions,
                    repartition=args.repartition,
                    max_batch_workers=args.max_batch_workers,
                    max_section_write_workers=args.max_section_write_workers,
                    lock_stale_minutes=args.lock_stale_minutes,
                    mode="backfill",
                )
                result = run_silver_day_core(
                    spark=spark,
                    cfg=cfg,
                    command=sys.argv,
                    args_dict=vars(args),
                    script_paths=[Path(__file__).resolve()],
                    run_context={
                        "start_date": args.start_date,
                        "end_date": args.end_date,
                        "state_path": str(state_path),
                        "run_started_at": run_started,
                    },
                    obs_dir=obs_dir,
                )
                day_state["status"] = "completed"
                day_state["completed_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
                day_state["rows"] = result["rows"]
                day_state["quarantined_rows"] = result.get("quarantined_rows", 0)
                _save_state(state_path, state)
                log.info("Backfill day done: %s rows=%s", day, result["rows"])
            except Exception as exc:
                day_state["status"] = "failed"
                day_state["error"] = str(exc)
                _save_state(state_path, state)
                failed.append(day)
                log.error("Backfill day failed: %s error=%s", day, exc, exc_info=True)
                if not args.continue_on_error:
                    raise

        if failed:
            raise RuntimeError(f"Silver backfill failed days: {failed}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
