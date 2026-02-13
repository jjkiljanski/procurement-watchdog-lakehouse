"""Parallel backfill runner for Silver and Gold daily jobs.

Runs multiple day-level Spark jobs concurrently on one machine with bounded
parallelism and explicit concurrency telemetry.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


@dataclass
class RunningJob:
    process: subprocess.Popen
    started_at: float
    stage: str
    day: str
    cmd: list[str]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill silver/gold in parallel.")
    parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--gold-dir", default="data/gold")
    parser.add_argument("--workers", type=int, default=2, help="Max concurrent Spark jobs")
    parser.add_argument(
        "--cores-per-job",
        type=int,
        default=1,
        help="Spark local cores per job (SPARK_MASTER=local[N])",
    )
    parser.add_argument(
        "--silver-repartition",
        type=int,
        default=0,
        help="Pass-through to build_silver.py --repartition (0=auto)",
    )
    parser.add_argument("--skip-gold", action="store_true")
    parser.add_argument("--only-gold", action="store_true")
    parser.add_argument(
        "--report-path",
        default="data/reports/backfill_parallel",
        help="Directory for concurrency/report json",
    )
    return parser.parse_args()


def _list_days(raw_dir: Path, start: date, end: date) -> list[str]:
    days: list[str] = []
    for path in sorted(raw_dir.glob("bzp_*.json")):
        day = path.stem.replace("bzp_", "")
        try:
            d = date.fromisoformat(day)
        except ValueError:
            continue
        if start <= d <= end:
            days.append(day)
    return days


def _stage_command(stage: str, day: str, args: argparse.Namespace) -> list[str]:
    if stage == "silver":
        return [
            sys.executable,
            "scripts/build_silver.py",
            day,
            "--raw-dir",
            args.raw_dir,
            "--silver-dir",
            args.silver_dir,
            "--spark-master",
            f"local[{args.cores_per_job}]",
            "--repartition",
            str(args.silver_repartition),
        ]
    if stage == "gold":
        return [
            sys.executable,
            "scripts/build_gold.py",
            day,
            "--silver-dir",
            args.silver_dir,
            "--gold-dir",
            args.gold_dir,
            "--spark-master",
            f"local[{args.cores_per_job}]",
        ]
    raise ValueError(f"Unknown stage: {stage}")


def _run_stage_parallel(stage: str, days: list[str], args: argparse.Namespace) -> dict:
    pending = list(days)
    active: dict[int, RunningJob] = {}
    completed: list[dict] = []
    failed_days: list[str] = []
    max_parallel_observed = 0

    while pending or active:
        while pending and len(active) < args.workers:
            day = pending.pop(0)
            cmd = _stage_command(stage, day, args)
            started_at = time.time()
            proc = subprocess.Popen(cmd)  # noqa: S603
            active[proc.pid] = RunningJob(
                process=proc,
                started_at=started_at,
                stage=stage,
                day=day,
                cmd=cmd,
            )
            max_parallel_observed = max(max_parallel_observed, len(active))
            log.info(
                "START stage=%s day=%s pid=%s active_jobs=%d",
                stage,
                day,
                proc.pid,
                len(active),
            )

        finished_pids: list[int] = []
        for pid, job in active.items():
            rc = job.process.poll()
            if rc is None:
                continue
            duration = time.time() - job.started_at
            completed.append(
                {
                    "stage": stage,
                    "day": job.day,
                    "pid": pid,
                    "returncode": rc,
                    "duration_sec": round(duration, 3),
                    "started_at_utc": datetime.utcfromtimestamp(job.started_at).isoformat() + "Z",
                    "ended_at_utc": datetime.utcnow().isoformat() + "Z",
                    "cmd": job.cmd,
                }
            )
            if rc != 0:
                failed_days.append(job.day)
                log.error(
                    "FAIL stage=%s day=%s pid=%s rc=%s duration_sec=%.2f",
                    stage,
                    job.day,
                    pid,
                    rc,
                    duration,
                )
            else:
                log.info(
                    "DONE stage=%s day=%s pid=%s duration_sec=%.2f",
                    stage,
                    job.day,
                    pid,
                    duration,
                )
            finished_pids.append(pid)

        for pid in finished_pids:
            active.pop(pid, None)

        if active:
            time.sleep(0.5)

    return {
        "stage": stage,
        "total_days": len(days),
        "failed_days": sorted(set(failed_days)),
        "max_parallel_jobs_observed": max_parallel_observed,
        "jobs": completed,
    }


def main() -> None:
    args = _parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    raw_dir = Path(args.raw_dir)
    if not raw_dir.exists():
        log.error("Raw dir not found: %s", raw_dir)
        sys.exit(1)

    days = _list_days(raw_dir, start, end)
    if not days:
        log.error("No daily raw files found in range %s..%s", start, end)
        sys.exit(1)

    log.info(
        "Backfill plan days=%d workers=%d cores_per_job=%d range=%s..%s",
        len(days),
        args.workers,
        args.cores_per_job,
        args.start_date,
        args.end_date,
    )

    result: dict = {
        "started_at_utc": datetime.utcnow().isoformat() + "Z",
        "range": {"start_date": args.start_date, "end_date": args.end_date},
        "workers": args.workers,
        "cores_per_job": args.cores_per_job,
        "days": days,
        "stages": [],
    }

    if not args.only_gold:
        silver_result = _run_stage_parallel("silver", days, args)
        result["stages"].append(silver_result)
        if silver_result["failed_days"]:
            log.error("Silver stage failed for %d days", len(silver_result["failed_days"]))
            if not args.skip_gold:
                log.warning("Skipping gold stage because silver had failures")
                args.skip_gold = True

    if not args.skip_gold:
        gold_days = days
        gold_result = _run_stage_parallel("gold", gold_days, args)
        result["stages"].append(gold_result)

    result["ended_at_utc"] = datetime.utcnow().isoformat() + "Z"

    out_dir = Path(args.report_path)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"parallel_backfill_{args.start_date}_{args.end_date}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info("Wrote backfill report: %s", out_path)

    has_failures = any(stage["failed_days"] for stage in result["stages"])
    if has_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
