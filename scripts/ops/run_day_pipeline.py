"""Run the full BZP pipeline for one day: fetch → bronze → silver → deltas.

Usage:
    python scripts/ops/run_day_pipeline.py [YYYY-MM-DD]

Defaults to yesterday when no date is given.

Each stage is run as a subprocess so its stdout/stderr flow straight to the
terminal.  The runner itself prints plain-text banners and a timing summary to
stdout so stage boundaries and elapsed times are easy to spot even when
interleaved with the JSON log lines from the sub-scripts.

The pipeline aborts immediately on the first stage failure.

Silver performance flags
------------------------
--max-batch-workers   Max concurrent notice-type batches (default 2).
--shuffle-partitions  spark.sql.shuffle.partitions for silver (default 8).

Tuned defaults keep memory usage under control on a single machine.  On GCP
Dataproc Serverless these are ignored (the workflow passes its own values).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"


def _banner(msg: str) -> None:
    line = "=" * 72
    print(f"\n{line}", flush=True)
    print(f"  {msg}", flush=True)
    print(f"{line}\n", flush=True)


def _run_stage(
    name: str,
    script: Path,
    target_date: str,
    extra_args: list[str] | None = None,
) -> float:
    """Run one stage as a subprocess. Returns elapsed seconds. Raises on failure."""
    ts = time.strftime("%H:%M:%S")
    _banner(f"[{ts}]  STAGE: {name.upper()}  |  date={target_date}  |  {script.name}")
    t0 = time.perf_counter()

    cmd = [sys.executable, str(script), target_date] + (extra_args or [])
    result = subprocess.run(cmd)

    elapsed = time.perf_counter() - t0
    ts = time.strftime("%H:%M:%S")

    if result.returncode != 0:
        _banner(f"[{ts}]  FAILED: {name}  |  elapsed={elapsed:.1f}s  |  rc={result.returncode}")
        raise RuntimeError(f"Stage '{name}' failed with exit code {result.returncode}")

    _banner(f"[{ts}]  DONE: {name}  |  elapsed={elapsed:.1f}s")
    return elapsed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full BZP pipeline for one day."
    )
    parser.add_argument(
        "target_date",
        nargs="?",
        help="Date in YYYY-MM-DD format (default: yesterday)",
    )
    parser.add_argument(
        "--max-batch-workers",
        type=int,
        default=2,
        help="Max concurrent silver notice-type batches (default 2)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=8,
        help="spark.sql.shuffle.partitions for silver (default 8)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    if args.target_date:
        target_date = args.target_date
        date.fromisoformat(target_date)  # validate — raises on bad format
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_extra = [
        "--max-batch-workers", str(args.max_batch_workers),
        "--shuffle-partitions", str(args.shuffle_partitions),
    ]

    stages: list[tuple[str, Path, list[str]]] = [
        ("fetch",  PIPELINE_DIR / "fetch_bzp_yesterday.py", []),
        ("bronze", PIPELINE_DIR / "build_bronze.py",        []),
        ("silver", PIPELINE_DIR / "build_silver_day.py",    silver_extra),
        ("deltas", PIPELINE_DIR / "build_silver_update_deltas.py", []),
    ]

    total_start = time.perf_counter()
    ts = time.strftime("%H:%M:%S")
    _banner(
        f"[{ts}]  PIPELINE START  |  date={target_date}  |  stages={len(stages)}"
        f"  |  silver: max-batch-workers={args.max_batch_workers}"
        f"  shuffle-partitions={args.shuffle_partitions}"
    )

    timings: list[tuple[str, float]] = []
    for name, script, extra in stages:
        elapsed = _run_stage(name, script, target_date, extra)
        timings.append((name, elapsed))

    total = time.perf_counter() - total_start
    ts = time.strftime("%H:%M:%S")

    _banner(f"[{ts}]  PIPELINE COMPLETE  |  total={total:.1f}s")
    print(f"  {'stage':<10}  {'elapsed':>10}")
    print(f"  {'-'*24}")
    for name, elapsed in timings:
        print(f"  {name:<10}  {elapsed:>8.1f}s")
    print(f"  {'-'*24}")
    print(f"  {'TOTAL':<10}  {total:>8.1f}s")
    print(flush=True)


if __name__ == "__main__":
    main()
