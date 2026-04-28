"""Run the full BZP backfill pipeline for a date range: fetch → bronze → silver → deltas.

Usage:
    python scripts/ops/run_backfill.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD

Each stage is run as a subprocess so its stdout/stderr flow straight to the
terminal.  The runner prints plain-text banners and a timing summary to stdout.

The pipeline aborts immediately on the first stage failure.

Silver range note
-----------------
``build_silver_range.py`` loops over **notice types** (not dates): it builds one
Spark plan per notice type that covers the entire date range.  This means the
DAG is compiled once per notice type (14 types total), not once per
(date, notice_type) pair — which would be 14 × N_days plans.

Tuning flags
------------
--max-section-write-workers   Max concurrent section-model writes per
                              notice-type batch (default 4, passed to silver).
--shuffle-partitions          spark.sql.shuffle.partitions for silver
                              (default 32 for backfill ranges).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent.parent / "pipeline"


def _banner(msg: str) -> None:
    line = "=" * 72
    print(f"\n{line}", flush=True)
    print(f"  {msg}", flush=True)
    print(f"{line}\n", flush=True)


def _run_stage(
    name: str,
    cmd: list[str],
) -> float:
    """Run one stage as a subprocess. Returns elapsed seconds. Raises on failure."""
    ts = time.strftime("%H:%M:%S")
    script_name = Path(cmd[1]).name if len(cmd) > 1 else name
    _banner(f"[{ts}]  STAGE: {name.upper()}  |  {script_name}")
    t0 = time.perf_counter()

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
        description="Run the full BZP backfill pipeline for a date range."
    )
    parser.add_argument("--start-date", required=True, help="First date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date",   required=True, help="Last date  YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--max-section-write-workers",
        type=int,
        default=4,
        help="Max concurrent section-model writes per silver batch (default 4)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=32,
        help="spark.sql.shuffle.partitions for silver (default 32)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess all dates even when manifests already exist",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    # Validate dates early.
    s = date.fromisoformat(args.start_date)
    e = date.fromisoformat(args.end_date)
    if e < s:
        raise SystemExit(f"--end-date {args.end_date} is before --start-date {args.start_date}")
    n_days = (e - s).days + 1

    force_flag = ["--force"] if args.force else []
    silver_extra = [
        "--shuffle-partitions", str(args.shuffle_partitions),
        "--max-section-write-workers", str(args.max_section_write_workers),
    ] + force_flag

    py = sys.executable
    stages: list[tuple[str, list[str]]] = [
        ("fetch",  [py, str(PIPELINE_DIR / "fetch_bzp_range.py"),
                    args.start_date, args.end_date] + force_flag),
        ("bronze", [py, str(PIPELINE_DIR / "build_bronze_range.py"),
                    "--start-date", args.start_date, "--end-date", args.end_date] + force_flag),
        ("silver", [py, str(PIPELINE_DIR / "build_silver_range.py"),
                    "--start-date", args.start_date, "--end-date", args.end_date] + silver_extra),
        # deltas always overwrite their date partition — --force is not applicable
        ("deltas", [py, str(PIPELINE_DIR / "build_silver_update_deltas.py"),
                    "--start-date", args.start_date, "--end-date", args.end_date]),
    ]

    total_start = time.perf_counter()
    ts = time.strftime("%H:%M:%S")
    _banner(
        f"[{ts}]  BACKFILL START"
        f"  |  {args.start_date} .. {args.end_date}  ({n_days} days)"
        f"  |  stages={len(stages)}"
        f"  |  silver: shuffle-partitions={args.shuffle_partitions}"
        f"  max-section-write-workers={args.max_section_write_workers}"
    )

    timings: list[tuple[str, float]] = []
    for name, cmd in stages:
        elapsed = _run_stage(name, cmd)
        timings.append((name, elapsed))

    total = time.perf_counter() - total_start
    ts = time.strftime("%H:%M:%S")

    _banner(f"[{ts}]  BACKFILL COMPLETE  |  total={total:.1f}s")
    print(f"  {'stage':<10}  {'elapsed':>10}")
    print(f"  {'-'*24}")
    for name, elapsed in timings:
        print(f"  {name:<10}  {elapsed:>8.1f}s")
    print(f"  {'-'*24}")
    print(f"  {'TOTAL':<10}  {total:>8.1f}s")
    print(flush=True)


if __name__ == "__main__":
    main()
