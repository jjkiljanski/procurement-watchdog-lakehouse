"""Run transform stack for one day (without API fetch).

Steps:
1. build_bronze
2. build_silver_day
3. build_silver_update_deltas
4. optional build_obs
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))
from procurement.logging import setup_logging

SCRIPTS_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPTS_DIR.parent / "pipeline"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run daily transforms without fetch step.")
    parser.add_argument("target_date", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--with-obs",
        action="store_true",
        help="Also run build_obs.py for target date",
    )
    return parser.parse_args()


def _run_step(name: str, cmd: list[str]) -> bool:
    log = logging.getLogger(__name__)
    log.info("=== Starting step: %s ===", name)
    rc = subprocess.run(cmd, capture_output=False, check=False).returncode
    if rc != 0:
        log.error("Step %s FAILED (exit code %d)", name, rc)
        return False
    log.info("Step %s completed successfully", name)
    return True


def main() -> int:
    args = _parse_args()
    setup_logging()
    log = logging.getLogger(__name__)

    target_date = args.target_date
    steps: list[tuple[str, list[str]]] = [
        ("bronze", [sys.executable, str(PIPELINE_DIR / "build_bronze.py"), target_date]),
        ("silver", [sys.executable, str(PIPELINE_DIR / "build_silver_day.py"), target_date]),
        ("deltas", [sys.executable, str(PIPELINE_DIR / "build_silver_update_deltas.py"), target_date]),
    ]
    if args.with_obs:
        steps.append(
            ("obs", [sys.executable, str(PIPELINE_DIR / "build_obs.py"), target_date])
        )

    failures: list[str] = []
    for name, cmd in steps:
        if not _run_step(name, cmd):
            failures.append(name)

    if failures:
        log.error("Transform stack finished with failures: %s", failures)
        return 1
    log.info("Transform stack finished successfully for %s", target_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
