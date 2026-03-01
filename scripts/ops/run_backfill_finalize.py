"""Finalize backfill range by rebuilding case_derived_facts and Gold as-of."""

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
    parser = argparse.ArgumentParser(description="Finalize backfill (case_derived + gold as-of).")
    parser.add_argument("target_date", help="As-of date in YYYY-MM-DD format")
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

    steps = [
        (
            "case-derived-full",
            [
                sys.executable,
                str(PIPELINE_DIR / "build_case_derived_facts.py"),
                args.target_date,
                "--mode",
                "full",
            ],
        ),
        (
            "gold-asof",
            [
                sys.executable,
                str(PIPELINE_DIR / "build_gold.py"),
                args.target_date,
                "--scope",
                "asof",
            ],
        ),
    ]
    failures: list[str] = []
    for name, cmd in steps:
        if not _run_step(name, cmd):
            failures.append(name)

    if failures:
        log.error("Backfill finalize finished with failures: %s", failures)
        return 1
    log.info("Backfill finalize finished successfully for %s", args.target_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

