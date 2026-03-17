"""Run the full BZP pipeline: fetch → bronze → silver → gold.

Usage:
    python scripts/ops/run_pipeline.py [YYYY-MM-DD]

Defaults to yesterday when no date is given.  Each step is wrapped in
try/except so partial failures are logged but don't crash the pipeline.
"""

import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.logging import setup_logging

SCRIPTS_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = SCRIPTS_DIR.parent / "pipeline"

STEPS = [
    ("fetch", PIPELINE_DIR / "fetch_bzp_yesterday.py"),
    ("bronze", PIPELINE_DIR / "build_bronze.py"),
    ("silver", PIPELINE_DIR / "build_silver.py"),
    ("gold", PIPELINE_DIR / "build_gold.py"),
    ("obs", PIPELINE_DIR / "build_obs.py"),
]


def run_step(name: str, script: Path, target_date: str) -> bool:
    """Run a pipeline step as a subprocess. Returns True on success."""
    log = logging.getLogger(__name__)
    log.info("=== Starting step: %s ===", name)

    result = subprocess.run(
        [sys.executable, str(script), target_date],
        capture_output=False,
    )

    if result.returncode != 0:
        log.error("Step %s FAILED (exit code %d)", name, result.returncode)
        return False

    log.info("Step %s completed successfully", name)
    return True


def main() -> None:
    if len(sys.argv) > 1:
        target_date = sys.argv[1]
        # Validate the date format
        date.fromisoformat(target_date)
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    log_file = Path("data/logs") / f"pipeline_{target_date}.log"
    setup_logging(log_file=log_file)
    log = logging.getLogger(__name__)

    log.info("Pipeline started for date=%s", target_date)

    failed_steps: list[str] = []

    for name, script in STEPS:
        try:
            ok = run_step(name, script, target_date)
            if not ok:
                failed_steps.append(name)
        except Exception:
            log.error("Step %s raised an exception", name, exc_info=True)
            failed_steps.append(name)

    if failed_steps:
        log.error("Pipeline finished with failures: %s", failed_steps)
        sys.exit(1)
    else:
        log.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()
