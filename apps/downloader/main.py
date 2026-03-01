from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from apps.common.runtime import (
    env_int,
    env_str,
    log_event,
    render_command,
    run_command,
    setup_json_logging,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Downloader runtime adapter (Cloud Run Job friendly).")
    parser.add_argument("--target-date", help="Override target date YYYY-MM-DD (defaults from env or yesterday)")
    parser.add_argument(
        "--command-template",
        default=None,
        help="Optional command template. Defaults to python scripts/pipeline/fetch_bzp_yesterday.py {target_date}",
    )
    return parser.parse_args()


def _resolve_target_date(explicit: str | None) -> str:
    if explicit:
        return explicit
    from_env = env_str("TARGET_DATE")
    if from_env:
        return from_env
    return (date.today() - timedelta(days=1)).isoformat()


def _write_done_marker(target_date: str) -> None:
    done_dir = env_str("STATE_DONE_DIR")
    if not done_dir:
        return
    p = Path(done_dir)
    p.mkdir(parents=True, exist_ok=True)
    marker = p / f"dt={target_date}.done"
    marker.write_text("", encoding="utf-8")
    log_event("downloader.done_marker_written", path=str(marker), target_date=target_date)


def main() -> int:
    args = _parse_args()
    setup_json_logging(env_str("LOG_LEVEL", "INFO") or "INFO")

    target_date = _resolve_target_date(args.target_date)
    timeout_sec = env_int("DOWNLOADER_TIMEOUT_SEC", 0) or None
    command_template = args.command_template or env_str(
        "DOWNLOADER_COMMAND_TEMPLATE",
        "python scripts/pipeline/fetch_bzp_yesterday.py {target_date}",
    )
    if not command_template:
        raise ValueError("Empty downloader command template")
    command = render_command(command_template, target_date=target_date)

    log_event("downloader.start", target_date=target_date, command=command, timeout_sec=timeout_sec)
    returncode, elapsed_sec = run_command(command, timeout_sec=timeout_sec)
    log_event(
        "downloader.finish",
        target_date=target_date,
        returncode=returncode,
        elapsed_sec=round(elapsed_sec, 2),
    )

    if returncode == 0:
        _write_done_marker(target_date)
    return returncode


if __name__ == "__main__":
    raise SystemExit(main())
