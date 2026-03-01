from __future__ import annotations

import json
import logging
import os
import shlex
import subprocess
import time
from typing import Any


def setup_json_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(message)s")


def log_event(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logging.getLogger(__name__).info(json.dumps(payload, ensure_ascii=False))


def env_str(name: str, default: str | None = None, required: bool = False) -> str | None:
    value = os.getenv(name, default)
    if required and (value is None or value == ""):
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def render_command(template: str, **params: str) -> list[str]:
    rendered = template.format(**params)
    return shlex.split(rendered)


def run_command(command: list[str], timeout_sec: int | None = None) -> tuple[int, float]:
    started = time.time()
    proc = subprocess.run(command, capture_output=False, timeout=timeout_sec, check=False)
    elapsed = time.time() - started
    return proc.returncode, elapsed

