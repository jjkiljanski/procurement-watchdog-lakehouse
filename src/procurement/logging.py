"""Shared structured logging configuration for the pipeline."""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Pipeline-specific extra fields promoted to top-level JSON keys in every log
# record that carries them.  In GCP Cloud Logging these become
# ``jsonPayload.<field>`` and can be used in log filters, e.g.:
#   jsonPayload.stage="bronze"
#   jsonPayload.stage="silver" AND jsonPayload.date="2025-10-01"
#   jsonPayload.stage="fetch" AND jsonPayload.status="skipped"
_STRUCTURED_FIELDS = ("stage", "date", "notice_type", "status", "elapsed_s", "runtime")


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log line.

    Pipeline-specific extra fields (stage, date, notice_type, status,
    elapsed_s) are promoted to top-level JSON keys when present so that GCP
    Cloud Logging indexes them as ``jsonPayload.*`` for filtering.
    """

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for field in _STRUCTURED_FIELDS:
            val = record.__dict__.get(field)
            if val is not None:
                entry[field] = val
        if record.exc_info and record.exc_info[0] is not None:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(
    level: int = logging.INFO,
    log_file: str | Path | None = None,
) -> None:
    """Configure the root logger with JSON output to stderr and optionally a file."""
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    formatter = JsonFormatter()

    stderr_handler = logging.StreamHandler(sys.stderr)
    stderr_handler.setFormatter(formatter)
    root.addHandler(stderr_handler)

    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)


class _StageAdapter(logging.LoggerAdapter):
    """LoggerAdapter that merges call-site extra= with the stage dict.

    Python < 3.13 LoggerAdapter.process() overwrites the caller's extra kwarg
    rather than merging it.  This subclass always merges so that per-call
    context (date, notice_type, status) coexists with the base stage field.
    """

    def process(self, msg, kwargs):
        call_extra = kwargs.get("extra") or {}
        kwargs["extra"] = {**self.extra, **call_extra}
        return msg, kwargs


def get_stage_logger(name: str, stage: str) -> logging.LoggerAdapter:
    """Return a LoggerAdapter that injects ``stage=<stage>`` into every log record.

    Usage::

        log = get_stage_logger(__name__, "bronze")
        log.info("Processed %s", target_date, extra={"date": target_date, "status": "ok"})
        # → {"stage": "bronze", "date": "2025-10-01", "status": "ok", ...}

    Per-call ``extra=`` kwargs are merged with the adapter's base extra, so
    adding per-record context works alongside the always-present stage field.
    """
    return _StageAdapter(logging.getLogger(name), {"stage": stage})
