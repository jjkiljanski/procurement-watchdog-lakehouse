from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from apps.common.runtime import (
    env_int,
    env_str,
    log_event,
    render_command,
    run_command,
    setup_json_logging,
)


def _default_target_date() -> str:
    return (date.today() - timedelta(days=1)).isoformat()


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class Handler(BaseHTTPRequestHandler):
    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            _json_response(self, 200, {"ok": True})
            return
        _json_response(self, 404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/launch":
            _json_response(self, 404, {"ok": False, "error": "not_found"})
            return
        try:
            payload = self._read_json()
            target_date = payload.get("target_date") or env_str("TARGET_DATE") or _default_target_date()
            command_template = env_str(
                "LAUNCHER_COMMAND_TEMPLATE",
                "python scripts/ops/run_transforms_for_day.py {target_date}",
            )
            if not command_template:
                raise ValueError("Missing LAUNCHER_COMMAND_TEMPLATE")
            timeout_sec = env_int("LAUNCHER_TIMEOUT_SEC", 7200) or None
            command = render_command(command_template, target_date=target_date)
            log_event("launcher.start", target_date=target_date, command=command)
            rc, elapsed_sec = run_command(command, timeout_sec=timeout_sec)
            log_event(
                "launcher.finish",
                target_date=target_date,
                returncode=rc,
                elapsed_sec=round(elapsed_sec, 2),
            )
            status = 200 if rc == 0 else 500
            _json_response(
                self,
                status,
                {
                    "ok": rc == 0,
                    "target_date": target_date,
                    "returncode": rc,
                    "elapsed_sec": round(elapsed_sec, 2),
                },
            )
        except Exception as exc:
            _json_response(self, 500, {"ok": False, "error": str(exc)})


def main() -> None:
    setup_json_logging(env_str("LOG_LEVEL", "INFO") or "INFO")
    host = "0.0.0.0"
    port = env_int("PORT", 8080)
    server = ThreadingHTTPServer((host, port), Handler)
    log_event("launcher.listen", host=host, port=port)
    server.serve_forever()


if __name__ == "__main__":
    main()
