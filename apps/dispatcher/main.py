from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from apps.common.runtime import (
    env_int,
    env_str,
    log_event,
    render_command,
    run_command,
    setup_json_logging,
)

PHASE_IDLE = "idle"
PHASE_FETCHING = "fetching"
PHASE_FINALIZING = "finalizing"
PHASE_DONE = "done"
PHASE_ERROR = "error"


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _iter_days(start_day: str, end_day: str):
    d = date.fromisoformat(start_day)
    e = date.fromisoformat(end_day)
    while d <= e:
        yield d.isoformat()
        d += timedelta(days=1)


def _next_unprocessed_day(start_day: str, end_day: str, state_done_dir: str) -> str | None:
    done_dir = Path(state_done_dir)
    done = {p.name for p in done_dir.glob("dt=*.done")} if done_dir.exists() else set()
    for day in _iter_days(start_day, end_day):
        if f"dt={day}.done" not in done:
            return day
    return None


def _default_state() -> dict[str, Any]:
    now = _now_iso()
    return {
        "phase": PHASE_IDLE,
        "start_date": None,
        "end_date": None,
        "created_at": now,
        "updated_at": now,
        "last_tick_at": None,
        "last_progress_at": None,
        "last_fetched_date": None,
        "finalize_requested_at": None,
        "finalize_completed_at": None,
        "scheduler_paused": False,
        "error": None,
    }


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return _default_state()
    raw = json.loads(path.read_text(encoding="utf-8"))
    state = _default_state()
    state.update(raw)
    return state


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _acquire_lock(lock_path: Path, timeout_sec: int, stale_sec: int) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.time()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                payload = {"pid": os.getpid(), "acquired_at": _now_iso()}
                os.write(fd, json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            finally:
                os.close(fd)
            return
        except FileExistsError:
            if lock_path.exists():
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_sec:
                    lock_path.unlink(missing_ok=True)
                    continue
            if time.time() - started > timeout_sec:
                raise TimeoutError(f"Failed to acquire lock within {timeout_sec}s: {lock_path}")
            time.sleep(1)


def _release_lock(lock_path: Path) -> None:
    lock_path.unlink(missing_ok=True)


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _resolve_target_date(state: dict[str, Any]) -> str:
    return state["end_date"] or env_str("BACKFILL_END_DATE") or date.today().isoformat()


def _run_template_event(
    event_prefix: str,
    command_template: str,
    timeout_sec: int | None,
    **params: str,
) -> tuple[int, float, list[str]]:
    command = render_command(command_template, **params)
    log_event(f"{event_prefix}.start", command=command, params=params)
    rc, elapsed_sec = run_command(command, timeout_sec=timeout_sec)
    log_event(
        f"{event_prefix}.finish",
        returncode=rc,
        elapsed_sec=round(elapsed_sec, 2),
        params=params,
    )
    return rc, elapsed_sec, command


def _run_tick(
    state: dict[str, Any],
    run_fetch: Callable[[str], tuple[int, float]],
    run_finalize: Callable[[str], tuple[int, float]],
    run_pause_scheduler: Callable[[], tuple[int, float]] | None,
    state_done_dir: str,
    backfill_enabled: bool,
) -> tuple[int, dict[str, Any], dict[str, Any]]:
    state["last_tick_at"] = _now_iso()
    if not backfill_enabled:
        return 200, state, {"ok": True, "status": "noop", "reason": "BACKFILL_ENABLED=false"}

    phase = state.get("phase", PHASE_IDLE)
    if phase == PHASE_IDLE:
        return 200, state, {"ok": True, "status": "noop", "reason": "phase=idle"}
    if phase == PHASE_DONE:
        return 200, state, {"ok": True, "status": "done"}
    if phase == PHASE_ERROR:
        return 500, state, {"ok": False, "status": "error", "error": state.get("error")}

    if phase == PHASE_FETCHING:
        start_day = state.get("start_date")
        end_day = state.get("end_date")
        if not start_day or not end_day:
            state["phase"] = PHASE_ERROR
            state["error"] = "Missing start_date/end_date in fetching phase"
            return 500, state, {"ok": False, "status": "error", "error": state["error"]}

        target_date = _next_unprocessed_day(start_day, end_day, state_done_dir)
        if target_date is not None:
            rc, elapsed = run_fetch(target_date)
            if rc != 0:
                state["phase"] = PHASE_ERROR
                state["error"] = f"Fetch failed for {target_date} (rc={rc})"
                return 500, state, {
                    "ok": False,
                    "status": "error",
                    "phase": state["phase"],
                    "target_date": target_date,
                    "returncode": rc,
                }
            state["last_fetched_date"] = target_date
            state["last_progress_at"] = _now_iso()
            return 200, state, {
                "ok": True,
                "status": "fetch_dispatched",
                "target_date": target_date,
                "elapsed_sec": round(elapsed, 2),
            }
        state["phase"] = PHASE_FINALIZING
        state["finalize_requested_at"] = _now_iso()

    if state.get("phase") == PHASE_FINALIZING:
        target_date = _resolve_target_date(state)
        rc, elapsed = run_finalize(target_date)
        if rc != 0:
            state["phase"] = PHASE_ERROR
            state["error"] = f"Finalize failed for {target_date} (rc={rc})"
            return 500, state, {
                "ok": False,
                "status": "error",
                "phase": state["phase"],
                "target_date": target_date,
                "returncode": rc,
            }

        state["phase"] = PHASE_DONE
        state["finalize_completed_at"] = _now_iso()
        state["last_progress_at"] = state["finalize_completed_at"]
        paused = False
        pause_rc = None
        if run_pause_scheduler is not None:
            pause_rc, _ = run_pause_scheduler()
            paused = pause_rc == 0
            state["scheduler_paused"] = paused
        return 200, state, {
            "ok": True,
            "status": "done",
            "target_date": target_date,
            "finalize_elapsed_sec": round(elapsed, 2),
            "scheduler_pause_attempted": run_pause_scheduler is not None,
            "scheduler_pause_ok": paused,
            "scheduler_pause_returncode": pause_rc,
        }

    return 500, state, {"ok": False, "status": "error", "error": f"Unsupported phase: {state.get('phase')}"}


class Handler(BaseHTTPRequestHandler):
    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _state_paths(self) -> tuple[Path, Path]:
        state_path = Path(env_str("BACKFILL_STATE_PATH", "data/state/backfill/state.json", required=True) or "")
        lock_path = Path(str(state_path) + ".lock")
        return state_path, lock_path

    def _with_state_lock(self, fn: Callable[[dict[str, Any]], tuple[int, dict[str, Any], dict[str, Any]]]) -> None:
        state_path, lock_path = self._state_paths()
        timeout_sec = env_int("DISPATCHER_LOCK_TIMEOUT_SEC", 30)
        stale_sec = env_int("DISPATCHER_LOCK_STALE_SEC", 600)
        _acquire_lock(lock_path, timeout_sec=timeout_sec, stale_sec=stale_sec)
        try:
            state = _load_state(state_path)
            status, new_state, payload = fn(state)
            _save_state(state_path, new_state)
            _json_response(self, status, payload)
        except Exception as exc:
            _json_response(self, 500, {"ok": False, "error": str(exc)})
        finally:
            _release_lock(lock_path)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/healthz":
            _json_response(self, 200, {"ok": True})
            return
        if self.path == "/backfill/status":
            state_path, _ = self._state_paths()
            state = _load_state(state_path)
            _json_response(self, 200, {"ok": True, "state": state})
            return
        _json_response(self, 404, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/backfill/start":
            payload = self._read_json()

            def _op(state: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any]]:
                force = bool(payload.get("force", False))
                if state.get("phase") in {PHASE_FETCHING, PHASE_FINALIZING} and not force:
                    return 409, state, {
                        "ok": False,
                        "error": "Backfill already active",
                        "phase": state.get("phase"),
                    }
                start_day = payload.get("start_date") or env_str("BACKFILL_START_DATE", required=True)
                end_day = payload.get("end_date") or env_str("BACKFILL_END_DATE", required=True)
                if not start_day or not end_day:
                    raise ValueError("Missing start_date/end_date")
                state.update(
                    {
                        "phase": PHASE_FETCHING,
                        "start_date": start_day,
                        "end_date": end_day,
                        "last_tick_at": None,
                        "last_progress_at": None,
                        "last_fetched_date": None,
                        "finalize_requested_at": None,
                        "finalize_completed_at": None,
                        "scheduler_paused": False,
                        "error": None,
                    }
                )
                return 200, state, {"ok": True, "status": "started", "state": state}

            self._with_state_lock(_op)
            return

        if self.path in {"/tick", "/dispatch"}:
            state_done_dir = env_str("STATE_DONE_DIR", "data/state/backfill", required=True)
            if not state_done_dir:
                _json_response(self, 500, {"ok": False, "error": "Missing STATE_DONE_DIR"})
                return

            fetch_tpl = env_str(
                "DISPATCHER_TRIGGER_COMMAND_TEMPLATE",
                "python apps/downloader/main.py --target-date {target_date}",
            )
            finalize_tpl = env_str(
                "DISPATCHER_FINALIZE_COMMAND_TEMPLATE",
                "python scripts/ops/run_backfill_finalize.py {target_date}",
            )
            pause_tpl = env_str("DISPATCHER_PAUSE_SCHEDULER_COMMAND_TEMPLATE")

            if not fetch_tpl or not finalize_tpl:
                _json_response(self, 500, {"ok": False, "error": "Missing dispatcher command template"})
                return

            fetch_timeout_sec = env_int("DISPATCHER_TRIGGER_TIMEOUT_SEC", 300) or None
            finalize_timeout_sec = env_int("DISPATCHER_FINALIZE_TIMEOUT_SEC", 43200) or None
            pause_timeout_sec = env_int("DISPATCHER_PAUSE_TIMEOUT_SEC", 120) or None
            enabled_raw = (env_str("BACKFILL_ENABLED", "true") or "true").strip().lower()
            backfill_enabled = enabled_raw in {"1", "true", "yes", "y"}

            def _run_fetch(day: str) -> tuple[int, float]:
                rc, elapsed, _ = _run_template_event(
                    "dispatcher.fetch",
                    fetch_tpl,
                    timeout_sec=fetch_timeout_sec,
                    target_date=day,
                )
                return rc, elapsed

            def _run_finalize(day: str) -> tuple[int, float]:
                rc, elapsed, _ = _run_template_event(
                    "dispatcher.finalize",
                    finalize_tpl,
                    timeout_sec=finalize_timeout_sec,
                    target_date=day,
                )
                return rc, elapsed

            run_pause_scheduler = None
            if pause_tpl:
                def _run_pause_scheduler() -> tuple[int, float]:
                    rc, elapsed, _ = _run_template_event(
                        "dispatcher.pause_scheduler",
                        pause_tpl,
                        timeout_sec=pause_timeout_sec,
                    )
                    return rc, elapsed
                run_pause_scheduler = _run_pause_scheduler

            def _op(state: dict[str, Any]) -> tuple[int, dict[str, Any], dict[str, Any]]:
                status, new_state, payload = _run_tick(
                    state=state,
                    run_fetch=_run_fetch,
                    run_finalize=_run_finalize,
                    run_pause_scheduler=run_pause_scheduler,
                    state_done_dir=state_done_dir,
                    backfill_enabled=backfill_enabled,
                )
                payload["state"] = new_state
                return status, new_state, payload

            self._with_state_lock(_op)
            return

        _json_response(self, 404, {"ok": False, "error": "not_found"})


def main() -> None:
    setup_json_logging(env_str("LOG_LEVEL", "INFO") or "INFO")
    host = "0.0.0.0"
    port = env_int("PORT", 8080)
    server = ThreadingHTTPServer((host, port), Handler)
    log_event("dispatcher.listen", host=host, port=port)
    server.serve_forever()


if __name__ == "__main__":
    main()

