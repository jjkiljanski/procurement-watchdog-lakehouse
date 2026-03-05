from __future__ import annotations

from pathlib import Path

from apps.dispatcher.main import (
    PHASE_FETCHING,
    PHASE_FINALIZING,
    PHASE_IDLE,
    _next_unprocessed_day,
    _run_tick,
)


def _base_state() -> dict:
    return {
        "phase": PHASE_IDLE,
        "start_date": "2025-01-01",
        "end_date": "2025-01-03",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "last_tick_at": None,
        "last_progress_at": None,
        "last_fetched_date": None,
        "finalize_requested_at": None,
        "finalize_completed_at": None,
        "scheduler_paused": False,
        "error": None,
    }


def test_next_unprocessed_day(tmp_path: Path) -> None:
    done_dir = tmp_path / "done"
    done_dir.mkdir(parents=True, exist_ok=True)
    (done_dir / "dt=2025-01-01.done").write_text("", encoding="utf-8")
    assert _next_unprocessed_day("2025-01-01", "2025-01-03", str(done_dir)) == "2025-01-02"


def test_tick_fetching_dispatches_one_day(tmp_path: Path) -> None:
    done_dir = tmp_path / "done"
    done_dir.mkdir(parents=True, exist_ok=True)
    state = _base_state()
    state["phase"] = PHASE_FETCHING
    called: list[str] = []

    def run_fetch(day: str):
        called.append(day)
        (done_dir / f"dt={day}.done").write_text("", encoding="utf-8")
        return 0, 0.3

    status, new_state, payload = _run_tick(
        state=state,
        run_fetch=run_fetch,
        run_finalize=lambda _: (0, 0.5),
        run_pause_scheduler=None,
        state_done_dir=str(done_dir),
        backfill_enabled=True,
    )
    assert status == 200
    assert payload["status"] == "fetch_dispatched"
    assert called == ["2025-01-01"]
    assert new_state["phase"] == PHASE_FETCHING
    assert new_state["last_fetched_date"] == "2025-01-01"


def test_tick_transitions_to_finalize_and_done(tmp_path: Path) -> None:
    done_dir = tmp_path / "done"
    done_dir.mkdir(parents=True, exist_ok=True)
    for day in ["2025-01-01", "2025-01-02", "2025-01-03"]:
        (done_dir / f"dt={day}.done").write_text("", encoding="utf-8")

    state = _base_state()
    state["phase"] = PHASE_FETCHING
    paused = {"called": 0}

    def run_pause():
        paused["called"] += 1
        return 0, 0.1

    status, new_state, payload = _run_tick(
        state=state,
        run_fetch=lambda _: (0, 0.1),
        run_finalize=lambda _: (0, 1.2),
        run_pause_scheduler=run_pause,
        state_done_dir=str(done_dir),
        backfill_enabled=True,
    )
    assert status == 200
    assert payload["status"] == "done"
    assert new_state["phase"] == "done"
    assert new_state["finalize_completed_at"] is not None
    assert paused["called"] == 1


def test_tick_noop_when_disabled(tmp_path: Path) -> None:
    state = _base_state()
    state["phase"] = PHASE_FETCHING
    status, _, payload = _run_tick(
        state=state,
        run_fetch=lambda _: (0, 0.1),
        run_finalize=lambda _: (0, 0.1),
        run_pause_scheduler=None,
        state_done_dir=str(tmp_path / "none"),
        backfill_enabled=False,
    )
    assert status == 200
    assert payload["status"] == "noop"

