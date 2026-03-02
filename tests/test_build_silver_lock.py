from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest


def _load_build_silver_module():
    return importlib.import_module("procurement.silver.build_core")


def test_acquire_day_lock_creates_owner_file(tmp_path: Path):
    module = _load_build_silver_module()

    lock_dir = module._acquire_day_lock(
        silver_dir=tmp_path,
        day="2025-10-01",
        run_id="run-abc",
        stale_minutes=60,
    )

    assert lock_dir.exists()
    owner_path = lock_dir / "owner.json"
    assert owner_path.exists()
    owner = json.loads(owner_path.read_text(encoding="utf-8"))
    assert owner["run_id"] == "run-abc"
    assert owner["target_date"] == "2025-10-01"


def test_acquire_day_lock_blocks_when_lock_is_fresh(tmp_path: Path):
    module = _load_build_silver_module()

    first_lock = module._acquire_day_lock(
        silver_dir=tmp_path,
        day="2025-10-01",
        run_id="run-owner",
        stale_minutes=60,
    )
    assert first_lock.exists()

    with pytest.raises(RuntimeError, match="Silver day lock already exists"):
        module._acquire_day_lock(
            silver_dir=tmp_path,
            day="2025-10-01",
            run_id="run-second",
            stale_minutes=60,
        )


def test_acquire_day_lock_replaces_stale_lock(tmp_path: Path):
    module = _load_build_silver_module()

    stale_lock = tmp_path / "_locks" / "silver_day=2025-10-01"
    stale_lock.mkdir(parents=True, exist_ok=False)
    (stale_lock / "owner.json").write_text(
        json.dumps({"run_id": "stale-run", "target_date": "2025-10-01"}),
        encoding="utf-8",
    )

    old_ts = 1_000_000_000
    stale_owner = stale_lock / "owner.json"
    stale_owner.touch(exist_ok=True)
    # Force very old mtime so the lock is considered stale.
    import os

    os.utime(stale_lock, (old_ts, old_ts))
    os.utime(stale_owner, (old_ts, old_ts))

    new_lock = module._acquire_day_lock(
        silver_dir=tmp_path,
        day="2025-10-01",
        run_id="fresh-run",
        stale_minutes=1,
    )
    owner = json.loads((new_lock / "owner.json").read_text(encoding="utf-8"))
    assert owner["run_id"] == "fresh-run"
