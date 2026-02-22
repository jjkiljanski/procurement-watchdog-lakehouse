from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path


def acquire_directory_lock(
    lock_dir: Path,
    owner_payload: dict,
    stale_seconds: int,
) -> Path:
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    stale_seconds = max(1, stale_seconds)

    if lock_dir.exists():
        age_sec = time.time() - lock_dir.stat().st_mtime
        if age_sec > stale_seconds:
            shutil.rmtree(lock_dir, ignore_errors=False)
        else:
            owner_path = lock_dir / "owner.json"
            owner = {}
            if owner_path.exists():
                try:
                    owner = json.loads(owner_path.read_text(encoding="utf-8"))
                except Exception:
                    owner = {}
            raise RuntimeError(f"Directory lock already exists: {lock_dir} owner={owner or 'unknown'}")

    lock_dir.mkdir(parents=False, exist_ok=False)
    (lock_dir / "owner.json").write_text(json.dumps(owner_payload, ensure_ascii=False), encoding="utf-8")
    return lock_dir


def release_directory_lock_if_owner(lock_dir: Path, owner_run_id: str | None) -> bool:
    if not lock_dir.exists():
        return False

    owner_path = lock_dir / "owner.json"
    existing_run_id: str | None = None
    if owner_path.exists():
        try:
            existing_run_id = json.loads(owner_path.read_text(encoding="utf-8")).get("run_id")
        except Exception:
            existing_run_id = None

    if existing_run_id is None or (owner_run_id is not None and existing_run_id == owner_run_id):
        shutil.rmtree(lock_dir, ignore_errors=True)
        return True
    return False


def acquire_token_file_lock(
    lock_path: Path,
    payload: dict,
    timeout_sec: int,
    poll_sec: int,
    stale_sec: int,
    break_stale_lock: bool,
) -> None:
    deadline = time.time() + timeout_sec
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as file_handle:
                json.dump(payload, file_handle, ensure_ascii=False, indent=2)
            return
        except FileExistsError:
            if break_stale_lock and lock_path.exists():
                age = time.time() - lock_path.stat().st_mtime
                if age > stale_sec:
                    lock_path.unlink(missing_ok=True)
                    continue
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out acquiring lock: {lock_path}")
            time.sleep(max(1, poll_sec))


def release_token_file_lock(lock_path: Path, token: str, token_key: str = "token") -> bool:
    if not lock_path.exists():
        return False
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if payload.get(token_key) == token:
        lock_path.unlink(missing_ok=True)
        return True
    return False

