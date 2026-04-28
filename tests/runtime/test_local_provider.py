"""Tests for src/procurement/runtime/providers/local.py.

All tests use tmp_path — no network, no external deps.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.runtime.providers.local import (
    LocalStateBackend,
    LocalStorageProvider,
    build_local_runtime,
)

# ---------------------------------------------------------------------------
# LocalStorageProvider — path resolution
# ---------------------------------------------------------------------------

class TestLocalStorageProviderResolve:
    def test_resolve_returns_absolute_path(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        result = s.resolve("bronze")
        assert Path(result).is_absolute()

    def test_resolve_appends_logical_path(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert s.resolve("bronze") == str(tmp_path / "bronze")

    def test_resolve_nested(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert s.resolve("silver/notice_type_tables") == str(
            tmp_path / "silver" / "notice_type_tables"
        )

    def test_obs_path_is_under_root(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        obs = s.obs_path()
        assert obs is not None
        assert str(obs).startswith(str(tmp_path))

    def test_obs_path_returns_path_object(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert isinstance(s.obs_path(), Path)


# ---------------------------------------------------------------------------
# LocalStorageProvider — exists / read_json / write_json
# ---------------------------------------------------------------------------

class TestLocalStorageProviderJson:
    def test_write_then_read_roundtrip(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        data = {"key": "value", "count": 42}
        s.write_json("state/foo.json", data)
        assert s.read_json("state/foo.json") == data

    def test_read_missing_returns_empty_dict(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert s.read_json("nonexistent.json") == {}

    def test_write_creates_parent_dirs(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        s.write_json("a/b/c/data.json", {"x": 1})
        assert (tmp_path / "a" / "b" / "c" / "data.json").exists()

    def test_write_is_atomic(self, tmp_path: Path):
        """No .tmp file should remain after a successful write."""
        s = LocalStorageProvider(tmp_path)
        s.write_json("data.json", {"v": 1})
        tmp_files = list(tmp_path.glob("**/*.tmp"))
        assert tmp_files == []

    def test_overwrite_updates_value(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        s.write_json("data.json", {"v": 1})
        s.write_json("data.json", {"v": 2})
        assert s.read_json("data.json") == {"v": 2}

    def test_unicode_content_preserved(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        s.write_json("data.json", {"name": "Łódź"})
        assert s.read_json("data.json") == {"name": "Łódź"}


# ---------------------------------------------------------------------------
# LocalStorageProvider — exists
# ---------------------------------------------------------------------------

class TestLocalStorageProviderExists:
    def test_exists_false_for_missing(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert not s.exists("does_not_exist")

    def test_exists_true_for_directory(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        (tmp_path / "bronze").mkdir()
        assert s.exists("bronze")

    def test_exists_true_for_file(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        (tmp_path / "file.txt").write_text("hi")
        assert s.exists("file.txt")


# ---------------------------------------------------------------------------
# LocalStorageProvider — list_prefixes
# ---------------------------------------------------------------------------

class TestLocalStorageProviderListPrefixes:
    def test_returns_empty_for_missing_dir(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        assert s.list_prefixes("nonexistent") == []

    def test_returns_immediate_child_dirs(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        (tmp_path / "partitions" / "dt=2025-10-01").mkdir(parents=True)
        (tmp_path / "partitions" / "dt=2025-10-02").mkdir(parents=True)
        prefixes = s.list_prefixes("partitions")
        assert prefixes == ["dt=2025-10-01", "dt=2025-10-02"]

    def test_does_not_return_files(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        (tmp_path / "dir").mkdir()
        (tmp_path / "dir" / "subdir").mkdir()
        (tmp_path / "dir" / "file.txt").write_text("x")
        assert s.list_prefixes("dir") == ["subdir"]

    def test_returns_sorted(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        for name in ["c", "a", "b"]:
            (tmp_path / name).mkdir()
        assert s.list_prefixes("") == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# LocalStorageProvider — acquire_lock
# ---------------------------------------------------------------------------

class TestLocalStorageProviderLock:
    def test_lock_acquired_and_released(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        with s.acquire_lock("my-lock"):
            lock_dir = tmp_path / "_locks" / "my-lock"
            assert lock_dir.exists()
        assert not lock_dir.exists()

    def test_second_lock_raises_while_first_held(self, tmp_path: Path):
        lock_dir = tmp_path / "_locks" / "day-lock"
        lock_dir.mkdir(parents=True)
        with pytest.raises(TimeoutError):
            # stale_timeout effectively 0 by manipulating time
            # Instead: create the lock dir manually, then try to acquire
            s2 = LocalStorageProvider(tmp_path)
            # Override timeout to be very short via a separate thread approach:
            # Directly test that mkdir raises FileExistsError → TimeoutError path
            import time as _time
            original_time = _time.time
            call_count = [0]

            def fast_time():
                call_count[0] += 1
                # First call returns now, second call returns now+400 (past deadline)
                return original_time() + (0 if call_count[0] == 1 else 400)

            import unittest.mock
            with unittest.mock.patch("time.time", side_effect=fast_time):
                with s2.acquire_lock("day-lock"):
                    pass

    def test_lock_released_on_exception(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        lock_dir = tmp_path / "_locks" / "err-lock"
        try:
            with s.acquire_lock("err-lock"):
                assert lock_dir.exists()
                raise ValueError("intentional error")
        except ValueError:
            pass
        assert not lock_dir.exists()

    def test_different_keys_do_not_conflict(self, tmp_path: Path):
        s = LocalStorageProvider(tmp_path)
        with s.acquire_lock("lock-a"):
            with s.acquire_lock("lock-b"):
                assert (tmp_path / "_locks" / "lock-a").exists()
                assert (tmp_path / "_locks" / "lock-b").exists()


# ---------------------------------------------------------------------------
# LocalStateBackend
# ---------------------------------------------------------------------------

class TestLocalStateBackend:
    def test_load_missing_returns_empty_dict(self, tmp_path: Path):
        backend = LocalStateBackend(tmp_path / "_state")
        assert backend.load("backfill_run") == {}

    def test_save_then_load_roundtrip(self, tmp_path: Path):
        backend = LocalStateBackend(tmp_path / "_state")
        data = {"2025-10-01": "completed", "2025-10-02": "failed"}
        backend.save("backfill_run", data)
        assert backend.load("backfill_run") == data

    def test_save_creates_parent_dirs(self, tmp_path: Path):
        state_dir = tmp_path / "deep" / "nested" / "_state"
        backend = LocalStateBackend(state_dir)
        backend.save("key", {"x": 1})
        assert (state_dir / "key.json").exists()

    def test_save_overwrites_existing(self, tmp_path: Path):
        backend = LocalStateBackend(tmp_path / "_state")
        backend.save("key", {"v": 1})
        backend.save("key", {"v": 2})
        assert backend.load("key") == {"v": 2}

    def test_different_keys_are_independent(self, tmp_path: Path):
        backend = LocalStateBackend(tmp_path / "_state")
        backend.save("key_a", {"a": 1})
        backend.save("key_b", {"b": 2})
        assert backend.load("key_a") == {"a": 1}
        assert backend.load("key_b") == {"b": 2}

    def test_save_is_atomic(self, tmp_path: Path):
        """No .tmp file should remain after save."""
        backend = LocalStateBackend(tmp_path / "_state")
        backend.save("key", {"v": 1})
        tmp_files = list((tmp_path / "_state").glob("*.tmp"))
        assert tmp_files == []


# ---------------------------------------------------------------------------
# build_local_runtime
# ---------------------------------------------------------------------------

class TestBuildLocalRuntime:
    def test_returns_runtime_config(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        from procurement.runtime.base import RuntimeConfig
        rt = build_local_runtime()
        assert isinstance(rt, RuntimeConfig)

    def test_env_is_local(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        assert build_local_runtime().env == "local"

    def test_storage_root_matches_env(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        rt = build_local_runtime()
        assert rt.storage.resolve("bronze") == str(tmp_path / "bronze")

    def test_invalid_spark_extra_config_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ):
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        monkeypatch.setenv("SPARK_APP_EXTRA_CONFIG", "{not valid json")
        with pytest.raises(ValueError, match="SPARK_APP_EXTRA_CONFIG"):
            build_local_runtime()

    def test_empty_extra_config_is_fine(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        monkeypatch.setenv("SPARK_APP_EXTRA_CONFIG", "")
        build_local_runtime()  # must not raise
