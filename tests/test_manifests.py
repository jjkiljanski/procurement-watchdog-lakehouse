"""Tests for src/procurement/manifests.py.

All tests use tmp_path (local filesystem) via LocalStorageProvider — no GCS
or network access required.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from procurement.manifests import _manifest_path, is_already_processed, write_processed_manifest
from procurement.runtime.providers.local import LocalStorageProvider


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _storage(tmp_path: Path) -> LocalStorageProvider:
    return LocalStorageProvider(data_root=tmp_path)


def _read_manifest(tmp_path: Path, layer: str, target_date: str) -> dict:
    path = tmp_path / "_processed" / layer / f"{target_date}.json"
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# _manifest_path
# ---------------------------------------------------------------------------

class TestManifestPath:
    def test_returns_correct_logical_path(self):
        assert _manifest_path("bronze", "2025-10-01") == "_processed/bronze/2025-10-01.json"

    def test_layer_and_date_are_embedded(self):
        path = _manifest_path("silver", "2024-01-31")
        assert "silver" in path
        assert "2024-01-31" in path


# ---------------------------------------------------------------------------
# write_processed_manifest
# ---------------------------------------------------------------------------

class TestWriteProcessedManifest:
    def test_creates_json_file_at_expected_path(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "abc123", storage)

        manifest_file = tmp_path / "_processed" / "bronze" / "2025-10-01.json"
        assert manifest_file.exists()

    def test_manifest_contains_required_fields(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("silver", "2025-10-02", "deadbeef", storage)

        data = _read_manifest(tmp_path, "silver", "2025-10-02")
        assert data["layer"] == "silver"
        assert data["target_date"] == "2025-10-02"
        assert data["script_hash"] == "deadbeef"
        assert "completed_at" in data

    def test_completed_at_is_iso8601_utc(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("fetch", "2025-10-03", "cafebabe", storage)

        data = _read_manifest(tmp_path, "fetch", "2025-10-03")
        # Must end with Z (UTC) and be parseable
        from datetime import datetime
        completed_at = data["completed_at"]
        assert completed_at.endswith("Z")
        datetime.fromisoformat(completed_at.replace("Z", "+00:00"))  # raises on invalid

    def test_overwrite_updates_hash(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "old_hash", storage)
        write_processed_manifest("bronze", "2025-10-01", "new_hash", storage)

        data = _read_manifest(tmp_path, "bronze", "2025-10-01")
        assert data["script_hash"] == "new_hash"

    def test_different_dates_are_independent(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "hash_a", storage)
        write_processed_manifest("bronze", "2025-10-02", "hash_b", storage)

        assert _read_manifest(tmp_path, "bronze", "2025-10-01")["script_hash"] == "hash_a"
        assert _read_manifest(tmp_path, "bronze", "2025-10-02")["script_hash"] == "hash_b"

    def test_different_layers_are_independent(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "bronze_hash", storage)
        write_processed_manifest("silver", "2025-10-01", "silver_hash", storage)

        assert _read_manifest(tmp_path, "bronze", "2025-10-01")["script_hash"] == "bronze_hash"
        assert _read_manifest(tmp_path, "silver", "2025-10-01")["script_hash"] == "silver_hash"

    def test_creates_parent_directories(self, tmp_path: Path):
        """Parent dirs must not pre-exist — storage provider creates them."""
        storage = _storage(tmp_path / "nested" / "root")
        write_processed_manifest("deltas", "2025-10-01", "abc", storage)
        assert (tmp_path / "nested" / "root" / "_processed" / "deltas" / "2025-10-01.json").exists()


# ---------------------------------------------------------------------------
# is_already_processed
# ---------------------------------------------------------------------------

class TestIsAlreadyProcessed:
    def test_returns_false_when_manifest_absent(self, tmp_path: Path):
        storage = _storage(tmp_path)
        assert not is_already_processed("bronze", "2025-10-01", "any_hash", storage)

    def test_returns_true_when_hash_matches(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "correct_hash", storage)
        assert is_already_processed("bronze", "2025-10-01", "correct_hash", storage)

    def test_returns_false_when_hash_differs(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "old_hash", storage)
        assert not is_already_processed("bronze", "2025-10-01", "new_hash", storage)

    def test_layer_mismatch_returns_false(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "some_hash", storage)
        assert not is_already_processed("silver", "2025-10-01", "some_hash", storage)

    def test_date_mismatch_returns_false(self, tmp_path: Path):
        storage = _storage(tmp_path)
        write_processed_manifest("bronze", "2025-10-01", "some_hash", storage)
        assert not is_already_processed("bronze", "2025-10-02", "some_hash", storage)

    def test_roundtrip_write_then_check(self, tmp_path: Path):
        """Full write→check cycle as a pipeline script would use it."""
        storage = _storage(tmp_path)
        script_hash = "abc123def456"

        assert not is_already_processed("silver", "2025-11-15", script_hash, storage)
        write_processed_manifest("silver", "2025-11-15", script_hash, storage)
        assert is_already_processed("silver", "2025-11-15", script_hash, storage)

        # Simulate a script upgrade: different hash → should re-run
        assert not is_already_processed("silver", "2025-11-15", "upgraded_hash", storage)
