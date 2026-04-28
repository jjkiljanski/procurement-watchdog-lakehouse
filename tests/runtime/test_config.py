"""Tests for src/procurement/runtime/config.py — get_runtime() factory."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))


from procurement.runtime.base import RuntimeConfig
from procurement.runtime.config import get_runtime
from procurement.runtime.providers.local import LocalStateBackend, LocalStorageProvider


class TestGetRuntimeLocal:
    def test_returns_runtime_config(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("RUNTIME_ENV", "local")
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        rt = get_runtime()
        assert isinstance(rt, RuntimeConfig)

    def test_env_is_local(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("RUNTIME_ENV", "local")
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        assert get_runtime().env == "local"

    def test_default_when_env_unset(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        rt = get_runtime()
        assert rt.env == "local"

    def test_storage_is_local_provider(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("RUNTIME_ENV", "local")
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        assert isinstance(get_runtime().storage, LocalStorageProvider)

    def test_state_is_local_backend(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("RUNTIME_ENV", "local")
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        assert isinstance(get_runtime().state, LocalStateBackend)

    def test_case_insensitive(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
        monkeypatch.setenv("RUNTIME_ENV", "LOCAL")
        monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
        assert get_runtime().env == "local"


class TestGetRuntimeGcp:
    def _set_required_gcp_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        monkeypatch.setenv("LAKEHOUSE_BUCKET", "test-bucket")
        monkeypatch.setenv("GCP_PROJECT", "test-project")
        monkeypatch.setenv("DATAPROC_REGION", "europe-west1")
        monkeypatch.setenv("DATAPROC_CONTAINER_IMAGE", "eu.gcr.io/proj/img:latest")

    def test_returns_gcp_runtime(self, monkeypatch: pytest.MonkeyPatch):
        self._set_required_gcp_env(monkeypatch)
        from procurement.runtime.providers.gcp import GCSStorageProvider
        rt = get_runtime()
        assert rt.env == "gcp"
        assert isinstance(rt.storage, GCSStorageProvider)

    def test_missing_required_env_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        monkeypatch.delenv("LAKEHOUSE_BUCKET", raising=False)
        monkeypatch.delenv("GCP_PROJECT", raising=False)
        monkeypatch.delenv("DATAPROC_REGION", raising=False)
        monkeypatch.delenv("DATAPROC_CONTAINER_IMAGE", raising=False)
        with pytest.raises(EnvironmentError):
            get_runtime()


class TestGetRuntimeUnknown:
    def test_raises_value_error(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "azure")
        with pytest.raises(ValueError, match="Unknown RUNTIME_ENV"):
            get_runtime()

    def test_error_mentions_supported_values(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "aws")
        with pytest.raises(ValueError, match="local.*gcp|gcp.*local"):
            get_runtime()
