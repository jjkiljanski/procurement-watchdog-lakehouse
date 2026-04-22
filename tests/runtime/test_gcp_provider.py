"""Tests for src/procurement/runtime/providers/gcp.py.

All GCS / Dataproc client calls are mocked via sys.modules — no GCP
credentials or installed google-cloud packages required.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))


# ---------------------------------------------------------------------------
# GCS mock fixture
# ---------------------------------------------------------------------------

class _NotFound(Exception):
    """Stand-in for google.cloud.exceptions.NotFound."""


def _make_gcs_mock() -> tuple[MagicMock, MagicMock]:
    """Return (mock_gcs_module, mock_client)."""
    mock_client = MagicMock()
    mock_gcs = MagicMock()
    mock_gcs.Client.return_value = mock_client

    mock_google_cloud = MagicMock()
    mock_google_cloud.storage = mock_gcs
    mock_google_cloud.exceptions = MagicMock()
    mock_google_cloud.exceptions.NotFound = _NotFound
    mock_google_cloud.exceptions.Conflict = type("Conflict", (Exception,), {})

    mock_google = MagicMock()
    mock_google.cloud = mock_google_cloud

    return mock_gcs, mock_client, mock_google, mock_google_cloud


@pytest.fixture()
def gcs_mocks():
    """Patch sys.modules with GCS mocks; yield (mock_gcs, mock_client)."""
    mock_gcs, mock_client, mock_google, mock_google_cloud = _make_gcs_mock()
    mock_exceptions = mock_google_cloud.exceptions

    with patch.dict(sys.modules, {
        "google": mock_google,
        "google.cloud": mock_google_cloud,
        "google.cloud.storage": mock_gcs,
        "google.cloud.exceptions": mock_exceptions,
        "google.cloud.dataproc_v1": MagicMock(),
    }):
        yield mock_gcs, mock_client


# ---------------------------------------------------------------------------
# GCSStorageProvider — resolve / obs_path
# ---------------------------------------------------------------------------

class TestGCSStorageProviderResolve:
    def test_resolve_returns_gs_uri(self, gcs_mocks):
        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert s.resolve("bronze") == "gs://my-bucket/bronze"

    def test_resolve_nested_path(self, gcs_mocks):
        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert s.resolve("silver/tables") == "gs://my-bucket/silver/tables"

    def test_bucket_property(self, gcs_mocks):
        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("test-bucket")
        assert s.bucket == "test-bucket"

    def test_obs_path_returns_none(self, gcs_mocks):
        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert s.obs_path() is None


# ---------------------------------------------------------------------------
# GCSStorageProvider — read_json / write_json
# ---------------------------------------------------------------------------

class TestGCSStorageProviderJson:
    def test_write_json_calls_upload(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")

        s.write_json("state/foo.json", {"key": "value"})

        mock_client.bucket.assert_called_with("my-bucket")
        blob = mock_client.bucket.return_value.blob.return_value
        blob.upload_from_string.assert_called_once()
        uploaded = blob.upload_from_string.call_args[0][0]
        assert json.loads(uploaded) == {"key": "value"}

    def test_read_json_returns_parsed_dict(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        blob = mock_client.bucket.return_value.blob.return_value
        blob.download_as_text.return_value = '{"layer": "bronze"}'

        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        result = s.read_json("state/foo.json")
        assert result == {"layer": "bronze"}

    def test_read_json_returns_empty_dict_on_not_found(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        blob = mock_client.bucket.return_value.blob.return_value
        blob.download_as_text.side_effect = _NotFound("missing")

        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert s.read_json("missing.json") == {}


# ---------------------------------------------------------------------------
# GCSStorageProvider — exists / list_prefixes
# ---------------------------------------------------------------------------

class TestGCSStorageProviderExists:
    def test_exists_true_when_blobs_found(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        mock_client.list_blobs.return_value = iter([MagicMock()])

        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert s.exists("bronze/notices")

    def test_exists_false_when_no_blobs(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        mock_client.list_blobs.return_value = iter([])

        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        assert not s.exists("bronze/notices")


class TestGCSStorageProviderListPrefixes:
    def test_returns_immediate_child_names(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        blobs_iter = MagicMock()
        blobs_iter.__iter__ = MagicMock(return_value=iter([]))
        blobs_iter.prefixes = [
            "silver/dt=2025-10-01/",
            "silver/dt=2025-10-02/",
        ]
        mock_client.list_blobs.return_value = blobs_iter

        from procurement.runtime.providers.gcp import GCSStorageProvider
        s = GCSStorageProvider("my-bucket")
        prefixes = s.list_prefixes("silver")
        assert prefixes == ["dt=2025-10-01", "dt=2025-10-02"]


# ---------------------------------------------------------------------------
# GCSStateBackend
# ---------------------------------------------------------------------------

class TestGCSStateBackend:
    def test_load_delegates_to_storage_read_json(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        blob = mock_client.bucket.return_value.blob.return_value
        blob.download_as_text.return_value = '{"status": "ok"}'

        from procurement.runtime.providers.gcp import GCSStorageProvider, GCSStateBackend
        storage = GCSStorageProvider("my-bucket")
        backend = GCSStateBackend(storage)
        data = backend.load("backfill_run")
        assert data == {"status": "ok"}

    def test_load_missing_returns_empty_dict(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks
        blob = mock_client.bucket.return_value.blob.return_value
        blob.download_as_text.side_effect = _NotFound("gone")

        from procurement.runtime.providers.gcp import GCSStorageProvider, GCSStateBackend
        storage = GCSStorageProvider("my-bucket")
        backend = GCSStateBackend(storage)
        assert backend.load("missing_key") == {}

    def test_save_writes_to_correct_path(self, gcs_mocks):
        mock_gcs, mock_client = gcs_mocks

        from procurement.runtime.providers.gcp import GCSStorageProvider, GCSStateBackend
        storage = GCSStorageProvider("my-bucket")
        backend = GCSStateBackend(storage)
        backend.save("backfill_run", {"dates": ["2025-10-01"]})

        # Verify the blob path contains _state/ prefix
        blob_path = mock_client.bucket.return_value.blob.call_args[0][0]
        assert "_state/backfill_run.json" in blob_path


# ---------------------------------------------------------------------------
# build_gcp_runtime — env var validation
# ---------------------------------------------------------------------------

class TestBuildGcpRuntime:
    def _set_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LAKEHOUSE_BUCKET", "test-bucket")
        monkeypatch.setenv("GCP_PROJECT", "test-project")
        monkeypatch.setenv("DATAPROC_REGION", "europe-west1")
        monkeypatch.setenv("DATAPROC_CONTAINER_IMAGE", "eu.gcr.io/proj/img:latest")

    def test_returns_runtime_config(self, monkeypatch: pytest.MonkeyPatch, gcs_mocks):
        self._set_all(monkeypatch)
        from procurement.runtime.providers.gcp import build_gcp_runtime
        from procurement.runtime.base import RuntimeConfig
        assert isinstance(build_gcp_runtime(), RuntimeConfig)

    def test_env_is_gcp(self, monkeypatch: pytest.MonkeyPatch, gcs_mocks):
        self._set_all(monkeypatch)
        from procurement.runtime.providers.gcp import build_gcp_runtime
        assert build_gcp_runtime().env == "gcp"

    @pytest.mark.parametrize("missing_var", [
        "LAKEHOUSE_BUCKET", "GCP_PROJECT", "DATAPROC_REGION", "DATAPROC_CONTAINER_IMAGE"
    ])
    def test_missing_required_env_raises(
        self, missing_var: str, monkeypatch: pytest.MonkeyPatch, gcs_mocks
    ):
        self._set_all(monkeypatch)
        monkeypatch.delenv(missing_var)
        from procurement.runtime.providers.gcp import build_gcp_runtime
        with pytest.raises(EnvironmentError, match=missing_var):
            build_gcp_runtime()

    def test_invalid_spark_extra_config_raises(
        self, monkeypatch: pytest.MonkeyPatch, gcs_mocks
    ):
        self._set_all(monkeypatch)
        monkeypatch.setenv("SPARK_APP_EXTRA_CONFIG", "{bad json")
        from procurement.runtime.providers.gcp import build_gcp_runtime
        with pytest.raises(ValueError, match="SPARK_APP_EXTRA_CONFIG"):
            build_gcp_runtime()

    def test_optional_service_account_accepted(
        self, monkeypatch: pytest.MonkeyPatch, gcs_mocks
    ):
        self._set_all(monkeypatch)
        monkeypatch.setenv("DATAPROC_SERVICE_ACCOUNT", "sa@proj.iam.gserviceaccount.com")
        from procurement.runtime.providers.gcp import build_gcp_runtime
        build_gcp_runtime()  # must not raise


# ---------------------------------------------------------------------------
# DataprocServerlessLauncher — submit_batch
# ---------------------------------------------------------------------------

class TestDataprocServerlessLauncherSubmitBatch:
    def _launcher(self):
        from procurement.runtime.providers.gcp import DataprocServerlessLauncher
        return DataprocServerlessLauncher(
            project="test-project",
            region="europe-west1",
            bucket="test-bucket",
            container_image="eu.gcr.io/img:latest",
        )

    def test_submit_batch_returns_0_on_success(self, gcs_mocks):
        import google.cloud.dataproc_v1 as dataproc  # gets our mock from sys.modules
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.state = dataproc.Batch.State.SUCCEEDED
        mock_op.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.create_batch.return_value = mock_op
        dataproc.BatchControllerClient.return_value = mock_client

        launcher = self._launcher()
        rc = launcher.submit_batch("gs://bucket/jobs/build_bronze.py", ["2025-10-01"], "bronze-job")
        assert rc == 0

    def test_submit_batch_returns_1_on_failure(self, gcs_mocks):
        import google.cloud.dataproc_v1 as dataproc
        mock_op = MagicMock()
        mock_result = MagicMock()
        mock_result.state = dataproc.Batch.State.FAILED
        mock_op.result.return_value = mock_result

        mock_client = MagicMock()
        mock_client.create_batch.return_value = mock_op
        dataproc.BatchControllerClient.return_value = mock_client

        launcher = self._launcher()
        rc = launcher.submit_batch("gs://bucket/jobs/build_bronze.py", ["2025-10-01"], "bronze-job")
        assert rc == 1

    def test_submit_batch_wait_false_does_not_block(self, gcs_mocks):
        import google.cloud.dataproc_v1 as dataproc
        mock_op = MagicMock()
        mock_client = MagicMock()
        mock_client.create_batch.return_value = mock_op
        dataproc.BatchControllerClient.return_value = mock_client

        launcher = self._launcher()
        rc = launcher.submit_batch(
            "gs://bucket/jobs/build_bronze.py", ["2025-10-01"], "bronze-job", wait=False
        )
        assert rc == 0
        mock_op.result.assert_not_called()
