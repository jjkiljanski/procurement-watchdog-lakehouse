"""Tests for the local Parquet write path in src/procurement/obs.py.

Also covers the BQ _ensure_bq_table NotFound branch (table auto-creation
when neither dataset nor table exist).

The BQ streaming-insert path is tested separately in tests/test_obs_bq.py.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import procurement.obs as obs_module
from procurement.obs import (
    write_dq_metrics,
    write_pipeline_run,
    write_quarantine_summary,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run_kwargs(**overrides) -> dict:
    base = dict(
        layer="bronze",
        target_date="2025-10-01",
        run_id="run-001",
        started_at="2025-10-02T03:00:00Z",
        completed_at="2025-10-02T03:05:00Z",
        status="ok",
        counts={"raw_total": 100},
        git_commit="abc123",
        script_hash="deadbeef",
    )
    base.update(overrides)
    return base


def _parquet_files(obs_dir: Path, table: str, target_date: str) -> list[Path]:
    return list((obs_dir / table / f"dt={target_date}").glob("*.parquet"))


# ---------------------------------------------------------------------------
# write_pipeline_run — local Parquet
# ---------------------------------------------------------------------------

class TestWritePipelineRunLocal:
    def test_creates_parquet_file(self, tmp_path: Path):
        obs_dir = tmp_path / "obs"
        write_pipeline_run(**_run_kwargs(), obs_dir=obs_dir)
        files = _parquet_files(obs_dir, "pipeline_runs", "2025-10-01")
        assert len(files) == 1

    def test_parquet_file_is_readable(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_pipeline_run(**_run_kwargs(), obs_dir=obs_dir)
        files = _parquet_files(obs_dir, "pipeline_runs", "2025-10-01")
        table = pq.read_table(str(files[0]))
        assert table.num_rows == 1

    def test_layer_and_target_date_stored(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_pipeline_run(**_run_kwargs(layer="silver", target_date="2025-11-01"), obs_dir=obs_dir)
        files = _parquet_files(obs_dir, "pipeline_runs", "2025-11-01")
        df = pq.read_table(str(files[0])).to_pydict()
        assert df["layer"] == ["silver"]
        assert df["target_date"] == ["2025-11-01"]

    def test_count_columns_written(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_pipeline_run(
            **_run_kwargs(counts={"raw_total": 50, "valid_total": 48}),
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "pipeline_runs", "2025-10-01")
        df = pq.read_table(str(files[0])).to_pydict()
        assert df["count_raw_total"] == [50]
        assert df["count_valid_total"] == [48]

    def test_multiple_runs_append(self, tmp_path: Path):
        obs_dir = tmp_path / "obs"
        for i in range(3):
            write_pipeline_run(**_run_kwargs(run_id=f"run-{i:03d}"), obs_dir=obs_dir)
        files = _parquet_files(obs_dir, "pipeline_runs", "2025-10-01")
        assert len(files) == 3  # each write appends a new file

    def test_does_not_call_bq_when_obs_dir_given(self, tmp_path: Path, monkeypatch):
        """obs_dir being set must short-circuit to Parquet — BQ must not be touched."""
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        obs_module._bq_tables_confirmed.clear()

        mock_bq = MagicMock()
        mock_google_cloud = MagicMock()
        mock_google_cloud.bigquery = mock_bq
        mock_google = MagicMock()
        mock_google.cloud = mock_google_cloud

        with patch.dict(sys.modules, {
            "google": mock_google,
            "google.cloud": mock_google_cloud,
            "google.cloud.bigquery": mock_bq,
            "google.cloud.exceptions": MagicMock(),
        }):
            write_pipeline_run(**_run_kwargs(), obs_dir=tmp_path / "obs")
            mock_bq.Client.assert_not_called()


# ---------------------------------------------------------------------------
# write_dq_metrics — local Parquet
# ---------------------------------------------------------------------------

class TestWriteDqMetricsLocal:
    def test_creates_parquet_file(self, tmp_path: Path):
        obs_dir = tmp_path / "obs"
        write_dq_metrics(
            layer="bronze",
            target_date="2025-10-01",
            notice_type=None,
            metrics={"valid_rate": 0.95},
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "dq_metrics", "2025-10-01")
        assert len(files) == 1

    def test_one_row_per_metric(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_dq_metrics(
            layer="bronze",
            target_date="2025-10-01",
            notice_type=None,
            metrics={"valid_rate": 0.95, "invalid_count": 5.0},
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "dq_metrics", "2025-10-01")
        table = pq.read_table(str(files[0]))
        assert table.num_rows == 2

    def test_notice_type_defaults_to_all(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_dq_metrics(
            layer="silver",
            target_date="2025-10-01",
            notice_type=None,
            metrics={"m": 1.0},
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "dq_metrics", "2025-10-01")
        df = pq.read_table(str(files[0])).to_pydict()
        assert df["notice_type"] == ["__all__"]

    def test_empty_metrics_writes_nothing(self, tmp_path: Path):
        obs_dir = tmp_path / "obs"
        write_dq_metrics(
            layer="bronze",
            target_date="2025-10-01",
            notice_type="ContractNotice",
            metrics={},
            obs_dir=obs_dir,
        )
        assert not (obs_dir / "dq_metrics").exists()


# ---------------------------------------------------------------------------
# write_quarantine_summary — local Parquet
# ---------------------------------------------------------------------------

class TestWriteQuarantineSummaryLocal:
    def test_creates_parquet_file(self, tmp_path: Path):
        obs_dir = tmp_path / "obs"
        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="ContractNotice",
            row_count=7,
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "quarantine_summary", "2025-10-01")
        assert len(files) == 1

    def test_correct_values_stored(self, tmp_path: Path):
        import pyarrow.parquet as pq

        obs_dir = tmp_path / "obs"
        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="TenderResultNotice",
            row_count=12,
            obs_dir=obs_dir,
        )
        files = _parquet_files(obs_dir, "quarantine_summary", "2025-10-01")
        df = pq.read_table(str(files[0])).to_pydict()
        assert df["notice_type"] == ["TenderResultNotice"]
        assert df["row_count"] == [12]


# ---------------------------------------------------------------------------
# _ensure_bq_table — NotFound branch (table/dataset auto-creation)
# ---------------------------------------------------------------------------

class _NotFound(Exception):
    pass


class _Conflict(Exception):
    pass


@pytest.fixture()
def bq_mocks():
    mock_bq = MagicMock()
    mock_exceptions = MagicMock()
    mock_exceptions.NotFound = _NotFound

    mock_client = MagicMock()
    # Simulate dataset and table both missing on first call
    mock_client.get_dataset.side_effect = _NotFound("no dataset")
    mock_client.get_table.side_effect = _NotFound("no table")
    mock_client.query.return_value = MagicMock()
    mock_bq.Client.return_value = mock_client
    mock_bq.DatasetReference = MagicMock(return_value=MagicMock())
    mock_bq.Dataset = MagicMock(return_value=MagicMock())

    mock_google_cloud = MagicMock()
    mock_google_cloud.bigquery = mock_bq
    mock_google_cloud.exceptions = mock_exceptions

    mock_google = MagicMock()
    mock_google.cloud = mock_google_cloud

    obs_module._bq_tables_confirmed.clear()

    with patch.dict(sys.modules, {
        "google": mock_google,
        "google.cloud": mock_google_cloud,
        "google.cloud.bigquery": mock_bq,
        "google.cloud.exceptions": mock_exceptions,
    }):
        yield mock_client


class TestEnsureBqTableNotFoundBranch:
    def test_creates_dataset_when_not_found(
        self, bq_mocks: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        monkeypatch.setenv("GCP_PROJECT", "test-project")
        monkeypatch.setenv("BQ_OBS_DATASET", "test_obs")

        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="ContractNotice",
            row_count=5,
            obs_dir=None,
        )

        # create_dataset should have been called (because get_dataset raised NotFound)
        bq_mocks.create_dataset.assert_called_once()

    def test_creates_table_when_not_found(
        self, bq_mocks: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        monkeypatch.setenv("GCP_PROJECT", "test-project")
        monkeypatch.setenv("BQ_OBS_DATASET", "test_obs")

        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="ContractNotice",
            row_count=5,
            obs_dir=None,
        )

        # CREATE TABLE IF NOT EXISTS query should have been issued
        bq_mocks.query.assert_called_once()
        ddl = bq_mocks.query.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "quarantine_summary" in ddl

    def test_insert_called_after_table_creation(
        self, bq_mocks: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        monkeypatch.setenv("GCP_PROJECT", "test-project")
        monkeypatch.setenv("BQ_OBS_DATASET", "test_obs")
        bq_mocks.insert_rows_json.return_value = []

        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="ContractNotice",
            row_count=5,
            obs_dir=None,
        )

        bq_mocks.insert_rows_json.assert_called_once()
