"""Tests for the BigQuery write backend in src/procurement/obs.py.

All BQ client calls are mocked via sys.modules patching — no GCP credentials,
no google-cloud-bigquery install required.  obs.py uses lazy imports (inside
functions) so we cannot use patch("procurement.obs.bigquery"); we must inject
the mock before the lazy import runs.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import procurement.obs as obs_module
from procurement.obs import (
    _bq_obs_dataset,
    _use_bq,
    write_dq_metrics,
    write_pipeline_run,
    write_quarantine_summary,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

class _NotFound(Exception):
    """Stand-in for google.cloud.exceptions.NotFound."""


@pytest.fixture()
def mock_bq_client():
    """Yield a configured mock BigQuery client and inject it via sys.modules.

    obs.py uses lazy imports (``from google.cloud import bigquery`` inside
    functions), so we must inject mocks for the entire google namespace before
    any obs function runs.  ``google`` and ``google.cloud`` are not installed
    in the local dev environment (only the [gcp] extras install them).

    Also clears the table-existence cache before each test so tests are
    independent of each other.
    """
    mock_bq = MagicMock()
    mock_exceptions = MagicMock()
    mock_exceptions.NotFound = _NotFound

    mock_client = MagicMock()
    mock_client.get_dataset.return_value = MagicMock()
    mock_client.get_table.return_value = MagicMock()
    mock_client.insert_rows_json.return_value = []  # empty list = no BQ errors
    mock_bq.Client.return_value = mock_client

    # ``from google.cloud import bigquery`` calls getattr(sys.modules["google.cloud"],
    # "bigquery").  Because google.cloud is a MagicMock, auto-attribute access would
    # return a fresh MagicMock rather than our configured mock_bq.  We must wire the
    # attributes explicitly so both lookup paths return the same object.
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


def _set_gcp_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RUNTIME_ENV", "gcp")
    monkeypatch.setenv("GCP_PROJECT", "test-project")
    monkeypatch.setenv("BQ_OBS_DATASET", "test_obs_dataset")


# ---------------------------------------------------------------------------
# _use_bq / _bq_obs_dataset
# ---------------------------------------------------------------------------

class TestUseBq:
    def test_false_when_local(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "local")
        assert not _use_bq()

    def test_true_when_gcp(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "gcp")
        assert _use_bq()

    def test_false_when_env_unset(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("RUNTIME_ENV", raising=False)
        assert not _use_bq()

    def test_case_insensitive(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("RUNTIME_ENV", "GCP")
        assert _use_bq()


class TestBqObsDataset:
    def test_default_dataset_name(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("BQ_OBS_DATASET", raising=False)
        assert _bq_obs_dataset() == "procurement_obs"

    def test_custom_dataset_name(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("BQ_OBS_DATASET", "my_custom_obs")
        assert _bq_obs_dataset() == "my_custom_obs"


# ---------------------------------------------------------------------------
# write_pipeline_run — BQ path
# ---------------------------------------------------------------------------

class TestWritePipelineRunBq:
    def test_calls_insert_rows_json(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_pipeline_run(
            layer="bronze",
            target_date="2025-10-01",
            run_id="run-001",
            started_at="2025-10-02T03:00:00Z",
            completed_at="2025-10-02T03:05:00Z",
            status="ok",
            counts={"raw_total": 100, "valid_total": 95},
            git_commit="abc1234",
            script_hash="deadbeef",
            obs_dir=None,
        )

        mock_bq_client.insert_rows_json.assert_called_once()
        table_ref, rows = mock_bq_client.insert_rows_json.call_args[0]
        assert "pipeline_runs" in table_ref
        assert len(rows) == 1
        row = rows[0]
        assert row["layer"] == "bronze"
        assert row["target_date"] == "2025-10-01"
        assert row["status"] == "ok"
        assert row["git_commit"] == "abc1234"
        assert row["script_hash"] == "deadbeef"

    def test_counts_serialised_to_json(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_pipeline_run(
            layer="silver",
            target_date="2025-10-01",
            run_id="run-002",
            started_at="2025-10-02T03:00:00Z",
            completed_at="2025-10-02T03:10:00Z",
            status="ok",
            counts={"rows_written": 500, "quarantined": 3},
            obs_dir=None,
        )

        _, rows = mock_bq_client.insert_rows_json.call_args[0]
        counts = json.loads(rows[0]["counts_json"])
        assert counts == {"rows_written": 500, "quarantined": 3}

    def test_no_op_when_local_runtime(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("RUNTIME_ENV", "local")

        write_pipeline_run(
            layer="bronze",
            target_date="2025-10-01",
            run_id="run-003",
            started_at="2025-10-02T03:00:00Z",
            completed_at="2025-10-02T03:01:00Z",
            status="ok",
            counts={},
            obs_dir=None,
        )

        mock_bq_client.insert_rows_json.assert_not_called()

    def test_bq_error_does_not_raise(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        """BQ write failures must be warnings-only; the pipeline must not crash."""
        _set_gcp_env(monkeypatch)
        mock_bq_client.insert_rows_json.side_effect = RuntimeError("BQ is down")

        # Must not raise
        write_pipeline_run(
            layer="bronze",
            target_date="2025-10-01",
            run_id="run-004",
            started_at="2025-10-02T03:00:00Z",
            completed_at="2025-10-02T03:01:00Z",
            status="ok",
            counts={},
            obs_dir=None,
        )


# ---------------------------------------------------------------------------
# write_dq_metrics — BQ path
# ---------------------------------------------------------------------------

class TestWriteDqMetricsBq:
    def test_one_row_per_metric(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_dq_metrics(
            layer="bronze",
            target_date="2025-10-01",
            notice_type=None,
            metrics={"valid_rate": 0.97, "invalid_count": 3.0},
            obs_dir=None,
        )

        _, rows = mock_bq_client.insert_rows_json.call_args[0]
        assert len(rows) == 2
        names = {r["metric_name"] for r in rows}
        assert names == {"valid_rate", "invalid_count"}

    def test_notice_type_defaults_to_all(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_dq_metrics(
            layer="silver",
            target_date="2025-10-01",
            notice_type=None,
            metrics={"some_metric": 1.0},
            obs_dir=None,
        )

        _, rows = mock_bq_client.insert_rows_json.call_args[0]
        assert rows[0]["notice_type"] == "__all__"

    def test_empty_metrics_does_not_call_insert(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_dq_metrics(
            layer="bronze",
            target_date="2025-10-01",
            notice_type="ContractNotice",
            metrics={},
            obs_dir=None,
        )

        mock_bq_client.insert_rows_json.assert_not_called()


# ---------------------------------------------------------------------------
# write_quarantine_summary — BQ path
# ---------------------------------------------------------------------------

class TestWriteQuarantineSummaryBq:
    def test_single_row_written(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        write_quarantine_summary(
            target_date="2025-10-01",
            notice_type="ContractNotice",
            row_count=17,
            obs_dir=None,
        )

        _, rows = mock_bq_client.insert_rows_json.call_args[0]
        assert len(rows) == 1
        assert rows[0]["target_date"] == "2025-10-01"
        assert rows[0]["notice_type"] == "ContractNotice"
        assert rows[0]["row_count"] == 17


# ---------------------------------------------------------------------------
# Table auto-creation caching
# ---------------------------------------------------------------------------

class TestEnsureBqTableCaching:
    def test_table_existence_checked_only_once(
        self, mock_bq_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ):
        _set_gcp_env(monkeypatch)

        for _ in range(3):
            write_quarantine_summary(
                target_date="2025-10-01",
                notice_type="ContractNotice",
                row_count=1,
                obs_dir=None,
            )

        # get_table checked once (first call), then the result is cached
        assert mock_bq_client.get_table.call_count == 1
        assert mock_bq_client.insert_rows_json.call_count == 3
