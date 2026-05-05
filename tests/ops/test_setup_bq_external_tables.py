from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from scripts.ops import setup_bq_external_tables as mod


def test_parse_args_defaults_to_terraform_silver_dataset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sys, "argv", ["setup_bq_external_tables.py"])
    monkeypatch.delenv("BQ_DATASET", raising=False)

    args = mod._parse_args()

    assert args.bq_dataset == "procurement_silver"


def test_create_iceberg_external_table_uses_connection_and_metadata_file():
    bq_client = MagicMock()

    mod._create_iceberg_external_table(
        bq_client=bq_client,
        project="procwatch-dev",
        dataset="procurement_silver",
        table_name="contract_notice_core",
        metadata_uri="gs://bucket/iceberg/t/metadata/00002.metadata.json",
        connection="procwatch-dev.EU.procwatch_iceberg",
        dry_run=False,
    )

    sql = bq_client.query.call_args.args[0]
    assert "WITH CONNECTION `procwatch-dev.EU.procwatch_iceberg`" in sql
    assert "metadata/*.metadata.json" not in sql
    assert "gs://bucket/iceberg/t/metadata/00002.metadata.json" in sql


def test_latest_metadata_uri_selects_highest_metadata_json_name():
    client = MagicMock()
    client.list_blobs.return_value = [
        SimpleNamespace(name="iceberg/common/common_envelope/metadata/00001-a.metadata.json"),
        SimpleNamespace(name="iceberg/common/common_envelope/metadata/00003-c.metadata.json"),
        SimpleNamespace(name="iceberg/common/common_envelope/metadata/version-hint.text"),
        SimpleNamespace(name="iceberg/common/common_envelope/metadata/00002-b.metadata.json"),
    ]

    result = mod._latest_metadata_uri(
        client,
        "bucket",
        "iceberg/common/common_envelope/",
    )

    assert result == "gs://bucket/iceberg/common/common_envelope/metadata/00003-c.metadata.json"


def test_iceberg_setup_requires_connection():
    args = SimpleNamespace(bq_connection=None)
    rt = SimpleNamespace(storage=SimpleNamespace(resolve=lambda _: "gs://bucket/iceberg"))
    bq_client = SimpleNamespace(project="procwatch-dev")

    with pytest.raises(ValueError, match="BQ_CONNECTION"):
        mod._run_iceberg_setup(args, rt, bq_client)
