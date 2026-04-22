"""Create or replace BigQuery external tables over GCS Parquet silver data.

This script is idempotent: run it once during initial GCP setup, then re-run
whenever the silver schema changes (new notice type, added column, etc.).

External tables point to GCS Parquet directories under
``gs://{LAKEHOUSE_BUCKET}/silver/``.  BigQuery infers the column schema from
Parquet metadata using ``autodetect=True``.  Hive partitioning is configured
so that ``noticeType`` and ``publicationDateDay`` partition columns are
automatically available as query filters.

What is created
---------------
For each notice type discovered under silver/notice_type_tables/:
  ``{BQ_DATASET}.{notice_type_snake_case}_{data_model}``

For common_envelope:
  ``{BQ_DATASET}.common_envelope``

For notice_update_deltas (if present):
  ``{BQ_DATASET}.notice_update_deltas_{notice_type_snake_case}``

Prerequisites
-------------
- ``RUNTIME_ENV=gcp`` + all required GCP env vars (see config/runtime_gcp.env.example)
- The BigQuery dataset must already exist (created by Terraform).
- The service account running this script needs:
    roles/bigquery.dataEditor  on the dataset
    roles/storage.objectViewer on LAKEHOUSE_BUCKET

Usage
-----
::

    # From local workstation with gcloud credentials configured:
    export $(grep -v '^#' config/runtime_gcp.env.example | xargs)
    python scripts/ops/setup_bq_external_tables.py

    # Dry run (print DDL without executing):
    python scripts/ops/setup_bq_external_tables.py --dry-run

    # Override dataset:
    python scripts/ops/setup_bq_external_tables.py --bq-dataset my_dataset
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "src"))

from procurement.logging import setup_logging
from procurement.runtime import get_runtime

setup_logging()
log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create BQ external tables over silver GCS Parquet.")
    parser.add_argument(
        "--bq-dataset",
        default=os.environ.get("BQ_DATASET", "procurement_silver"),
        help="BigQuery dataset name (default: procurement_silver or BQ_DATASET env var)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the table definitions without executing them",
    )
    return parser.parse_args()


def _to_bq_table_name(notice_type: str, data_model: str) -> str:
    """Convert notice type + data model to a BigQuery-safe table name."""
    import re

    # CamelCase → snake_case
    nt = re.sub(r"(?<!^)(?=[A-Z])", "_", notice_type).lower()
    dm = data_model.replace(".", "_").lower()
    return f"{nt}__{dm}"


def _list_silver_tables(silver_uri: str, sub_path: str) -> list[tuple[str, str, str]]:
    """Discover Parquet table partitions under ``silver_uri/{sub_path}``.

    Returns list of (notice_type, data_model, gcs_uri) tuples.
    """
    from google.cloud import storage as gcs

    bucket_name = silver_uri.replace("gs://", "").split("/")[0]
    prefix_base = silver_uri.replace(f"gs://{bucket_name}/", "").rstrip("/")
    full_prefix = f"{prefix_base}/{sub_path}/"

    client = gcs.Client()
    blobs = client.list_blobs(bucket_name, prefix=full_prefix, delimiter="/")
    _ = list(blobs)

    results = []
    for nt_prefix in blobs.prefixes:
        # nt_prefix: silver/notice_type_tables/noticeType=ContractNotice/
        nt_value = nt_prefix.rstrip("/").split("/")[-1].replace("noticeType=", "")
        nt_blobs = client.list_blobs(bucket_name, prefix=nt_prefix, delimiter="/")
        _ = list(nt_blobs)
        for dm_prefix in nt_blobs.prefixes:
            dm_value = dm_prefix.rstrip("/").split("/")[-1].replace("data_model=", "")
            table_uri = f"gs://{bucket_name}/{dm_prefix.rstrip('/')}"
            results.append((nt_value, dm_value, table_uri))
    return results


def _create_external_table(
    client,
    dataset_ref,
    table_name: str,
    gcs_uri: str,
    hive_partition_columns: list[str],
    dry_run: bool,
) -> None:
    from google.cloud import bigquery

    table_id = f"{dataset_ref.project}.{dataset_ref.dataset_id}.{table_name}"

    external_config = bigquery.ExternalConfig(bigquery.SourceFormat.PARQUET)
    external_config.source_uris = [f"{gcs_uri}/**/*.parquet"]
    external_config.autodetect = True
    external_config.hive_partitioning_options = bigquery.HivePartitioningOptions(
        mode="AUTO",
        source_uri_prefix=gcs_uri,
        require_partition_filter=False,
    )

    table = bigquery.Table(table_id)
    table.external_data_configuration = external_config

    if dry_run:
        log.info("[DRY RUN] Would create/replace external table %s → %s", table_id, gcs_uri)
        return

    # Delete existing table if present (CREATE OR REPLACE semantics).
    client.delete_table(table_id, not_found_ok=True)
    client.create_table(table)
    log.info("Created external table %s → %s", table_id, gcs_uri)


def main() -> None:
    args = _parse_args()

    rt = get_runtime()
    if rt.env != "gcp":
        log.error(
            "setup_bq_external_tables.py requires RUNTIME_ENV=gcp. "
            "Current env: %s",
            rt.env,
        )
        sys.exit(1)

    from google.cloud import bigquery

    bq_client = bigquery.Client()
    dataset_ref = bq_client.dataset(args.bq_dataset)

    silver_uri = rt.storage.resolve("silver")
    log.info("Silver GCS URI: %s", silver_uri)
    log.info("BigQuery dataset: %s.%s", bq_client.project, args.bq_dataset)

    created = 0
    skipped = 0

    # ---------------------------------------------------------------
    # 1. notice_type_tables: noticeType=* / data_model=*
    # ---------------------------------------------------------------
    log.info("Discovering silver notice_type_tables…")
    tables = _list_silver_tables(silver_uri, "notice_type_tables")
    if not tables:
        log.warning("No notice_type_tables found under %s — nothing to create.", silver_uri)
    for notice_type, data_model, table_uri in tables:
        table_name = _to_bq_table_name(notice_type, data_model)
        try:
            _create_external_table(
                client=bq_client,
                dataset_ref=dataset_ref,
                table_name=table_name,
                gcs_uri=table_uri,
                hive_partition_columns=["publicationDateDay"],
                dry_run=args.dry_run,
            )
            created += 1
        except Exception as exc:
            log.error("Failed to create %s: %s", table_name, exc)
            skipped += 1

    # ---------------------------------------------------------------
    # 2. common_envelope
    # ---------------------------------------------------------------
    envelope_uri = f"{silver_uri}/common_envelope"
    try:
        _create_external_table(
            client=bq_client,
            dataset_ref=dataset_ref,
            table_name="common_envelope",
            gcs_uri=envelope_uri,
            hive_partition_columns=["publicationDateDay"],
            dry_run=args.dry_run,
        )
        created += 1
    except Exception as exc:
        log.error("Failed to create common_envelope: %s", exc)
        skipped += 1

    # ---------------------------------------------------------------
    # 3. notice_update_deltas: noticeType=*
    # ---------------------------------------------------------------
    log.info("Discovering notice_update_deltas…")
    from google.cloud import storage as gcs

    bucket_name = silver_uri.replace("gs://", "").split("/")[0]
    silver_prefix = silver_uri.replace(f"gs://{bucket_name}/", "").rstrip("/")
    deltas_prefix = f"{silver_prefix}/notice_update_deltas/"

    gcs_client = gcs.Client()
    delta_blobs = gcs_client.list_blobs(bucket_name, prefix=deltas_prefix, delimiter="/")
    _ = list(delta_blobs)
    for nt_prefix in delta_blobs.prefixes:
        nt_value = nt_prefix.rstrip("/").split("/")[-1].replace("noticeType=", "")
        import re

        nt_snake = re.sub(r"(?<!^)(?=[A-Z])", "_", nt_value).lower()
        table_name = f"notice_update_deltas__{nt_snake}"
        table_uri = f"gs://{bucket_name}/{nt_prefix.rstrip('/')}"
        try:
            _create_external_table(
                client=bq_client,
                dataset_ref=dataset_ref,
                table_name=table_name,
                gcs_uri=table_uri,
                hive_partition_columns=["publicationDateDay"],
                dry_run=args.dry_run,
            )
            created += 1
        except Exception as exc:
            log.error("Failed to create %s: %s", table_name, exc)
            skipped += 1

    log.info(
        "Done. created=%d skipped=%d dry_run=%s",
        created,
        skipped,
        args.dry_run,
    )


if __name__ == "__main__":
    main()
