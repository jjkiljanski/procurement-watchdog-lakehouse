"""Create or replace BigQuery external tables over GCS silver data.

This script is idempotent: run it once during initial GCP setup, then re-run
whenever the silver schema changes (new notice type, added column, etc.).

Two formats are supported, selected via ``--format``:

``parquet`` (legacy)
    External tables point to GCS Parquet directories under
    ``gs://{LAKEHOUSE_BUCKET}/silver/``.  BigQuery infers the column schema
    from Parquet metadata using ``autodetect=True``.  Hive partitioning is
    configured so that ``noticeType`` and ``publicationDateDay`` partition
    columns are available as query filters.

``iceberg`` (default)
    External tables point to Apache Iceberg table metadata files under
    ``gs://{LAKEHOUSE_BUCKET}/iceberg/``.  BigQuery reads the latest Iceberg
    snapshot automatically — no DDL updates needed when new days are written.
    Tables are created via BigQuery SQL DDL (``FORMAT='ICEBERG'``).

What is created (iceberg mode)
-------------------------------
For each table discovered under iceberg/notice_type_tables/:
  ``{BQ_DATASET}.{bq_table_name}``  (e.g. ``contract_notice_core``,
  ``contract_notice_part``).  Iceberg directory names use double-underscore
  separators (e.g. ``contract_notice__part_core``); these are normalised to
  single-underscore names that match the analytics dbt sources, with the
  ``part_core`` data-model suffix shortened to ``part``.

For shared tables:
  ``{BQ_DATASET}.common_envelope``
  ``{BQ_DATASET}.quarantine``

For notice_update_deltas Iceberg tables (one per original notice type):
  ``{BQ_DATASET}.{notice_type_snake_case}_delta``
  (e.g. ``contract_notice_delta``)

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

    # Iceberg format (default):
    python scripts/ops/setup_bq_external_tables.py --format iceberg

    # Legacy Parquet format:
    python scripts/ops/setup_bq_external_tables.py --format parquet

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
    parser = argparse.ArgumentParser(description="Create BQ external tables over silver GCS data.")
    parser.add_argument(
        "--bq-dataset",
        default=os.environ.get("BQ_DATASET", "silver"),
        help="BigQuery dataset name (default: silver or BQ_DATASET env var)",
    )
    parser.add_argument(
        "--format",
        choices=["iceberg", "parquet"],
        default="iceberg",
        help="External table format: iceberg (default) or parquet (legacy)",
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


# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------


def _iceberg_dir_to_bq_table_name(iceberg_dir_name: str) -> str:
    """Convert an Iceberg directory name to a BigQuery-compatible table name.

    Iceberg dirs use ``__`` as the separator between notice type and data model
    (e.g. ``contract_notice__part_core``).  Analytics dbt sources expect
    single-underscore names (e.g. ``contract_notice_part``).

    The ``part_core`` data-model suffix is normalised to ``part`` to match the
    analytics naming convention.  All other suffixes are left unchanged.
    """
    parts = iceberg_dir_name.split("__", 1)
    if len(parts) == 2:
        nt, dm = parts
        if dm == "part_core":
            dm = "part"
        return f"{nt}_{dm}"
    return iceberg_dir_name


def _list_iceberg_tables(iceberg_warehouse_uri: str, namespace: str) -> list[tuple[str, str]]:
    """List Iceberg tables in *namespace* under the GCS warehouse.

    Returns list of ``(table_name, metadata_glob_uri)`` tuples.
    ``metadata_glob_uri`` matches all ``*.metadata.json`` files in the table's
    metadata directory — BigQuery resolves the latest snapshot automatically.
    """
    from google.cloud import storage as gcs

    bucket_name = iceberg_warehouse_uri.replace("gs://", "").split("/")[0]
    warehouse_prefix = iceberg_warehouse_uri.replace(f"gs://{bucket_name}/", "").rstrip("/")
    ns_prefix = f"{warehouse_prefix}/{namespace}/"

    client = gcs.Client()
    blobs = client.list_blobs(bucket_name, prefix=ns_prefix, delimiter="/")
    _ = list(blobs)

    results = []
    for table_prefix in blobs.prefixes:
        table_name = table_prefix.rstrip("/").split("/")[-1]
        metadata_glob = f"gs://{bucket_name}/{table_prefix}metadata/*.metadata.json"
        results.append((table_name, metadata_glob))
    return results


def _create_iceberg_external_table(
    bq_client,
    project: str,
    dataset: str,
    table_name: str,
    metadata_uri: str,
    dry_run: bool,
) -> None:
    """Create or replace a BigQuery external Iceberg table via SQL DDL."""
    table_id = f"`{project}.{dataset}.{table_name}`"
    sql = (
        f"CREATE OR REPLACE EXTERNAL TABLE {table_id}\n"
        f"OPTIONS (\n"
        f"  format = 'ICEBERG',\n"
        f"  uris = ['{metadata_uri}']\n"
        f")"
    )
    if dry_run:
        log.info("[DRY RUN] Would execute:\n%s", sql)
        return
    bq_client.query(sql).result()
    log.info("Created Iceberg external table %s.%s.%s", project, dataset, table_name)


def _run_iceberg_setup(args, rt, bq_client) -> tuple[int, int]:
    """Create BQ external Iceberg tables for all silver Iceberg tables."""

    iceberg_uri = rt.storage.resolve("iceberg")
    log.info("Iceberg warehouse GCS URI: %s", iceberg_uri)

    project = bq_client.project
    dataset = args.bq_dataset
    created = 0
    skipped = 0

    # ── notice_type_tables ──────────────────────────────────────────────────
    log.info("Discovering Iceberg notice_type_tables…")
    section_tables = _list_iceberg_tables(iceberg_uri, "notice_type_tables")
    if not section_tables:
        log.warning("No Iceberg tables found under %s/notice_type_tables/ — nothing to create.", iceberg_uri)
    for iceberg_name, metadata_uri in section_tables:
        bq_table_name = _iceberg_dir_to_bq_table_name(iceberg_name)
        try:
            _create_iceberg_external_table(bq_client, project, dataset, bq_table_name, metadata_uri, args.dry_run)
            created += 1
        except Exception as exc:
            log.error("Failed to create %s: %s", bq_table_name, exc)
            skipped += 1

    # ── common tables (common_envelope, quarantine) ─────────────────────────
    log.info("Discovering Iceberg common tables…")
    common_tables = _list_iceberg_tables(iceberg_uri, "common")
    for iceberg_name, metadata_uri in common_tables:
        bq_table_name = _iceberg_dir_to_bq_table_name(iceberg_name)
        try:
            _create_iceberg_external_table(bq_client, project, dataset, bq_table_name, metadata_uri, args.dry_run)
            created += 1
        except Exception as exc:
            log.error("Failed to create %s: %s", bq_table_name, exc)
            skipped += 1

    # ── notice_update_deltas (Iceberg) ─────────────────────────────────────
    log.info("Discovering Iceberg notice_update_deltas…")
    delta_tables = _list_iceberg_tables(iceberg_uri, "notice_update_deltas")
    if not delta_tables:
        log.info("No Iceberg tables found under %s/notice_update_deltas/ — skipping.", iceberg_uri)
    for iceberg_name, metadata_uri in delta_tables:
        bq_table_name = f"{iceberg_name}_delta"
        try:
            _create_iceberg_external_table(
                bq_client, project, dataset, bq_table_name, metadata_uri, args.dry_run
            )
            created += 1
        except Exception as exc:
            log.error("Failed to create %s: %s", bq_table_name, exc)
            skipped += 1

    return created, skipped


def _run_parquet_setup(args, rt, bq_client) -> tuple[int, int]:
    """Create BQ external Parquet tables for all legacy silver Parquet tables."""
    import re

    silver_uri = rt.storage.resolve("silver")
    log.info("Silver GCS URI (Parquet): %s", silver_uri)

    dataset_ref = bq_client.dataset(args.bq_dataset)
    created = 0
    skipped = 0

    # notice_type_tables
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

    # common_envelope
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

    # notice_update_deltas
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

    return created, skipped


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
    log.info("BigQuery dataset: %s.%s", bq_client.project, args.bq_dataset)
    log.info("Format: %s", args.format)

    if args.format == "iceberg":
        created, skipped = _run_iceberg_setup(args, rt, bq_client)
    else:
        created, skipped = _run_parquet_setup(args, rt, bq_client)

    log.info(
        "Done. created=%d skipped=%d format=%s dry_run=%s",
        created,
        skipped,
        args.format,
        args.dry_run,
    )


if __name__ == "__main__":
    main()
