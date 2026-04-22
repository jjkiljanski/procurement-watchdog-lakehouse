# Apache Iceberg — Migration Path

The runtime abstraction layer is intentionally designed to make a future
migration of the **silver layer** from plain Parquet to Apache Iceberg tables
straightforward.  This document describes why Iceberg is planned, what is
already wired up, and what needs to change to complete the migration.

---

## Why Iceberg for Silver

| Problem with current plain Parquet | How Iceberg solves it |
|---|---|
| File-based day locks (`_locks/silver_day=*/`) needed to prevent concurrent writes to the same partition | ACID commits — Iceberg serialises concurrent writers at the commit level; no application-level locks needed |
| Schema evolution requires partition rewrites | Iceberg supports `ALTER TABLE ADD COLUMN` without touching existing data files |
| No built-in time travel | Every Iceberg write creates a snapshot; point-in-time queries are native |
| BQ external tables need manual DDL updates when schemas change | BigLake-managed Iceberg catalogs expose tables to BigQuery automatically |
| Cross-day dedup logic requires a Spark query over all existing partitions | Iceberg `MERGE INTO` can upsert by `objectId` in a single atomic operation |
| Manual `_processed/{layer}/{date}.json` manifest files required for idempotent backfill skip-checks | Iceberg snapshot `summary` properties can store `script_hash` at write time; the backfill DAG can query `{table}.snapshots` to check whether a partition was written with the current script version |

---

## Current State (GCS Parquet)

Silver writes are currently plain Parquet partitioned by
`noticeType/publicationDateDay` on GCS (or local filesystem).  The silver
pipeline uses file-based locks to prevent concurrent writes.  BigQuery access
is via external tables created by `scripts/ops/setup_bq_external_tables.py`.

---

## What Is Already Wired Up

The `SparkLauncher.get_session()` in both providers already registers the
Iceberg extensions:

```python
.config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
.config("spark.sql.catalog.silver", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.silver.type", "hadoop")
.config("spark.sql.catalog.silver.warehouse", "<warehouse_path>")
```

This means:

- The `silver` catalog is registered on every SparkSession.
- Iceberg DDL (`CREATE TABLE … USING iceberg`) and DML
  (`df.writeTo("silver.notice_type_tables.contract_notice").append()`) are
  available without additional config.
- The warehouse is `data/iceberg/` (local) or `gs://{bucket}/iceberg/` (GCP).

The Iceberg JARs are **not yet included** in `Dockerfile.spark`.  Adding them
is the first concrete step.

---

## Migration Steps

### Step 1: Add Iceberg JARs to Dockerfile.spark

```dockerfile
# In Dockerfile.spark — add to pip install:
RUN pip install --no-cache-dir ".[gcp]" "pyiceberg[gcs]>=0.7"

# OR use the Iceberg Spark runtime JAR directly:
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 pyspark-shell"
```

For Dataproc Serverless, use the `--jars` argument in the batch config or
include the JAR in the container image.

### Step 2: Migrate silver writes in spark_table_builder.py

Replace:
```python
df.write.mode("overwrite").partitionBy("publicationDateDay").parquet(output_path)
```

With:
```python
# Create table if it does not exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.notice_type_tables.{table_name}
    USING iceberg
    PARTITIONED BY (publicationDateDay)
    LOCATION '{warehouse}/notice_type_tables/{table_name}'
""")

# Append new partition data
df.writeTo(f"silver.notice_type_tables.{table_name}") \
  .overwritePartitions()
```

### Step 3: Remove file-based locks from silver

Once silver writes go through Iceberg, the `_locks/silver_day=*/` lock
directories are no longer needed.  Remove the `_acquire_day_lock` / `_release`
calls from `pipeline_orchestrator.py`.

### Step 4: Update BigQuery integration

**Option A — GCS external Iceberg tables (simplest)**

```sql
CREATE OR REPLACE EXTERNAL TABLE procurement_silver.contract_notice__core
OPTIONS (
  format = 'ICEBERG',
  uris = ['gs://{LAKEHOUSE_BUCKET}/iceberg/notice_type_tables/contract_notice__core/metadata/*.metadata.json']
);
```

BigQuery resolves the latest snapshot automatically.  Update
`setup_bq_external_tables.py` to use `format = 'ICEBERG'` instead of Parquet.

**Option B — BigLake Metastore (recommended for production)**

Register the Iceberg catalog with BigLake Metastore so BQ can discover
snapshots automatically without DDL updates:

```python
# In providers/gcp.py — update get_session() Iceberg catalog config:
.config("spark.sql.catalog.silver.catalog-impl",
        "org.apache.iceberg.gcp.biglake.BigLakeCatalog")
.config("spark.sql.catalog.silver.gcp_project",  GCP_PROJECT)
.config("spark.sql.catalog.silver.gcp_location", DATAPROC_REGION)
.config("spark.sql.catalog.silver.blms_catalog",
        f"projects/{GCP_PROJECT}/locations/{DATAPROC_REGION}/catalogs/silver")
```

Terraform provisions the BigLake catalog resource.

### Step 5: Bronze dedup simplification

Bronze dedup currently runs a Spark query against existing bronze Parquet.  With
Iceberg bronze tables, this becomes a native `MERGE INTO`:

```python
spark.sql(f"""
    MERGE INTO silver.bronze.notices AS target
    USING new_records AS source
    ON target.objectId = source.objectId
      AND target.publicationDateDay != '{target_date}'
    WHEN NOT MATCHED THEN INSERT *
""")
```

### Step 6: Replace manifest files with Iceberg snapshot properties

The current `src/procurement/manifests.py` module writes
`_processed/{layer}/{date}.json` marker files so that the backfill DAG can
skip batches that were already processed with the same script version.  Once
silver (and optionally bronze) writes go through Iceberg, this out-of-band
mechanism can be replaced with native Iceberg features.

**Write side** — attach `script_hash` to the Iceberg snapshot at commit time:

```python
df.writeTo(f"silver.notice_type_tables.{table_name}") \
  .option("write-audit-publish.enabled", "false") \
  .tableProperty("write.summary.partition-limit", "100") \
  .overwritePartitions()

# After the write, stamp the snapshot with script metadata:
spark.sql(f"""
    ALTER TABLE silver.notice_type_tables.{table_name}
    SET TBLPROPERTIES (
        'last_script_hash'  = '{script_hash}',
        'last_target_date'  = '{target_date}',
        'last_completed_at' = '{completed_at}'
    )
""")
```

> **Note**: `SET TBLPROPERTIES` updates table-level properties, not per-snapshot
> properties.  For per-partition-per-snapshot tracking, query the
> `{table}.partitions` and `{table}.snapshots` metadata tables instead — see
> the read side below.

**Read side** — check the latest snapshot that touched a given partition:

```sql
-- Was publicationDateDay=2025-10-01 written in the most recent snapshot?
SELECT
    s.committed_at,
    s.summary['spark.app.id']   AS app_id,
    -- custom properties written via TBLPROPERTIES are visible here:
    t.last_script_hash,
    t.last_target_date
FROM silver.notice_type_tables.contract_notice.history  AS h
JOIN silver.notice_type_tables.contract_notice.snapshots AS s
  ON h.snapshot_id = s.snapshot_id
CROSS JOIN (
    SELECT * FROM silver.notice_type_tables.contract_notice.properties
) AS t
ORDER BY s.committed_at DESC
LIMIT 1;
```

Once this is in place:

1. Remove `write_processed_manifest` calls from all pipeline scripts.
2. Remove `_check_manifest` / `_gcs_blob_sha256` helpers from `backfill_dag.py`.
3. Replace the skip logic in each `submit_*_batch` task with a BQ or Spark SQL
   query against the Iceberg metadata tables.
4. Delete `src/procurement/manifests.py` and the `_processed/` GCS prefix.

Until the Iceberg migration is complete, the manifest files in
`_processed/{layer}/{date}.json` remain the authoritative skip-check mechanism
for the backfill DAG.

---

## Catalog Configuration Reference

### Local (HadoopCatalog)

```python
spark.sql.catalog.silver                = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.silver.type           = hadoop
spark.sql.catalog.silver.warehouse      = data/iceberg
```

### GCP — HadoopCatalog on GCS (current, no server needed)

```python
spark.sql.catalog.silver                = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.silver.type           = hadoop
spark.sql.catalog.silver.warehouse      = gs://{bucket}/iceberg
```

### GCP — BigLake Metastore (planned, BQ auto-discovery)

```python
spark.sql.catalog.silver.catalog-impl   = org.apache.iceberg.gcp.biglake.BigLakeCatalog
spark.sql.catalog.silver.gcp_project    = {project}
spark.sql.catalog.silver.gcp_location   = {region}
spark.sql.catalog.silver.blms_catalog   = projects/{project}/locations/{region}/catalogs/silver
spark.sql.catalog.silver.warehouse      = gs://{bucket}/iceberg
```
