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
