# Apache Iceberg — Silver Layer Reference

The silver layer uses **Apache Iceberg** (HadoopCatalog) instead of plain Parquet.
This document describes the catalog configuration, JAR setup, write patterns, and
BigQuery integration.

For table layout and partitioning decisions see the
[Apache Iceberg section in cloud_architecture.md](cloud_architecture.md#apache-iceberg--silver-layer).

---

## Why Iceberg

| Problem with plain Parquet | How Iceberg solves it |
|---|---|
| File-based day locks needed to prevent concurrent writes to the same partition | ACID commits — Iceberg serialises concurrent writers at the commit level |
| Schema evolution requires partition rewrites | Iceberg supports column additions without touching existing data files |
| No built-in time travel | Every write creates a snapshot; point-in-time queries are native |
| BQ external tables need manual DDL updates on schema changes | Iceberg external tables in BigQuery auto-resolve the latest snapshot |

---

## Catalog Configuration

The `silver` Iceberg catalog is registered on every SparkSession by both
`local.py` and `gcp.py`:

| Setting | Local | GCP |
|---|---|---|
| `spark.sql.catalog.silver` | `org.apache.iceberg.spark.SparkCatalog` | same |
| `spark.sql.catalog.silver.type` | `hadoop` | `hadoop` |
| `spark.sql.catalog.silver.warehouse` | `data/iceberg` | `gs://{bucket}/iceberg` |

---

## Iceberg JAR Delivery

The Iceberg Spark runtime JAR
(`iceberg-spark-runtime-3.5_2.12-1.5.2.jar`) is downloaded into the container
at build time (`Dockerfile.spark`) and placed at `/opt/iceberg-spark-runtime.jar`.

- **Local**: `SPARK_EXTRA_CLASSPATH=/opt/iceberg-spark-runtime.jar` puts it on
  the driver classpath for in-container Spark sessions.
- **Dataproc Serverless**: every batch config includes
  `jar_file_uris=["file:///opt/iceberg-spark-runtime.jar"]` so executors also
  have it on their classpaths (see `gcp.py: submit_batch()`).

---

## Write Patterns

### Section tables and delta tables

```python
df.writeTo("silver.notice_type_tables.contract_notice__core") \
  .overwritePartitions()
```

`overwritePartitions()` replaces only the partitions present in the DataFrame —
concurrent batches writing different `publicationDateDay` values are safe.

### Quarantine

Same as section tables — partitioned by `(publicationDateDay, notice_type)`,
so each batch owns a distinct partition.

### Common envelope

The day partition is deleted before the ThreadPoolExecutor starts; each batch
then appends:

```python
spark.sql("DELETE FROM silver.common.common_envelope WHERE publicationDateDay = '...'")
df.writeTo("silver.common.common_envelope").append()
```

Iceberg ACID commits ensure `append()` calls from concurrent threads are
serialised correctly.

---

## BigQuery Integration

CI/CD runs `scripts/ops/setup_bq_external_tables.py` on every deploy to create
or replace BigQuery external Iceberg table definitions:

```bash
python scripts/ops/setup_bq_external_tables.py
```

BigQuery resolves the latest Iceberg snapshot automatically — no DDL updates
are needed when new days are written. Re-run the script only when new notice
types appear or the silver schema changes significantly.

---

## BigLake Metastore (future option)

For auto-discovery without re-running the setup script after schema changes,
the catalog backend can be swapped to BigLake Metastore by replacing
`spark.sql.catalog.silver.type = hadoop` with:

```
spark.sql.catalog.silver.catalog-impl   = org.apache.iceberg.gcp.biglake.BigLakeCatalog
spark.sql.catalog.silver.gcp_project    = {project}
spark.sql.catalog.silver.gcp_location   = {region}
spark.sql.catalog.silver.blms_catalog   = projects/{project}/locations/{region}/catalogs/silver
spark.sql.catalog.silver.warehouse      = gs://{bucket}/iceberg
```

This is not currently configured — the HadoopCatalog approach is sufficient
because CI/CD refreshes the BQ table definitions on every deploy.
