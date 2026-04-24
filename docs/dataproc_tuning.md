# Dataproc Serverless — Resource & Tuning Guide

This document gives recommended Spark and Dataproc Serverless settings for
each pipeline stage. All values are read from the batch `runtimeConfig.properties`
block in the Cloud Workflows definitions (`workflows/daily.yaml`,
`workflows/backfill.yaml`).

---

## Quick Reference

| Stage | Driver memory | Executor memory | Executor instances | Shuffle partitions |
|---|---|---|---|---|
| `build_bronze[_range].py` | 4g | 4g | 2 (daily) / 4 (backfill) | 16 |
| `build_silver_day.py` | 4g | 8g | 4 | 32 |
| `build_silver_range.py` | 8g | 8g | 8 | 64 |
| `build_silver_update_deltas.py` | 4g | 4g | 2 (daily) / 4 (backfill) | 16 |

---

## Why these numbers

### Bronze

Bronze is IO-bound (JSON read → Pydantic validation → Parquet write).
HTML parsing does not happen here.  Memory pressure is low; the bottleneck is
GCS throughput.  2 executors suffice for a single day; 4 for a full-year
backfill.

### Silver (daily — `build_silver_day.py`)

Silver processes 14 notice types in parallel batches (default `max-batch-workers 4`
on Dataproc; set lower on memory-constrained hosts).  Each batch:

- Reads Bronze Parquet for one notice type
- Runs the HTML section parser (CPU-heavy, Python UDF)
- Writes one or more Iceberg section tables

The HTML parser for heavy notice types (`ContractNotice`,
`TenderResultNotice`, `ContractPerformingNotice`) allocates significant heap
per row.  8 GB per executor prevents executor GC pressure on those batches.

4 executor instances give a good balance between parallelism and memory
footprint on a standard Dataproc Serverless batch.

### Silver (backfill — `build_silver_range.py`)

The range script loops over notice types (not dates).  Each notice-type
iteration issues **one Spark plan covering all dates in the range** — the DAG
is built once per notice type, not once per (date, notice_type) pair.

For a year-long backfill this means:
- 14 Spark plans total (one per notice type)
- Each plan reads and shuffles N_days × notice_type_rows rows
- Executor memory must handle larger shuffle spills

8 executors × 8 GB handles a 12-month backfill for all notice types without
spilling to disk.

For shorter ranges (< 1 month) you can use 4 executors.

### Deltas

NoticeUpdateNotice is a low-volume type (~12.5% of notices).  Delta tables are
small.  2 executors at 4 GB is sufficient for daily; 4 for backfill.

---

## Dataproc Serverless batch properties

Pass these in the `runtimeConfig.properties` map of each batch submission
(already wired in `src/procurement/runtime/providers/gcp.py:submit_batch()`):

### Daily silver batch

```yaml
runtimeConfig:
  properties:
    spark.driver.memory: "4g"
    spark.executor.instances: "4"
    spark.executor.memory: "8g"
    spark.executor.memoryOverhead: "2g"
    spark.sql.shuffle.partitions: "32"
    spark.dataproc.executor.compute.tier: "standard"
    spark.dataproc.driver.compute.tier: "standard"
```

Pipeline-level flag (passed as script argument):

```
--max-batch-workers 4     # concurrent notice-type batches
```

### Backfill silver batch

```yaml
runtimeConfig:
  properties:
    spark.driver.memory: "8g"
    spark.executor.instances: "8"
    spark.executor.memory: "8g"
    spark.executor.memoryOverhead: "2g"
    spark.sql.shuffle.partitions: "64"
    spark.dataproc.executor.compute.tier: "standard"
    spark.dataproc.driver.compute.tier: "standard"
```

Pipeline-level flag (passed as script argument):

```
--shuffle-partitions 64   # script CLI flag; overrides spark.sql.shuffle.partitions
                          # for per-batch adaptive tuning
```

---

## Local benchmark baseline (WSL2, 8 GB host RAM)

All runs use Docker on WSL2 ext4 (`~/procurement-tests`), `--max-batch-workers 2`,
`--shuffle-partitions 8`.  Windows filesystem mounts are ~10× slower for Iceberg
due to small-file metadata overhead — always use WSL2 or GCP for silver.

### Daily pipeline — single day (2025-10-01)

| Stage | Elapsed |
|---|---|
| fetch | 32 s |
| bronze | 45 s |
| silver | 256 s |
| deltas | 48 s |
| **total** | **381 s** |

### Backfill — 4 days (2025-04-01..2025-04-04)

`--shuffle-partitions 8 --max-section-write-workers 2`

| Stage | Elapsed | Notes |
|---|---|---|
| fetch | 1.5 s | all 4 days already cached (manifest skip) |
| bronze | 12.4 s | all 4 days already cached (manifest skip) |
| silver | 450 s | 7839 rows, 10 notice types, **10 Spark plans** (one per type) |
| deltas | 78 s | |
| **total** | **542 s** | |

Silver per-type breakdown (10 types, processed in priority order):

| Notice type | Rows | Elapsed |
|---|---|---|
| SmallContractNotice | 51 | ~32 s |
| AgreementUpdateNotice | 60 | ~36 s |
| AgreementIntentionNotice | 51 | ~28 s |
| NoticeUpdateNotice | 999 | ~29 s |
| TenderResultNotice | ~1800 | ~110 s (repartition 4→8) |
| ContractPerformingNotice | 2501 | ~65 s (repartition 4→8) |
| ContractNotice | ~2300 | ~100 s (repartition 4→8, core alone 68 s) |
| CircumstancesFulfillmentNotice | 28 | ~10 s |
| ConcessionAgreementNotice | 1 | ~12 s |
| ConcessionNotice | 0 | <1 s |

Key observation: **the DAG is compiled once per notice type**, not once per
(date × notice type).  For a 4-day range this means 10 plans instead of 40;
for a 365-day range it means 10 plans instead of ~5110.

On GCP Dataproc Serverless with the recommended settings (8 executors × 8 GB),
silver should complete in approximately **60–120 s** for a single day and
**300–600 s** for a full month's backfill (no Windows filesystem overhead,
true parallel executors, no memory pressure).

---

## Memory scaling rules of thumb

| Scenario | Total container memory cap | Recommended `max-batch-workers` |
|---|---|---|
| Local, 8 GB host | ~5 GB available to Spark | 2 |
| Local, 16 GB host | ~12 GB available to Spark | 4 |
| Dataproc Serverless standard (4 exe × 8 GB) | 32 GB executor pool | 4–6 |
| Dataproc Serverless standard (8 exe × 8 GB) | 64 GB executor pool | 8+ |

The Iceberg `common_envelope` table uses `append()` writes protected by
pre-run partition deletion.  Concurrent batch workers write to disjoint
notice-type partitions and do not contend on `common_envelope`.

---

## Cost-saving tips

- **Backfill**: use `--force false` (the default) so already-processed
  `(date, notice_type)` pairs are skipped on resume.  Interrupted backfills
  resume cheaply.
- **Daily**: use `spot`/`preemptible` VMs for executors (`spark.dataproc.executor.compute.tier: "spot"`).
  The daily batch takes < 10 minutes; spot reclaim risk is negligible at that
  timescale.
- **Shuffle disk**: Dataproc Serverless uses ephemeral local SSD for shuffle.
  Increasing `spark.executor.memoryOverhead` to 2g reduces GCS shuffle spill.

---

## Tuning the Iceberg metadata overhead

Each Iceberg `overwritePartitions()` commit writes 5–8 small metadata files
(manifest, snapshot JSON).  On GCS these are written in parallel and are
cheap.  On a local Windows filesystem these cause significant latency
(~30 s per commit) — avoid running silver locally on Windows mounts.
Use WSL2 ext4 (`~/...`) or GCP.

To reduce metadata file count for very large tables:
```
spark.sql.catalog.silver.write.metadata.compression-codec=gzip
spark.sql.catalog.silver.write.target-file-size-bytes=134217728   # 128 MB
```

---

## Where to change these settings

- **Cloud Workflows** (`workflows/daily.yaml`, `workflows/backfill.yaml`):
  `runtimeConfig.properties` block of each `create_dataproc_batch` step.
- **Local runners** (`scripts/ops/run_day_pipeline.py`,
  `scripts/ops/run_backfill.py`): CLI flags `--max-batch-workers`,
  `--shuffle-partitions`.
- **Default Spark session** (`src/procurement/silver/pipeline_orchestrator.py:build_spark_session()`):
  add `.config(...)` calls for any setting that should always apply regardless
  of invocation path.
