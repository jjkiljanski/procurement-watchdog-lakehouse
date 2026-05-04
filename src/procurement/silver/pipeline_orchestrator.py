"""Core Silver build logic shared by day and range wrappers.

Two public entry points
-----------------------
``run_silver_day_core``
    Builds Silver for **one calendar day**.  Processes each notice-type
    partition in parallel (bounded ThreadPoolExecutor) and writes one Iceberg
    partition per notice-type.  Used by the daily Dataproc batch.

``run_silver_range_core``
    Builds Silver for a **date range** (backfill).  Loops over notice types
    (≈14 iterations) — for each notice type it reads *all* date partitions in
    the range in a single Spark plan, processes them, and writes all date
    partitions at once with ``overwritePartitions()``.  This eliminates the
    per-day Spark DAG preparation overhead (≈6 s × 14 × 365 → 6 s × 14).

Shared core
-----------
Both entry points delegate per-batch processing to ``_run_batch_core()``,
which handles: persist → count → (repartition in day mode) → section extraction
→ column parsers → Pydantic validation → quarantine write → envelope write →
unpersist.  Range mode skips repartition: bronze Parquet is already partitioned
by date on disk, giving one task per date with no shuffle on read or write.
The only caller-supplied difference is ``write_section_fn``:

* Day mode  — filters to ``target_date`` before writing.
* Range mode — writes all date partitions at once (``overwritePartitions()``).
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from procurement.runtime.base import StorageProvider

from procurement.logging import get_stage_logger
from procurement.obs import (
    git_commit_sha,
    now_utc_iso,
    sha256_file,
    write_dq_metrics,
    write_pipeline_run,
    write_quarantine_summary,
)
from procurement.silver.common_envelope import (
    ENVELOPE_COLUMNS,
    build_envelope_df,
    validate_envelope_schema,
)
from procurement.silver.notice_schemas import (
    normalized_notice_type_token,
)
from procurement.silver.section_pipeline.final_schema_validator import (
    apply_pydantic_validation,
    validate_section_models,
)
from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
from procurement.silver.section_pipeline.spark_table_builder import (
    apply_column_parsers,
    build_section_tables,
    detect_section_parse_error_quarantine,
    detect_unknown_section_quarantine,
    make_html_sections_udf,
    prebuild_all_parser_udfs,
)

log = get_stage_logger(__name__, "silver")


HEAVY_HTML_NOTICE_TYPES = {
    "ContractNotice",
    "TenderResultNotice",
    "ContractPerformingNotice",
}

# Processing order: light types first (finish fast, free worker slots early);
# heavy HTML types last (run offset from each other to avoid peak memory).
_BATCH_PRIORITY: dict[str, int] = {
    "SmallContractNotice": 0,
    "NoticeUpdateConcession": 1,
    "CompetitionResultNotice": 2,
    "CompetitionNotice": 3,
    "AgreementUpdateNotice": 4,
    "AgreementIntentionNotice": 5,
    "NoticeUpdateNotice": 50,
    "TenderResultNotice": 51,
    "ContractPerformingNotice": 52,
    "ContractNotice": 53,
}


@dataclass(frozen=True)
class CoreRunConfig:
    target_date: str
    bronze_dir: str
    silver_dir: str
    raw_dir: str = "data/raw"
    input_layer: str = "auto"  # auto|bronze|raw
    shuffle_partitions: int = 0
    repartition: int = 0
    max_batch_workers: int = 0
    max_section_write_workers: int = 0
    lock_stale_minutes: int = 360  # kept for call-site compatibility; no longer used
    mode: str = "day"  # day|backfill
    profile_json: str = ""


def build_spark_session(master: str, app_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.parquet.columnarReaderBatchSize", "1024")
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=4m -XX:+ExplicitGCInvokesConcurrent")
        .getOrCreate()
    )


def _auto_target_partitions(raw_count: int, default_parallelism: int) -> int:
    max_parallel = max(2, default_parallelism * 2)
    size_based = max(2, raw_count // 2000)
    return min(max_parallel, size_based)


def _adaptive_target_partitions(notice_type: str | None, row_count: int, default_parallelism: int) -> int:
    max_parallel = max(2, default_parallelism * 2)
    notice = notice_type or ""
    if notice in HEAVY_HTML_NOTICE_TYPES and row_count >= 200:
        target = max(default_parallelism, (row_count + 149) // 150)
        return min(max_parallel, max(2, target))
    return _auto_target_partitions(row_count, default_parallelism)


def _maybe_repartition_batch(
    df: "DataFrame",
    notice_type: str | None,
    row_count: int,
    repartition_arg: int,
    spark: "SparkSession",
    notice_type_token: str,
) -> "DataFrame":
    current = df.rdd.getNumPartitions()
    is_heavy = (notice_type or "") in HEAVY_HTML_NOTICE_TYPES
    target = (
        repartition_arg
        if repartition_arg > 0
        else _adaptive_target_partitions(notice_type, row_count, spark.sparkContext.defaultParallelism)
    )
    if row_count < 2000 and not is_heavy:
        log.debug(
            "Batch noticeType=%s kept partitions=%d (rows=%d; tiny batch)",
            notice_type_token,
            current,
            row_count,
        )
        return df
    if target > current:
        log.info("Batch noticeType=%s repartition %d -> %d (rows=%d)", notice_type_token, current, target, row_count)
        return df.repartition(target)
    return df


# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------


def _iceberg_table_exists(spark: "SparkSession", full_table_name: str) -> bool:
    """Check if an Iceberg table exists by issuing DESCRIBE TABLE."""
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def _iceberg_ensure_namespace(spark: "SparkSession", namespace: str) -> None:
    """Create an Iceberg namespace if it does not exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def _iceberg_ensure_shared_tables(spark: "SparkSession") -> None:
    """Create shared Iceberg tables (quarantine, common_envelope) before parallel batch writes.

    Must be called single-threaded before the ThreadPoolExecutor starts so that
    no two batch workers race to create the same table.
    """
    _iceberg_ensure_namespace(spark, "silver.notice_type_tables")
    _iceberg_ensure_namespace(spark, "silver.common")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.common.quarantine (
            objectId STRING,
            publicationDateDay STRING,
            notice_type STRING,
            data_model STRING,
            _parse_errors ARRAY<STRING>
        ) USING iceberg
        PARTITIONED BY (publicationDateDay, notice_type)
    """)
    log.info("Ensured Iceberg table: silver.common.quarantine")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.common.common_envelope (
            objectId STRING,
            noticeType STRING,
            noticeNumber STRING,
            bzpNumber STRING,
            publicationDate STRING,
            publicationDateDay STRING,
            cpvCode STRING,
            isTenderAmountBelowEU BOOLEAN,
            orderObject STRING,
            clientType STRING,
            orderType STRING,
            tenderType STRING,
            organizationName STRING,
            organizationCity STRING,
            organizationCountry STRING,
            organizationNationalId STRING,
            organizationId STRING,
            organizationProvince STRING,
            tenderId STRING,
            submittingOffersDate STRING,
            procedureResult STRING,
            contractors ARRAY<STRUCT<contractorName STRING, contractorCity STRING, contractorProvince STRING, contractorCountry STRING, contractorNationalId STRING>>,
            clientTypeName STRING,
            provinceName STRING,
            caseId STRING,
            noticeStage STRING
        ) USING iceberg
        PARTITIONED BY (publicationDateDay)
    """)
    log.info("Ensured Iceberg table: silver.common.common_envelope")


def _iceberg_notice_type_table_name(notice_type_token: str, data_model: str) -> str:
    """Return the Iceberg table name for a section table.

    Converts CamelCase notice type token to snake_case and normalises the
    data_model name, then joins them with double-underscore.

    Examples
    --------
    >>> _iceberg_notice_type_table_name("ContractNotice", "core")
    "contract_notice__core"
    >>> _iceberg_notice_type_table_name("ContractNotice", "part.core")
    "contract_notice__part_core"
    """
    if notice_type_token in ("__NULL__", "__EMPTY__"):
        nt_snake = "unknown"
    else:
        nt_snake = re.sub(r"(?<!^)(?=[A-Z])", "_", notice_type_token).lower()
    dm_clean = data_model.replace(".", "_").lower()
    return f"{nt_snake}__{dm_clean}"


def _discover_notice_types_for_range(bronze_notices_root: str) -> list[tuple[str | None, str]]:
    """Return ``(notice_type, nt_directory_path)`` for every notice type present in bronze.

    Unlike ``_discover_bronze_partitions`` (which scopes to one date), this
    function returns the top-level ``noticeType=X/`` directory for each notice
    type.  The caller filters to a date range via a Spark predicate so that
    Spark's partition pruning eliminates unneeded date sub-directories.

    Supports both local filesystem paths and ``gs://`` URIs.
    """
    if bronze_notices_root.startswith("gs://"):
        from google.cloud import storage as gcs

        no_scheme = bronze_notices_root[5:]
        bucket_name, _, prefix_rest = no_scheme.partition("/")
        prefix = prefix_rest.rstrip("/") + "/"

        client = gcs.Client()
        blobs = client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
        _ = list(blobs)

        result: list[tuple[str | None, str]] = []
        for nt_prefix in blobs.prefixes:
            token = nt_prefix.rstrip("/").split("/")[-1].replace("noticeType=", "")
            nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
            result.append((nt, f"gs://{bucket_name}/{nt_prefix.rstrip('/')}"))
        return sorted(result, key=lambda x: ("" if x[0] is None else str(x[0])))
    else:
        bronze_root = Path(bronze_notices_root)
        if not bronze_root.exists():
            return []
        result = []
        for nt_dir in sorted(bronze_root.glob("noticeType=*")):
            if not nt_dir.is_dir():
                continue
            token = nt_dir.name.replace("noticeType=", "")
            nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
            result.append((nt, str(nt_dir)))
        return result


def _discover_bronze_partitions(bronze_notices_root: str, target_date: str) -> list[tuple[str | None, str]]:
    """Return ``(notice_type, partition_path)`` pairs for *target_date*.

    Supports both local filesystem paths and ``gs://`` URIs.
    """
    if bronze_notices_root.startswith("gs://"):
        from google.cloud import storage as gcs

        no_scheme = bronze_notices_root[5:]
        bucket_name, _, prefix_rest = no_scheme.partition("/")
        prefix = prefix_rest.rstrip("/") + "/"

        client = gcs.Client()
        blobs = client.list_blobs(bucket_name, prefix=prefix, delimiter="/")
        _ = list(blobs)

        result: list[tuple[str | None, str]] = []
        for nt_prefix in blobs.prefixes:
            token = nt_prefix.rstrip("/").split("/")[-1].replace("noticeType=", "")
            date_prefix = f"{nt_prefix}publicationDateDay={target_date}/"
            date_blobs = client.list_blobs(bucket_name, prefix=date_prefix, max_results=1)
            if any(True for _ in date_blobs):
                nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
                result.append((nt, f"gs://{bucket_name}/{date_prefix.rstrip('/')}"))
        return sorted(result, key=lambda x: ("" if x[0] is None else str(x[0])))
    else:
        bronze_root = Path(bronze_notices_root)
        paths = sorted(bronze_root.glob(f"noticeType=*/publicationDateDay={target_date}"))
        result = []
        for p in paths:
            token = p.parent.name.replace("noticeType=", "")
            nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
            result.append((nt, str(p)))
        return result


# ---------------------------------------------------------------------------
# Shared batch-processing core
# ---------------------------------------------------------------------------


def _run_batch_core(
    spark: "SparkSession",
    batch_raw: "DataFrame",
    notice_type: str | None,
    notice_token: str,
    all_profiles: dict,
    sections_udf,
    write_section_fn: "Callable[[str, Any], None]",
    repartition_arg: int,
    max_section_write_workers: int,
    _global_shuffle: int,
    _default_parallelism: int,
    obs_dir: "Path | None",
    quarantine_obs_date: str,
    skip_repartition: bool = False,
) -> dict:
    """Process one notice-type batch end-to-end.  Shared by day and range modes.

    Handles: persist → count → repartition → HTML section extraction → column
    parsers → Pydantic validation → quarantine write → envelope write →
    unpersist.

    Parameters
    ----------
    batch_raw:
        DataFrame of notices for this batch (not yet persisted).  For day mode:
        one ``(notice_type, date)`` partition.  For range mode: all dates in the
        range for one notice type.
    write_section_fn:
        ``(model: str, model_df: DataFrame) -> None`` — writes one section-model
        DataFrame to Iceberg.  Day mode filters to ``target_date`` first; range
        mode writes all date partitions at once.
    quarantine_obs_date:
        Date used for quarantine-summary observability writes.  Day mode:
        ``target_date``.  Range mode: ``start_date``.
    skip_repartition:
        When True, skip the count-based repartition entirely.  Used in range
        mode: bronze Parquet is already partitioned by publicationDateDay on
        disk, so Spark reads it as N_days naturally date-aligned tasks.
        Skipping repartition avoids a shuffle on read and a second shuffle on
        overwritePartitions() write (each task already owns exactly one date).

    Returns
    -------
    dict with keys ``rows`` (int) and ``profile`` (timing/count metadata).
    """
    from pyspark.sql.functions import array, col, lit
    from pyspark.storagelevel import StorageLevel

    batch_profile: dict = {"noticeType": notice_token, "shuffle_partitions": _global_shuffle}
    _sections_cache = None
    _c2_persisted: list = []
    _c3_persisted: list = []
    _raw_cache = batch_raw.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        count_t0 = time.perf_counter()
        batch_count = _raw_cache.count()
        batch_profile["count_sec"] = round(time.perf_counter() - count_t0, 3)
        batch_profile["rows"] = batch_count

        if batch_count == 0:
            return {"rows": 0, "profile": batch_profile}

        if skip_repartition:
            batch_raw_proc = _raw_cache
        else:
            batch_raw_proc = _maybe_repartition_batch(
                df=_raw_cache,
                notice_type=notice_type,
                row_count=batch_count,
                repartition_arg=repartition_arg,
                spark=spark,
                notice_type_token=notice_token,
            )

        notice_profile = all_profiles.get(notice_type or "", {})
        all_quarantine_dfs: list = []

        if notice_type is not None and not notice_profile:
            log.warning(
                "notice_type=%s has no registered profile; quarantining %d rows",
                notice_type,
                batch_count,
            )
            all_quarantine_dfs.append(
                _raw_cache.select(
                    col("objectId"),
                    col("publicationDateDay"),
                    lit(notice_type).alias("notice_type"),
                    lit("unknown").alias("data_model"),
                    array(lit(f"no registered profile for notice type: {notice_type}")).alias("_parse_errors"),
                )
            )
            section_tables: dict = {}
        else:
            _t = time.perf_counter()
            section_tables, _sections_cache = build_section_tables(
                batch_raw_proc,
                notice_type=notice_type,
                profile=notice_profile,
                sections_udf=sections_udf,
            )
            batch_profile["build_sections_sec"] = round(time.perf_counter() - _t, 3)

            c0_qdf = detect_section_parse_error_quarantine(_sections_cache, notice_type)
            if c0_qdf is not None:
                all_quarantine_dfs.append(c0_qdf)

            c1_qdf = detect_unknown_section_quarantine(_sections_cache, notice_type)
            if c1_qdf is not None:
                all_quarantine_dfs.append(c1_qdf)

            _t = time.perf_counter()
            section_tables, _, _c2_persisted = apply_column_parsers(section_tables, notice_profile, notice_type)
            batch_profile["apply_parsers_sec"] = round(time.perf_counter() - _t, 3)

            _t = time.perf_counter()
            section_tables, c3_qdf, _c3_persisted = apply_pydantic_validation(section_tables, notice_type)
            batch_profile["apply_pydantic_sec"] = round(time.perf_counter() - _t, 3)
            if c3_qdf is not None:
                all_quarantine_dfs.append(c3_qdf)

            section_tables = validate_section_models(section_tables, notice_type)

        _t = time.perf_counter()
        _all_writers = list(section_tables.items())
        if len(_all_writers) > 1 and max_section_write_workers > 1:
            with ThreadPoolExecutor(max_workers=min(len(_all_writers), max_section_write_workers)) as _pool:
                _futs = [_pool.submit(write_section_fn, m, df) for m, df in _all_writers]
                for _f in as_completed(_futs):
                    _f.result()
        else:
            for m, df in _all_writers:
                write_section_fn(m, df)
        batch_profile["write_sections_sec"] = round(time.perf_counter() - _t, 3)
        batch_profile["section_models"] = sorted(section_tables.keys())

        _t = time.perf_counter()
        if all_quarantine_dfs:
            quarantine_df = all_quarantine_dfs[0]
            for _qdf in all_quarantine_dfs[1:]:
                quarantine_df = quarantine_df.union(_qdf)
            q_count = quarantine_df.count()
            if q_count > 0:
                quarantine_df.writeTo("silver.common.quarantine").overwritePartitions()
                log.info(
                    "Wrote quarantine noticeType=%s rows=%d -> silver.common.quarantine",
                    notice_token,
                    q_count,
                )
                write_quarantine_summary(
                    target_date=quarantine_obs_date,
                    notice_type=notice_token,
                    row_count=q_count,
                    obs_dir=obs_dir,
                )
            else:
                log.debug("Skipped empty quarantine write noticeType=%s", notice_token)
        batch_profile["write_quarantine_sec"] = round(time.perf_counter() - _t, 3)

        _t = time.perf_counter()
        envelope_df = build_envelope_df(_raw_cache)
        validate_envelope_schema(envelope_df)
        envelope_df.writeTo("silver.common.common_envelope").append()
        batch_profile["envelope_sec"] = round(time.perf_counter() - _t, 3)

        batch_profile["valid_rows"] = batch_count
        return {"rows": batch_count, "profile": batch_profile}

    finally:
        for _df in _c2_persisted:
            _df.unpersist()
        for _df in _c3_persisted:
            _df.unpersist()
        if _sections_cache is not None:
            _sections_cache.unpersist()
        _raw_cache.unpersist()


# ---------------------------------------------------------------------------
# Day build
# ---------------------------------------------------------------------------


def run_silver_day_core(
    spark: "SparkSession",
    cfg: CoreRunConfig,
    *,
    command: list[str] | None = None,
    args_dict: dict | None = None,
    script_paths: list[Path] | None = None,
    run_context: dict | None = None,
    obs_dir: Path | None = None,
    storage: "StorageProvider | None" = None,
    script_hash: str | None = None,
) -> dict:
    """Build Silver for one day.

    Writes a per-(date, notice_type) manifest after each notice-type batch
    succeeds when *storage* and *script_hash* are provided.  Both must be
    supplied together; if either is absent manifest writes are skipped.
    """
    from pyspark.sql.functions import col, lit, to_date

    target_date = cfg.target_date
    started_at = now_utc_iso()
    run_id = f"silver_daily_{target_date}_{int(time.time() * 1000)}"

    bronze_notices_root = (
        cfg.bronze_dir.rstrip("/") + "/notices"
        if cfg.bronze_dir.startswith("gs://")
        else str(Path(cfg.bronze_dir) / "notices")
    )

    if cfg.shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))

    # Load all section profiles once; build the HTML→sections UDF once for the run.
    all_profiles = load_all_profiles()
    sections_udf = make_html_sections_udf(all_profiles)
    log.info("Loaded section profiles for notice types: %s", sorted(all_profiles))
    prebuild_all_parser_udfs(all_profiles)

    # ── Iceberg setup ─────────────────────────────────────────────────────────
    # Pre-create namespaces and shared tables (quarantine, common_envelope) here,
    # single-threaded, before the ThreadPoolExecutor starts batch workers.
    # Section tables are created on first write inside each batch worker.
    _iceberg_ensure_shared_tables(spark)
    # Pre-delete the envelope day partition so that concurrent batch appends
    # land in a clean slot and re-runs do not duplicate data.
    try:
        spark.sql(
            f"DELETE FROM silver.common.common_envelope WHERE publicationDateDay = '{target_date}'"
        )
        log.info("Pre-deleted common_envelope partition for %s", target_date)
    except Exception as exc:
        log.debug("Envelope pre-delete skipped (table may be newly created): %s", exc)
    # ─────────────────────────────────────────────────────────────────────────

    bronze_partition_pairs = _discover_bronze_partitions(bronze_notices_root, target_date)
    use_bronze = cfg.input_layer in ("auto", "bronze") and len(bronze_partition_pairs) > 0
    if cfg.input_layer == "bronze" and not bronze_partition_pairs:
        raise ValueError(f"Bronze partitions for {target_date} not found under {bronze_notices_root}")

    df_raw = None
    if not use_bronze:
        if cfg.raw_dir.startswith("gs://"):
            raw_path_str = cfg.raw_dir.rstrip("/") + f"/bzp_{target_date}.json"
        else:
            raw_path_local = Path(cfg.raw_dir) / f"bzp_{target_date}.json"
            if not raw_path_local.exists():
                raise ValueError(f"Raw file not found: {raw_path_local}")
            raw_path_str = str(raw_path_local)
        df_raw = spark.read.json(raw_path_str, multiLine=True)
        log.warning("Bronze input not available; falling back to raw JSON: %s", raw_path_str)

    if use_bronze:
        notice_batches: list[tuple[str | None, str | None]] = list(bronze_partition_pairs)
        notice_batches.sort(key=lambda x: (
            x[0] is None,
            _BATCH_PRIORITY.get(x[0], 99),
            "" if x[0] is None else str(x[0]),
        ))
        log.info(
            "Processing Bronze partition batches in order: %s",
            [normalized_notice_type_token(nt) for nt, _ in notice_batches],
        )
    else:
        notice_types = [row.noticeType for row in df_raw.select("noticeType").distinct().collect()]
        notice_types.sort(key=lambda x: (x is None, "" if x is None else str(x)))
        notice_batches = [(nt, None) for nt in notice_types]
        log.info("Processing raw noticeType batches in order: %s", notice_types)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    _default_parallelism = spark.sparkContext.defaultParallelism
    _global_shuffle = (
        cfg.shuffle_partitions
        if cfg.shuffle_partitions > 0
        else max(4, min(_default_parallelism * 2, 32))
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(_global_shuffle))

    batch_workers = (
        cfg.max_batch_workers
        if cfg.max_batch_workers > 0
        else min(len(notice_batches), max(1, min(4, _default_parallelism)))
    )
    max_section_write_workers = (
        cfg.max_section_write_workers
        if cfg.max_section_write_workers > 0
        else 1
    )

    run_start = time.perf_counter()
    profile: dict = {"target_date": target_date, "input_layer": "bronze" if use_bronze else "raw", "batches": []}
    _lock = threading.Lock()
    _accum: dict = {"rows": 0, "profiles": []}

    def _process_batch(notice_type: str | None, batch_path: str | None) -> None:
        batch_t0 = time.perf_counter()
        notice_token = normalized_notice_type_token(notice_type)

        if use_bronze:
            assert batch_path is not None
            batch_raw = spark.read.option("basePath", bronze_notices_root).parquet(batch_path)
        else:
            if notice_type is None:
                batch_raw = df_raw.filter(col("noticeType").isNull())
            else:
                batch_raw = df_raw.filter(col("noticeType") == lit(notice_type))

        batch_raw = batch_raw.withColumn(
            "publicationDateDay", to_date(col("publicationDate")).cast("string")
        )

        # Day-mode write: filter to target_date before overwriting the partition.
        def _write_section(model: str, model_df, _nt=notice_token) -> None:
            table_name = _iceberg_notice_type_table_name(_nt, model)
            full_table = f"silver.notice_type_tables.{table_name}"
            _wt = time.perf_counter()
            df_day = model_df.filter(col("publicationDateDay") == lit(target_date))
            if not _iceberg_table_exists(spark, full_table):
                df_day.writeTo(full_table).partitionedBy("publicationDateDay").create()
            else:
                df_day.writeTo(full_table).overwritePartitions()
            log.info(
                "Wrote section table noticeType=%s model=%s -> %s (%.1fs)",
                _nt, model, full_table, time.perf_counter() - _wt,
                extra={"notice_type": _nt},
            )

        result = _run_batch_core(
            spark=spark,
            batch_raw=batch_raw,
            notice_type=notice_type,
            notice_token=notice_token,
            all_profiles=all_profiles,
            sections_udf=sections_udf,
            write_section_fn=_write_section,
            repartition_arg=cfg.repartition,
            max_section_write_workers=max_section_write_workers,
            _global_shuffle=_global_shuffle,
            _default_parallelism=_default_parallelism,
            obs_dir=obs_dir,
            quarantine_obs_date=target_date,
        )

        result["profile"]["batch_total_sec"] = round(time.perf_counter() - batch_t0, 3)
        result["profile"]["batch_path"] = batch_path
        log.info(
            "Batch noticeType=%s rows=%d wrote specific+envelope in %.2fs",
            notice_token,
            result["rows"],
            time.perf_counter() - batch_t0,
            extra={"notice_type": notice_token, "date": target_date, "status": "ok"},
        )

        # Write per-(date, notice_type) manifest so individual batches can be
        # skipped on re-runs without reprocessing the whole day.
        if storage is not None and script_hash is not None and notice_type is not None:
            from procurement.manifests import write_processed_manifest as _wpm
            _wpm(
                layer="silver",
                target_date=target_date,
                script_hash=script_hash,
                storage=storage,
                notice_type=notice_token,
            )

        with _lock:
            _accum["rows"] += result["rows"]
            _accum["profiles"].append(result["profile"])

    # Run notice-type batches in parallel, capped to avoid Spark resource contention.
    with ThreadPoolExecutor(max_workers=batch_workers) as _executor:
        _futures = {
            _executor.submit(_process_batch, nt, bp): (nt, bp)
            for nt, bp in notice_batches
        }
        for _fut in as_completed(_futures):
            _fut.result()  # re-raise any exception from the batch worker

    total_rows = _accum["rows"]
    profile["batches"] = _accum["profiles"]

    validation_metrics = {
        "expected_columns": len(ENVELOPE_COLUMNS),
        "actual_columns": len(ENVELOPE_COLUMNS),
        "missing_columns": [],
        "extra_columns": [],
    }

    profile["total_input_rows"] = total_rows
    profile["validation"] = {"common_envelope": validation_metrics}
    profile["run_total_sec"] = round(time.perf_counter() - run_start, 3)

    if cfg.profile_json:
        profile_path = Path(cfg.profile_json)
        profile_path.parent.mkdir(parents=True, exist_ok=True)
        profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Wrote profile JSON to %s", profile_path)

    completed_at = now_utc_iso()
    entry_script = (script_paths or [None])[0]
    write_pipeline_run(
        layer="silver",
        target_date=target_date,
        run_id=run_id,
        started_at=started_at,
        completed_at=completed_at,
        status="ok",
        counts={"input_rows": total_rows},
        git_commit=git_commit_sha(),
        script_hash=sha256_file(entry_script) if entry_script else None,
        obs_dir=obs_dir,
    )
    write_dq_metrics(
        layer="silver",
        target_date=target_date,
        notice_type=None,
        metrics={
            k: v
            for k, v in (validation_metrics or {}).items()
            if isinstance(v, (int, float))
        },
        run_id=run_id,
        obs_dir=obs_dir,
    )
    for batch in profile.get("batches", []):
        nt = batch.get("noticeType")
        batch_rows = batch.get("rows", 0)
        if batch_rows > 0:
            write_dq_metrics(
                layer="silver",
                target_date=target_date,
                notice_type=nt,
                metrics={"input_rows": batch_rows},
                run_id=run_id,
                obs_dir=obs_dir,
            )

    return {
        "rows": total_rows,
        "input_paths": [p for _, p in notice_batches if p is not None],
        "validation_metrics": validation_metrics,
        "profile": profile,
    }


# ---------------------------------------------------------------------------
# Range build (backfill)
# ---------------------------------------------------------------------------


def run_silver_range_core(
    spark: "SparkSession",
    start_date: str,
    end_date: str,
    bronze_dir: str,
    *,
    repartition: int = 0,
    shuffle_partitions: int = 0,
    max_section_write_workers: int = 0,
    obs_dir: "Path | None" = None,
    script_paths: "list[Path] | None" = None,
    storage: "StorageProvider | None" = None,
    script_hash: str | None = None,
    force: bool = False,
) -> dict:
    """Build Silver for a date range — one Spark plan per notice type.

    Loop structure (≈14 iterations, not 365×14):

        for notice_type in all_notice_types:
            df = spark.read.parquet("bronze/noticeType={nt}/")
                    .filter(publicationDateDay BETWEEN start AND end)
            # one Spark plan compiled once for all dates of this notice type
            _run_batch_core(df, write_section_fn=lambda m, df: df.writeTo(...).overwritePartitions())

    This eliminates per-day Spark DAG preparation (≈6 s × 14 × N_days overhead
    compared to the per-date loop in ``build_silver_backfill.py``).

    Per-(date, notice_type) manifests are written after each notice-type batch
    when *storage* and *script_hash* are supplied.  On re-runs only batches
    whose manifest is absent or stale are reprocessed.  Pass ``force=True`` to
    reprocess everything regardless of manifest state.

    Returns a summary dict with ``rows``, ``start_date``, ``end_date``, and
    ``batches`` (per-notice-type timing profiles).
    """
    from pyspark.sql.functions import col, lit, to_date

    started_at = now_utc_iso()
    run_id = f"silver_range_{start_date}_{end_date}_{int(time.time() * 1000)}"

    bronze_notices_root = (
        bronze_dir.rstrip("/") + "/notices"
        if bronze_dir.startswith("gs://")
        else str(Path(bronze_dir) / "notices")
    )

    if shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))

    all_profiles = load_all_profiles()
    sections_udf = make_html_sections_udf(all_profiles)
    log.info("Loaded section profiles for notice types: %s", sorted(all_profiles))
    prebuild_all_parser_udfs(all_profiles)

    _iceberg_ensure_shared_tables(spark)

    # Pre-delete envelope for the full range so per-notice-type appends land in
    # a clean slot.  Iceberg ACID serialises concurrent commits.
    try:
        spark.sql(
            f"DELETE FROM silver.common.common_envelope "
            f"WHERE publicationDateDay >= '{start_date}' AND publicationDateDay <= '{end_date}'"
        )
        log.info("Pre-deleted common_envelope partitions for %s..%s", start_date, end_date)
    except Exception as exc:
        log.debug("Envelope pre-delete skipped (table may not exist yet): %s", exc)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    _default_parallelism = spark.sparkContext.defaultParallelism
    _global_shuffle = (
        shuffle_partitions
        if shuffle_partitions > 0
        else max(4, min(_default_parallelism * 2, 32))
    )
    spark.conf.set("spark.sql.shuffle.partitions", str(_global_shuffle))

    _max_section_write_workers = max_section_write_workers if max_section_write_workers > 0 else 1

    notice_type_pairs = _discover_notice_types_for_range(bronze_notices_root)
    notice_type_pairs.sort(key=lambda x: (
        x[0] is None,
        _BATCH_PRIORITY.get(x[0], 99) if x[0] else 99,
        "" if x[0] is None else str(x[0]),
    ))
    log.info(
        "Processing %d notice types for range %s..%s: %s",
        len(notice_type_pairs), start_date, end_date,
        [normalized_notice_type_token(nt) for nt, _ in notice_type_pairs],
    )

    run_start = time.perf_counter()
    total_rows = 0
    batch_profiles: list[dict] = []

    for notice_type, nt_path in notice_type_pairs:
        notice_token = normalized_notice_type_token(notice_type)

        # Skip if all dates in range already have a matching manifest.
        if (
            not force
            and storage is not None
            and script_hash is not None
            and notice_type is not None
        ):
            from datetime import date as _date
            from datetime import timedelta as _td

            from procurement.manifests import is_already_processed as _iap
            _start = _date.fromisoformat(start_date)
            _end = _date.fromisoformat(end_date)
            _d = _start
            _all_done = True
            while _d <= _end:
                if not _iap("silver", _d.isoformat(), script_hash, storage, notice_token):
                    _all_done = False
                    break
                _d += _td(days=1)
            if _all_done:
                log.info(
                    "Skipping noticeType=%s for %s..%s — all dates already processed",
                    notice_token, start_date, end_date,
                )
                continue

        batch_t0 = time.perf_counter()

        # One Spark plan for ALL dates of this notice type in the range.
        # Spark's partition pruning limits actual file reads to the date range.
        batch_raw = (
            spark.read.option("basePath", bronze_notices_root).parquet(nt_path)
            .filter(
                (col("publicationDateDay") >= lit(start_date))
                & (col("publicationDateDay") <= lit(end_date))
            )
            .withColumn("publicationDateDay", to_date(col("publicationDate")).cast("string"))
        )

        # Range-mode write: no per-date filter — overwritePartitions() rewrites
        # exactly the date partitions present in the data, leaving all other
        # existing partitions untouched.
        def _write_section_range(model: str, model_df, _nt=notice_token) -> None:
            table_name = _iceberg_notice_type_table_name(_nt, model)
            full_table = f"silver.notice_type_tables.{table_name}"
            _wt = time.perf_counter()
            if not _iceberg_table_exists(spark, full_table):
                model_df.writeTo(full_table).partitionedBy("publicationDateDay").create()
            else:
                model_df.writeTo(full_table).overwritePartitions()
            log.info(
                "Wrote section table (range) noticeType=%s model=%s -> %s (%.1fs)",
                _nt, model, full_table, time.perf_counter() - _wt,
            )

        result = _run_batch_core(
            spark=spark,
            batch_raw=batch_raw,
            notice_type=notice_type,
            notice_token=notice_token,
            all_profiles=all_profiles,
            sections_udf=sections_udf,
            write_section_fn=_write_section_range,
            repartition_arg=repartition,
            max_section_write_workers=_max_section_write_workers,
            _global_shuffle=_global_shuffle,
            _default_parallelism=_default_parallelism,
            obs_dir=obs_dir,
            quarantine_obs_date=start_date,
            skip_repartition=True,
        )

        elapsed = time.perf_counter() - batch_t0
        result["profile"]["batch_total_sec"] = round(elapsed, 3)
        log.info(
            "Range batch noticeType=%s rows=%d done in %.2fs",
            notice_token, result["rows"], elapsed,
        )

        # Write per-(date, notice_type) manifest for every date in range.
        if storage is not None and script_hash is not None and notice_type is not None:
            from datetime import date as _date
            from datetime import timedelta as _td

            from procurement.manifests import write_processed_manifest as _wpm
            _d = _date.fromisoformat(start_date)
            _end_d = _date.fromisoformat(end_date)
            while _d <= _end_d:
                _wpm(
                    layer="silver",
                    target_date=_d.isoformat(),
                    script_hash=script_hash,
                    storage=storage,
                    notice_type=notice_token,
                )
                _d += _td(days=1)

        total_rows += result["rows"]
        batch_profiles.append(result["profile"])

    elapsed_total = time.perf_counter() - run_start
    log.info(
        "run_silver_range_core complete: %s..%s rows=%d elapsed=%.1fs",
        start_date, end_date, total_rows, elapsed_total,
    )

    entry_script = (script_paths or [None])[0]
    write_pipeline_run(
        layer="silver",
        target_date=start_date,
        run_id=run_id,
        started_at=started_at,
        completed_at=now_utc_iso(),
        status="ok",
        counts={
            "input_rows": total_rows,
            "date_range_start": start_date,
            "date_range_end": end_date,
        },
        git_commit=git_commit_sha(),
        script_hash=sha256_file(entry_script) if entry_script else None,
        obs_dir=obs_dir,
    )

    return {
        "rows": total_rows,
        "start_date": start_date,
        "end_date": end_date,
        "batches": batch_profiles,
    }
