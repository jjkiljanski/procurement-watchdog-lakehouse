"""Core Silver build logic shared by day and backfill wrappers."""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

from procurement.common.locks import acquire_directory_lock, release_directory_lock_if_owner
from procurement.obs import git_commit_sha, now_utc_iso, sha256_file, write_dq_metrics, write_pipeline_run, write_quarantine_summary
from procurement.silver.notice_schemas import (
    normalized_notice_type_token,
)
from procurement.silver.common_envelope import (
    ENVELOPE_COLUMNS,
    build_envelope_df,
    validate_envelope_schema,
)
from procurement.silver.section_pipeline.notice_schema_reader import load_all_profiles
from procurement.silver.section_pipeline.final_schema_validator import apply_pydantic_validation, validate_section_models
from procurement.silver.section_pipeline.spark_table_builder import apply_column_parsers, build_section_tables, detect_section_parse_error_quarantine, detect_unknown_section_quarantine, make_html_sections_udf, prebuild_all_parser_udfs

log = logging.getLogger(__name__)


HEAVY_HTML_NOTICE_TYPES = {
    "ContractNotice",
    "TenderResultNotice",
    "ContractPerformingNotice",
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
    lock_stale_minutes: int = 360
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


def _finalize_envelope_tmp_dir(envelope_tmp_dir: Path, envelope_day_dir: Path) -> None:
    """Move per-batch envelope parquet files into the final day directory.

    This avoids an extra Spark read+write cycle over all envelope rows after
    batch processing completes.
    """
    if not envelope_tmp_dir.exists():
        return
    envelope_day_dir.mkdir(parents=True, exist_ok=True)
    for batch_dir in sorted(p for p in envelope_tmp_dir.iterdir() if p.is_dir()):
        for child in batch_dir.iterdir():
            if child.is_file():
                shutil.move(str(child), str(envelope_day_dir / child.name))
    _safe_rmtree(envelope_tmp_dir, "envelope tmp dir")


def _safe_rmtree(path: Path, label: str) -> None:
    if not path.exists():
        return
    try:
        shutil.rmtree(path, ignore_errors=False)
    except (PermissionError, OSError) as exc:
        log.warning("Could not pre-delete %s at %s: %s", label, path, exc)


def _acquire_day_lock(silver_dir: Path, day: str, run_id: str, stale_minutes: int) -> Path:
    owner = {
        "run_id": run_id,
        "pid": os.getpid(),
        "started_at_epoch": int(time.time()),
        "started_at_utc": now_utc_iso(),
        "target_date": day,
    }
    lock_dir = silver_dir / "_locks" / f"silver_day={day}"
    try:
        return acquire_directory_lock(
            lock_dir=lock_dir,
            owner_payload=owner,
            stale_seconds=max(1, stale_minutes) * 60,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if msg.startswith("Directory lock already exists:"):
            msg = msg.replace("Directory lock already exists:", f"Silver day lock already exists for {day}:", 1)
        raise RuntimeError(msg) from exc


def run_silver_day_core(
    spark: "SparkSession",
    cfg: CoreRunConfig,
    *,
    command: list[str] | None = None,
    args_dict: dict | None = None,
    script_paths: list[Path] | None = None,
    run_context: dict | None = None,
    obs_dir: Path | None = None,
) -> dict:
    """Build Silver for one day. Shared by day and backfill wrappers."""
    from pyspark.sql.functions import col, lit, to_date
    from pyspark.storagelevel import StorageLevel

    target_date = cfg.target_date
    started_at = now_utc_iso()
    run_id = f"{target_date}_{int(time.time() * 1000)}_{os.getpid()}_{uuid.uuid4().hex[:8]}"

    silver_dir = Path(cfg.silver_dir)
    bronze_dir = Path(cfg.bronze_dir)
    bronze_root = bronze_dir / "notices"

    day_lock_dir = _acquire_day_lock(
        silver_dir=silver_dir,
        day=target_date,
        run_id=run_id,
        stale_minutes=cfg.lock_stale_minutes,
    )
    log.info("Acquired day lock: %s", day_lock_dir)

    if cfg.shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))

    # Load all section profiles once; build the HTML→sections UDF once for the run.
    all_profiles = load_all_profiles()
    sections_udf = make_html_sections_udf(all_profiles)
    log.info("Loaded section profiles for notice types: %s", sorted(all_profiles))
    prebuild_all_parser_udfs(all_profiles)

    try:
        bronze_paths = sorted(bronze_root.glob(f"noticeType=*/publicationDateDay={target_date}"))
        use_bronze = cfg.input_layer in ("auto", "bronze") and len(bronze_paths) > 0
        if cfg.input_layer == "bronze" and not bronze_paths:
            raise ValueError(f"Bronze partitions for {target_date} not found under {bronze_root}")

        if not use_bronze:
            raw_path = Path(cfg.raw_dir) / f"bzp_{target_date}.json"
            if not raw_path.exists():
                raise ValueError(f"Raw file not found: {raw_path}")
            df_raw = spark.read.json(str(raw_path), multiLine=True)
            log.warning("Bronze input not available; falling back to raw JSON: %s", raw_path)

        if use_bronze:
            notice_batches: list[tuple[str | None, str | None]] = []
            for p in bronze_paths:
                token = p.parent.name.replace("noticeType=", "")
                nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
                notice_batches.append((nt, str(p)))
            # Light batches run first (wave 1) so they finish quickly (~10-20 s each with
            # pre-built UDFs), freeing worker slots at staggered times.  Heavy batches then
            # start offset from each other by ~5-10 s, so their write phases don't all
            # overlap — cutting peak Spark resource contention during the write stage.
            # Heavy types that are absent from the dict get the default priority (50) and
            # sort after the light types but before any truly unknown types (99).
            _BATCH_PRIORITY: dict[str, int] = {
                "SmallContractNotice": 0,
                "NoticeUpdateConcession": 1,
                "CompetitionResultNotice": 2,
                "CompetitionNotice": 3,
                "AgreementUpdateNotice": 4,
                "AgreementIntentionNotice": 5,
                # heavy types — run after the light wave has freed staggered slots
                "NoticeUpdateNotice": 50,
                "TenderResultNotice": 51,
                "ContractPerformingNotice": 52,
                "ContractNotice": 53,
            }
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

        specific_root = silver_dir / "notice_type_tables"
        envelope_day_dir = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
        envelope_tmp_dir = silver_dir / "_tmp" / "silver_envelope_buffer" / f"day={target_date}" / f"run={run_id}"

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        _safe_rmtree(envelope_day_dir, "envelope day dir")
        envelope_day_dir.mkdir(parents=True, exist_ok=True)

        # Set shuffle partitions once globally — per-batch setting races across threads.
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
            batch_profile: dict = {
                "noticeType": notice_token,
                "shuffle_partitions": _global_shuffle,
            }
            if use_bronze:
                assert batch_path is not None
                batch_raw = spark.read.option("basePath", str(bronze_root)).parquet(batch_path)
                batch_profile["read_mode"] = "bronze_partition"
                batch_profile["read_path"] = batch_path
            else:
                if notice_type is None:
                    batch_raw = df_raw.filter(col("noticeType").isNull())
                else:
                    batch_raw = df_raw.filter(col("noticeType") == lit(notice_type))
                batch_profile["read_mode"] = "raw_filter"

            # Ensure publicationDateDay is present on the raw batch for section tables.
            batch_raw = batch_raw.withColumn(
                "publicationDateDay", to_date(col("publicationDate")).cast("string")
            )

            # Persist raw batch: avoids re-reading Parquet for count(), sections UDF, envelope UDF.
            _raw_cache = batch_raw.persist(StorageLevel.MEMORY_AND_DISK)
            _sections_cache = None
            _c2_persisted: list = []  # parser-level model DFs (from apply_column_parsers)
            _c3_persisted: list = []  # pydantic-level DFs (df_with_errors per model)
            batch_count = 0

            try:
                count_t0 = time.perf_counter()
                batch_count = _raw_cache.count()
                batch_profile["count_sec"] = round(time.perf_counter() - count_t0, 3)
                batch_profile["rows"] = batch_count

                batch_raw = _maybe_repartition_batch(
                    df=_raw_cache,
                    notice_type=notice_type,
                    row_count=batch_count,
                    repartition_arg=cfg.repartition,
                    spark=spark,
                    notice_type_token=notice_token,
                )

                # --- Section tables (profile-driven) ---
                notice_profile = all_profiles.get(notice_type or "", {})
                all_quarantine_dfs: list = []

                if notice_type is not None and not notice_profile:
                    # Case 4: notice type has no registered profile → quarantine entire batch
                    from pyspark.sql.functions import array
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
                    section_tables = {}
                else:
                    _t = time.perf_counter()
                    section_tables, _sections_cache = build_section_tables(
                        batch_raw,
                        notice_type=notice_type,
                        profile=notice_profile,
                        sections_udf=sections_udf,
                    )
                    batch_profile["build_sections_sec"] = round(time.perf_counter() - _t, 3)

                    # Case 0: rows where HTML section extraction failed structurally
                    # (e.g. a core section appearing more than once in a notice).
                    # These rows are already excluded from all model tables by
                    # build_section_tables; here we route them to quarantine.
                    c0_qdf = detect_section_parse_error_quarantine(_sections_cache, notice_type)
                    if c0_qdf is not None:
                        all_quarantine_dfs.append(c0_qdf)

                    # Case 1: rows with section numbers absent from the profile
                    c1_qdf = detect_unknown_section_quarantine(_sections_cache, notice_type)
                    if c1_qdf is not None:
                        all_quarantine_dfs.append(c1_qdf)

                    # Case 2: column-level parsing (fault-tolerant; errors go
                    # into the per-row parse_errors column, not quarantine).
                    # apply_column_parsers persists each model DF so that
                    # downstream steps (Pydantic, quarantine scans) read from
                    # cache once the parallel section-table writes populate it.
                    _t = time.perf_counter()
                    section_tables, _, _c2_persisted = apply_column_parsers(section_tables, notice_profile, notice_type)
                    batch_profile["apply_parsers_sec"] = round(time.perf_counter() - _t, 3)

                    # Case 3: Pydantic contract check — catches parser bugs
                    # (wrong return type without ParseError). Should never fire
                    # in practice; rows that do fail go to quarantine.
                    _t = time.perf_counter()
                    section_tables, c3_qdf, _c3_persisted = apply_pydantic_validation(section_tables, notice_type)
                    batch_profile["apply_pydantic_sec"] = round(time.perf_counter() - _t, 3)
                    if c3_qdf is not None:
                        all_quarantine_dfs.append(c3_qdf)

                    # Driver-side schema presence check (logs warnings, no data routing)
                    section_tables = validate_section_models(section_tables, notice_type)

                def _write_section(model: str, model_df) -> None:
                    model_day_dir = (
                        specific_root
                        / f"noticeType={notice_token}"
                        / f"data_model={model}"
                        / f"publicationDateDay={target_date}"
                    )
                    _safe_rmtree(model_day_dir, f"section table noticeType={notice_token} model={model}")
                    # Use append (not overwrite) — rmtree above already cleared the dir,
                    # so append = fresh write without Spark's partition-clearing logic,
                    # which is not safe across concurrent jobs sharing a parent dir.
                    _wt = time.perf_counter()
                    model_df.write.mode("append").parquet(str(model_day_dir))
                    log.info(
                        "Wrote section table noticeType=%s model=%s -> %s (%.1fs)",
                        notice_token, model, model_day_dir,
                        time.perf_counter() - _wt,
                    )

                # Write all section models (parallel when multiple models).
                _t = time.perf_counter()
                _all_writers: list = list(section_tables.items())
                if len(_all_writers) > 1 and max_section_write_workers > 1:
                    with ThreadPoolExecutor(max_workers=min(len(_all_writers), max_section_write_workers)) as _pool:
                        _section_futs = [_pool.submit(_write_section, m, df) for m, df in _all_writers]
                        for _f in as_completed(_section_futs):
                            _f.result()
                else:
                    for m, df in _all_writers:
                        _write_section(m, df)
                batch_profile["write_sections_sec"] = round(time.perf_counter() - _t, 3)

                batch_profile["section_models"] = sorted(section_tables.keys())

                # --- Quarantine (cases 1–4) ---
                # Must run after section writes — case 2/3 quarantine reads from
                # persisted model DFs that are materialised during section writes.
                _t = time.perf_counter()
                if all_quarantine_dfs:
                    quarantine_df: DataFrame | None = all_quarantine_dfs[0]
                    for _qdf in all_quarantine_dfs[1:]:
                        quarantine_df = quarantine_df.union(_qdf)
                    q_count = quarantine_df.count()
                    if q_count > 0:
                        quarantine_day_dir = (
                            silver_dir
                            / "quarantine"
                            / f"noticeType={notice_token}"
                            / f"publicationDateDay={target_date}"
                        )
                        _safe_rmtree(quarantine_day_dir, f"quarantine noticeType={notice_token}")
                        quarantine_df.write.mode("append").parquet(str(quarantine_day_dir))
                        log.info(
                            "Wrote quarantine data noticeType=%s rows=%d -> %s",
                            notice_token,
                            q_count,
                            quarantine_day_dir,
                        )
                        write_quarantine_summary(
                            target_date=target_date,
                            notice_type=notice_token,
                            row_count=q_count,
                            obs_dir=obs_dir,
                        )
                    else:
                        log.debug("Skipped empty quarantine write noticeType=%s", notice_token)
                batch_profile["write_quarantine_sec"] = round(time.perf_counter() - _t, 3)

                # --- Envelope ---
                _t = time.perf_counter()
                envelope_df = build_envelope_df(_raw_cache)
                validate_envelope_schema(envelope_df)
                batch_envelope_tmp = envelope_tmp_dir / f"batch={notice_token}"
                envelope_df.write.mode("overwrite").parquet(str(batch_envelope_tmp))
                batch_profile["envelope_sec"] = round(time.perf_counter() - _t, 3)

                batch_profile["valid_rows"] = batch_count
            finally:
                for _df in _c2_persisted:
                    _df.unpersist()
                for _df in _c3_persisted:
                    _df.unpersist()
                if _sections_cache is not None:
                    _sections_cache.unpersist()
                _raw_cache.unpersist()

            batch_profile["batch_total_sec"] = round(time.perf_counter() - batch_t0, 3)
            batch_profile["batch_path"] = batch_path
            log.info(
                "Batch noticeType=%s rows=%d wrote specific+envelope in %.2fs",
                notice_token,
                batch_count,
                time.perf_counter() - batch_t0,
            )
            with _lock:
                _accum["rows"] += batch_count
                _accum["profiles"].append(batch_profile)

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

        # Merge per-batch envelope subdirs into the final day partition.
        # Drop the auto-discovered "batch" partition column from the temp dir structure.
        _finalize_envelope_tmp_dir(envelope_tmp_dir, envelope_day_dir)
        validation_metrics = {"expected_columns": len(ENVELOPE_COLUMNS), "actual_columns": len(ENVELOPE_COLUMNS), "missing_columns": [], "extra_columns": []}

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
                    obs_dir=obs_dir,
                )

        return {
            "rows": total_rows,
            "input_paths": [p for _, p in notice_batches if p is not None],
            "validation_metrics": validation_metrics,
            "profile": profile,
        }
    finally:
        if day_lock_dir.exists():
            if release_directory_lock_if_owner(day_lock_dir, run_id):
                log.info("Released day lock: %s", day_lock_dir)
