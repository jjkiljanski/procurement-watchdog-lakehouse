"""Core Silver build logic shared by day and backfill wrappers."""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from procurement.common.locks import acquire_directory_lock, release_directory_lock_if_owner
from procurement.lineage import atomic_write_json, git_commit_sha, now_utc_iso, script_hashes, sha256_file
from procurement.silver.notice_types import (
    html_extracted_fields_for_notice_type,
    normalized_notice_type_token,
    specific_columns_for_notice_type,
)
from procurement.silver.spark_transforms import (
    build_contract_notice_parts_table,
    build_silver_for_notice_type,
)
from procurement.silver.validation import (
    summarize_notice_validation,
    validate_common_envelope,
    with_notice_validation_errors,
)

log = logging.getLogger(__name__)


ENVELOPE_COLUMNS = [
    "objectId",
    "noticeType",
    "noticeNumber",
    "bzpNumber",
    "publicationDate",
    "publicationDateDay",
    "isTenderAmountBelowEU",
    "orderObject",
    "clientType",
    "clientTypeName",
    "orderType",
    "tenderType",
    "organizationName",
    "organizationCity",
    "organizationProvince",
    "provinceName",
    "organizationCountry",
    "organizationNationalId",
    "organizationNationalId_parsed",
    "organizationId",
    "tenderId",
    "caseId",
    "caseId_shard",
    "noticeStage",
    "organizationNameNormalized",
    "street",
    "postal_code",
]

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
        .getOrCreate()
    )


def _select_existing(df: "DataFrame", columns: list[str]) -> "DataFrame":
    return df.select(*[c for c in columns if c in df.columns])


def _compact_html_extracted(df: "DataFrame", html_fields: list[str]) -> "DataFrame":
    if "htmlExtracted" not in df.columns:
        return df
    if not html_fields:
        return df.drop("htmlExtracted")
    from pyspark.sql.functions import col, struct

    cols = [col(f"htmlExtracted.{name}").alias(name) for name in html_fields]
    return df.withColumn("htmlExtracted", struct(*cols))


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
        log.info(
            "Batch noticeType=%s repartition %d -> %d (rows=%d)",
            notice_type_token,
            current,
            target,
            row_count,
        )
        return df.repartition(target)
    return df


def _load_bronze_lineage_ref(bronze_dir: Path, day: str) -> dict | None:
    meta_path = bronze_dir / "_meta" / f"day={day}.json"
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "manifest_path": str(meta_path),
            "manifest_sha256": sha256_file(meta_path),
            "error": "failed_to_parse_bronze_manifest",
        }
    return {
        "manifest_path": str(meta_path),
        "manifest_sha256": sha256_file(meta_path),
        "code": payload.get("code"),
        "counts": payload.get("counts"),
    }


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
) -> dict:
    """Build Silver for one day. Shared by day and backfill wrappers."""
    from pyspark.sql.functions import col, lit, pmod, size, to_date, when, xxhash64
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
            notice_batches.sort(key=lambda x: (x[0] is None, "" if x[0] is None else str(x[0])))
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
        quarantine_root = silver_dir / "_quarantine" / "notice_rows"
        quarantine_day_dir = quarantine_root / f"publicationDateDay={target_date}"

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        _safe_rmtree(envelope_day_dir, "envelope day dir")
        _safe_rmtree(quarantine_day_dir, "quarantine day dir")

        run_start = time.perf_counter()
        profile: dict = {"target_date": target_date, "input_layer": "bronze" if use_bronze else "raw", "batches": []}
        total_rows = 0
        total_invalid_rows = 0

        for notice_type, batch_path in notice_batches:
            batch_t0 = time.perf_counter()
            notice_token = normalized_notice_type_token(notice_type)
            batch_profile: dict = {"noticeType": notice_token}
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

            count_t0 = time.perf_counter()
            batch_count = batch_raw.count()
            batch_profile["count_sec"] = round(time.perf_counter() - count_t0, 3)
            batch_profile["rows"] = batch_count
            total_rows += batch_count

            if cfg.shuffle_partitions <= 0:
                per_batch_shuffle = (
                    cfg.repartition
                    if cfg.repartition > 0
                    else _adaptive_target_partitions(notice_type, batch_count, spark.sparkContext.defaultParallelism)
                )
                spark.conf.set("spark.sql.shuffle.partitions", str(per_batch_shuffle))
                batch_profile["shuffle_partitions"] = per_batch_shuffle
            else:
                batch_profile["shuffle_partitions"] = cfg.shuffle_partitions

            batch_raw = _maybe_repartition_batch(
                df=batch_raw,
                notice_type=notice_type,
                row_count=batch_count,
                repartition_arg=cfg.repartition,
                spark=spark,
                notice_type_token=notice_token,
            )

            specific_columns = specific_columns_for_notice_type(notice_type)
            html_fields = html_extracted_fields_for_notice_type(notice_type)
            required_columns = set(ENVELOPE_COLUMNS) | set(specific_columns)

            cached_batch = None
            try:
                batch_silver = build_silver_for_notice_type(
                    batch_raw,
                    notice_type=notice_type,
                    required_columns=required_columns,
                ).withColumn("publicationDateDay", to_date(col("publicationDate")).cast("string")).withColumn(
                    "caseId_shard",
                    when(col("caseId").isNotNull(), pmod(xxhash64(col("caseId")), lit(64)).cast("int")),
                )

                batch_silver, validation_rules = with_notice_validation_errors(
                    batch_silver,
                    target_date=target_date,
                    notice_type=notice_type,
                )
                cached_batch = batch_silver.persist(StorageLevel.MEMORY_AND_DISK)
                batch_validation = summarize_notice_validation(
                    cached_batch,
                    target_date=target_date,
                    notice_type=notice_type,
                    rules=validation_rules,
                )
                invalid_batch = cached_batch.filter(size(col("__validation_errors")) > 0)
                valid_batch = cached_batch.filter(size(col("__validation_errors")) == 0).drop("__validation_errors")
                batch_invalid_rows = int(batch_validation.get("invalid_rows", 0))
                total_invalid_rows += batch_invalid_rows

                if batch_invalid_rows > 0:
                    (
                        invalid_batch.withColumn("validation_notice_type", lit(notice_token))
                        .write.mode("append")
                        .partitionBy("publicationDateDay")
                        .parquet(str(quarantine_root))
                    )

                if notice_type == "ContractNotice":
                    specific_df = valid_batch
                else:
                    specific_df = _select_existing(valid_batch, ["caseId_shard", *specific_columns])
                    specific_df = _compact_html_extracted(specific_df, html_fields)

                specific_day_dir = specific_root / f"noticeType={notice_token}" / f"publicationDateDay={target_date}"
                _safe_rmtree(specific_day_dir, f"specific day dir noticeType={notice_token}")
                specific_df.write.mode("overwrite").parquet(str(specific_day_dir))

                if notice_type == "ContractNotice":
                    parts_token = "ContractNotice_parts"
                    parts_df = build_contract_notice_parts_table(valid_batch)
                    parts_day_dir = specific_root / f"noticeType={parts_token}" / f"publicationDateDay={target_date}"
                    _safe_rmtree(parts_day_dir, f"specific day dir noticeType={parts_token}")
                    parts_df.write.mode("overwrite").parquet(str(parts_day_dir))

                envelope_df = _select_existing(valid_batch, ENVELOPE_COLUMNS)
                envelope_df.write.mode("append").parquet(str(envelope_tmp_dir))

                batch_profile["validation"] = batch_validation
                batch_profile["invalid_rows"] = batch_invalid_rows
                batch_profile["valid_rows"] = batch_count - batch_invalid_rows
            finally:
                if cached_batch is not None:
                    cached_batch.unpersist()

            batch_profile["batch_total_sec"] = round(time.perf_counter() - batch_t0, 3)
            batch_profile["batch_path"] = batch_path
            profile["batches"].append(batch_profile)
            log.info(
                "Batch noticeType=%s rows=%d wrote specific+envelope in %.2fs",
                notice_token,
                batch_count,
                time.perf_counter() - batch_t0,
            )

        envelope_day_df = spark.read.parquet(str(envelope_tmp_dir))
        envelope_day_df.write.mode("overwrite").parquet(str(envelope_day_dir))
        _safe_rmtree(envelope_tmp_dir, "envelope tmp dir")
        envelope_validation_df = spark.read.parquet(str(envelope_day_dir))
        validation_metrics = validate_common_envelope(envelope_validation_df, target_date=target_date)

        profile["total_input_rows"] = total_rows
        profile["total_quarantined_rows"] = total_invalid_rows
        profile["validation"] = {"common_envelope": validation_metrics}
        profile["run_total_sec"] = round(time.perf_counter() - run_start, 3)

        if cfg.profile_json:
            profile_path = Path(cfg.profile_json)
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info("Wrote profile JSON to %s", profile_path)

        repo_root = Path(__file__).resolve().parent.parent.parent
        code_paths = list(script_paths or [])
        core_paths = [
            Path(__file__).resolve(),
            repo_root / "src" / "procurement" / "silver" / "spark_transforms.py",
            repo_root / "src" / "procurement" / "silver" / "html_parser.py",
            repo_root / "src" / "procurement" / "silver" / "notice_types" / "definitions.py",
        ]
        for p in core_paths:
            if p not in code_paths:
                code_paths.append(p)

        if use_bronze:
            input_manifest = {
                "mode": "bronze",
                "bronze_root": str(bronze_root),
                "paths": [p for _, p in notice_batches if p is not None],
                "bronze_lineage": _load_bronze_lineage_ref(bronze_dir, target_date),
            }
        else:
            raw_path = Path(cfg.raw_dir) / f"bzp_{target_date}.json"
            input_manifest = {
                "mode": "raw",
                "raw_path": str(raw_path),
                "raw_sha256": sha256_file(raw_path) if raw_path.exists() else None,
            }

        lineage = {
            "layer": "silver",
            "mode": cfg.mode,
            "target_date": target_date,
            "started_at": started_at,
            "completed_at": now_utc_iso(),
            "run_context": run_context or {},
            "inputs": input_manifest,
            "outputs": {
                "common_envelope_partition": str(envelope_day_dir),
                "notice_type_tables_root": str(specific_root),
                "quarantine_partition": str(quarantine_day_dir),
            },
            "performance": profile,
            "code": {
                "git_commit": git_commit_sha(repo_root),
                "script_hashes": script_hashes(code_paths),
                "command": command or sys.argv,
                "args": args_dict or {},
            },
        }
        lineage_path = silver_dir / "_meta" / f"day={target_date}.json"
        atomic_write_json(lineage_path, lineage)
        log.info("Wrote silver lineage manifest to %s", lineage_path)

        return {
            "rows": total_rows,
            "quarantined_rows": total_invalid_rows,
            "input_paths": [p for _, p in notice_batches if p is not None],
            "validation_metrics": validation_metrics,
            "profile": profile,
            "lineage_path": str(lineage_path),
        }
    finally:
        if day_lock_dir.exists():
            if release_directory_lock_if_owner(day_lock_dir, run_id):
                log.info("Released day lock: %s", day_lock_dir)
