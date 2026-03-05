"""Build silver layer from raw BZP JSON using PySpark.

Reads:
  Preferred: <bronze-dir>/notices/noticeType=*/publicationDateDay=YYYY-MM-DD/
  Fallback:  <raw-dir>/bzp_YYYY-MM-DD.json
Writes:
  <silver-dir>/common_envelope/publicationDateDay=YYYY-MM-DD/
  <silver-dir>/notice_type_tables/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
import uuid
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)
# Also propagate to Spark worker processes via PYTHONPATH
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging
from procurement.lineage import atomic_write_json, git_commit_sha, now_utc_iso, script_hashes, sha256_file
from procurement.common.locks import acquire_directory_lock, release_directory_lock_if_owner

setup_logging()
log = logging.getLogger(__name__)

from procurement.silver.notice_schemas import (
    html_extracted_fields_for_notice_type,
    normalized_notice_type_token,
    specific_columns_for_notice_type,
)
from procurement.silver.legacy.validation import (
    summarize_notice_validation,
    validate_common_envelope,
    with_notice_validation_errors,
)


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

def _select_existing(df: "DataFrame", columns: list[str]) -> "DataFrame":
    return df.select(*[c for c in columns if c in df.columns])


def _compact_html_extracted(
    df: "DataFrame",
    html_fields: list[str],
) -> "DataFrame":
    if "htmlExtracted" not in df.columns:
        return df
    if not html_fields:
        return df.drop("htmlExtracted")
    from pyspark.sql.functions import col, struct

    compact_cols = [col(f"htmlExtracted.{field_name}").alias(field_name) for field_name in html_fields]
    return df.withColumn("htmlExtracted", struct(*compact_cols))


def _auto_target_partitions(raw_count: int, default_parallelism: int) -> int:
    max_parallel = max(2, default_parallelism * 2)
    size_based_target = max(2, raw_count // 2000)
    return min(max_parallel, size_based_target)


HEAVY_HTML_NOTICE_TYPES = {
    "ContractNotice",
    "TenderResultNotice",
    "ContractPerformingNotice",
}


def _adaptive_target_partitions(
    notice_type: str | None,
    row_count: int,
    default_parallelism: int,
) -> int:
    """Choose partitions for one notice-type batch.

    Goal:
    - keep small/light batches cheap,
    - fan out heavy HTML parsing notice types to use available cores.
    """
    max_parallel = max(2, default_parallelism * 2)
    notice = notice_type or ""
    if notice in HEAVY_HTML_NOTICE_TYPES:
        # For heavy parsers, target at least core-level parallelism when batch is non-trivial.
        if row_count >= 200:
            target = max(default_parallelism, (row_count + 149) // 150)
            return min(max_parallel, max(2, target))
    # General/default rule for light batches.
    return _auto_target_partitions(row_count, default_parallelism)


def _maybe_repartition_batch(
    df: "DataFrame",
    notice_type: str | None,
    row_count: int,
    args: argparse.Namespace,
    spark: "SparkSession",
    notice_type_token: str,
) -> "DataFrame":
    current_parts = df.rdd.getNumPartitions()
    is_heavy = (notice_type or "") in HEAVY_HTML_NOTICE_TYPES
    target_parts = (
        args.repartition
        if args.repartition > 0
        else _adaptive_target_partitions(
            notice_type=notice_type,
            row_count=row_count,
            default_parallelism=spark.sparkContext.defaultParallelism,
        )
    )
    # Keep tiny non-heavy batches as-is to avoid shuffle overhead.
    if row_count < 2000 and not is_heavy:
        log.debug(
            "Batch noticeType=%s kept partitions=%d (rows=%d; tiny batch)",
            notice_type_token,
            current_parts,
            row_count,
        )
        return df
    if target_parts > current_parts:
        log.info(
            "Batch noticeType=%s repartition %d -> %d (rows=%d)",
            notice_type_token,
            current_parts,
            target_parts,
            row_count,
        )
        return df.repartition(target_parts)
    log.debug(
        "Batch noticeType=%s kept partitions=%d (target=%d, rows=%d)",
        notice_type_token,
        current_parts,
        target_parts,
        row_count,
    )
    return df


def _load_bronze_lineage_ref(bronze_dir: Path, target_date: str) -> dict | None:
    meta_path = bronze_dir / "_meta" / f"day={target_date}.json"
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build silver parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Bronze root directory (expects notices/noticeType=*/publicationDateDay=YYYY-MM-DD)",
    )
    parser.add_argument(
        "--input-layer",
        choices=["auto", "bronze", "raw"],
        default="auto",
        help="Input source selection for silver build",
    )
    parser.add_argument("--raw-dir", default="data/raw", help="Directory with raw daily JSON files")
    parser.add_argument(
        "--silver-dir",
        default="data/silver",
        help="Output directory for silver parquet files",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[2])",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = Spark default)",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Repartition raw DataFrame before heavy HTML parsing (0 = auto by defaultParallelism*2)",
    )
    parser.add_argument(
        "--profile-json",
        default="",
        help="Optional path to write step-level performance profile as JSON",
    )
    parser.add_argument(
        "--lock-stale-minutes",
        type=int,
        default=360,
        help="Treat an existing day lock as stale after this many minutes",
    )
    return parser.parse_args()


def _acquire_day_lock(silver_dir: Path, target_date: str, run_id: str, stale_minutes: int) -> Path:
    owner = {
        "run_id": run_id,
        "pid": os.getpid(),
        "started_at_epoch": int(time.time()),
        "started_at_utc": now_utc_iso(),
        "target_date": target_date,
    }
    lock_dir = silver_dir / "_locks" / f"silver_day={target_date}"
    try:
        return acquire_directory_lock(
            lock_dir=lock_dir,
            owner_payload=owner,
            stale_seconds=max(1, stale_minutes) * 60,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if msg.startswith("Directory lock already exists:"):
            msg = msg.replace(
                "Directory lock already exists:",
                f"Silver day lock already exists for {target_date}:",
                1,
            )
        raise RuntimeError(msg) from exc


def main() -> None:
    args = _parse_args()
    started_at = now_utc_iso()
    if args.target_date:
        target_date = args.target_date
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    from pyspark.sql import SparkSession
    from pyspark.storagelevel import StorageLevel
    from procurement.silver.legacy.spark_transforms import (
        build_contract_notice_parts_table,
        build_silver_for_notice_type,
    )

    spark = (
        SparkSession.builder.appName("bzp-silver")
        .master(args.spark_master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
        .getOrCreate()
    )
    if args.shuffle_partitions > 0:
        spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        log.info("Set spark.sql.shuffle.partitions=%d", args.shuffle_partitions)

    day_lock_dir: Path | None = None
    run_id: str | None = None
    try:
        from pyspark.sql.functions import col, lit, pmod, size, to_date, when, xxhash64

        bronze_root = Path(args.bronze_dir) / "notices"
        bronze_paths = sorted(
            bronze_root.glob(f"noticeType=*/publicationDateDay={target_date}")
        )
        use_bronze = args.input_layer in ("auto", "bronze") and len(bronze_paths) > 0
        if args.input_layer == "bronze" and not bronze_paths:
            log.error("Bronze partitions for %s not found under %s", target_date, bronze_root)
            sys.exit(1)

        if not use_bronze:
            raw_path = Path(args.raw_dir) / f"bzp_{target_date}.json"
            if not raw_path.exists():
                log.error("Raw file not found: %s", raw_path)
                sys.exit(1)
            log.warning("Bronze input not available; falling back to raw JSON: %s", raw_path)
            df_raw = spark.read.json(str(raw_path), multiLine=True)

        # Step 1: process Bronze in noticeType-sorted batches.
        if use_bronze:
            # Process each physical Bronze partition path directly.
            notice_batches: list[tuple[str | None, str]] = []
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
            # Fallback mode: one raw JSON input, then type-filter.
            notice_types = [row.noticeType for row in df_raw.select("noticeType").distinct().collect()]
            notice_types_sorted = sorted(
                notice_types,
                key=lambda x: (x is None, "" if x is None else str(x)),
            )
            notice_batches = [(nt, None) for nt in notice_types_sorted]
            log.info("Processing raw noticeType batches in order: %s", notice_types_sorted)

        run_start = time.perf_counter()
        profile: dict = {"target_date": target_date, "input_layer": "bronze" if use_bronze else "raw", "batches": []}
        run_id = f"{target_date}_{int(time.time() * 1000)}_{os.getpid()}_{uuid.uuid4().hex[:8]}"

        silver_dir = Path(args.silver_dir)
        day_lock_dir = _acquire_day_lock(
            silver_dir=silver_dir,
            target_date=target_date,
            run_id=run_id,
            stale_minutes=args.lock_stale_minutes,
        )
        log.info("Acquired day lock: %s", day_lock_dir)
        specific_root = silver_dir / "notice_type_tables"
        envelope_day_dir = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
        envelope_tmp_dir = (
            silver_dir
            / "_tmp"
            / "silver_envelope_buffer"
            / f"day={target_date}"
            / f"run={run_id}"
        )
        quarantine_root = str(silver_dir / "_quarantine" / "notice_rows")
        quarantine_day_dir = silver_dir / "_quarantine" / "notice_rows" / f"publicationDateDay={target_date}"

        # Overwrite only touched publicationDateDay partitions.
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # For per-batch envelope appends, clear the target day first to keep reruns idempotent.
        if envelope_day_dir.exists():
            shutil.rmtree(envelope_day_dir, ignore_errors=False)
            log.info("Cleared existing envelope day partition: %s", envelope_day_dir)
        if quarantine_day_dir.exists():
            shutil.rmtree(quarantine_day_dir, ignore_errors=False)
            log.info("Cleared existing quarantine day partition: %s", quarantine_day_dir)

        total_input_rows = 0
        total_invalid_rows = 0
        for notice_type, batch_path in notice_batches:
            batch_profile: dict = {"noticeType": normalized_notice_type_token(notice_type)}
            t0 = time.perf_counter()
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
            batch_profile["read_sec"] = round(time.perf_counter() - t0, 3)
            notice_type_token = normalized_notice_type_token(notice_type)
            t1 = time.perf_counter()
            batch_count = batch_raw.count()
            batch_profile["count_sec"] = round(time.perf_counter() - t1, 3)
            total_input_rows += batch_count
            batch_profile["rows"] = batch_count
            if args.shuffle_partitions <= 0:
                per_batch_shuffle = (
                    args.repartition
                    if args.repartition > 0
                    else _adaptive_target_partitions(
                        notice_type=notice_type,
                        row_count=batch_count,
                        default_parallelism=spark.sparkContext.defaultParallelism,
                    )
                )
                spark.conf.set("spark.sql.shuffle.partitions", str(per_batch_shuffle))
                batch_profile["shuffle_partitions"] = per_batch_shuffle
            t2 = time.perf_counter()
            batch_raw = _maybe_repartition_batch(
                batch_raw,
                notice_type,
                batch_count,
                args,
                spark,
                notice_type_token,
            )
            batch_profile["repartition_sec"] = round(time.perf_counter() - t2, 3)
            specific_columns = specific_columns_for_notice_type(notice_type)
            html_fields = html_extracted_fields_for_notice_type(notice_type)
            required_columns = set(ENVELOPE_COLUMNS) | set(specific_columns)
            batch_start = time.perf_counter()
            cached_batch = None
            try:
                batch_silver = build_silver_for_notice_type(
                    batch_raw,
                    notice_type=notice_type,
                    required_columns=required_columns,
                ).withColumn(
                    "publicationDateDay",
                    to_date(col("publicationDate")).cast("string"),
                ).withColumn(
                    "caseId_shard",
                    when(col("caseId").isNotNull(), pmod(xxhash64(col("caseId")), lit(64)).cast("int")),
                )

                batch_silver, validation_rules = with_notice_validation_errors(
                    batch_silver,
                    target_date=target_date,
                    notice_type=notice_type,
                )
                # Cache validated frame before downstream actions
                # (summary + quarantine write + two output writes).
                cached_batch = batch_silver.persist(StorageLevel.MEMORY_AND_DISK)
                # First aggregate both materializes transformations and returns
                # row-level validation totals in a single pass.
                t3 = time.perf_counter()
                batch_profile["validation"] = summarize_notice_validation(
                    cached_batch,
                    target_date=target_date,
                    notice_type=notice_type,
                    rules=validation_rules,
                )
                batch_profile["transform_materialize_sec"] = round(time.perf_counter() - t3, 3)
                batch_silver_rows = int(batch_profile["validation"].get("total_rows", 0))
                batch_profile["transformed_rows"] = batch_silver_rows
                invalid_batch = cached_batch.filter(size(col("__validation_errors")) > 0)
                valid_batch = cached_batch.filter(size(col("__validation_errors")) == 0).drop("__validation_errors")
                batch_invalid_rows = int(batch_profile["validation"].get("invalid_rows", 0))
                batch_valid_rows = batch_silver_rows - batch_invalid_rows
                total_invalid_rows += batch_invalid_rows
                batch_profile["invalid_rows"] = batch_invalid_rows
                batch_profile["valid_rows"] = batch_valid_rows
                if batch_invalid_rows > 0:
                    (
                        invalid_batch.withColumn(
                            "validation_notice_type",
                            lit(notice_type_token),
                        )
                        .write.mode("append")
                        .partitionBy("publicationDateDay")
                        .parquet(quarantine_root)
                    )

                if notice_type == "ContractNotice":
                    # ContractNotice uses section-driven parsing model; keep full output.
                    specific_df = valid_batch
                else:
                    # Other notice types still use selective schema projection.
                    specific_df = _select_existing(valid_batch, ["caseId_shard", *specific_columns])
                    specific_df = _compact_html_extracted(specific_df, html_fields)
                specific_out = str(specific_root / f"noticeType={notice_type_token}")
                specific_day_dir = specific_root / f"noticeType={notice_type_token}" / f"publicationDateDay={target_date}"
                if specific_day_dir.exists():
                    shutil.rmtree(specific_day_dir, ignore_errors=False)
                    log.info("Cleared existing specific day partition: %s", specific_day_dir)
                t4 = time.perf_counter()
                (
                    specific_df.write.mode("overwrite")
                    .partitionBy("publicationDateDay")
                    .parquet(specific_out)
                )
                batch_profile["write_specific_sec"] = round(time.perf_counter() - t4, 3)

                if notice_type == "ContractNotice":
                    contract_parts_df = build_contract_notice_parts_table(valid_batch)
                    parts_token = "ContractNotice_parts"
                    parts_out = str(specific_root / f"noticeType={parts_token}")
                    parts_day_dir = specific_root / f"noticeType={parts_token}" / f"publicationDateDay={target_date}"
                    if parts_day_dir.exists():
                        shutil.rmtree(parts_day_dir, ignore_errors=False)
                        log.info("Cleared existing specific day partition: %s", parts_day_dir)
                    t4b = time.perf_counter()
                    (
                        contract_parts_df.write.mode("overwrite")
                        .partitionBy("publicationDateDay")
                        .parquet(parts_out)
                    )
                    batch_profile["write_specific_parts_sec"] = round(time.perf_counter() - t4b, 3)

                envelope_batch = _select_existing(valid_batch, ENVELOPE_COLUMNS)
                t5 = time.perf_counter()
                envelope_batch.write.mode("append").parquet(str(envelope_tmp_dir))
                batch_profile["buffer_envelope_sec"] = round(time.perf_counter() - t5, 3)
            finally:
                if cached_batch is not None:
                    cached_batch.unpersist()

            batch_profile["batch_total_sec"] = round(time.perf_counter() - batch_start, 3)
            profile["batches"].append(batch_profile)
            log.info(
                "Batch noticeType=%s rows=%d wrote specific+buffered envelope (%.2fs)",
                notice_type_token,
                batch_count,
                time.perf_counter() - batch_start,
            )

        if total_input_rows == 0:
            log.warning("No Silver rows produced for %s", target_date)
            return

        t_env = time.perf_counter()
        if envelope_tmp_dir.exists():
            spark.read.parquet(str(envelope_tmp_dir)).write.mode("overwrite").parquet(str(envelope_day_dir))
            shutil.rmtree(envelope_tmp_dir, ignore_errors=False)
        profile["write_envelope_once_sec"] = round(time.perf_counter() - t_env, 3)

        log.info("Completed Silver build total_input_rows=%d", total_input_rows)
        envelope_validation_df = spark.read.parquet(str(envelope_day_dir))
        envelope_validation_metrics = validate_common_envelope(
            envelope_validation_df,
            target_date=target_date,
        )
        log.info("Silver validation metrics day=%s: %s", target_date, envelope_validation_metrics)
        profile["total_input_rows"] = total_input_rows
        profile["total_quarantined_rows"] = total_invalid_rows
        profile["validation"] = {"common_envelope": envelope_validation_metrics}
        profile["run_total_sec"] = round(time.perf_counter() - run_start, 3)
        if args.profile_json:
            profile_path = Path(args.profile_json)
            profile_path.parent.mkdir(parents=True, exist_ok=True)
            profile_path.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")
            log.info("Wrote profile JSON to %s", profile_path)
        else:
            log.info("Profile summary: %s", profile)

        repo_root = Path(__file__).resolve().parent.parent.parent
        input_manifest: dict
        if use_bronze:
            input_manifest = {
                "mode": "bronze",
                "bronze_root": str(bronze_root),
                "paths": [str(p) for p in bronze_paths],
                "bronze_lineage": _load_bronze_lineage_ref(Path(args.bronze_dir), target_date),
            }
        else:
            raw_path = Path(args.raw_dir) / f"bzp_{target_date}.json"
            input_manifest = {
                "mode": "raw",
                "raw_path": str(raw_path),
                "raw_sha256": sha256_file(raw_path) if raw_path.exists() else None,
            }

        lineage = {
            "layer": "silver",
            "target_date": target_date,
            "started_at": started_at,
            "completed_at": now_utc_iso(),
            "inputs": input_manifest,
            "outputs": {
                "common_envelope": str(silver_dir / "common_envelope" / f"publicationDateDay={target_date}"),
                "notice_type_tables_root": str(silver_dir / "notice_type_tables"),
                "quarantine_partition": str(quarantine_day_dir),
                "envelope_tmp_run": str(envelope_tmp_dir),
                "profile_json": str(Path(args.profile_json)) if args.profile_json else None,
            },
            "performance": profile,
            "code": {
                "git_commit": git_commit_sha(repo_root),
                "script_hashes": script_hashes(
                    [
                        Path(__file__).resolve(),
                        repo_root / "src" / "procurement" / "silver" / "spark_transforms.py",
                        repo_root / "src" / "procurement" / "silver" / "html_parser.py",
                        repo_root / "src" / "procurement" / "silver" / "notice_types" / "definitions.py",
                    ]
                ),
                "command": sys.argv,
                "args": vars(args),
            },
        }
        lineage_path = silver_dir / "_meta" / f"day={target_date}.json"
        atomic_write_json(lineage_path, lineage)
        log.info("Wrote silver lineage manifest to %s", lineage_path)
    finally:
        if day_lock_dir is not None and day_lock_dir.exists():
            if release_directory_lock_if_owner(day_lock_dir, run_id):
                log.info("Released day lock: %s", day_lock_dir)
        spark.stop()


if __name__ == "__main__":
    main()
