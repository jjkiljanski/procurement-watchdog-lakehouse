"""Backfill Silver for a date range in one long-lived Spark job.

Key properties:
- Reads Bronze partitions day-by-day, noticeType-by-noticeType.
- Keeps memory bounded (one batch in memory at a time).
- Uses a checkpoint state file for restart safety.
- Never trusts existing Silver files as completion proof.

State semantics:
- A day is considered done only if state[day].status == "completed".
- If interrupted mid-day, status remains "in_progress".
- On resume, any non-completed day is fully rebuilt and committed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging
from procurement.lineage import atomic_write_json, git_commit_sha, now_utc_iso, script_hashes
from procurement.common.locks import acquire_directory_lock, release_directory_lock_if_owner
from procurement.silver.notice_types import (
    html_extracted_fields_for_notice_type,
    normalized_notice_type_token,
    specific_columns_for_notice_type,
)
from procurement.silver.spark_transforms import build_silver_for_notice_type
from procurement.silver.spark_transforms import build_contract_notice_parts_table
from procurement.silver.validation import (
    summarize_notice_validation,
    validate_common_envelope,
    with_notice_validation_errors,
)

setup_logging()
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
            "Day batch noticeType=%s kept partitions=%d (rows=%d; tiny batch)",
            notice_type_token,
            current,
            row_count,
        )
        return df
    if target > current:
        log.info(
            "Day batch noticeType=%s repartition %d -> %d (rows=%d)",
            notice_type_token,
            current,
            target,
            row_count,
        )
        return df.repartition(target)
    log.debug(
        "Day batch noticeType=%s kept partitions=%d (target=%d, rows=%d)",
        notice_type_token,
        current,
        target,
        row_count,
    )
    return df


def _load_bronze_lineage_ref(bronze_dir: Path, day: str) -> dict | None:
    meta_path = bronze_dir / "_meta" / f"day={day}.json"
    if not meta_path.exists():
        return None
    try:
        payload = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return {"manifest_path": str(meta_path), "error": "failed_to_parse_bronze_manifest"}
    return {
        "manifest_path": str(meta_path),
        "code": payload.get("code"),
        "counts": payload.get("counts"),
    }


def _date_list(start_date: str, end_date: str) -> list[str]:
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if end < start:
        raise ValueError(f"end-date {end_date} is before start-date {start_date}")
    out: list[str] = []
    d = start
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _safe_rmtree(path: Path, label: str) -> None:
    if not path.exists():
        return
    try:
        shutil.rmtree(path, ignore_errors=False)
    except (PermissionError, OSError) as exc:
        # Overwrite writes are still attempted; this only skips eager cleanup.
        log.warning("Could not pre-delete %s at %s: %s", label, path, exc)


def _default_state_path(silver_dir: Path, start_date: str, end_date: str) -> Path:
    return silver_dir / "_state" / f"silver_backfill_{start_date}_{end_date}.json"


def _load_or_init_state(
    state_path: Path,
    days: list[str],
    start_date: str,
    end_date: str,
    reset_state: bool,
) -> dict:
    if reset_state and state_path.exists():
        state_path.unlink()
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state.get("start_date") != start_date or state.get("end_date") != end_date:
            raise ValueError(
                f"State file range mismatch: {state.get('start_date')}..{state.get('end_date')} "
                f"!= {start_date}..{end_date}"
            )
        existing_days = state.get("days", {})
        for day in days:
            existing_days.setdefault(day, {"status": "pending", "attempts": 0})
        state["days"] = existing_days
        return state
    state = {
        "version": 1,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "start_date": start_date,
        "end_date": end_date,
        "days": {day: {"status": "pending", "attempts": 0} for day in days},
    }
    atomic_write_json(state_path, state)
    return state


def _save_state(state_path: Path, state: dict) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    atomic_write_json(state_path, state)


def _acquire_day_lock(silver_dir: Path, day: str, run_id: str, stale_minutes: int) -> Path:
    owner = {
        "run_id": run_id,
        "pid": os.getpid(),
        "started_at_epoch": int(time.time()),
        "started_at_utc": now_utc_iso(),
        "target_date": day,
        "mode": "backfill",
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
            msg = msg.replace(
                "Directory lock already exists:",
                f"Silver day lock already exists for {day}:",
                1,
            )
        raise RuntimeError(msg) from exc


def _process_day(
    spark: "SparkSession",
    day: str,
    bronze_dir: Path,
    silver_dir: Path,
    shuffle_partitions: int,
    repartition: int,
    state: dict,
    state_path: Path,
    lock_run_id: str,
    lock_stale_minutes: int,
) -> tuple[int, int, list[dict], list[str], dict[str, int | float]]:
    from pyspark.sql.functions import col, lit, pmod, size, to_date, when, xxhash64
    from pyspark.storagelevel import StorageLevel

    bronze_root = bronze_dir / "notices"
    bronze_paths = sorted(bronze_root.glob(f"noticeType=*/publicationDateDay={day}"))
    if not bronze_paths:
        raise ValueError(f"No Bronze partitions found for day={day} under {bronze_root}")
    day_lock_dir = _acquire_day_lock(
        silver_dir=silver_dir,
        day=day,
        run_id=lock_run_id,
        stale_minutes=lock_stale_minutes,
    )
    log.info("Backfill acquired day lock: %s", day_lock_dir)
    try:
        day_state = state["days"][day]
        day_state["status"] = "in_progress"
        day_state["attempts"] = int(day_state.get("attempts", 0)) + 1
        day_state["started_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        day_state["current_notice_type"] = None
        day_state.pop("error", None)
        _save_state(state_path, state)

        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        quarantine_root = silver_dir / "_quarantine" / "notice_rows"
        quarantine_day_dir = quarantine_root / f"publicationDateDay={day}"
        _safe_rmtree(quarantine_day_dir, "quarantine day dir")
        notice_batches: list[tuple[str | None, str]] = []
        for p in bronze_paths:
            token = p.parent.name.replace("noticeType=", "")
            nt = None if token in ("__NULL__", "__HIVE_DEFAULT_PARTITION__") else token
            notice_batches.append((nt, str(p)))
        notice_batches.sort(key=lambda x: (x[0] is None, "" if x[0] is None else str(x[0])))

        log.info(
            "Day=%s processing noticeType batches: %s",
            day,
            [normalized_notice_type_token(nt) for nt, _ in notice_batches],
        )

        total_rows = 0
        total_invalid_rows = 0
        batch_profiles: list[dict] = []
        envelope_tmp = silver_dir / "_tmp" / "silver_backfill_envelope" / f"day={day}" / f"attempt={day_state['attempts']}"
        for notice_type, batch_path in notice_batches:
            batch_t0 = time.perf_counter()
            notice_token = normalized_notice_type_token(notice_type)
            day_state["current_notice_type"] = notice_token
            _save_state(state_path, state)

            batch_raw = spark.read.option("basePath", str(bronze_root)).parquet(batch_path)
            batch_count = batch_raw.count()
            total_rows += batch_count

            if shuffle_partitions > 0:
                per_batch_shuffle = shuffle_partitions
            else:
                per_batch_shuffle = (
                    repartition
                    if repartition > 0
                    else _adaptive_target_partitions(notice_type, batch_count, spark.sparkContext.defaultParallelism)
                )
            spark.conf.set("spark.sql.shuffle.partitions", str(per_batch_shuffle))
            batch_raw = _maybe_repartition_batch(
                batch_raw,
                notice_type,
                batch_count,
                repartition,
                spark,
                notice_token,
            )

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
                ).withColumn("publicationDateDay", to_date(col("publicationDate")).cast("string")).withColumn(
                    "caseId_shard",
                    when(col("caseId").isNotNull(), pmod(xxhash64(col("caseId")), lit(64)).cast("int")),
                )
                cached_batch = batch_silver.persist(StorageLevel.MEMORY_AND_DISK)
                batch_silver_rows = cached_batch.count()  # materialize once
                batch_silver, validation_rules = with_notice_validation_errors(
                    cached_batch,
                    target_date=day,
                    notice_type=notice_type,
                )
                batch_validation = summarize_notice_validation(
                    batch_silver,
                    target_date=day,
                    notice_type=notice_type,
                    rules=validation_rules,
                )
                invalid_batch = batch_silver.filter(size(col("__validation_errors")) > 0)
                valid_batch = batch_silver.filter(size(col("__validation_errors")) == 0).drop("__validation_errors")
                batch_invalid_rows = invalid_batch.count()
                batch_valid_rows = batch_silver_rows - batch_invalid_rows
                total_invalid_rows += batch_invalid_rows

                specific_df = _select_existing(valid_batch, ["caseId_shard", *specific_columns])
                specific_df = _compact_html_extracted(specific_df, html_fields)
                specific_day_dir = silver_dir / "notice_type_tables" / f"noticeType={notice_token}" / f"publicationDateDay={day}"
                _safe_rmtree(specific_day_dir, f"specific day dir noticeType={notice_token}")
                (
                    specific_df.write.mode("overwrite")
                    .parquet(str(specific_day_dir))
                )
                if notice_type == "ContractNotice":
                    parts_token = "ContractNotice_parts"
                    parts_df = build_contract_notice_parts_table(valid_batch)
                    parts_day_dir = silver_dir / "notice_type_tables" / f"noticeType={parts_token}" / f"publicationDateDay={day}"
                    _safe_rmtree(parts_day_dir, f"specific day dir noticeType={parts_token}")
                    (
                        parts_df.write.mode("overwrite")
                        .parquet(str(parts_day_dir))
                    )

                envelope_df = _select_existing(valid_batch, ENVELOPE_COLUMNS)
                envelope_df.write.mode("append").parquet(str(envelope_tmp))
                if batch_invalid_rows > 0:
                    # Quarantine schema can differ by notice type (e.g. nested contractors).
                    # Write per notice type to avoid cross-type schema merge failures.
                    quarantine_notice_dir = quarantine_root / f"publicationDateDay={day}" / f"noticeType={notice_token}"
                    _safe_rmtree(quarantine_notice_dir, f"quarantine notice dir noticeType={notice_token}")
                    (
                        invalid_batch.withColumn("validation_notice_type", lit(notice_token))
                        .write.mode("overwrite")
                        .parquet(str(quarantine_notice_dir))
                    )
            finally:
                if cached_batch is not None:
                    cached_batch.unpersist()

            log.info(
                "Day=%s noticeType=%s rows=%d wrote specific+envelope in %.2fs",
                day,
                notice_token,
                batch_count,
                time.perf_counter() - batch_start,
            )
            batch_profiles.append(
                {
                    "noticeType": notice_token,
                    "rows": batch_count,
                    "shuffle_partitions": per_batch_shuffle,
                    "validation": batch_validation,
                    "invalid_rows": batch_invalid_rows,
                    "valid_rows": batch_valid_rows,
                    "batch_total_sec": round(time.perf_counter() - batch_t0, 3),
                    "batch_path": batch_path,
                }
            )

        # Commit envelope for this day in one overwrite write.
        envelope_day_df = spark.read.parquet(str(envelope_tmp))
        envelope_day_dir = silver_dir / "common_envelope" / f"publicationDateDay={day}"
        _safe_rmtree(envelope_day_dir, "envelope day dir")
        (
            envelope_day_df.write.mode("overwrite")
            .parquet(str(envelope_day_dir))
        )
        # No day-level quarantine compaction: per-noticeType writes above are intentional
        # to avoid schema merge issues across heterogeneous invalid rows.
        envelope_validation_df = spark.read.parquet(
            str(silver_dir / "common_envelope" / f"publicationDateDay={day}")
        )
        validation_metrics = validate_common_envelope(envelope_validation_df, target_date=day)

        day_state["status"] = "completed"
        day_state["completed_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        day_state["current_notice_type"] = None
        day_state["rows"] = total_rows
        day_state["quarantined_rows"] = total_invalid_rows
        _save_state(state_path, state)
        return total_rows, total_invalid_rows, batch_profiles, [p for _, p in notice_batches], validation_metrics
    finally:
        if day_lock_dir.exists():
            if release_directory_lock_if_owner(day_lock_dir, lock_run_id):
                log.info("Backfill released day lock: %s", day_lock_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill Silver over date range in one Spark job.")
    parser.add_argument("--start-date", required=True, help="Start day YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="End day YYYY-MM-DD")
    parser.add_argument("--bronze-dir", default="data/bronze", help="Bronze root directory")
    parser.add_argument("--silver-dir", default="data/silver", help="Silver root directory")
    parser.add_argument("--state-path", default="", help="Checkpoint state JSON path")
    parser.add_argument("--reset-state", action="store_true", help="Delete existing state and restart range")
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue other days if one day fails (failed day remains resumable)",
    )
    parser.add_argument(
        "--max-days",
        type=int,
        default=0,
        help="Optional cap of days to process in one run (0 = all pending days)",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[6])",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=0,
        help="Override spark.sql.shuffle.partitions (0 = adaptive per batch)",
    )
    parser.add_argument(
        "--repartition",
        type=int,
        default=0,
        help="Force repartition count per batch (0 = adaptive)",
    )
    parser.add_argument(
        "--lock-stale-minutes",
        type=int,
        default=240,
        help="Treat an existing day lock as stale after this many minutes",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    days = _date_list(args.start_date, args.end_date)
    silver_dir = Path(args.silver_dir)
    bronze_dir = Path(args.bronze_dir)
    state_path = Path(args.state_path) if args.state_path else _default_state_path(
        silver_dir, args.start_date, args.end_date
    )

    state = _load_or_init_state(
        state_path=state_path,
        days=days,
        start_date=args.start_date,
        end_date=args.end_date,
        reset_state=args.reset_state,
    )
    pending_days = [d for d in days if state["days"][d].get("status") != "completed"]
    if args.max_days > 0:
        pending_days = pending_days[: args.max_days]
    if not pending_days:
        log.info("No pending days to process. Range already completed: %s..%s", args.start_date, args.end_date)
        return

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("bzp-silver-backfill")
        .master(args.spark_master)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
        .getOrCreate()
    )
    try:
        failed: list[str] = []
        run_started = now_utc_iso()
        repo_root = Path(__file__).resolve().parent.parent.parent
        common_script_hashes = script_hashes(
            [
                Path(__file__).resolve(),
                repo_root / "src" / "procurement" / "silver" / "spark_transforms.py",
                repo_root / "src" / "procurement" / "silver" / "html_parser.py",
                repo_root / "src" / "procurement" / "silver" / "notice_types" / "definitions.py",
            ]
        )
        for day in pending_days:
            log.info("Backfill day start: %s", day)
            try:
                day_run_id = f"backfill_{args.start_date}_{args.end_date}_{int(time.time() * 1000)}_{os.getpid()}"
                rows, quarantined_rows, batch_profiles, input_paths, validation_metrics = _process_day(
                    spark=spark,
                    day=day,
                    bronze_dir=bronze_dir,
                    silver_dir=silver_dir,
                    shuffle_partitions=args.shuffle_partitions,
                    repartition=args.repartition,
                    state=state,
                    state_path=state_path,
                    lock_run_id=day_run_id,
                    lock_stale_minutes=args.lock_stale_minutes,
                )
                log.info("Backfill day done: %s rows=%d", day, rows)
                day_manifest = {
                    "layer": "silver",
                    "mode": "backfill",
                    "target_date": day,
                    "run": {
                        "start_date": args.start_date,
                        "end_date": args.end_date,
                        "state_path": str(state_path),
                        "run_started_at": run_started,
                    },
                    "started_at": state["days"][day].get("started_at"),
                    "completed_at": state["days"][day].get("completed_at"),
                    "inputs": {
                        "bronze_root": str(bronze_dir / "notices"),
                        "paths": input_paths,
                        "bronze_lineage": _load_bronze_lineage_ref(bronze_dir, day),
                    },
                    "outputs": {
                        "common_envelope_partition": str(silver_dir / "common_envelope" / f"publicationDateDay={day}"),
                        "notice_type_tables_root": str(silver_dir / "notice_type_tables"),
                        "quarantine_partition": str(silver_dir / "_quarantine" / "notice_rows" / f"publicationDateDay={day}"),
                    },
                    "performance": {
                        "rows": rows,
                        "quarantined_rows": quarantined_rows,
                        "batches": batch_profiles,
                        "validation": {"common_envelope": validation_metrics},
                    },
                    "code": {
                        "git_commit": git_commit_sha(repo_root),
                        "script_hashes": common_script_hashes,
                        "command": sys.argv,
                        "args": vars(args),
                    },
                }
                atomic_write_json(silver_dir / "_meta" / f"day={day}.json", day_manifest)
            except Exception as exc:
                day_state = state["days"][day]
                day_state["status"] = "failed"
                day_state["error"] = str(exc)
                day_state["current_notice_type"] = day_state.get("current_notice_type")
                _save_state(state_path, state)
                failed.append(day)
                log.error("Backfill day failed: %s error=%s", day, exc, exc_info=True)
                if not args.continue_on_error:
                    raise

        if failed:
            raise RuntimeError(f"Silver backfill failed days: {failed}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
