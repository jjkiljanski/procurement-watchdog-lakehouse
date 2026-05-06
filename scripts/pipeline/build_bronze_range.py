"""Build Bronze Parquet for a date range from Bronze-Raw JSON.

Processes each date sequentially within a **single Spark session**, paying the
Dataproc Serverless cold-start cost once for the whole range instead of once
per date.

Cross-day deduplication works correctly because each date's records are written
before the next date's dedup query runs — the growing Bronze Parquet is always
up-to-date when each ``_deduplicate_via_spark()`` call reads it.

Per-date manifests are written after each successful date so interrupted runs
can resume from where they left off.  Use ``--force`` to reprocess dates
regardless of manifest state.

Usage
-----
    python build_bronze_range.py --start-date 2025-01-01 --end-date 2025-12-31
    python build_bronze_range.py --start-date 2025-01-01 --end-date 2025-12-31 --force
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent.parent
_src = str(_repo / "src")
_SRC_PKG = Path(_src) / "procurement"
_scripts_pipeline = str(_repo / "scripts" / "pipeline")
sys.path.insert(0, _src)
sys.path.insert(0, _scripts_pipeline)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

# build_bronze helpers (same directory — import as a module via sys.path)
import build_bronze as _bb  # noqa: E402

from procurement.logging import get_stage_logger, setup_logging
from procurement.manifests import is_already_processed, write_processed_manifest
from procurement.obs import now_utc_iso, sha256_paths, write_dq_metrics, write_pipeline_run
from procurement.runtime import get_runtime

setup_logging()
log = get_stage_logger(__name__, "bronze")


def _collect_pre_range_object_ids(spark, bronze_notices_uri: str, start_date: str) -> set[str]:
    """Return all objectIds in bronze for dates strictly before *start_date*.

    Called once before the date loop so the expensive full-table scan is paid
    only once for the whole range run.  Uses partition pruning on
    ``publicationDateDay`` to limit the scan.  Returns an empty set when no
    bronze data exists yet.
    """
    from pyspark.sql import functions as F

    try:
        return {
            row.objectId
            for row in (
                spark.read.parquet(bronze_notices_uri)
                .filter(F.col("publicationDateDay") < start_date)
                .select("objectId")
                .distinct()
                .toLocalIterator()
            )
        }
    except Exception:
        return set()


def _collect_object_ids_for_date(spark, bronze_notices_uri: str, target_date: str) -> set[str]:
    """Return all objectIds in bronze for exactly *target_date*.

    Used when a date is skipped (manifest already matches) so that its
    objectIds are still added to the running seen-set before the next date is
    processed.  Partition pruning limits the scan to only the target date's
    partitions.  Returns an empty set when no data exists for that date.
    """
    from pyspark.sql import functions as F

    try:
        return {
            row.objectId
            for row in (
                spark.read.parquet(bronze_notices_uri)
                .filter(F.col("publicationDateDay") == target_date)
                .select("objectId")
                .distinct()
                .toLocalIterator()
            )
        }
    except Exception:
        return set()


def _date_range(start: str, end: str) -> list[str]:
    """Return sorted list of ISO date strings from *start* to *end* inclusive."""
    s = date.fromisoformat(start)
    e = date.fromisoformat(end)
    if e < s:
        raise ValueError(f"end-date {end} is before start-date {start}")
    result: list[str] = []
    d = s
    while d <= e:
        result.append(d.isoformat())
        d += timedelta(days=1)
    return result


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Bronze Parquet for a date range in one Spark session."
    )
    parser.add_argument("--start-date", required=True, help="First date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="Last date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--bronze-raw-dir",
        default=None,
        help="Directory with raw JSON payloads.  Defaults to the runtime-resolved 'bronze_raw' path.",
    )
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help="Bronze output directory.  Defaults to the runtime-resolved 'bronze' path.",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*]).  Defaults to SPARK_MASTER env var.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess dates even when a matching manifest exists.",
    )
    return parser.parse_args()


def _process_date(
    spark,
    target_date: str,
    bronze_raw_dir: str,
    bronze_dir: str,
    obs_dir,
    script_hash: str,
    dependency_hashes: dict[str, str],
    storage,
    seen_ids: set[str],
) -> tuple[bool, set[str]]:
    """Process one date within the shared Spark session.

    Returns ``(wrote_notices, new_ids)`` where *new_ids* is the set of
    objectIds written to bronze.  The caller should union *new_ids* into the
    running seen-set before processing the next date.

    Raises on unrecoverable errors so the caller can decide whether to abort.
    """
    from pyspark.sql.functions import col, to_date

    from procurement.bronze.models import notice_record_hash

    input_files = _bb._candidate_input_files(bronze_raw_dir, target_date)
    if not input_files:
        log.warning("No Bronze-Raw input files found for %s — skipping", target_date)
        return False, set()

    raw_records = _bb._load_raw_records(input_files)
    log.info("Date %s: loaded %d raw records", target_date, len(raw_records))

    started_at = now_utc_iso()
    bronze_notices_uri = f"{bronze_dir.rstrip('/')}/notices"

    deduped_records, dedup_stats = _bb._deduplicate_via_spark(
        spark, raw_records, target_date, bronze_notices_uri, prebuilt_seen_ids=seen_ids
    )

    valid, errors = _bb.validate_raw(deduped_records)
    valid_rows = [
        {**model.model_dump(), "recordHash": notice_record_hash(model)}
        for model in valid
    ]

    wrote_notices = False
    new_ids: set[str] = set()
    if valid_rows:
        df = spark.createDataFrame(valid_rows, schema=_bb.BRONZE_SPARK_SCHEMA).withColumn(
            "publicationDateDay", to_date(col("publicationDate")).cast("string")
        )
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("overwrite").partitionBy("noticeType", "publicationDateDay").parquet(
            bronze_notices_uri
        )
        wrote_notices = True
        new_ids = {row["objectId"] for row in valid_rows if row.get("objectId")}
        log.info(
            "Date %s: wrote %d rows to Bronze Parquet", target_date, len(valid_rows),
            extra={"date": target_date, "status": "ok"},
        )
    else:
        log.warning("Date %s: no valid records after dedup/validation", target_date)

    if errors:
        _bb._write_errors(bronze_dir, target_date, errors)

    run_id = f"bronze_range_{target_date}_{int(time.time() * 1000)}"
    write_pipeline_run(
        layer="bronze",
        target_date=target_date,
        run_id=run_id,
        started_at=started_at,
        completed_at=now_utc_iso(),
        status="ok" if wrote_notices else "empty",
        counts={
            "raw_total": len(raw_records),
            "after_dedup_total": len(deduped_records),
            "valid_total": len(valid),
            "invalid_total": len(errors),
            "dropped_duplicates_in_input": dedup_stats["dropped_duplicates_in_input"],
            "dropped_duplicates_seen_other_day": dedup_stats["dropped_duplicates_seen_index_other_day"],
        },
        git_commit=None,
        script_hash=script_hash,
        obs_dir=obs_dir,
    )
    if deduped_records:
        write_dq_metrics(
            layer="bronze",
            target_date=target_date,
            notice_type=None,
            metrics={
                "raw_total": len(raw_records),
                "after_dedup_count": len(deduped_records),
                "valid_count": len(valid),
                "invalid_count": len(errors),
                "valid_rate": len(valid) / len(deduped_records),
                "dedup_cross_day_rate": (
                    dedup_stats["dropped_duplicates_seen_index_other_day"] / len(raw_records)
                    if raw_records else 0.0
                ),
            },
            run_id=run_id,
            obs_dir=obs_dir,
        )

    write_processed_manifest(
        layer="bronze",
        target_date=target_date,
        script_hash=script_hash,
        storage=storage,
        dependency_hashes=dependency_hashes,
    )
    return wrote_notices, new_ids


def main() -> None:
    args = _parse_args()
    dates = _date_range(args.start_date, args.end_date)

    rt = get_runtime()
    bronze_raw_dir = args.bronze_raw_dir or rt.storage.resolve("bronze_raw")
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    log.info(
        "Bronze range: %s..%s (%d dates, force=%s) runtime=%s bronze_raw_dir=%s bronze_dir=%s",
        args.start_date, args.end_date, len(dates), args.force, rt.env, bronze_raw_dir, bronze_dir,
        extra={"runtime": rt.env},
    )
    obs_dir = rt.storage.obs_path()
    script_hash = sha256_paths(Path(__file__), _SRC_PKG / "bronze")
    dependency_hashes = {
        "fetch": sha256_paths(Path(__file__).with_name("fetch_bzp_range.py"), _SRC_PKG / "fetch")
    }

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    bronze_notices_uri = f"{bronze_dir.rstrip('/')}/notices"

    spark = rt.spark.get_session("bzp-bronze-range", **extra)
    try:
        # One scan to pre-load all objectIds from dates before this range.
        # Subsequent per-date processing uses a running in-memory set instead
        # of issuing a full-table scan on every date iteration.
        log.info(
            "Pre-computing bronze objectIds before %s (one-time scan)...", dates[0],
            extra={"status": "started"},
        )
        seen_ids: set[str] = _collect_pre_range_object_ids(spark, bronze_notices_uri, dates[0])
        log.info("Pre-range seen objectIds: %d", len(seen_ids))

        processed = 0
        skipped = 0
        failed_dates: list[str] = []
        for target_date in dates:
            if not args.force and is_already_processed(
                "bronze",
                target_date,
                script_hash,
                rt.storage,
                dependency_hashes=dependency_hashes,
            ):
                log.info(
                    "Skipping bronze for %s — manifest and dependency hashes match", target_date,
                    extra={"date": target_date, "status": "skipped"},
                )
                # Collect this date's IDs so subsequent dates can still dedup
                # against them without hitting the full bronze table.
                skipped_ids = _collect_object_ids_for_date(spark, bronze_notices_uri, target_date)
                seen_ids |= skipped_ids
                skipped += 1
                continue

            try:
                _, new_ids = _process_date(
                    spark=spark,
                    target_date=target_date,
                    bronze_raw_dir=bronze_raw_dir,
                    bronze_dir=bronze_dir,
                    obs_dir=obs_dir,
                    script_hash=script_hash,
                    dependency_hashes=dependency_hashes,
                    storage=rt.storage,
                    seen_ids=seen_ids,
                )
                seen_ids |= new_ids
                processed += 1
            except Exception as exc:
                log.error("Bronze failed for %s: %s", target_date, exc, exc_info=True)
                failed_dates.append(target_date)
                raise

        log.info(
            "Bronze range done: processed=%d skipped=%d failed=%d",
            processed, skipped, len(failed_dates),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
