"""Build Bronze Parquet for a date range from Bronze-Raw JSON.

The range builder keeps the existing per-date manifest semantics, but batches
Parquet writes by date chunk. This avoids thousands of tiny Spark write jobs
during long backfills while still allowing reruns to skip individual dates.

Cross-day deduplication is handled with a running in-memory objectId set:

* objectIds from Bronze partitions before the range are loaded once,
* skipped dates add their already-written objectIds to the set,
* processed dates add their newly valid objectIds before the next date.

Usage
-----
    python build_bronze_range.py --start-date 2025-01-01 --end-date 2025-12-31
    python build_bronze_range.py --start-date 2025-01-01 --end-date 2025-12-31 --chunk-days 31
    python build_bronze_range.py --start-date 2025-01-01 --end-date 2025-12-31 --force
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

_repo = Path(__file__).resolve().parent.parent.parent
_src = str(_repo / "src")
_SRC_PKG = Path(_src) / "procurement"
_scripts_pipeline = str(_repo / "scripts" / "pipeline")
sys.path.insert(0, _src)
sys.path.insert(0, _scripts_pipeline)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

import build_bronze as _bb  # noqa: E402

from procurement.logging import get_stage_logger, setup_logging
from procurement.manifests import is_already_processed, write_processed_manifest
from procurement.obs import now_utc_iso, sha256_paths, write_dq_metrics, write_pipeline_run
from procurement.runtime import get_runtime

setup_logging()
log = get_stage_logger(__name__, "bronze")


@dataclass
class _PreparedDate:
    target_date: str
    started_at: str
    raw_total: int
    after_dedup_total: int
    valid_total: int
    invalid_total: int
    dedup_stats: dict
    errors: list[dict]
    valid_rows: list[dict]
    new_ids: set[str]


def _collect_pre_range_object_ids(spark, bronze_notices_uri: str, start_date: str) -> set[str]:
    """Return all objectIds in bronze for dates strictly before *start_date*."""
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
    """Return all objectIds in bronze for exactly *target_date*."""
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


def _chunks(items: list[str], size: int) -> list[list[str]]:
    if size < 1:
        raise ValueError("chunk size must be at least 1")
    return [items[i : i + size] for i in range(0, len(items), size)]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Bronze Parquet for a date range in one Spark session."
    )
    parser.add_argument("--start-date", required=True, help="First date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end-date", required=True, help="Last date YYYY-MM-DD (inclusive)")
    parser.add_argument(
        "--bronze-raw-dir",
        default=None,
        help="Directory with raw JSON payloads. Defaults to the runtime-resolved 'bronze_raw' path.",
    )
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help="Bronze output directory. Defaults to the runtime-resolved 'bronze' path.",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER"),
        help="Spark master string (e.g. local[*]). Defaults to SPARK_MASTER env var.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess dates even when a matching manifest exists.",
    )
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=int(os.environ.get("BRONZE_CHUNK_DAYS", "31")),
        help=(
            "Maximum number of dates to accumulate before one Bronze Parquet write. "
            "Defaults to 31 or BRONZE_CHUNK_DAYS."
        ),
    )
    return parser.parse_args()


def _prepare_date(
    target_date: str,
    bronze_raw_dir: str,
    seen_ids: set[str],
) -> _PreparedDate | None:
    """Load, deduplicate, and validate one date without writing Parquet."""
    from procurement.bronze.models import notice_record_hash

    input_files = _bb._candidate_input_files(bronze_raw_dir, target_date)
    if not input_files:
        log.warning("No Bronze-Raw input files found for %s - skipping", target_date)
        return None

    raw_records = _bb._load_raw_records(input_files)
    log.info("Date %s: loaded %d raw records", target_date, len(raw_records))

    started_at = now_utc_iso()
    deduped_records, dedup_stats = _bb.apply_dedup_filter(raw_records, seen_ids)

    valid, errors = _bb.validate_raw(deduped_records)
    valid_rows = [
        {**model.model_dump(), "recordHash": notice_record_hash(model)}
        for model in valid
    ]
    new_ids = {row["objectId"] for row in valid_rows if row.get("objectId")}

    if valid_rows:
        log.info(
            "Date %s: prepared %d valid Bronze rows for chunk write",
            target_date,
            len(valid_rows),
            extra={"date": target_date, "status": "prepared"},
        )
    else:
        log.warning("Date %s: no valid records after dedup/validation", target_date)

    return _PreparedDate(
        target_date=target_date,
        started_at=started_at,
        raw_total=len(raw_records),
        after_dedup_total=len(deduped_records),
        valid_total=len(valid),
        invalid_total=len(errors),
        dedup_stats=dedup_stats,
        errors=errors,
        valid_rows=valid_rows,
        new_ids=new_ids,
    )


def _write_chunk(spark, prepared: list[_PreparedDate], bronze_notices_uri: str) -> bool:
    """Write all valid rows for a chunk with one dynamic partition overwrite."""
    from pyspark.sql.functions import col, to_date

    valid_rows = [row for item in prepared for row in item.valid_rows]
    if not valid_rows:
        return False

    df = spark.createDataFrame(valid_rows, schema=_bb.BRONZE_SPARK_SCHEMA).withColumn(
        "publicationDateDay", to_date(col("publicationDate")).cast("string")
    )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite").partitionBy("noticeType", "publicationDateDay").parquet(
        bronze_notices_uri
    )
    log.info(
        "Wrote Bronze chunk rows=%d dates=%d to %s",
        len(valid_rows),
        len({item.target_date for item in prepared}),
        bronze_notices_uri,
    )
    return True


def _finalize_date(
    item: _PreparedDate,
    bronze_dir: str,
    obs_dir,
    script_hash: str,
    dependency_hashes: dict[str, str],
    storage,
) -> None:
    """Write errors, observations, and manifest after the chunk write succeeds."""
    if item.errors:
        _bb._write_errors(bronze_dir, item.target_date, item.errors)

    run_id = f"bronze_range_{item.target_date}_{int(time.time() * 1000)}"
    wrote_notices = bool(item.valid_rows)
    write_pipeline_run(
        layer="bronze",
        target_date=item.target_date,
        run_id=run_id,
        started_at=item.started_at,
        completed_at=now_utc_iso(),
        status="ok" if wrote_notices else "empty",
        counts={
            "raw_total": item.raw_total,
            "after_dedup_total": item.after_dedup_total,
            "valid_total": item.valid_total,
            "invalid_total": item.invalid_total,
            "dropped_duplicates_in_input": item.dedup_stats["dropped_duplicates_in_input"],
            "dropped_duplicates_seen_other_day": item.dedup_stats[
                "dropped_duplicates_seen_index_other_day"
            ],
        },
        git_commit=None,
        script_hash=script_hash,
        obs_dir=obs_dir,
    )
    if item.after_dedup_total:
        write_dq_metrics(
            layer="bronze",
            target_date=item.target_date,
            notice_type=None,
            metrics={
                "raw_total": item.raw_total,
                "after_dedup_count": item.after_dedup_total,
                "valid_count": item.valid_total,
                "invalid_count": item.invalid_total,
                "valid_rate": item.valid_total / item.after_dedup_total,
                "dedup_cross_day_rate": (
                    item.dedup_stats["dropped_duplicates_seen_index_other_day"] / item.raw_total
                    if item.raw_total else 0.0
                ),
            },
            run_id=run_id,
            obs_dir=obs_dir,
        )

    write_processed_manifest(
        layer="bronze",
        target_date=item.target_date,
        script_hash=script_hash,
        storage=storage,
        dependency_hashes=dependency_hashes,
    )
    log.info(
        "Date %s: finalized Bronze manifest",
        item.target_date,
        extra={"date": item.target_date, "status": "ok" if wrote_notices else "empty"},
    )


def main() -> None:
    args = _parse_args()
    dates = _date_range(args.start_date, args.end_date)
    chunks = _chunks(dates, args.chunk_days)

    rt = get_runtime()
    bronze_raw_dir = args.bronze_raw_dir or rt.storage.resolve("bronze_raw")
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    log.info(
        (
            "Bronze range: %s..%s (%d dates, chunk_days=%d, chunks=%d, force=%s) "
            "runtime=%s bronze_raw_dir=%s bronze_dir=%s"
        ),
        args.start_date,
        args.end_date,
        len(dates),
        args.chunk_days,
        len(chunks),
        args.force,
        rt.env,
        bronze_raw_dir,
        bronze_dir,
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
        log.info(
            "Pre-computing bronze objectIds before %s (one-time scan)...",
            dates[0],
            extra={"status": "started"},
        )
        seen_ids: set[str] = _collect_pre_range_object_ids(spark, bronze_notices_uri, dates[0])
        log.info("Pre-range seen objectIds: %d", len(seen_ids))

        processed = 0
        skipped = 0
        failed_dates: list[str] = []

        for chunk_index, chunk_dates in enumerate(chunks, start=1):
            prepared: list[_PreparedDate] = []
            log.info(
                "Preparing Bronze chunk %d/%d dates=%s..%s",
                chunk_index,
                len(chunks),
                chunk_dates[0],
                chunk_dates[-1],
            )

            for target_date in chunk_dates:
                if not args.force and is_already_processed(
                    "bronze",
                    target_date,
                    script_hash,
                    rt.storage,
                    dependency_hashes=dependency_hashes,
                ):
                    log.info(
                        "Skipping bronze for %s - manifest and dependency hashes match",
                        target_date,
                        extra={"date": target_date, "status": "skipped"},
                    )
                    skipped_ids = _collect_object_ids_for_date(
                        spark, bronze_notices_uri, target_date
                    )
                    seen_ids |= skipped_ids
                    skipped += 1
                    continue

                try:
                    item = _prepare_date(
                        target_date=target_date,
                        bronze_raw_dir=bronze_raw_dir,
                        seen_ids=seen_ids,
                    )
                    if item is None:
                        continue
                    seen_ids |= item.new_ids
                    prepared.append(item)
                    processed += 1
                except Exception as exc:
                    log.error("Bronze failed for %s: %s", target_date, exc, exc_info=True)
                    failed_dates.append(target_date)
                    raise

            if not prepared:
                continue

            _write_chunk(spark, prepared, bronze_notices_uri)
            for item in prepared:
                _finalize_date(
                    item=item,
                    bronze_dir=bronze_dir,
                    obs_dir=obs_dir,
                    script_hash=script_hash,
                    dependency_hashes=dependency_hashes,
                    storage=rt.storage,
                )

        log.info(
            "Bronze range done: processed=%d skipped=%d failed=%d",
            processed,
            skipped,
            len(failed_dates),
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
