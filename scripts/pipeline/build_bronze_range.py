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
import logging
import os
import sys
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
    storage,
) -> bool:
    """Process one date within the shared Spark session.

    Returns True if Bronze Parquet was written, False when no raw input exists.
    Raises on unrecoverable errors so the caller can decide whether to abort the
    range or continue.
    """
    from pyspark.sql.functions import col, to_date
    from procurement.bronze.models import notice_record_hash

    input_files = _bb._candidate_input_files(bronze_raw_dir, target_date)
    if not input_files:
        log.warning("No Bronze-Raw input files found for %s — skipping", target_date)
        return False

    raw_records = _bb._load_raw_records(input_files)
    log.info("Date %s: loaded %d raw records", target_date, len(raw_records))

    started_at = now_utc_iso()
    bronze_notices_uri = f"{bronze_dir.rstrip('/')}/notices"

    deduped_records, dedup_stats = _bb._deduplicate_via_spark(
        spark, raw_records, target_date, bronze_notices_uri
    )

    valid, errors = _bb.validate_raw(deduped_records)
    valid_rows = [
        {**model.model_dump(), "recordHash": notice_record_hash(model)}
        for model in valid
    ]

    wrote_notices = False
    if valid_rows:
        df = spark.createDataFrame(valid_rows, schema=_bb.BRONZE_SPARK_SCHEMA).withColumn(
            "publicationDateDay", to_date(col("publicationDate")).cast("string")
        )
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        df.write.mode("overwrite").partitionBy("noticeType", "publicationDateDay").parquet(
            bronze_notices_uri
        )
        wrote_notices = True
        log.info(
            "Date %s: wrote %d rows to Bronze Parquet", target_date, len(valid_rows),
            extra={"date": target_date, "status": "ok"},
        )
    else:
        log.warning("Date %s: no valid records after dedup/validation", target_date)

    if errors:
        _bb._write_errors(bronze_dir, target_date, errors)

    run_id = f"bronze_{target_date}_{os.getpid()}"
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
                "valid_rate": len(valid) / len(deduped_records),
                "invalid_count": len(errors),
                "dedup_cross_day_rate": (
                    dedup_stats["dropped_duplicates_seen_index_other_day"] / len(raw_records)
                    if raw_records else 0.0
                ),
            },
            obs_dir=obs_dir,
        )

    write_processed_manifest(
        layer="bronze",
        target_date=target_date,
        script_hash=script_hash,
        storage=storage,
    )
    return wrote_notices


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

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    spark = rt.spark.get_session("bzp-bronze-range", **extra)
    try:
        processed = 0
        skipped = 0
        failed_dates: list[str] = []
        for target_date in dates:
            if not args.force and is_already_processed(
                "bronze", target_date, script_hash, rt.storage
            ):
                log.info(
                    "Skipping bronze for %s — manifest matches current script", target_date,
                    extra={"date": target_date, "status": "skipped"},
                )
                skipped += 1
                continue

            try:
                _process_date(
                    spark=spark,
                    target_date=target_date,
                    bronze_raw_dir=bronze_raw_dir,
                    bronze_dir=bronze_dir,
                    obs_dir=obs_dir,
                    script_hash=script_hash,
                    storage=rt.storage,
                )
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
