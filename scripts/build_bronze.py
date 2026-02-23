"""Build Bronze Parquet from Bronze-Raw JSON.

Reads:
  <bronze-raw-dir>/bzp_YYYY-MM-DD.json
  optional chunks: <bronze-raw-dir>/bzp_YYYY-MM-DD_*.json

Writes:
  <bronze-dir>/notices/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/
  <bronze-dir>/errors/bzp_YYYY-MM-DD_errors.json (only when validation failures exist)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

from procurement.bronze.models import BzpNoticeBronze, notice_record_hash
from procurement.lineage import atomic_write_json, git_commit_sha, now_utc_iso, script_hashes, sha256_file
from procurement.logging import setup_logging
from pydantic import ValidationError

setup_logging()
log = logging.getLogger(__name__)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Bronze Parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--bronze-raw-dir",
        default="data/bronze_raw",
        help="Directory with raw JSON payloads",
    )
    parser.add_argument(
        "--bronze-dir",
        default="data/bronze",
        help="Bronze output directory",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[4])",
    )
    return parser.parse_args()


def _candidate_input_files(bronze_raw_dir: Path, target_date: str) -> list[Path]:
    # Support both single-file and chunked daily files.
    direct = bronze_raw_dir / f"bzp_{target_date}.json"
    chunked = sorted(bronze_raw_dir.glob(f"bzp_{target_date}_*.json"))
    files: list[Path] = []
    if direct.exists():
        files.append(direct)
    files.extend(chunked)
    return files


def _load_raw_records(input_files: list[Path]) -> list[dict]:
    records: list[dict] = []
    for path in input_files:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            raise ValueError(f"Expected JSON array in {path}, got {type(payload).__name__}")
        records.extend(payload)
    return records


def _chunked(values: list[str], size: int) -> list[list[str]]:
    return [values[i : i + size] for i in range(0, len(values), size)]


def _open_seen_index(index_db_path: Path) -> sqlite3.Connection:
    index_db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(index_db_path), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_notice_ids (
            object_id TEXT PRIMARY KEY,
            first_target_date TEXT NOT NULL,
            first_publication_day TEXT,
            first_seen_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_seen_notice_ids_first_target_date
        ON seen_notice_ids (first_target_date)
        """
    )
    conn.commit()
    return conn


def _deduplicate_against_seen_index(
    raw_records: list[dict],
    target_date: str,
    index_db_path: Path,
) -> tuple[list[dict], dict]:
    """Drop rows already seen on *other* target_date runs.

    Notes:
    - Same-day reruns stay idempotent: IDs first seen on this target_date are allowed.
    - Intra-batch duplicate objectIds are dropped as well.
    """

    unique_ids = sorted(
        {
            str(rec.get("objectId")).strip()
            for rec in raw_records
            if rec.get("objectId") is not None and str(rec.get("objectId")).strip()
        }
    )

    existing_first_day: dict[str, str] = {}
    conn = _open_seen_index(index_db_path)
    try:
        for chunk in _chunked(unique_ids, 900):
            placeholders = ",".join("?" for _ in chunk)
            sql = (
                "SELECT object_id, first_target_date "
                f"FROM seen_notice_ids WHERE object_id IN ({placeholders})"
            )
            rows = conn.execute(sql, chunk).fetchall()
            for object_id, first_target_date in rows:
                existing_first_day[str(object_id)] = str(first_target_date)
    finally:
        conn.close()

    in_file_seen: set[str] = set()
    filtered: list[dict] = []
    dropped_in_file = 0
    dropped_seen_other_day = 0

    for rec in raw_records:
        object_id_raw = rec.get("objectId")
        object_id = str(object_id_raw).strip() if object_id_raw is not None else ""
        if not object_id:
            filtered.append(rec)
            continue

        if object_id in in_file_seen:
            dropped_in_file += 1
            continue
        in_file_seen.add(object_id)

        seen_day = existing_first_day.get(object_id)
        if seen_day is not None and seen_day != target_date:
            dropped_seen_other_day += 1
            continue
        filtered.append(rec)

    stats = {
        "input_rows": len(raw_records),
        "output_rows": len(filtered),
        "dropped_duplicates_in_input": dropped_in_file,
        "dropped_duplicates_seen_index_other_day": dropped_seen_other_day,
    }
    return filtered, stats


def _update_seen_index(
    index_db_path: Path,
    object_ids: list[str],
    target_date: str,
    publication_day_hint: str | None,
) -> int:
    object_ids_unique = sorted({oid.strip() for oid in object_ids if oid and oid.strip()})
    if not object_ids_unique:
        return 0

    inserted = 0
    conn = _open_seen_index(index_db_path)
    try:
        now_iso = now_utc_iso()
        rows = [
            (oid, target_date, publication_day_hint, now_iso)
            for oid in object_ids_unique
        ]
        cur = conn.executemany(
            """
            INSERT OR IGNORE INTO seen_notice_ids
            (object_id, first_target_date, first_publication_day, first_seen_at)
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )
        inserted = int(cur.rowcount) if cur.rowcount is not None else 0
        conn.commit()
    finally:
        conn.close()
    return inserted


def validate_raw(raw_records: list[dict]) -> tuple[list[BzpNoticeBronze], list[dict]]:
    """Validate raw dicts and split into valid models + error dicts."""
    valid: list[BzpNoticeBronze] = []
    errors: list[dict] = []

    for idx, record in enumerate(raw_records):
        try:
            valid.append(BzpNoticeBronze.model_validate(record))
        except ValidationError as exc:
            log.warning(
                "Record %d (objectId=%s) failed validation: %s",
                idx,
                record.get("objectId", "?"),
                exc.error_count(),
            )
            errors.append(
                {
                    "index": idx,
                    "objectId": record.get("objectId"),
                    "errors": exc.errors(),
                    "raw": record,
                }
            )

    return valid, errors


def main() -> None:
    args = _parse_args()
    target_date = args.target_date or (date.today() - timedelta(days=1)).isoformat()
    started_at = now_utc_iso()

    bronze_raw_dir = Path(args.bronze_raw_dir)
    bronze_dir = Path(args.bronze_dir)
    input_files = _candidate_input_files(bronze_raw_dir, target_date)

    if not input_files:
        # Backward compatibility for older layout.
        legacy_raw = Path("data/raw") / f"bzp_{target_date}.json"
        if legacy_raw.exists():
            input_files = [legacy_raw]
            log.warning("Using legacy raw input path: %s", legacy_raw)
        else:
            log.error(
                "No Bronze-Raw input files found for %s under %s",
                target_date,
                bronze_raw_dir,
            )
            sys.exit(1)

    log.info("Reading Bronze-Raw files for %s: %s", target_date, [str(p) for p in input_files])
    raw_records = _load_raw_records(input_files)
    log.info("Loaded %d raw records", len(raw_records))

    index_db_path = bronze_dir / "_index" / "seen_notice_ids" / "seen_notice_ids.sqlite"
    deduped_records, dedup_stats = _deduplicate_against_seen_index(
        raw_records,
        target_date,
        index_db_path,
    )
    if dedup_stats["dropped_duplicates_in_input"] or dedup_stats["dropped_duplicates_seen_index_other_day"]:
        log.info(
            "Dedup filtered rows: in_input=%d seen_index_other_day=%d (remaining=%d)",
            dedup_stats["dropped_duplicates_in_input"],
            dedup_stats["dropped_duplicates_seen_index_other_day"],
            dedup_stats["output_rows"],
        )

    valid, errors = validate_raw(deduped_records)
    valid_rows = [
        {
            **model.model_dump(),
            "recordHash": notice_record_hash(model),
        }
        for model in valid
    ]

    wrote_notices = False

    if valid_rows:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, to_date

        spark = (
            SparkSession.builder.appName("bzp-bronze")
            .master(args.spark_master)
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        try:
            df = spark.createDataFrame(valid_rows).withColumn(
                "publicationDateDay",
                to_date(col("publicationDate")).cast("string"),
            )
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            notices_root = bronze_dir / "notices"
            (
                df.write.mode("overwrite")
                .partitionBy("noticeType", "publicationDateDay")
                .parquet(str(notices_root))
            )
            wrote_notices = True
            log.info(
                "Wrote Bronze Parquet rows=%d to %s partitioned by noticeType/publicationDateDay",
                len(valid_rows),
                notices_root,
            )
        finally:
            spark.stop()
    else:
        log.warning("No valid records after validation (post-dedup); skipping Bronze Parquet write.")

    if errors:
        errors_dir = bronze_dir / "errors"
        errors_dir.mkdir(parents=True, exist_ok=True)
        errors_path = errors_dir / f"bzp_{target_date}_errors.json"
        errors_path.write_text(
            json.dumps(errors, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        log.info("Wrote %d validation errors to %s", len(errors), errors_path)

    partition_counter = Counter(
        (
            str(row.get("noticeType")),
            str(row.get("publicationDate", ""))[:10],
        )
        for row in valid_rows
    )
    partition_rows = [
        {"noticeType": nt, "publicationDateDay": day, "rows": cnt}
        for (nt, day), cnt in sorted(partition_counter.items(), key=lambda x: (x[0][1], x[0][0]))
    ]

    repo_root = Path(__file__).resolve().parent.parent
    meta_path = bronze_dir / "_meta" / f"day={target_date}.json"
    inserted_seen = _update_seen_index(
        index_db_path=index_db_path,
        object_ids=[str(row.get("objectId", "")).strip() for row in valid_rows],
        target_date=target_date,
        publication_day_hint=target_date,
    )
    log.info("Seen index updated: inserted_new_ids=%d path=%s", inserted_seen, index_db_path)

    manifest = {
        "layer": "bronze",
        "target_date": target_date,
        "started_at": started_at,
        "completed_at": now_utc_iso(),
        "inputs": [
            {"path": str(p), "sha256": sha256_file(p)}
            for p in input_files
            if p.exists()
        ],
        "outputs": {
            "notices_root": str(bronze_dir / "notices"),
            "wrote_notices": wrote_notices,
            "partition_rows": partition_rows,
            "errors_path": str(bronze_dir / "errors" / f"bzp_{target_date}_errors.json") if errors else None,
            "seen_index_db_path": str(index_db_path),
        },
        "counts": {
            "raw_total": len(raw_records),
            "after_dedup_total": len(deduped_records),
            "valid_total": len(valid),
            "invalid_total": len(errors),
            "dropped_duplicates_in_input": dedup_stats["dropped_duplicates_in_input"],
            "dropped_duplicates_seen_index_other_day": dedup_stats["dropped_duplicates_seen_index_other_day"],
            "seen_index_inserted_new_ids": inserted_seen,
        },
        "code": {
            "git_commit": git_commit_sha(repo_root),
            "script_hashes": script_hashes(
                [
                    Path(__file__).resolve(),
                    repo_root / "src" / "procurement" / "bronze" / "models.py",
                ]
            ),
            "command": sys.argv,
        },
    }
    atomic_write_json(meta_path, manifest)
    log.info("Wrote bronze lineage manifest to %s", meta_path)

    log.info(
        "Summary: total=%d  after_dedup=%d  valid=%d  invalid=%d  dropped_seen=%d",
        len(raw_records),
        len(deduped_records),
        len(valid),
        len(errors),
        dedup_stats["dropped_duplicates_seen_index_other_day"],
    )


if __name__ == "__main__":
    main()
