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
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

from procurement.bronze.models import BzpNoticeBronze
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

    valid, errors = validate_raw(raw_records)
    valid_rows = [model.model_dump() for model in valid]

    if not valid_rows:
        log.error("No valid records after validation; nothing to write.")
        sys.exit(1)

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
        log.info(
            "Wrote Bronze Parquet rows=%d to %s partitioned by noticeType/publicationDateDay",
            len(valid_rows),
            notices_root,
        )
    finally:
        spark.stop()

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
            "partition_rows": partition_rows,
            "errors_path": str(bronze_dir / "errors" / f"bzp_{target_date}_errors.json") if errors else None,
        },
        "counts": {
            "raw_total": len(raw_records),
            "valid_total": len(valid),
            "invalid_total": len(errors),
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
        "Summary: total=%d  valid=%d  invalid=%d",
        len(raw_records),
        len(valid),
        len(errors),
    )


if __name__ == "__main__":
    main()
