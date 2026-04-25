"""Build Bronze Parquet from Bronze-Raw JSON.

Reads:
  <bronze-raw-dir>/bzp_YYYY-MM-DD.json
  optional chunks: <bronze-raw-dir>/bzp_YYYY-MM-DD_*.json

Writes:
  <bronze-dir>/notices/noticeType=<TYPE>/publicationDateDay=YYYY-MM-DD/
  <bronze-dir>/errors/bzp_YYYY-MM-DD_errors.json  (only when validation failures exist)

Path resolution
---------------
``bronze-raw-dir`` and ``bronze-dir`` default to the runtime-resolved paths:

- **local**  (``RUNTIME_ENV=local``):  ``{LOCAL_DATA_ROOT}/bronze_raw/`` and
  ``{LOCAL_DATA_ROOT}/bronze/``
- **GCP**    (``RUNTIME_ENV=gcp``):    ``gs://{LAKEHOUSE_BUCKET}/bronze_raw/``
  and ``gs://{LAKEHOUSE_BUCKET}/bronze/``

Pass ``--bronze-raw-dir`` / ``--bronze-dir`` explicitly to override.

Deduplication
-------------
Cross-day deduplication is performed via a Spark query against the existing
Bronze Parquet: any ``objectId`` already present in a **different** day's
partition is dropped.  Same-day reruns remain fully idempotent (the partition
is overwritten).  This replaces the former SQLite seen-index which was not
compatible with GCS-backed storage.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent.parent / "src")
_SRC_PKG = Path(_src) / "procurement"
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")

from procurement.bronze.models import BzpNoticeBronze, notice_record_hash
from procurement.obs import git_commit_sha, now_utc_iso, sha256_paths, write_dq_metrics, write_pipeline_run
from procurement.logging import setup_logging
from procurement.manifests import write_processed_manifest
from procurement.runtime import get_runtime
from pydantic import ValidationError
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

setup_logging()
log = logging.getLogger(__name__)

BRONZE_SPARK_SCHEMA = StructType(
    [
        StructField("objectId", StringType(), nullable=False),
        StructField("noticeType", StringType(), nullable=False),
        StructField("noticeNumber", StringType(), nullable=False),
        StructField("bzpNumber", StringType(), nullable=False),
        StructField("publicationDate", StringType(), nullable=False),
        StructField("isTenderAmountBelowEU", BooleanType(), nullable=False),
        StructField("orderObject", StringType(), nullable=True),
        StructField("cpvCode", StringType(), nullable=False),
        StructField("htmlBody", StringType(), nullable=False),
        StructField("clientType", StringType(), nullable=True),
        StructField("orderType", StringType(), nullable=True),
        StructField("tenderType", StringType(), nullable=True),
        StructField("submittingOffersDate", StringType(), nullable=True),
        StructField("procedureResult", StringType(), nullable=True),
        StructField("organizationName", StringType(), nullable=False),
        StructField("organizationCity", StringType(), nullable=False),
        StructField("organizationProvince", StringType(), nullable=True),
        StructField("organizationCountry", StringType(), nullable=False),
        StructField("organizationNationalId", StringType(), nullable=False),
        StructField("organizationId", StringType(), nullable=False),
        StructField("tenderId", StringType(), nullable=True),
        StructField(
            "contractors",
            ArrayType(
                StructType(
                    [
                        StructField("contractorName", StringType(), nullable=True),
                        StructField("contractorCity", StringType(), nullable=True),
                        StructField("contractorProvince", StringType(), nullable=True),
                        StructField("contractorCountry", StringType(), nullable=True),
                        StructField("contractorNationalId", StringType(), nullable=True),
                    ]
                )
            ),
            nullable=True,
        ),
        StructField("recordHash", StringType(), nullable=False),
    ]
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Bronze Parquet for one date.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--bronze-raw-dir",
        default=None,
        help=(
            "Directory with raw JSON payloads.  Defaults to the runtime-resolved "
            "'bronze_raw' path."
        ),
    )
    parser.add_argument(
        "--bronze-dir",
        default=None,
        help="Bronze output directory.  Defaults to the runtime-resolved 'bronze' path.",
    )
    parser.add_argument(
        "--spark-master",
        default=None,
        help="Spark master string (e.g. local[*], local[4]).  Defaults to SPARK_MASTER env var.",
    )
    return parser.parse_args()


def _candidate_input_files(bronze_raw_dir: str, target_date: str) -> list[str]:
    """Return URIs / paths of candidate input files for *target_date*.

    Supports both local filesystem paths and ``gs://`` URIs.
    """
    if bronze_raw_dir.startswith("gs://"):
        from google.cloud import storage as gcs

        without_scheme = bronze_raw_dir[5:]
        bucket_name, _, prefix = without_scheme.partition("/")
        client = gcs.Client()
        bucket = client.bucket(bucket_name)

        prefix_norm = prefix.rstrip("/") + "/" if prefix else ""
        files: list[str] = []

        # Single file: bzp_YYYY-MM-DD.json
        direct_blob = prefix_norm + f"bzp_{target_date}.json"
        if bucket.blob(direct_blob).exists():
            files.append(f"gs://{bucket_name}/{direct_blob}")

        # Chunked files: bzp_YYYY-MM-DD_*.json
        for blob in client.list_blobs(bucket_name, prefix=prefix_norm + f"bzp_{target_date}_"):
            if blob.name.endswith(".json"):
                files.append(f"gs://{bucket_name}/{blob.name}")

        return sorted(files)

    # Local filesystem
    base = Path(bronze_raw_dir)
    direct = base / f"bzp_{target_date}.json"
    chunked = sorted(base.glob(f"bzp_{target_date}_*.json"))
    result: list[str] = []
    if direct.exists():
        result.append(str(direct))
    result.extend(str(p) for p in chunked)
    return result


def _load_raw_records(input_files: list[str]) -> list[dict]:
    """Load JSON arrays from local paths or GCS URIs."""
    records: list[dict] = []
    for path_or_uri in input_files:
        if path_or_uri.startswith("gs://"):
            from google.cloud import storage as gcs

            without_scheme = path_or_uri[5:]
            bucket_name, _, blob_name = without_scheme.partition("/")
            content = gcs.Client().bucket(bucket_name).blob(blob_name).download_as_text(encoding="utf-8")
            payload = json.loads(content)
        else:
            payload = json.loads(Path(path_or_uri).read_text(encoding="utf-8"))

        if not isinstance(payload, list):
            raise ValueError(f"Expected JSON array in {path_or_uri}, got {type(payload).__name__}")
        records.extend(payload)
    return records


def _deduplicate_via_spark(
    spark,
    records: list[dict],
    target_date: str,
    bronze_notices_uri: str,
) -> tuple[list[dict], dict]:
    """Drop objectIds already present in a *different* day's Bronze partition.

    Uses a Spark query against the existing Bronze Parquet.  If no Bronze data
    exists yet (first run), all records pass through.

    Same-day reruns are idempotent: the target partition is overwritten, so
    records whose first appearance was on *target_date* are always allowed.
    """
    from pyspark.sql import functions as F

    unique_ids = {
        str(r.get("objectId")).strip()
        for r in records
        if r.get("objectId") is not None and str(r.get("objectId")).strip()
    }

    ids_seen_other_day: set[str] = set()
    try:
        existing = (
            spark.read.parquet(bronze_notices_uri)
            .filter(F.col("publicationDateDay") != target_date)
            .select("objectId")
            .distinct()
        )
        ids_seen_other_day = {row.objectId for row in existing.toLocalIterator()}
    except Exception:
        # No existing Bronze data yet — first run.
        pass

    in_file_seen: set[str] = set()
    filtered: list[dict] = []
    dropped_in_file = 0
    dropped_seen_other_day = 0

    for rec in records:
        object_id_raw = rec.get("objectId")
        object_id = str(object_id_raw).strip() if object_id_raw is not None else ""

        if not object_id:
            filtered.append(rec)
            continue

        if object_id in in_file_seen:
            dropped_in_file += 1
            continue
        in_file_seen.add(object_id)

        if object_id in ids_seen_other_day:
            dropped_seen_other_day += 1
            continue

        filtered.append(rec)

    stats = {
        "input_rows": len(records),
        "output_rows": len(filtered),
        "dropped_duplicates_in_input": dropped_in_file,
        "dropped_duplicates_seen_index_other_day": dropped_seen_other_day,
    }
    return filtered, stats


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


def _write_errors(bronze_dir: str, target_date: str, errors: list[dict]) -> None:
    """Write validation errors to local path or GCS URI."""
    serialised = json.dumps(errors, ensure_ascii=False, indent=2, default=str)
    filename = f"bzp_{target_date}_errors.json"

    if bronze_dir.startswith("gs://"):
        from google.cloud import storage as gcs

        without_scheme = bronze_dir[5:]
        bucket_name, _, prefix = without_scheme.partition("/")
        blob_name = f"{prefix.rstrip('/')}/errors/{filename}" if prefix else f"errors/{filename}"
        gcs.Client().bucket(bucket_name).blob(blob_name).upload_from_string(
            serialised.encode("utf-8"),
            content_type="application/json",
        )
        log.info("Wrote %d validation errors to gs://%s/%s", len(errors), bucket_name, blob_name)
    else:
        errors_dir = Path(bronze_dir) / "errors"
        errors_dir.mkdir(parents=True, exist_ok=True)
        errors_path = errors_dir / filename
        errors_path.write_text(serialised, encoding="utf-8")
        log.info("Wrote %d validation errors to %s", len(errors), errors_path)


def main() -> None:
    args = _parse_args()
    target_date = args.target_date or (date.today() - timedelta(days=1)).isoformat()
    started_at = now_utc_iso()

    rt = get_runtime()
    bronze_raw_dir = args.bronze_raw_dir or rt.storage.resolve("bronze_raw")
    bronze_dir = args.bronze_dir or rt.storage.resolve("bronze")
    script_hash = sha256_paths(Path(__file__), _SRC_PKG / "bronze")

    input_files = _candidate_input_files(bronze_raw_dir, target_date)

    if not input_files:
        log.error(
            "No Bronze-Raw input files found for %s under %s",
            target_date,
            bronze_raw_dir,
        )
        sys.exit(1)

    log.info("Reading Bronze-Raw files for %s: %s", target_date, input_files)
    raw_records = _load_raw_records(input_files)
    log.info("Loaded %d raw records", len(raw_records))

    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, to_date

    extra: dict[str, str] = {}
    if args.spark_master:
        extra["spark.master"] = args.spark_master

    spark = rt.spark.get_session("bzp-bronze", **extra)
    # Override master if explicitly passed (local provider ignores extra config
    # on the builder; DataprocServerless has no master).
    if args.spark_master and rt.env == "local":
        spark.stop()
        from procurement.runtime.providers.local import LocalSparkLauncher
        spark = LocalSparkLauncher(master=args.spark_master).get_session("bzp-bronze")

    try:
        bronze_notices_uri = f"{bronze_dir.rstrip('/')}/notices"
        deduped_records, dedup_stats = _deduplicate_via_spark(
            spark, raw_records, target_date, bronze_notices_uri
        )
        if dedup_stats["dropped_duplicates_in_input"] or dedup_stats["dropped_duplicates_seen_index_other_day"]:
            log.info(
                "Dedup filtered rows: in_input=%d seen_other_day=%d (remaining=%d)",
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
            df = spark.createDataFrame(valid_rows, schema=BRONZE_SPARK_SCHEMA).withColumn(
                "publicationDateDay",
                to_date(col("publicationDate")).cast("string"),
            )
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            (
                df.write.mode("overwrite")
                .partitionBy("noticeType", "publicationDateDay")
                .parquet(bronze_notices_uri)
            )
            wrote_notices = True
            log.info(
                "Wrote Bronze Parquet rows=%d to %s partitioned by noticeType/publicationDateDay",
                len(valid_rows),
                bronze_notices_uri,
            )
        else:
            log.warning("No valid records after validation (post-dedup); skipping Bronze Parquet write.")

    finally:
        spark.stop()

    if errors:
        _write_errors(bronze_dir, target_date, errors)

    obs_dir = rt.storage.obs_path()
    run_id = f"bronze_{target_date}_{os.getpid()}"
    counts = {
        "raw_total": len(raw_records),
        "after_dedup_total": len(deduped_records),
        "valid_total": len(valid),
        "invalid_total": len(errors),
        "dropped_duplicates_in_input": dedup_stats["dropped_duplicates_in_input"],
        "dropped_duplicates_seen_other_day": dedup_stats["dropped_duplicates_seen_index_other_day"],
    }
    write_pipeline_run(
        layer="bronze",
        target_date=target_date,
        run_id=run_id,
        started_at=started_at,
        completed_at=now_utc_iso(),
        status="ok" if wrote_notices else "empty",
        counts=counts,
        git_commit=git_commit_sha(),
        script_hash=script_hash,
        obs_dir=obs_dir,
    )
    if len(deduped_records) > 0:
        write_dq_metrics(
            layer="bronze",
            target_date=target_date,
            notice_type=None,
            metrics={
                "valid_rate": len(valid) / len(deduped_records),
                "invalid_count": len(errors),
                "dedup_cross_day_rate": (
                    dedup_stats["dropped_duplicates_seen_index_other_day"] / len(raw_records)
                    if raw_records
                    else 0.0
                ),
            },
            obs_dir=obs_dir,
        )

    log.info(
        "Summary: total=%d  after_dedup=%d  valid=%d  invalid=%d  dropped_seen=%d",
        len(raw_records),
        len(deduped_records),
        len(valid),
        len(errors),
        dedup_stats["dropped_duplicates_seen_index_other_day"],
    )

    write_processed_manifest(
        layer="bronze",
        target_date=target_date,
        script_hash=script_hash,
        storage=rt.storage,
    )
    log.info("Written processed manifest: layer=bronze date=%s", target_date)


if __name__ == "__main__":
    main()
