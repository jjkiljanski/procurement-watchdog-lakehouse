"""Build Silver case-derived lifecycle facts with single-writer protocol.

Modes:
  full:
    Rebuild case_derived_facts snapshot from all Silver notices up to target date.
  incremental:
    Recompute only cases touched on target date and merge with previous snapshot.

Reads:
  data/silver/common_envelope/publicationDateDay=YYYY-MM-DD/
  data/silver/notice_type_tables/noticeType=*/publicationDateDay=YYYY-MM-DD/

Writes:
  data/silver/case_derived_facts/snapshots/version=<RUN_ID>/data/
  data/silver/case_derived_facts/CURRENT.json
  data/silver/case_derived_facts/_meta/case_derived.lock
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    concat_ws,
    count,
    datediff,
    expr,
    first,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    sum as spark_sum,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import ArrayType, DataType, StructType

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging

setup_logging()
log = logging.getLogger(__name__)


def has_field(df: DataFrame, field_path: str) -> bool:
    current: DataType = df.schema
    for part in field_path.split("."):
        if isinstance(current, StructType):
            field = next((f for f in current.fields if f.name == part), None)
            if field is None:
                return False
            current = field.dataType
            continue
        if isinstance(current, ArrayType):
            current = current.elementType
            if isinstance(current, StructType):
                field = next((f for f in current.fields if f.name == part), None)
                if field is None:
                    return False
                current = field.dataType
                continue
        return False
    return True


def safe_col(df: DataFrame, field_path: str, cast_to: str | None = None):
    from pyspark.sql.functions import lit

    if has_field(df, field_path):
        return col(field_path)
    if cast_to is None:
        return lit(None)
    return lit(None).cast(cast_to)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Silver case_derived_facts snapshot.")
    parser.add_argument("target_date", nargs="?", help="Date in YYYY-MM-DD format")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Build mode for case_derived_facts",
    )
    parser.add_argument("--silver-dir", default="data/silver", help="Silver root directory")
    parser.add_argument(
        "--output-dir",
        default="data/silver/case_derived_facts",
        help="Output directory for case_derived_facts (snapshots + pointer)",
    )
    parser.add_argument(
        "--spark-master",
        default=os.environ.get("SPARK_MASTER", "local[*]"),
        help="Spark master string (e.g. local[*], local[2])",
    )
    parser.add_argument(
        "--lock-timeout-sec",
        type=int,
        default=1800,
        help="Max wait for lock acquisition in seconds",
    )
    parser.add_argument(
        "--lock-poll-sec",
        type=int,
        default=5,
        help="Polling interval while waiting for lock",
    )
    parser.add_argument(
        "--lock-stale-sec",
        type=int,
        default=21600,
        help="Age threshold to consider lock stale (seconds)",
    )
    parser.add_argument(
        "--break-stale-lock",
        action="store_true",
        help="Allow removing stale lock files",
    )
    return parser.parse_args()


def _union_paths(spark: "SparkSession", paths: list[str], base_path: str | None = None) -> DataFrame:
    frames = []
    for path in paths:
        reader = spark.read
        if base_path:
            reader = reader.option("basePath", base_path)
        frames.append(reader.parquet(path))
    if not frames:
        raise ValueError("No paths to read")
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)


def _paths_up_to(base_dir: Path, partition_key: str, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted(base_dir.glob(f"{partition_key}=*")):
        if not p.is_dir():
            continue
        token = p.name.replace(f"{partition_key}=", "")
        if token <= target_date:
            out.append(str(p))
    return out


def _specific_paths_up_to(silver_dir: Path, target_date: str) -> list[str]:
    out: list[str] = []
    for p in sorted(silver_dir.glob("notice_type_tables/noticeType=*/publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        if token <= target_date:
            out.append(str(p))
    return out


def _read_notices_merged(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    envelope_paths = _paths_up_to(envelope_root, "publicationDateDay", target_date)
    specific_paths = _specific_paths_up_to(silver_dir, target_date)
    if not envelope_paths or not specific_paths:
        raise ValueError(
            f"Missing silver inputs for <= {target_date}: envelope={len(envelope_paths)} specific={len(specific_paths)}"
    )

    envelope_raw = _union_paths(spark, envelope_paths, base_path=str(envelope_root))
    # Avoid duplicate partition/data column warnings on noticeType by reading paths directly.
    specific_raw = _union_paths(spark, specific_paths)

    envelope_slim = envelope_raw.select(
        safe_col(envelope_raw, "objectId", "string").alias("objectId"),
        safe_col(envelope_raw, "caseId", "string").alias("env_caseId"),
        safe_col(envelope_raw, "organizationId", "string").alias("env_organizationId"),
        safe_col(envelope_raw, "noticeType", "string").alias("env_noticeType"),
        safe_col(envelope_raw, "publicationDate", "string").alias("env_publicationDate"),
        safe_col(envelope_raw, "submittingOffersDate", "string").alias("env_submittingOffersDate"),
    )

    merged = specific_raw.join(envelope_slim, on="objectId", how="left").select(
        safe_col(specific_raw, "objectId", "string").alias("objectId"),
        coalesce(
            safe_col(specific_raw, "caseId", "string"),
            safe_col(envelope_slim, "env_caseId", "string"),
        ).alias("caseId"),
        coalesce(
            safe_col(specific_raw, "organizationId", "string"),
            safe_col(envelope_slim, "env_organizationId", "string"),
        ).alias("organizationId"),
        coalesce(
            safe_col(specific_raw, "noticeType", "string"),
            safe_col(envelope_slim, "env_noticeType", "string"),
        ).alias("noticeType"),
        coalesce(
            safe_col(specific_raw, "publicationDate", "string"),
            safe_col(envelope_slim, "env_publicationDate", "string"),
        ).alias("publicationDate"),
        coalesce(
            safe_col(specific_raw, "submittingOffersDate", "string"),
            safe_col(envelope_slim, "env_submittingOffersDate", "string"),
        ).alias("submittingOffersDate"),
        safe_col(specific_raw, "htmlExtracted", "struct<notice_change:struct<changes:array<struct<changed_section:string,change_description:string>>>,contract_execution:struct<contract_date:string,executed_on_time:boolean,executed_properly:boolean,execution_end_date:string,execution_period:string,num_changes:bigint>,values:struct<contract_value:double,total_paid:double>>").alias(
            "htmlExtracted"
        ),
    )

    if case_ids is not None:
        merged = merged.join(case_ids.select("caseId"), on="caseId", how="inner")

    return merged.filter(col("caseId").isNotNull())


def _build_case_derived(notices: DataFrame) -> DataFrame:
    with_metrics = (
        notices.withColumn("publication_date", to_date(col("publicationDate")))
        .withColumn(
            "biddingWindowDays",
            datediff(to_timestamp(col("submittingOffersDate")), to_timestamp(col("publicationDate"))),
        )
        .withColumn(
            "updateDeltaText",
            lower(
                expr(
                    "concat_ws(' ', transform(coalesce(htmlExtracted.notice_change.changes, array()), "
                    "x -> concat_ws(' ', coalesce(x.changed_section, ''), coalesce(x.change_description, ''))))"
                )
            ),
        )
        .withColumn(
            "deadlineChanged",
            col("updateDeltaText").rlike("termin|deadline|skladania ofert|otwarcia ofert"),
        )
        .withColumn(
            "criteriaChanged",
            col("updateDeltaText").rlike("kryter|cena|waga"),
        )
        .withColumn(
            "scopeChanged",
            col("updateDeltaText").rlike("zakres|przedmiot|opis"),
        )
        .withColumn(
            "executionDurationDays",
            coalesce(
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:dni|dzien|days?)', 1) as int)"
                    ),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:tygod\\w*|weeks?)', 1) as int)"
                    )
                    * lit(7),
                ),
                when(
                    col("htmlExtracted.contract_execution.execution_period").isNotNull(),
                    expr(
                        "try_cast(regexp_extract(lower(htmlExtracted.contract_execution.execution_period), "
                        "'(\\d+)\\s*(?:miesi\\w*|months?)', 1) as int)"
                    )
                    * lit(30),
                ),
                when(
                    col("htmlExtracted.contract_execution.contract_date").isNotNull()
                    & col("htmlExtracted.contract_execution.execution_end_date").isNotNull(),
                    datediff(
                        to_date(col("htmlExtracted.contract_execution.execution_end_date")),
                        to_date(col("htmlExtracted.contract_execution.contract_date")),
                    ),
                ),
            ),
        )
        .withColumn(
            "paidRatio",
            when(
                col("htmlExtracted.values.contract_value").isNotNull()
                & (col("htmlExtracted.values.contract_value") != 0)
                & col("htmlExtracted.values.total_paid").isNotNull(),
                col("htmlExtracted.values.total_paid") / col("htmlExtracted.values.contract_value"),
            ),
        )
        .withColumn(
            "executionDelayed",
            when(
                col("htmlExtracted.contract_execution.executed_on_time").isNotNull(),
                ~col("htmlExtracted.contract_execution.executed_on_time"),
            ),
        )
        .withColumn(
            "executionRiskFlag",
            when(
                col("noticeType") == lit("ContractPerformingNotice"),
                coalesce(col("executionDelayed"), lit(False))
                | (coalesce(col("paidRatio"), lit(0.0)) > lit(1.05))
                | (coalesce(col("htmlExtracted.contract_execution.num_changes"), lit(0)) > lit(0))
                | (col("htmlExtracted.contract_execution.executed_properly") == lit(False)),
            ),
        )
        .withColumn(
            "init_date",
            when(
                col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"),
                col("publication_date"),
            ),
        )
        .withColumn(
            "result_date",
            when(col("noticeType") == lit("TenderResultNotice"), col("publication_date")),
        )
        .withColumn(
            "execution_completion_date",
            when(
                col("noticeType") == lit("ContractPerformingNotice"),
                coalesce(
                    to_date(col("htmlExtracted.contract_execution.execution_end_date")),
                    col("publication_date"),
                ),
            ),
        )
    )

    return (
        with_metrics.groupBy("caseId")
        .agg(
            first("organizationId", ignorenulls=True).alias("buyer_id"),
            spark_min("publication_date").alias("first_publicationDate"),
            spark_max("publication_date").alias("last_publicationDate"),
            count(lit(1)).alias("num_notices"),
            spark_sum(when(col("noticeType") == lit("NoticeUpdateNotice"), lit(1)).otherwise(lit(0)))
            .cast("long")
            .alias("num_updates"),
            spark_max(when(col("noticeType").isin("ContractNotice", "ContractOrOrderNotice", "SmallContractNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_init"),
            spark_max(when(col("noticeType") == lit("TenderResultNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_result"),
            spark_max(when(col("noticeType") == lit("ContractPerformingNotice"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("has_execution"),
            spark_min("init_date").alias("first_init_date"),
            spark_min("result_date").alias("first_result_date"),
            spark_min("execution_completion_date").alias("first_execution_completion_date"),
            spark_sum(when(col("deadlineChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "deadline_changed_count"
            ),
            spark_sum(when(col("criteriaChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "criteria_changed_count"
            ),
            spark_sum(when(col("scopeChanged"), lit(1)).otherwise(lit(0))).cast("long").alias(
                "scope_changed_count"
            ),
            spark_max(when(col("executionDelayed"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_delayed_any"),
            spark_max(when(col("executionRiskFlag"), lit(1)).otherwise(lit(0)))
            .cast("boolean")
            .alias("execution_risk_any"),
            spark_max("paidRatio").alias("paid_ratio_max"),
            percentile_approx(col("paidRatio"), 0.5, 1000).alias("paid_ratio_median"),
            percentile_approx(col("biddingWindowDays"), 0.5, 1000).alias("bidding_window_days_median"),
            percentile_approx(col("executionDurationDays"), 0.5, 1000).alias(
                "execution_duration_days_median"
            ),
        )
        .withColumn(
            "time_to_award_days",
            when(
                col("first_init_date").isNotNull() & col("first_result_date").isNotNull(),
                datediff(col("first_result_date"), col("first_init_date")),
            ),
        )
        .withColumn(
            "award_to_completion_days",
            when(
                col("first_result_date").isNotNull() & col("first_execution_completion_date").isNotNull(),
                datediff(col("first_execution_completion_date"), col("first_result_date")),
            ),
        )
        .drop("first_init_date", "first_result_date", "first_execution_completion_date")
    )


def _now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _lock_path(output_dir: Path) -> Path:
    return output_dir / "_meta" / "case_derived.lock"


def _pointer_path(output_dir: Path) -> Path:
    return output_dir / "CURRENT.json"


def _acquire_lock(
    output_dir: Path,
    timeout_sec: int,
    poll_sec: int,
    stale_sec: int,
    break_stale_lock: bool,
) -> str:
    lock = _lock_path(output_dir)
    token = str(uuid.uuid4())
    deadline = time.time() + timeout_sec
    lock.parent.mkdir(parents=True, exist_ok=True)
    while True:
        payload = {
            "token": token,
            "pid": os.getpid(),
            "started_at": _now_iso(),
            "host": os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME"),
        }
        try:
            fd = os.open(str(lock), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            return token
        except FileExistsError:
            if break_stale_lock and lock.exists():
                age = time.time() - lock.stat().st_mtime
                if age > stale_sec:
                    log.warning("Breaking stale lock %s age=%.1fs", lock, age)
                    lock.unlink(missing_ok=True)
                    continue
            if time.time() >= deadline:
                raise TimeoutError(f"Timed out acquiring lock: {lock}")
            time.sleep(max(1, poll_sec))


def _release_lock(output_dir: Path, token: str) -> None:
    lock = _lock_path(output_dir)
    if not lock.exists():
        return
    try:
        payload = json.loads(lock.read_text(encoding="utf-8"))
    except Exception:
        return
    if payload.get("token") == token:
        lock.unlink(missing_ok=True)


def _snapshot_root(output_dir: Path) -> Path:
    return output_dir / "snapshots"


def _list_snapshots(output_dir: Path) -> list[dict]:
    root = _snapshot_root(output_dir)
    out: list[dict] = []
    if root.exists():
        for manifest in root.glob("version=*/manifest.json"):
            try:
                m = json.loads(manifest.read_text(encoding="utf-8"))
                if m.get("asOfDate") and m.get("version"):
                    m["data_path"] = str(manifest.parent / "data")
                    out.append(m)
            except Exception:
                continue
    # Backward compatibility with legacy paths.
    for p in output_dir.glob("asOfDate=*"):
        if p.is_dir():
            asof = p.name.replace("asOfDate=", "")
            out.append(
                {
                    "version": f"legacy-{asof}",
                    "asOfDate": asof,
                    "data_path": str(p),
                    "legacy": True,
                }
            )
    return sorted(out, key=lambda x: (x.get("asOfDate", ""), x.get("version", "")))


def _latest_snapshot_before(output_dir: Path, target_date: str) -> dict | None:
    items = [s for s in _list_snapshots(output_dir) if s.get("asOfDate", "") < target_date]
    return items[-1] if items else None


def _earliest_snapshot_after(output_dir: Path, target_date: str) -> dict | None:
    items = [s for s in _list_snapshots(output_dir) if s.get("asOfDate", "") > target_date]
    return items[0] if items else None


def _parse_iso_day(day: str) -> date:
    return datetime.strptime(day, "%Y-%m-%d").date()


def _write_snapshot(df: DataFrame, output_dir: Path, target_date: str, mode: str) -> dict:
    run_id = f"{target_date}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    version_dir = _snapshot_root(output_dir) / f"version={run_id}"
    data_path = version_dir / "data"
    rows = df.count()
    df.write.mode("overwrite").parquet(str(data_path))
    manifest = {
        "version": run_id,
        "asOfDate": target_date,
        "mode": mode,
        "rows": rows,
        "created_at": _now_iso(),
        "data_path": str(data_path),
    }
    _atomic_write_json(version_dir / "manifest.json", manifest)
    return manifest


def _update_pointer(output_dir: Path, manifest: dict) -> None:
    pointer = {
        "current_version": manifest["version"],
        "asOfDate": manifest["asOfDate"],
        "updated_at": _now_iso(),
        "rows": manifest["rows"],
        "data_path": manifest["data_path"],
    }
    _atomic_write_json(_pointer_path(output_dir), pointer)


def _touched_case_ids(spark: "SparkSession", silver_dir: Path, target_date: str) -> DataFrame:
    daily_envelope = silver_dir / "common_envelope" / f"publicationDateDay={target_date}"
    if not daily_envelope.exists():
        raise ValueError(f"Missing daily envelope partition: {daily_envelope}")
    daily_df = spark.read.parquet(str(daily_envelope))
    return daily_df.select(safe_col(daily_df, "caseId", "string").alias("caseId")).filter(
        col("caseId").isNotNull()
    ).distinct()


def _touched_case_ids_in_range(
    spark: "SparkSession",
    silver_dir: Path,
    start_exclusive: str,
    end_inclusive: str,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    if not envelope_root.exists():
        raise ValueError(f"Missing envelope root: {envelope_root}")

    start_day = _parse_iso_day(start_exclusive)
    end_day = _parse_iso_day(end_inclusive)
    if start_day >= end_day:
        return spark.createDataFrame([], "caseId string")

    paths: list[str] = []
    for p in sorted(envelope_root.glob("publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        day = _parse_iso_day(token)
        if start_day < day <= end_day:
            paths.append(str(p))

    if not paths:
        return spark.createDataFrame([], "caseId string")

    daily_df = _union_paths(spark, paths, base_path=str(envelope_root))
    return (
        daily_df.select(safe_col(daily_df, "caseId", "string").alias("caseId"))
        .filter(col("caseId").isNotNull())
        .distinct()
    )


def main() -> None:
    args = _parse_args()
    if args.target_date:
        target_date = args.target_date
    else:
        target_date = (date.today() - timedelta(days=1)).isoformat()

    silver_dir = Path(args.silver_dir)
    output_dir = Path(args.output_dir)
    lock_token = _acquire_lock(
        output_dir=output_dir,
        timeout_sec=args.lock_timeout_sec,
        poll_sec=args.lock_poll_sec,
        stale_sec=args.lock_stale_sec,
        break_stale_lock=args.break_stale_lock,
    )

    from pyspark.sql import SparkSession
    spark = None
    try:
        spark = (
            SparkSession.builder.appName("bzp-silver-case-derived")
            .master(args.spark_master)
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        output_dir.mkdir(parents=True, exist_ok=True)
        if args.mode == "full":
            notices = _read_notices_merged(spark, silver_dir, target_date)
            case_df = _build_case_derived(notices).withColumn("asOfDate", lit(target_date))
            manifest = _write_snapshot(case_df, output_dir, target_date, mode="full")
            _update_pointer(output_dir, manifest)
            log.info(
                "Built full case_derived_facts snapshot asOfDate=%s rows=%d version=%s",
                target_date,
                manifest["rows"],
                manifest["version"],
            )
            return

        prev_snap = _latest_snapshot_before(output_dir, target_date)
        next_snap = _earliest_snapshot_after(output_dir, target_date)

        if prev_snap is None and next_snap is None:
            log.warning("No neighboring snapshot found around %s, falling back to full mode", target_date)
            notices = _read_notices_merged(spark, silver_dir, target_date)
            case_df = _build_case_derived(notices).withColumn("asOfDate", lit(target_date))
            manifest = _write_snapshot(case_df, output_dir, target_date, mode="full_fallback")
            _update_pointer(output_dir, manifest)
            log.info(
                "Built full (fallback) case_derived_facts asOfDate=%s rows=%d version=%s",
                target_date,
                manifest["rows"],
                manifest["version"],
            )
            return

        chosen_direction = "forward"
        anchor = prev_snap
        if prev_snap is None:
            chosen_direction = "backward"
            anchor = next_snap
        elif next_snap is not None:
            # Choose nearer anchor to minimize recomputation window.
            target_day = _parse_iso_day(target_date)
            prev_gap = (target_day - _parse_iso_day(prev_snap["asOfDate"])).days
            next_gap = (_parse_iso_day(next_snap["asOfDate"]) - target_day).days
            if next_gap < prev_gap:
                chosen_direction = "backward"
                anchor = next_snap

        assert anchor is not None
        anchor_date = anchor["asOfDate"]
        anchor_df = spark.read.parquet(anchor["data_path"])

        if chosen_direction == "forward":
            affected = _touched_case_ids_in_range(spark, silver_dir, anchor_date, target_date)
        else:
            affected = _touched_case_ids_in_range(spark, silver_dir, target_date, anchor_date)
        affected_count = affected.count()

        if affected_count == 0:
            log.info(
                "No affected cases between %s and %s; cloning snapshot",
                anchor_date,
                target_date,
            )
            out = anchor_df.drop("asOfDate").withColumn("asOfDate", lit(target_date))
            manifest = _write_snapshot(out, output_dir, target_date, mode=f"incremental_{chosen_direction}")
            _update_pointer(output_dir, manifest)
            log.info(
                "Built incremental case_derived_facts asOfDate=%s rows=%d direction=%s affected_cases=0 version=%s",
                target_date,
                manifest["rows"],
                chosen_direction,
                manifest["version"],
            )
            return

        notices_affected = _read_notices_merged(spark, silver_dir, target_date, case_ids=affected)
        recomputed = _build_case_derived(notices_affected)
        unchanged = anchor_df.join(affected, on="caseId", how="left_anti").drop("asOfDate")
        out = unchanged.unionByName(recomputed, allowMissingColumns=True).withColumn(
            "asOfDate", lit(target_date)
        )
        manifest = _write_snapshot(out, output_dir, target_date, mode=f"incremental_{chosen_direction}")
        _update_pointer(output_dir, manifest)
        log.info(
            "Built incremental case_derived_facts asOfDate=%s rows=%d direction=%s anchor=%s affected_cases=%d version=%s",
            target_date,
            manifest["rows"],
            chosen_direction,
            anchor_date,
            affected_count,
            manifest["version"],
        )
    finally:
        if spark is not None:
            spark.stop()
        _release_lock(output_dir, lock_token)


if __name__ == "__main__":
    main()
