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
import shutil
import sys
import threading
import time
import uuid
from datetime import UTC, date, datetime, timedelta
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    array,
    array_contains,
    array_distinct,
    array_sort,
    broadcast,
    collect_set,
    col,
    coalesce,
    concat,
    concat_ws,
    countDistinct,
    count,
    datediff,
    expr,
    first,
    lit,
    lower,
    max as spark_max,
    min as spark_min,
    percentile_approx,
    regexp_replace,
    sum as spark_sum,
    substring,
    to_date,
    to_json,
    to_timestamp,
    when,
    size,
    length,
    trim,
    explode_outer,
)
from pyspark.sql.types import ArrayType, DataType, StructType

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging
from procurement.common.locks import acquire_token_file_lock, release_token_file_lock

setup_logging()
log = logging.getLogger(__name__)
DEBUG_FAILING_PATH = False


def _run_with_heartbeat(label: str, fn, interval_sec: int = 60):
    stop = threading.Event()
    started = time.perf_counter()

    def _beat():
        while not stop.wait(interval_sec):
            elapsed = int(time.perf_counter() - started)
            log.info("%s still running elapsed_sec=%d", label, elapsed)

    t = threading.Thread(target=_beat, daemon=True)
    t.start()
    try:
        return fn()
    finally:
        stop.set()
        elapsed = round(time.perf_counter() - started, 2)
        log.info("%s finished elapsed_sec=%.2f", label, elapsed)


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


def _with_case_shard(
    df: DataFrame,
    shard_count: int,
    case_col: str = "caseId",
    shard_col: str = "caseId_shard",
) -> DataFrame:
    effective = max(1, int(shard_count))
    if shard_col in df.columns:
        return df.withColumn(shard_col, col(shard_col).cast("int"))
    return df.withColumn(
        shard_col,
        expr(
            f"CASE WHEN {case_col} IS NULL THEN NULL ELSE pmod(xxhash64({case_col}), {effective}) END"
        ).cast("int"),
    )


def _require_eu_lookup_parquet(parquet_path: Path) -> Path:
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Missing EU countries parquet lookup: {parquet_path}. "
            "Run one-off conversion refs/eu_countries.csv -> refs/eu_countries.parquet first."
        )
    return parquet_path


def _load_eu_country_name_lookup(
    spark: "SparkSession",
    lookup_parquet_path: Path,
) -> DataFrame:
    ref = spark.read.parquet(str(lookup_parquet_path))
    return (
        ref.select(explode_outer(col("pl_name_and_variants")).alias("country_name_ref"))
        .where(col("country_name_ref").isNotNull())
        .select(lower(trim(col("country_name_ref"))).alias("country_name_ref_norm"))
        .distinct()
    )


def _load_cpv_lookup_csv(
    spark: "SparkSession",
    csv_path: Path,
    key_len: int,
    code_alias: str,
    desc_alias: str,
) -> DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Missing CPV mapping CSV: {csv_path}")
    raw = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .csv(str(csv_path))
    )
    code_digits = regexp_replace(coalesce(col("CODE"), lit("")), "[^0-9]", "")
    return (
        raw.select(
            when(length(code_digits) >= lit(key_len), substring(code_digits, 1, key_len)).alias(
                code_alias
            ),
            trim(col("PL")).alias(desc_alias),
        )
        .where(col(code_alias).isNotNull())
        .groupBy(code_alias)
        .agg(first(col(desc_alias), ignorenulls=True).alias(desc_alias))
    )


def _add_cpv_features(
    df: DataFrame,
    cpv8_lookup: DataFrame,
    cpv4_lookup: DataFrame,
    cpv2_lookup: DataFrame,
) -> DataFrame:
    cpv_digits = regexp_replace(coalesce(col("cpvMainCode"), lit("")), "[^0-9]", "")
    out = (
        df.withColumn("cpv_8", when(length(cpv_digits) >= lit(8), substring(cpv_digits, 1, 8)))
        .withColumn("cpv_4", when(length(cpv_digits) >= lit(4), substring(cpv_digits, 1, 4)))
        .withColumn("cpv_2", when(length(cpv_digits) >= lit(2), substring(cpv_digits, 1, 2)))
    )
    out = out.join(broadcast(cpv8_lookup), on="cpv_8", how="left")
    out = out.join(broadcast(cpv4_lookup), on="cpv_4", how="left")
    out = out.join(broadcast(cpv2_lookup), on="cpv_2", how="left")
    out = out.withColumn(
        "cpv_8_display",
        when(col("cpv_8").isNotNull(), concat(col("cpv_8"), lit(": "), col("cpv_8_pl"))),
    )
    out = out.withColumn(
        "cpv_4_display",
        when(col("cpv_4").isNotNull(), concat(col("cpv_4"), lit(": "), col("cpv_4_pl"))),
    )
    out = out.withColumn(
        "cpv_2_display",
        when(col("cpv_2").isNotNull(), concat(col("cpv_2"), lit(": "), col("cpv_2_pl"))),
    )
    return out.drop("cpv_8_pl", "cpv_4_pl", "cpv_2_pl")


def _flatten_enum_items(items: list[dict]) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []

    def _join_labels(parts: list[str]) -> str:
        cleaned = [p.strip() for p in parts if p and p.strip()]
        if not cleaned:
            return ""
        return " ".join(cleaned)

    def _walk(node: dict, path_labels: list[str]) -> None:
        identifier = node.get("identifier")
        key = node.get("key")
        key_label = str(key).strip() if key is not None else ""
        full_label = _join_labels([*path_labels, key_label])
        if identifier is not None and key is not None:
            out.append((str(identifier), full_label))
        children = node.get("items")
        if children is None:
            children = node.get("Items")
        if isinstance(children, list):
            for child in children:
                if isinstance(child, dict):
                    _walk(child, [*path_labels, key_label])

    for item in items:
        if isinstance(item, dict):
            _walk(item, [])
    return out


def _load_tender_type_lookup(
    spark: "SparkSession",
    enum_paths: list[Path],
) -> DataFrame:
    pairs: list[tuple[str, str]] = []
    for path in enum_paths:
        if not path.exists():
            raise FileNotFoundError(f"Missing tenderType enum file: {path}")
        payload = json.loads(path.read_text(encoding="utf-8"))
        pairs.extend(_flatten_enum_items(payload.get("items", [])))
    if not pairs:
        return spark.createDataFrame([], "tenderType string, tenderType_value string")
    return (
        spark.createDataFrame(pairs, schema="tenderType string, tenderType_value string")
        .groupBy("tenderType")
        .agg(first(col("tenderType_value"), ignorenulls=True).alias("tenderType_value"))
    )


def _replace_tender_type_code(
    df: DataFrame,
    tender_type_lookup: DataFrame,
) -> DataFrame:
    out = df.join(broadcast(tender_type_lookup), on="tenderType", how="left")
    return (
        out.withColumn("tenderType_raw", col("tenderType"))
        .withColumn("tenderType", coalesce(col("tenderType_value"), col("tenderType")))
        .drop("tenderType_value")
    )


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
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Number of hash shards for case_derived_facts snapshot writes (1 = no practical sharding)",
    )
    parser.add_argument(
        "--shuffle-partitions",
        type=int,
        default=64,
        help="spark.sql.shuffle.partitions for case_derived job",
    )
    parser.add_argument(
        "--eu-lookup-parquet",
        default="refs/eu_countries.parquet",
        help="Parquet lookup with EU country Polish names/variants",
    )
    parser.add_argument(
        "--cpv-8-mapping-csv",
        default="refs/cpv_mapping.csv",
        help="CSV lookup for CPV 8-digit code to PL description (sep=';')",
    )
    parser.add_argument(
        "--cpv-4-mapping-csv",
        default="refs/cpv_4_mapping.csv",
        help="CSV lookup for CPV 4-digit code to PL description (sep=';')",
    )
    parser.add_argument(
        "--cpv-2-mapping-csv",
        default="refs/cpv_2_mapping.csv",
        help="CSV lookup for CPV 2-digit code to PL description (sep=';')",
    )
    parser.add_argument(
        "--tender-type-enum-017",
        default="refs/bzp_api/ENUM.017.json",
        help="Path to ENUM.017.json (tenderType dictionary)",
    )
    parser.add_argument(
        "--tender-type-enum-018",
        default="refs/bzp_api/ENUM.018.json",
        help="Path to ENUM.018.json (tenderType dictionary)",
    )
    parser.add_argument(
        "--tender-type-enum-019",
        default="refs/bzp_api/ENUM.019.json",
        help="Path to ENUM.019.json (tenderType dictionary)",
    )
    parser.add_argument(
        "--debug-failing-path",
        action="store_true",
        help="On parquet read failure, log exact failing partition/file path(s) before raising",
    )
    return parser.parse_args()


def _union_paths(spark: "SparkSession", paths: list[str], base_path: str | None = None) -> DataFrame:
    log.info("Reading parquet paths=%d base_path=%s", len(paths), base_path or "")
    if not paths:
        raise ValueError("No paths to read")
    reader = spark.read
    if base_path:
        reader = reader.option("basePath", base_path)
    # Bulk read is much faster than N reads + unionByName in Python loop.
    try:
        return reader.parquet(*paths)
    except Exception:
        if not DEBUG_FAILING_PATH:
            raise
        log.exception("Bulk parquet read failed. Starting failing-path diagnosis...")
        for p in paths:
            try:
                probe = spark.read
                if base_path:
                    probe = probe.option("basePath", base_path)
                probe_df = probe.parquet(p)
                # Force physical read of at least one row.
                probe_df.limit(1).collect()
            except Exception:
                log.exception("Failing partition path detected: %s", p)
                part_files = sorted(Path(p).glob("part-*.parquet"))
                for part_file in part_files:
                    try:
                        spark.read.parquet(str(part_file)).limit(1).collect()
                    except Exception:
                        log.exception("Failing parquet file detected: %s", part_file)
                        break
                break
        raise


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


def _specific_paths_by_notice_type_up_to(
    silver_dir: Path,
    target_date: str,
    allowed_notice_types: set[str] | None = None,
) -> dict[str, list[str]]:
    grouped: dict[str, list[str]] = {}
    for p in sorted(silver_dir.glob("notice_type_tables/noticeType=*/publicationDateDay=*")):
        if not p.is_dir():
            continue
        day_token = p.name.replace("publicationDateDay=", "")
        if day_token > target_date:
            continue
        notice_token = p.parent.name.replace("noticeType=", "")
        if allowed_notice_types is not None and notice_token not in allowed_notice_types:
            continue
        grouped.setdefault(notice_token, []).append(str(p))
    return grouped


def _read_envelope_rows(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
    shard_count: int = 64,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    envelope_paths = _paths_up_to(envelope_root, "publicationDateDay", target_date)
    if not envelope_paths:
        raise ValueError(f"Missing silver envelope inputs for <= {target_date}")

    envelope_raw = _union_paths(spark, envelope_paths, base_path=str(envelope_root))
    out = envelope_raw.select(
        safe_col(envelope_raw, "caseId", "string").alias("caseId"),
        safe_col(envelope_raw, "caseId_shard", "int").alias("caseId_shard"),
        safe_col(envelope_raw, "noticeType", "string").alias("noticeType"),
        safe_col(envelope_raw, "publicationDate", "string").alias("publicationDate"),
        safe_col(envelope_raw, "bzpNumber", "string").alias("bzpNumber"),
        safe_col(envelope_raw, "isTenderAmountBelowEU", "boolean").alias("isTenderAmountBelowEU"),
        safe_col(envelope_raw, "orderObject", "string").alias("orderObject"),
        safe_col(envelope_raw, "clientTypeName", "string").alias("clientTypeName"),
        safe_col(envelope_raw, "orderType", "string").alias("orderType"),
        safe_col(envelope_raw, "tenderType", "string").alias("tenderType"),
        safe_col(envelope_raw, "organizationName", "string").alias("organizationName"),
        safe_col(envelope_raw, "organizationCity", "string").alias("organizationCity"),
        safe_col(envelope_raw, "provinceName", "string").alias("provinceName"),
        safe_col(envelope_raw, "organizationCountry", "string").alias("organizationCountry"),
        safe_col(envelope_raw, "organizationNationalId", "string").alias("organizationNationalId"),
        safe_col(envelope_raw, "organizationNameNormalized", "string").alias("organizationNameNormalized"),
        safe_col(envelope_raw, "street", "string").alias("street"),
        safe_col(envelope_raw, "postal_code", "string").alias("postal_code"),
    ).filter(col("caseId").isNotNull())
    out = _with_case_shard(out, shard_count=shard_count)
    if case_ids is not None:
        case_keys = _with_case_shard(
            case_ids.select("caseId", *([c for c in ["caseId_shard"] if c in case_ids.columns])),
            shard_count=shard_count,
        ).select("caseId", "caseId_shard")
        out = out.join(case_keys, on=["caseId", "caseId_shard"], how="inner")
    return out


def _read_specific_rows(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
    shard_count: int = 64,
) -> DataFrame:
    notice_types_needed = {
        "AgreementIntentionNotice",
        "ContractNotice",
        "CompetitionNotice",
        "ConcessionNotice",
        "TenderResultNotice",
        "CompetitionResultNotice",
        "ConcessionAgreementNotice",
        "ContractPerformingNotice",
        "NoticeUpdateNotice",
        "NoticeUpdateConcessionNotice",
        "AgreementUpdateNotice",
        "ConcessionUpdateAgreementNotice",
    }
    specific_paths_by_type = _specific_paths_by_notice_type_up_to(
        silver_dir=silver_dir,
        target_date=target_date,
        allowed_notice_types=notice_types_needed,
    )
    if not specific_paths_by_type:
        empty = spark.createDataFrame(
            [],
            (
                "caseId string, noticeType string, publicationDate string, "
                "caseId_shard int, "
                "ai_street_512 string, value_estimated_procurement_ai_35 double, ai_prior_market_consultation_31 string, "
                "cpvMainCode_source string, numCriteria array<int>, priceWeight array<double>, cn_notice_concerns string, "
                "cn_criteria_aspects_4310_flag array<boolean>, cpvMainCode array<string>, cpvCode string, "
                "submittingOffersDate string, comp_num_awarded_63 int, value_competition_prizes_64 double, "
                "value_competition_followon_order_651 double, comp_requirements_72 string, "
                "trn_value_bid_lowest double, trn_value_bid_highest double, trn_value_winning_offer double, "
                "cpn_contractor_countries_437 array<string>"
            ),
        )
        return empty

    frames: list[DataFrame] = []
    for notice_type_token, paths in sorted(specific_paths_by_type.items()):
        frame_raw = _union_paths(
            spark=spark,
            paths=paths,
            base_path=str(silver_dir / "notice_type_tables" / f"noticeType={notice_type_token}"),
        )
        frame = frame_raw.select(
            safe_col(frame_raw, "caseId", "string").alias("caseId"),
            safe_col(frame_raw, "caseId_shard", "int").alias("caseId_shard"),
            safe_col(frame_raw, "noticeType", "string").alias("noticeType"),
            safe_col(frame_raw, "publicationDate", "string").alias("publicationDate"),
            safe_col(frame_raw, "ai_street_512", "string").alias("ai_street_512"),
            safe_col(frame_raw, "value_estimated_procurement_ai_35", "double").alias(
                "value_estimated_procurement_ai_35"
            ),
            safe_col(frame_raw, "ai_prior_market_consultation_31", "string").alias(
                "ai_prior_market_consultation_31"
            ),
            safe_col(frame_raw, "cpvMainCode_source", "string").alias("cpvMainCode_source"),
            safe_col(frame_raw, "numCriteria", "array<int>").alias("numCriteria"),
            safe_col(frame_raw, "priceWeight", "array<double>").alias("priceWeight"),
            safe_col(frame_raw, "cn_notice_concerns", "string").alias("cn_notice_concerns"),
            safe_col(frame_raw, "cn_criteria_aspects_4310_flag", "array<boolean>").alias(
                "cn_criteria_aspects_4310_flag"
            ),
            safe_col(frame_raw, "cpvMainCode", "array<string>").alias("cpvMainCode"),
            safe_col(frame_raw, "cpvCode", "string").alias("cpvCode"),
            safe_col(frame_raw, "cpvCodes", "array<string>").alias("cpvCodes"),
            safe_col(frame_raw, "submittingOffersDate", "string").alias("submittingOffersDate"),
            safe_col(frame_raw, "comp_num_awarded_63", "int").alias("comp_num_awarded_63"),
            safe_col(frame_raw, "value_competition_prizes_64", "double").alias(
                "value_competition_prizes_64"
            ),
            safe_col(frame_raw, "value_competition_followon_order_651", "double").alias(
                "value_competition_followon_order_651"
            ),
            safe_col(frame_raw, "comp_requirements_72", "string").alias("comp_requirements_72"),
            safe_col(frame_raw, "procedureResultParsed", "array<string>").alias("procedureResultParsed"),
            safe_col(frame_raw, "trn_notice_concerns", "string").alias("trn_notice_concerns"),
            safe_col(frame_raw, "trn_value_bid_lowest", "double").alias("trn_value_bid_lowest"),
            safe_col(frame_raw, "trn_value_bid_highest", "double").alias("trn_value_bid_highest"),
            safe_col(frame_raw, "trn_value_winning_offer", "double").alias("trn_value_winning_offer"),
            safe_col(frame_raw, "cpn_contract_date_41", "string").alias("cpn_contract_date_41"),
            safe_col(frame_raw, "cpn_execution_end_date_52", "string").alias("cpn_execution_end_date_52"),
            safe_col(frame_raw, "cpn_contractor_countries_437", "array<string>").alias(
                "cpn_contractor_countries_437"
            ),
            safe_col(frame_raw, "executed_in_time", "boolean").alias("executed_in_time"),
            safe_col(frame_raw, "proper_execution", "boolean").alias("proper_execution"),
            safe_col(frame_raw, "value_contract_reported_execution_44", "double").alias(
                "value_contract_reported_execution_44"
            ),
            safe_col(frame_raw, "value_paid_total_55", "double").alias("value_paid_total_55"),
            (
                to_json(col("trn_parts"))
                if has_field(frame_raw, "trn_parts")
                else lit(None).cast("string")
            ).alias("trn_parts"),
        )
        frames.append(frame)
    out = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames).filter(
        col("caseId").isNotNull()
    )
    out = _with_case_shard(out, shard_count=shard_count)
    if case_ids is not None:
        case_keys = _with_case_shard(
            case_ids.select("caseId", *([c for c in ["caseId_shard"] if c in case_ids.columns])),
            shard_count=shard_count,
        ).select("caseId", "caseId_shard")
        out = out.join(case_keys, on=["caseId", "caseId_shard"], how="inner")
    return out


def _build_notice_specific_features(specific_rows: DataFrame) -> DataFrame:
    rows = specific_rows.withColumn("publication_ts", to_timestamp(col("publicationDate")))
    grouped = rows.groupBy("caseId")

    cpv_candidate_expr = (
        "CASE WHEN noticeType IN ('AgreementIntentionNotice','ContractNotice','CompetitionNotice','ConcessionNotice') "
        "THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END"
    )
    cpv_pick = spark_min(
        expr(
            "named_struct("
            f"'is_null', CASE WHEN {cpv_candidate_expr} IS NULL THEN 1 ELSE 0 END, "
            "'ts', publication_ts, "
            f"'src', CASE WHEN {cpv_candidate_expr} IS NOT NULL THEN noticeType END, "
            f"'v', {cpv_candidate_expr}"
            ")"
        )
    ).alias("__cpv_pick")

    out = grouped.agg(
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='AgreementIntentionNotice' "
                "THEN CASE WHEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END)"
            )
        ).getField("v").alias("ai_cpvMainCode"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='AgreementIntentionNotice' "
                "THEN CASE WHEN ai_street_512 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN ai_street_512 END)"
            )
        ).getField("v").alias("contractor_street"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='AgreementIntentionNotice' "
                "THEN CASE WHEN value_estimated_procurement_ai_35 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN value_estimated_procurement_ai_35 END)"
            )
        ).getField("v").alias("value_estimated_procurement_ai"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='AgreementIntentionNotice' "
                "THEN CASE WHEN ai_prior_market_consultation_31 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN ai_prior_market_consultation_31 END)"
            )
        ).getField("v").alias("ai_prior_market_consultation"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='AgreementIntentionNotice' "
                "THEN CASE WHEN cpvMainCode_source IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN cpvMainCode_source END)"
            )
        ).getField("v").alias("cpvMainCode_source"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractNotice' "
                "THEN CASE WHEN size(numCriteria) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractNotice' THEN size(numCriteria) END)"
            )
        ).getField("v").alias("num_parts"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractNotice' "
                "THEN CASE WHEN numCriteria IS NOT NULL AND size(numCriteria) > 0 THEN 0 ELSE 1 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractNotice' AND numCriteria IS NOT NULL AND size(numCriteria) > 0 "
                "THEN aggregate(transform(numCriteria, x -> cast(x as double)), cast(0.0 as double), (acc, x) -> acc + x) / size(numCriteria) END)"
            )
        ).getField("v").alias("av_numCriteria"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractNotice' "
                "THEN CASE WHEN priceWeight IS NOT NULL AND size(priceWeight) > 0 THEN 0 ELSE 1 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractNotice' AND priceWeight IS NOT NULL AND size(priceWeight) > 0 "
                "THEN aggregate(transform(priceWeight, x -> cast(x as double)), cast(0.0 as double), (acc, x) -> acc + x) / size(priceWeight) END)"
            )
        ).getField("v").alias("priceWeight"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractNotice' "
                "THEN CASE WHEN cn_notice_concerns IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractNotice' THEN cn_notice_concerns END)"
            )
        ).getField("v").alias("cn_notice_concerns"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractNotice' "
                "THEN CASE WHEN cn_criteria_aspects_4310_flag IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractNotice' THEN cn_criteria_aspects_4310_flag END)"
            )
        ).getField("v").alias("criteria_esg"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType IN ('ContractNotice','ConcessionNotice') "
                "THEN CASE WHEN submittingOffersDate IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType IN ('ContractNotice','ConcessionNotice') THEN submittingOffersDate END)"
            )
        ).getField("v").alias("submittingOffersDate"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionNotice' "
                "THEN CASE WHEN comp_num_awarded_63 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionNotice' THEN comp_num_awarded_63 END)"
            )
        ).getField("v").alias("comp_num_awarded_63"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionNotice' "
                "THEN CASE WHEN value_competition_prizes_64 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionNotice' THEN value_competition_prizes_64 END)"
            )
        ).getField("v").alias("value_competition_prizes_64"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionNotice' "
                "THEN CASE WHEN value_competition_followon_order_651 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionNotice' THEN value_competition_followon_order_651 END)"
            )
        ).getField("v").alias("value_competition_followon_order_651"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionNotice' "
                "THEN CASE WHEN comp_requirements_72 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionNotice' THEN comp_requirements_72 END)"
            )
        ).getField("v").alias("comp_requirements_72"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN procedureResultParsed IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN procedureResultParsed END)"
            )
        ).getField("v").alias("procedureResultParsed"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_parts IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN trn_parts END)"
            )
        ).getField("v").alias("trn_parts"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_notice_concerns IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN trn_notice_concerns END)"
            )
        ).getField("v").alias("trn_notice_concerns"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_value_bid_lowest IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN trn_value_bid_lowest END)"
            )
        ).getField("v").alias("trn_value_bid_lowest"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_value_bid_highest IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN trn_value_bid_highest END)"
            )
        ).getField("v").alias("trn_value_bid_highest"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_value_winning_offer IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN trn_value_winning_offer END)"
            )
        ).getField("v").alias("trn_value_winning_offer"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_value_bid_lowest IS NOT NULL AND trn_value_bid_highest IS NOT NULL THEN 0 ELSE 1 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' AND trn_value_bid_lowest IS NOT NULL AND trn_value_bid_highest IS NOT NULL "
                "THEN trn_value_bid_highest - trn_value_bid_lowest END)"
            )
        ).getField("v").alias("offers_value_spread"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN trn_value_bid_lowest IS NOT NULL AND trn_value_bid_highest IS NOT NULL AND trn_value_winning_offer IS NOT NULL AND trn_value_winning_offer <> 0 THEN 0 ELSE 1 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' AND trn_value_bid_lowest IS NOT NULL AND trn_value_bid_highest IS NOT NULL AND trn_value_winning_offer IS NOT NULL AND trn_value_winning_offer <> 0 "
                "THEN (trn_value_bid_highest - trn_value_bid_lowest) / trn_value_winning_offer END)"
            )
        ).getField("v").alias("offers_value_spread_standardized"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='TenderResultNotice' "
                "THEN CASE WHEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='TenderResultNotice' THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END)"
            )
        ).getField("v").alias("trn_cpvMainCode"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionResultNotice' "
                "THEN CASE WHEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionResultNotice' THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END)"
            )
        ).getField("v").alias("competition_result_cpvMainCode"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ConcessionAgreementNotice' "
                "THEN CASE WHEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ConcessionAgreementNotice' THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END)"
            )
        ).getField("v").alias("concession_agreement_cpvMainCode"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='CompetitionResultNotice' "
                "THEN CASE WHEN trn_notice_concerns IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionResultNotice' THEN trn_notice_concerns END)"
            )
        ).getField("v").alias("competition_result_notice_concerns"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ConcessionAgreementNotice' "
                "THEN CASE WHEN trn_notice_concerns IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ConcessionAgreementNotice' THEN trn_notice_concerns END)"
            )
        ).getField("v").alias("concession_agreement_notice_concerns"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN coalesce(element_at(cpvMainCode, 1), cpvCode, element_at(cpvCodes, 1)) END)"
            )
        ).getField("v").alias("cpn_cpvMainCode"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN cpn_contract_date_41 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN cpn_contract_date_41 END)"
            )
        ).getField("v").alias("cpn_contract_date_41"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN cpn_execution_end_date_52 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN cpn_execution_end_date_52 END)"
            )
        ).getField("v").alias("cpn_execution_end_date"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN executed_in_time IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN executed_in_time END)"
            )
        ).getField("v").alias("executed_in_time"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN proper_execution IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN proper_execution END)"
            )
        ).getField("v").alias("proper_execution"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN value_contract_reported_execution_44 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN value_contract_reported_execution_44 END)"
            )
        ).getField("v").alias("value_contract_reported_execution"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ContractPerformingNotice' "
                "THEN CASE WHEN value_paid_total_55 IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN value_paid_total_55 END)"
            )
        ).getField("v").alias("value_paid_total_55"),
        cpv_pick,
    )
    return (
        out.withColumn("cpvMainCode_init", col("__cpv_pick.v"))
        .withColumn("cpvMainCode_source_init", col("__cpv_pick.src"))
        .drop("__cpv_pick")
    )


def _build_cpn_contractor_non_eu_flag(
    specific_rows: DataFrame,
    eu_country_name_lookup: DataFrame,
) -> DataFrame:
    cpn_rows = specific_rows.filter(col("noticeType") == lit("ContractPerformingNotice"))
    exploded = (
        cpn_rows.select(
            col("caseId"),
            explode_outer(col("cpn_contractor_countries_437")).alias("country_name_raw"),
        )
        .where(col("caseId").isNotNull())
        .withColumn("country_name_norm", lower(trim(col("country_name_raw"))))
    )
    tagged = exploded.join(
        eu_country_name_lookup,
        exploded.country_name_norm == eu_country_name_lookup.country_name_ref_norm,
        how="left",
    )
    return tagged.groupBy("caseId").agg(
        when(
            spark_max(when(col("country_name_norm").isNotNull(), lit(1)).otherwise(lit(0))) == lit(0),
            lit(None).cast("boolean"),
        )
        .otherwise(
            spark_max(
                when(
                    col("country_name_norm").isNotNull()
                    & col("country_name_ref_norm").isNull(),
                    lit(1),
                ).otherwise(lit(0))
            )
            == lit(1)
        )
        .alias("contractor_non_eu")
    )


def _apply_result_fallbacks(df: DataFrame) -> DataFrame:
    cond_trn = (
        col("cpvMainCode_init").isNull()
        & (~col("has_ContractNotice"))
        & (~col("has_AgreementIntentionNotice"))
        & col("trn_cpvMainCode").isNotNull()
    )
    cond_comp = (
        col("cpvMainCode_init").isNull()
        & (~cond_trn)
        & (~col("has_CompetitionNotice"))
        & col("competition_result_cpvMainCode").isNotNull()
    )
    cond_concession = (
        col("cpvMainCode_init").isNull()
        & (~cond_trn)
        & (~cond_comp)
        & (~col("has_ConcessionNotice"))
        & col("concession_agreement_cpvMainCode").isNotNull()
    )
    cond_cpn = (
        col("cpvMainCode_init").isNull()
        & (~cond_trn)
        & (~cond_comp)
        & (~cond_concession)
        & (~col("has_ContractNotice"))
        & (~col("has_TenderResultNotice"))
        & col("cpn_cpvMainCode").isNotNull()
    )

    out = (
        df.withColumn(
            "cpvMainCode",
            when(cond_trn, col("trn_cpvMainCode"))
            .when(cond_comp, col("competition_result_cpvMainCode"))
            .when(cond_concession, col("concession_agreement_cpvMainCode"))
            .when(cond_cpn, col("cpn_cpvMainCode"))
            .otherwise(col("cpvMainCode_init")),
        )
        .withColumn(
            "cpvMainCode_source",
            when(cond_trn, lit("TenderResultNotice"))
            .when(cond_comp, lit("CompetitionResultNotice"))
            .when(cond_concession, lit("ConcessionAgreementNotice"))
            .when(cond_cpn, lit("ContractPerformingNotice"))
            .otherwise(col("cpvMainCode_source_init")),
        )
    )
    out = out.withColumn(
        "cpv_drift_trn",
        when(
            col("trn_cpvMainCode").isNotNull() & col("cpvMainCode").isNotNull(),
            col("trn_cpvMainCode") != col("cpvMainCode"),
        ).otherwise(lit(None).cast("boolean")),
    )
    out = out.withColumn(
        "cpv_drift_cpn",
        when(
            col("cpn_cpvMainCode").isNotNull() & col("cpvMainCode").isNotNull(),
            col("cpn_cpvMainCode") != col("cpvMainCode"),
        ).otherwise(lit(None).cast("boolean")),
    )
    out = out.withColumn(
        "notice_concerns_source",
        when(
            (~col("has_CompetitionNotice")) & col("competition_result_notice_concerns").isNotNull(),
            lit("CompetitionResultNotice"),
        )
        .when(
            (~col("has_ConcessionNotice")) & col("concession_agreement_notice_concerns").isNotNull(),
            lit("ConcessionAgreementNotice"),
        )
        .otherwise(lit(None).cast("string")),
    )
    out = out.withColumn(
        "paid_ratio",
        when(
            col("value_contract_reported_execution").isNotNull()
            & (col("value_contract_reported_execution") != lit(0))
            & col("value_paid_total_55").isNotNull(),
            col("value_paid_total_55") / col("value_contract_reported_execution"),
        ).otherwise(lit(None).cast("double")),
    )
    return out.withColumn("cpv_source", col("cpvMainCode_source"))


def _build_case_derived(envelope_rows: DataFrame) -> DataFrame:
    fields = [
        "bzpNumber",
        "isTenderAmountBelowEU",
        "orderObject",
        "clientTypeName",
        "orderType",
        "tenderType",
        "organizationName",
        "organizationCity",
        "provinceName",
        "organizationCountry",
        "organizationNationalId",
        "organizationNameNormalized",
        "street",
        "postal_code",
    ]

    rows = (
        envelope_rows.withColumn("publication_ts", to_timestamp(col("publicationDate")))
        .withColumn("publication_date", to_date(col("publicationDate")))
    )
    grouped = rows.groupBy("caseId")

    agg_exprs = []
    for field in fields:
        agg_exprs.append(
            spark_min(
                expr(
                    f"named_struct('is_null', CASE WHEN {field} IS NULL THEN 1 ELSE 0 END, "
                    f"'ts', publication_ts, 'v', {field})"
                )
            ).getField("v").alias(field)
        )

    inconsistency_flags = [
        countDistinct(when(col(field).isNotNull(), col(field))).alias(f"__cnt_{field}")
        for field in fields
    ]

    out = grouped.agg(
        *agg_exprs,
        *inconsistency_flags,
        array_sort(array_distinct(collect_set(col("noticeType")))).alias("notice_types_set"),
        spark_min(col("publication_date")).alias("first_publication_date"),
        spark_max(col("publication_date")).alias("last_publication_date"),
        count(lit(1)).cast("long").alias("num_notices_total"),
        spark_sum(when(col("noticeType") == lit("NoticeUpdateNotice"), lit(1)).otherwise(lit(0)))
        .cast("long")
        .alias("n_changes_init_notice"),
        spark_sum(when(col("noticeType") == lit("NoticeUpdateConcession"), lit(1)).otherwise(lit(0)))
        .cast("long")
        .alias("n_changes_concession_init_notice"),
        spark_sum(when(col("noticeType") == lit("AgreementUpdateNotice"), lit(1)).otherwise(lit(0)))
        .cast("long")
        .alias("n_changes_agreement_notice"),
        spark_sum(
            when(col("noticeType") == lit("ConcessionUpdateAgreementNotice"), lit(1)).otherwise(lit(0))
        )
        .cast("long")
        .alias("n_changes_concession_agreement_notice"),
    )

    out = out.withColumn(
        "inconsistent_fields",
        expr(
            "filter(array({}), x -> x is not null)".format(
                ", ".join([f"CASE WHEN __cnt_{f} > 1 THEN '{f}' ELSE NULL END" for f in fields])
            )
        ),
    )
    out = out.withColumn("is_envelope_consistent", size(col("inconsistent_fields")) == lit(0))
    out = (
        out.withColumn("has_ContractNotice", array_contains(col("notice_types_set"), lit("ContractNotice")))
        .withColumn(
            "has_AgreementIntentionNotice",
            array_contains(col("notice_types_set"), lit("AgreementIntentionNotice")),
        )
        .withColumn(
            "has_TenderResultNotice",
            array_contains(col("notice_types_set"), lit("TenderResultNotice")),
        )
        .withColumn(
            "has_ContractPerformingNotice",
            array_contains(col("notice_types_set"), lit("ContractPerformingNotice")),
        )
        .withColumn(
            "has_CompetitionNotice",
            array_contains(col("notice_types_set"), lit("CompetitionNotice")),
        )
        .withColumn(
            "has_CompetitionResultNotice",
            array_contains(col("notice_types_set"), lit("CompetitionResultNotice")),
        )
        .withColumn(
            "has_SmallContractNotice",
            array_contains(col("notice_types_set"), lit("SmallContractNotice")),
        )
        .withColumn(
            "has_ConcessionNotice",
            array_contains(col("notice_types_set"), lit("ConcessionNotice")),
        )
        .withColumn(
            "has_ConcessionIntentionAgreementNotice",
            array_contains(col("notice_types_set"), lit("ConcessionIntentionAgreementNotice")),
        )
        .withColumn(
            "has_ConcessionAgreementNotice",
            array_contains(col("notice_types_set"), lit("ConcessionAgreementNotice")),
        )
    )
    out = out.withColumn(
        "has_init",
        (
            col("has_ContractNotice")
            | col("has_AgreementIntentionNotice")
            | col("has_CompetitionNotice")
            | col("has_SmallContractNotice")
            | col("has_ConcessionNotice")
            | col("has_ConcessionIntentionAgreementNotice")
        ),
    )
    out = out.withColumn(
        "has_result",
        (
            col("has_TenderResultNotice")
            | col("has_CompetitionResultNotice")
            | col("has_ConcessionAgreementNotice")
        ),
    )
    out = out.withColumn("has_execution", col("has_ContractPerformingNotice"))
    out = out.withColumn(
        "case_type",
        when(
            col("has_CompetitionNotice") | col("has_CompetitionResultNotice"),
            lit("COMPETITION"),
        )
        .when(col("has_SmallContractNotice"), lit("SMALL_CONTRACT"))
        .when(col("has_ConcessionIntentionAgreementNotice"), lit("CONCESSION_INTENTION"))
        .when(
            col("has_ConcessionNotice") | col("has_ConcessionAgreementNotice"),
            lit("CONCESSION_NOTICE"),
        )
        .when(col("has_AgreementIntentionNotice"), lit("PROCUREMENT_INTENTION"))
        .when(
            col("has_ContractNotice")
            | col("has_TenderResultNotice")
            | col("has_ContractPerformingNotice"),
            lit("PROCUREMENT_COMPETITIVE"),
        )
        .otherwise(lit("UNKNOWN")),
    )

    drop_cols = [f"__cnt_{f}" for f in fields]
    return out.drop(*drop_cols)


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
    payload = {
        "token": token,
        "pid": os.getpid(),
        "started_at": _now_iso(),
        "host": os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME"),
    }
    acquire_token_file_lock(
        lock_path=lock,
        payload=payload,
        timeout_sec=timeout_sec,
        poll_sec=poll_sec,
        stale_sec=stale_sec,
        break_stale_lock=break_stale_lock,
    )
    return token


def _release_lock(output_dir: Path, token: str) -> None:
    release_token_file_lock(_lock_path(output_dir), token=token, token_key="token")


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


def _write_snapshot(
    df: DataFrame,
    output_dir: Path,
    target_date: str,
    mode: str,
    shard_count: int,
) -> dict:
    run_id = f"{target_date}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    version_dir = _snapshot_root(output_dir) / f"version={run_id}"
    data_path = version_dir / "data"
    effective_shards = max(1, int(shard_count))
    log.info("Snapshot write start mode=%s asOfDate=%s run_id=%s", mode, target_date, run_id)
    rows = None
    write_df = df
    if "shard" in write_df.columns:
        write_df = write_df.drop("shard")
    write_df = write_df.withColumn(
        "shard",
        expr(
            f"CASE WHEN caseId IS NULL THEN 0 ELSE pmod(xxhash64(caseId), {effective_shards}) END"
        ).cast("int"),
    )
    from pyspark.storagelevel import StorageLevel
    cached_write_df = write_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows = _run_with_heartbeat(
            "snapshot_count_rows",
            lambda: cached_write_df.count(),
            interval_sec=60,
        )
        _run_with_heartbeat(
            "snapshot_write_partitioned",
            lambda: cached_write_df.write.mode("overwrite").partitionBy("shard").parquet(str(data_path)),
            interval_sec=60,
        )
    finally:
        cached_write_df.unpersist()
    manifest = {
        "version": run_id,
        "asOfDate": target_date,
        "mode": mode,
        "rows": rows,
        "partitioning": {"strategy": "case_id_hash_shard", "shard_count": effective_shards},
        "created_at": _now_iso(),
        "data_path": str(data_path),
    }
    _atomic_write_json(version_dir / "manifest.json", manifest)
    log.info(
        "Snapshot write complete mode=%s asOfDate=%s run_id=%s rows=%d path=%s",
        mode,
        target_date,
        run_id,
        rows,
        data_path,
    )
    return manifest


def _read_snapshot_shard_or_empty(
    spark: "SparkSession",
    shard_path: Path,
    empty_schema_df: DataFrame,
) -> DataFrame:
    if shard_path.exists():
        return spark.read.parquet(str(shard_path))
    return empty_schema_df.limit(0)


def _write_snapshot_incremental_by_shard(
    spark: "SparkSession",
    anchor_manifest: dict,
    recomputed_df: DataFrame,
    affected_case_ids: DataFrame,
    output_dir: Path,
    target_date: str,
    mode: str,
    shard_count: int,
) -> dict:
    effective_shards = max(1, int(shard_count))
    run_id = f"{target_date}_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    version_dir = _snapshot_root(output_dir) / f"version={run_id}"
    data_path = version_dir / "data"
    anchor_data_path = Path(anchor_manifest["data_path"])

    if version_dir.exists():
        shutil.rmtree(version_dir, ignore_errors=False)
    data_path.mkdir(parents=True, exist_ok=True)
    log.info(
        "Incremental shard write start asOfDate=%s mode=%s run_id=%s shard_count=%d",
        target_date,
        mode,
        run_id,
        effective_shards,
    )

    affected_keys = (
        _with_case_shard(affected_case_ids.select("caseId", "caseId_shard"), shard_count=effective_shards)
        .select("caseId", "caseId_shard")
        .where(col("caseId").isNotNull() & col("caseId_shard").isNotNull())
        .distinct()
    )
    affected_shards = {
        int(r["caseId_shard"])
        for r in affected_keys.select("caseId_shard").distinct().collect()
    }
    log.info(
        "Incremental affected cases=%d affected_shards=%d",
        affected_keys.count(),
        len(affected_shards),
    )

    recomputed_with_shard = _with_case_shard(recomputed_df, shard_count=effective_shards).drop("asOfDate")
    base_schema_df = recomputed_with_shard.limit(0).withColumn("asOfDate", lit(target_date))

    processed_shards = 0
    for shard in range(effective_shards):
        shard_dir_name = f"shard={shard}"
        src_shard_path = anchor_data_path / shard_dir_name
        dst_shard_path = data_path / shard_dir_name

        if shard not in affected_shards:
            if src_shard_path.exists():
                shutil.copytree(src_shard_path, dst_shard_path, dirs_exist_ok=False)
            processed_shards += 1
            if processed_shards % 8 == 0 or processed_shards == effective_shards:
                log.info(
                    "Incremental shard progress processed=%d/%d (copy/pass-through)",
                    processed_shards,
                    effective_shards,
                )
            continue

        log.info("Incremental shard=%d merge start", shard)
        anchor_shard = _read_snapshot_shard_or_empty(spark, src_shard_path, base_schema_df)
        affected_case_ids_shard = affected_keys.where(col("caseId_shard") == lit(shard)).select("caseId").distinct()
        unchanged_shard = anchor_shard.join(affected_case_ids_shard, on="caseId", how="left_anti").drop(
            "asOfDate",
            "shard",
        )
        recomputed_shard = recomputed_with_shard.where(col("caseId_shard") == lit(shard)).drop("caseId_shard")
        out_shard = unchanged_shard.unionByName(recomputed_shard, allowMissingColumns=True).withColumn(
            "asOfDate", lit(target_date)
        )
        out_shard.write.mode("overwrite").parquet(str(dst_shard_path))
        processed_shards += 1
        log.info(
            "Incremental shard=%d merge complete processed=%d/%d",
            shard,
            processed_shards,
            effective_shards,
        )

    rows = _run_with_heartbeat(
        "incremental_snapshot_count_rows",
        lambda: spark.read.parquet(str(data_path)).count(),
        interval_sec=60,
    )
    manifest = {
        "version": run_id,
        "asOfDate": target_date,
        "mode": mode,
        "rows": rows,
        "partitioning": {"strategy": "case_id_hash_shard", "shard_count": effective_shards},
        "created_at": _now_iso(),
        "data_path": str(data_path),
    }
    _atomic_write_json(version_dir / "manifest.json", manifest)
    log.info(
        "Incremental shard write complete asOfDate=%s run_id=%s rows=%d",
        target_date,
        run_id,
        rows,
    )
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
    return (
        daily_df.select(
            safe_col(daily_df, "caseId", "string").alias("caseId"),
            safe_col(daily_df, "caseId_shard", "int").alias("caseId_shard"),
        )
        .filter(col("caseId").isNotNull())
        .distinct()
    )


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
        return spark.createDataFrame([], "caseId string, caseId_shard int")

    paths: list[str] = []
    for p in sorted(envelope_root.glob("publicationDateDay=*")):
        if not p.is_dir():
            continue
        token = p.name.replace("publicationDateDay=", "")
        day = _parse_iso_day(token)
        if start_day < day <= end_day:
            paths.append(str(p))

    if not paths:
        return spark.createDataFrame([], "caseId string, caseId_shard int")

    daily_df = _union_paths(spark, paths, base_path=str(envelope_root))
    return (
        daily_df.select(
            safe_col(daily_df, "caseId", "string").alias("caseId"),
            safe_col(daily_df, "caseId_shard", "int").alias("caseId_shard"),
        )
        .filter(col("caseId").isNotNull())
        .distinct()
    )


def main() -> None:
    args = _parse_args()
    global DEBUG_FAILING_PATH
    DEBUG_FAILING_PATH = bool(args.debug_failing_path)
    if DEBUG_FAILING_PATH:
        log.info("Parquet failing-path diagnostics enabled")
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
    eu_country_name_lookup = None
    cpv8_lookup = None
    cpv4_lookup = None
    cpv2_lookup = None
    tender_type_lookup = None
    try:
        spark = (
            SparkSession.builder.appName("bzp-silver-case-derived")
            .master(args.spark_master)
            .config("spark.pyspark.python", sys.executable)
            .config("spark.pyspark.driver.python", sys.executable)
            .config("spark.sql.ansi.enabled", "false")
            .getOrCreate()
        )
        if int(args.shuffle_partitions) > 0:
            spark.conf.set("spark.sql.shuffle.partitions", str(int(args.shuffle_partitions)))
            log.info("Configured spark.sql.shuffle.partitions=%s", int(args.shuffle_partitions))
        output_dir.mkdir(parents=True, exist_ok=True)
        log.info(
            "case_derived_facts start mode=%s target_date=%s silver_dir=%s output_dir=%s",
            args.mode,
            target_date,
            silver_dir,
            output_dir,
        )
        from pyspark.storagelevel import StorageLevel

        eu_lookup_path = _require_eu_lookup_parquet(Path(args.eu_lookup_parquet))
        log.info("Loading EU lookup parquet path=%s", eu_lookup_path)
        eu_country_name_lookup = _load_eu_country_name_lookup(spark, eu_lookup_path).persist(
            StorageLevel.MEMORY_ONLY
        )
        # Materialize once and keep cached for all feature joins in this run.
        eu_rows = _run_with_heartbeat(
            "eu_lookup_cache_materialize",
            lambda: eu_country_name_lookup.count(),
            interval_sec=30,
        )
        log.info("EU lookup cached rows=%d", eu_rows)
        cpv8_lookup = _load_cpv_lookup_csv(
            spark=spark,
            csv_path=Path(args.cpv_8_mapping_csv),
            key_len=8,
            code_alias="cpv_8",
            desc_alias="cpv_8_pl",
        ).persist(StorageLevel.MEMORY_ONLY)
        cpv4_lookup = _load_cpv_lookup_csv(
            spark=spark,
            csv_path=Path(args.cpv_4_mapping_csv),
            key_len=4,
            code_alias="cpv_4",
            desc_alias="cpv_4_pl",
        ).persist(StorageLevel.MEMORY_ONLY)
        cpv2_lookup = _load_cpv_lookup_csv(
            spark=spark,
            csv_path=Path(args.cpv_2_mapping_csv),
            key_len=2,
            code_alias="cpv_2",
            desc_alias="cpv_2_pl",
        ).persist(StorageLevel.MEMORY_ONLY)
        log.info(
            "CPV lookups cached rows cpv8=%d cpv4=%d cpv2=%d",
            cpv8_lookup.count(),
            cpv4_lookup.count(),
            cpv2_lookup.count(),
        )
        tender_type_lookup = _load_tender_type_lookup(
            spark=spark,
            enum_paths=[
                Path(args.tender_type_enum_017),
                Path(args.tender_type_enum_018),
                Path(args.tender_type_enum_019),
            ],
        ).persist(StorageLevel.MEMORY_ONLY)
        log.info("TenderType lookup cached rows=%d", tender_type_lookup.count())

        if args.mode == "full":
            log.info("Full mode: reading envelope+specific rows")
            envelope_rows = _read_envelope_rows(
                spark, silver_dir, target_date, shard_count=args.shard_count
            )
            specific_rows = _read_specific_rows(
                spark, silver_dir, target_date, shard_count=args.shard_count
            )
            log.info("Full mode: building feature tables")
            specific_features = _build_notice_specific_features(specific_rows)
            cpn_country_flags = _build_cpn_contractor_non_eu_flag(
                specific_rows, eu_country_name_lookup
            )
            log.info("Full mode: building case derived dataframe")
            case_df = _apply_result_fallbacks(
                _build_case_derived(envelope_rows).join(
                    specific_features, on="caseId", how="left"
                )
            ).join(cpn_country_flags, on="caseId", how="left").withColumn("asOfDate", lit(target_date))
            case_df = _add_cpv_features(case_df, cpv8_lookup, cpv4_lookup, cpv2_lookup)
            case_df = _replace_tender_type_code(case_df, tender_type_lookup)
            manifest = _write_snapshot(
                case_df,
                output_dir,
                target_date,
                mode="full",
                shard_count=args.shard_count,
            )
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
            log.info("Full fallback mode: reading envelope+specific rows")
            envelope_rows = _read_envelope_rows(
                spark, silver_dir, target_date, shard_count=args.shard_count
            )
            specific_rows = _read_specific_rows(
                spark, silver_dir, target_date, shard_count=args.shard_count
            )
            log.info("Full fallback mode: building feature tables")
            specific_features = _build_notice_specific_features(specific_rows)
            cpn_country_flags = _build_cpn_contractor_non_eu_flag(
                specific_rows, eu_country_name_lookup
            )
            log.info("Full fallback mode: building case derived dataframe")
            case_df = _apply_result_fallbacks(
                _build_case_derived(envelope_rows).join(
                    specific_features, on="caseId", how="left"
                )
            ).join(cpn_country_flags, on="caseId", how="left").withColumn("asOfDate", lit(target_date))
            case_df = _add_cpv_features(case_df, cpv8_lookup, cpv4_lookup, cpv2_lookup)
            case_df = _replace_tender_type_code(case_df, tender_type_lookup)
            manifest = _write_snapshot(
                case_df,
                output_dir,
                target_date,
                mode="full_fallback",
                shard_count=args.shard_count,
            )
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

        if chosen_direction == "forward":
            affected = _touched_case_ids_in_range(spark, silver_dir, anchor_date, target_date)
        else:
            affected = _touched_case_ids_in_range(spark, silver_dir, target_date, anchor_date)
        affected = _with_case_shard(affected, shard_count=args.shard_count)
        affected_count = affected.count()
        log.info(
            "Incremental mode: direction=%s anchor=%s target=%s affected_cases=%d",
            chosen_direction,
            anchor_date,
            target_date,
            affected_count,
        )

        if affected_count == 0:
            log.info(
                "No affected cases between %s and %s; cloning snapshot",
                anchor_date,
                target_date,
            )
            anchor_df = spark.read.parquet(anchor["data_path"])
            out = anchor_df.drop("asOfDate").withColumn("asOfDate", lit(target_date))
            out = _add_cpv_features(out, cpv8_lookup, cpv4_lookup, cpv2_lookup)
            out = _replace_tender_type_code(out, tender_type_lookup)
            manifest = _write_snapshot(
                out,
                output_dir,
                target_date,
                mode=f"incremental_{chosen_direction}",
                shard_count=args.shard_count,
            )
            _update_pointer(output_dir, manifest)
            log.info(
                "Built incremental case_derived_facts asOfDate=%s rows=%d direction=%s affected_cases=0 version=%s",
                target_date,
                manifest["rows"],
                chosen_direction,
                manifest["version"],
            )
            return

        envelope_affected = _read_envelope_rows(
            spark,
            silver_dir,
            target_date,
            case_ids=affected,
            shard_count=args.shard_count,
        )
        specific_affected = _read_specific_rows(
            spark,
            silver_dir,
            target_date,
            case_ids=affected,
            shard_count=args.shard_count,
        )
        log.info("Incremental mode: building recomputed affected dataframe")
        specific_features = _build_notice_specific_features(specific_affected)
        cpn_country_flags = _build_cpn_contractor_non_eu_flag(
            specific_affected, eu_country_name_lookup
        )
        recomputed = _apply_result_fallbacks(
            _build_case_derived(envelope_affected).join(
                specific_features, on="caseId", how="left"
            )
        ).join(cpn_country_flags, on="caseId", how="left")
        recomputed = _add_cpv_features(recomputed, cpv8_lookup, cpv4_lookup, cpv2_lookup)
        recomputed = _replace_tender_type_code(recomputed, tender_type_lookup)
        manifest = _write_snapshot_incremental_by_shard(
            spark=spark,
            anchor_manifest=anchor,
            recomputed_df=recomputed,
            affected_case_ids=affected,
            output_dir=output_dir,
            target_date=target_date,
            mode=f"incremental_{chosen_direction}",
            shard_count=args.shard_count,
        )
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
        if cpv8_lookup is not None:
            cpv8_lookup.unpersist()
        if cpv4_lookup is not None:
            cpv4_lookup.unpersist()
        if cpv2_lookup is not None:
            cpv2_lookup.unpersist()
        if tender_type_lookup is not None:
            tender_type_lookup.unpersist()
        if eu_country_name_lookup is not None:
            eu_country_name_lookup.unpersist()
        if spark is not None:
            spark.stop()
        _release_lock(output_dir, lock_token)


if __name__ == "__main__":
    main()
