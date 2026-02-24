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
    collect_set,
    col,
    coalesce,
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
    sum as spark_sum,
    to_date,
    to_json,
    to_timestamp,
    when,
    size,
)
from pyspark.sql.types import ArrayType, DataType, StructType

# Allow imports from project root / src
_src = str(Path(__file__).resolve().parent.parent / "src")
sys.path.insert(0, _src)
os.environ["PYTHONPATH"] = _src + os.pathsep + os.environ.get("PYTHONPATH", "")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from procurement.logging import setup_logging
from procurement.common.locks import acquire_token_file_lock, release_token_file_lock

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
    parser.add_argument(
        "--shard-count",
        type=int,
        default=64,
        help="Number of hash shards for case_derived_facts snapshot writes",
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


def _read_envelope_rows(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
) -> DataFrame:
    envelope_root = silver_dir / "common_envelope"
    envelope_paths = _paths_up_to(envelope_root, "publicationDateDay", target_date)
    if not envelope_paths:
        raise ValueError(f"Missing silver envelope inputs for <= {target_date}")

    envelope_raw = _union_paths(spark, envelope_paths, base_path=str(envelope_root))
    out = envelope_raw.select(
        safe_col(envelope_raw, "caseId", "string").alias("caseId"),
        safe_col(envelope_raw, "noticeType", "string").alias("noticeType"),
        safe_col(envelope_raw, "publicationDate", "string").alias("publicationDate"),
        safe_col(envelope_raw, "bzpNumber", "string").alias("bzpNumber"),
        safe_col(envelope_raw, "isTenderAmountBelowEU", "boolean").alias("isTenderAmountBelowEU"),
        safe_col(envelope_raw, "orderObject", "string").alias("orderObject"),
        safe_col(envelope_raw, "clientTypeName", "string").alias("clientTypeName"),
        safe_col(envelope_raw, "orderType", "string").alias("orderType"),
        safe_col(envelope_raw, "tenderType", "string").alias("tenderType"),
        safe_col(envelope_raw, "organizationCity", "string").alias("organizationCity"),
        safe_col(envelope_raw, "provinceName", "string").alias("provinceName"),
        safe_col(envelope_raw, "organizationCountry", "string").alias("organizationCountry"),
        safe_col(envelope_raw, "organizationNationalId", "string").alias("organizationNationalId"),
        safe_col(envelope_raw, "organizationNameNormalized", "string").alias("organizationNameNormalized"),
        safe_col(envelope_raw, "street", "string").alias("street"),
        safe_col(envelope_raw, "postal_code", "string").alias("postal_code"),
    ).filter(col("caseId").isNotNull())
    if case_ids is not None:
        out = out.join(case_ids.select("caseId"), on="caseId", how="inner")
    return out


def _read_specific_rows(
    spark: "SparkSession",
    silver_dir: Path,
    target_date: str,
    case_ids: DataFrame | None = None,
) -> DataFrame:
    specific_paths = _specific_paths_up_to(silver_dir, target_date)
    if not specific_paths:
        empty = spark.createDataFrame(
            [],
            (
                "caseId string, noticeType string, publicationDate string, contractors array<map<string,string>>, "
                "ai_street_512 string, value_estimated_procurement_ai_35 double, ai_prior_market_consultation_31 string, "
                "cpvMainCode_source string, numCriteria array<int>, priceWeight array<double>, cn_notice_concerns string, "
                "cn_criteria_aspects_4310_flag array<boolean>, cpvMainCode array<string>, cpvCode string, "
                "submittingOffersDate string, comp_num_awarded_63 int, value_competition_prizes_64 double, "
                "value_competition_followon_order_651 double, comp_requirements_72 string"
            ),
        )
        return empty

    frames: list[DataFrame] = []
    for path in specific_paths:
        frame_raw = spark.read.parquet(path)
        contractor_count_col = (
            size(col("contractors"))
            if has_field(frame_raw, "contractors")
            else lit(None).cast("int")
        )
        frame = frame_raw.select(
            safe_col(frame_raw, "caseId", "string").alias("caseId"),
            safe_col(frame_raw, "noticeType", "string").alias("noticeType"),
            safe_col(frame_raw, "publicationDate", "string").alias("publicationDate"),
            contractor_count_col.alias("contractor_count_raw"),
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
            safe_col(frame_raw, "cpn_contract_date_41", "string").alias("cpn_contract_date_41"),
            safe_col(frame_raw, "cpn_execution_end_date_52", "string").alias("cpn_execution_end_date_52"),
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
    if case_ids is not None:
        out = out.join(case_ids.select("caseId"), on="caseId", how="inner")
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
                "THEN CASE WHEN contractor_count_raw IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='AgreementIntentionNotice' THEN contractor_count_raw END)"
            )
        ).getField("v").alias("contractor_count"),
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
                "THEN CASE WHEN contractor_count_raw IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='CompetitionResultNotice' THEN contractor_count_raw END)"
            )
        ).getField("v").alias("contractor_count_comp_result"),
        spark_min(
            expr(
                "named_struct('is_null', CASE WHEN noticeType='ConcessionAgreementNotice' "
                "THEN CASE WHEN contractor_count_raw IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ConcessionAgreementNotice' THEN contractor_count_raw END)"
            )
        ).getField("v").alias("contractor_count_concession_agreement"),
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
                "THEN CASE WHEN contractor_count_raw IS NULL THEN 1 ELSE 0 END ELSE 1 END, "
                "'ts', publication_ts, "
                "'v', CASE WHEN noticeType='ContractPerformingNotice' THEN contractor_count_raw END)"
            )
        ).getField("v").alias("contractor_count_cpn"),
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
        "contractors_source",
        when(
            (~col("has_CompetitionNotice")) & col("contractor_count_comp_result").isNotNull(),
            lit("CompetitionResultNotice"),
        )
        .when(
            (~col("has_ConcessionNotice")) & col("contractor_count_concession_agreement").isNotNull(),
            lit("ConcessionAgreementNotice"),
        )
        .when(cond_cpn & col("contractor_count_cpn").isNotNull(), lit("ContractPerformingNotice"))
        .otherwise(lit(None).cast("string")),
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
    rows = df.count()
    write_df = df
    if "shard" in write_df.columns:
        write_df = write_df.drop("shard")
    write_df = write_df.withColumn(
        "shard",
        expr(
            f"CASE WHEN caseId IS NULL THEN 0 ELSE pmod(xxhash64(caseId), {effective_shards}) END"
        ).cast("int"),
    )
    write_df.write.mode("overwrite").partitionBy("shard").parquet(str(data_path))
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
            envelope_rows = _read_envelope_rows(spark, silver_dir, target_date)
            specific_rows = _read_specific_rows(spark, silver_dir, target_date)
            case_df = _apply_result_fallbacks(
                _build_case_derived(envelope_rows).join(
                    _build_notice_specific_features(specific_rows), on="caseId", how="left"
                )
            ).withColumn("asOfDate", lit(target_date))
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
            envelope_rows = _read_envelope_rows(spark, silver_dir, target_date)
            specific_rows = _read_specific_rows(spark, silver_dir, target_date)
            case_df = _apply_result_fallbacks(
                _build_case_derived(envelope_rows).join(
                    _build_notice_specific_features(specific_rows), on="caseId", how="left"
                )
            ).withColumn("asOfDate", lit(target_date))
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

        envelope_affected = _read_envelope_rows(spark, silver_dir, target_date, case_ids=affected)
        specific_affected = _read_specific_rows(spark, silver_dir, target_date, case_ids=affected)
        recomputed = _apply_result_fallbacks(
            _build_case_derived(envelope_affected).join(
                _build_notice_specific_features(specific_affected), on="caseId", how="left"
            )
        )
        unchanged = anchor_df.join(affected, on="caseId", how="left_anti").drop("asOfDate")
        out = unchanged.unionByName(recomputed, allowMissingColumns=True).withColumn(
            "asOfDate", lit(target_date)
        )
        manifest = _write_snapshot(
            out,
            output_dir,
            target_date,
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
        if spark is not None:
            spark.stop()
        _release_lock(output_dir, lock_token)


if __name__ == "__main__":
    main()
