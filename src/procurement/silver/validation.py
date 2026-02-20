"""Spark-side validation checks for Silver outputs."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    lit,
    size,
    sum,
    to_date,
    to_timestamp,
    trim,
    when,
)

log = logging.getLogger(__name__)

POSTAL_CODE_REGEX = r"^[0-9]{2}-[0-9]{3}$"
CPV_REGEX = r"^\d{8}-\d$"

NOTICE_STAGE_BY_TYPE = {
    "TenderResultNotice": "RESULT",
    "ContractPerformingNotice": "EXECUTION",
    "NoticeUpdateNotice": "UPDATE",
    "AgreementUpdateNotice": "UPDATE",
}


def _warn_if_positive(target_date: str, label: str, value: int) -> None:
    if value > 0:
        log.warning("Silver validation warning day=%s: %s=%d", target_date, label, value)


def validate_common_envelope(df: DataFrame, target_date: str) -> dict[str, int | float]:
    """Validate key quality rules for common envelope output.

    Rules:
    - required columns exist,
    - `street` is non-null/non-empty,
    - `postal_code` is in `XX-XXX` format when provided.
    """
    required = ("street", "postal_code")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Silver common_envelope missing required columns: {missing}")

    metrics = (
        df.agg(
            count(lit(1)).alias("total_rows"),
            sum(
                when(
                    col("street").isNotNull() & (trim(col("street")) != lit("")),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("street_non_null_rows"),
            sum(
                when(
                    col("postal_code").isNotNull() & (trim(col("postal_code")) != lit("")),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("postal_present_rows"),
            sum(
                when(
                    col("postal_code").isNotNull()
                    & (trim(col("postal_code")) != lit(""))
                    & col("postal_code").rlike(POSTAL_CODE_REGEX),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("postal_valid_rows"),
        )
        .collect()[0]
        .asDict()
    )

    total_rows = int(metrics["total_rows"] or 0)
    street_non_null_rows = int(metrics["street_non_null_rows"] or 0)
    postal_present_rows = int(metrics["postal_present_rows"] or 0)
    postal_valid_rows = int(metrics["postal_valid_rows"] or 0)

    street_null_rows = total_rows - street_non_null_rows
    postal_invalid_rows = postal_present_rows - postal_valid_rows

    if street_null_rows > 0:
        log.warning(
            "Silver validation warning day=%s: street null/empty rows=%d of %d",
            target_date,
            street_null_rows,
            total_rows,
        )
    if postal_invalid_rows > 0:
        log.warning(
            "Silver validation warning day=%s: postal_code invalid format rows=%d of present=%d",
            target_date,
            postal_invalid_rows,
            postal_present_rows,
        )

    return {
        "total_rows": total_rows,
        "street_non_null_rows": street_non_null_rows,
        "street_null_rows": street_null_rows,
        "postal_present_rows": postal_present_rows,
        "postal_valid_rows": postal_valid_rows,
        "postal_invalid_rows": postal_invalid_rows,
    }


def validate_notice_batch(
    df: DataFrame,
    target_date: str,
    notice_type: str | None,
) -> dict[str, int]:
    """Validate one transformed Silver notice batch.

    The checks are warning-oriented and do not fail the job unless columns are missing.
    """
    required = ("objectId", "publicationDate", "noticeType", "organizationId", "caseId")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Silver notice batch missing required columns: {missing}")

    metrics = (
        df.agg(
            count(lit(1)).alias("total_rows"),
            sum(when(col("objectId").isNull() | (trim(col("objectId")) == lit("")), lit(1)).otherwise(lit(0))).alias(
                "null_objectId_rows"
            ),
            sum(
                when(col("organizationId").isNull() | (trim(col("organizationId")) == lit("")), lit(1)).otherwise(lit(0))
            ).alias("null_organizationId_rows"),
            sum(when(col("caseId").isNull() | (trim(col("caseId")) == lit("")), lit(1)).otherwise(lit(0))).alias(
                "null_caseId_rows"
            ),
            sum(when(to_date(col("publicationDate")).isNull(), lit(1)).otherwise(lit(0))).alias(
                "invalid_publicationDate_rows"
            ),
            sum(
                when(
                    (to_date(col("publicationDate")).isNotNull())
                    & (to_date(col("publicationDate")).cast("string") != lit(target_date)),
                    lit(1),
                ).otherwise(lit(0))
            ).alias("publicationDate_off_partition_rows"),
            (count(lit(1)) - countDistinct(col("objectId"))).alias("duplicate_objectId_rows"),
        )
        .collect()[0]
        .asDict()
    )

    out = {k: int(v or 0) for k, v in metrics.items()}
    for k in (
        "null_objectId_rows",
        "null_organizationId_rows",
        "null_caseId_rows",
        "invalid_publicationDate_rows",
        "publicationDate_off_partition_rows",
        "duplicate_objectId_rows",
    ):
        _warn_if_positive(target_date, f"{notice_type or '__NULL__'}.{k}", out[k])

    if "noticeStage" in df.columns:
        expected_stage = NOTICE_STAGE_BY_TYPE.get(notice_type, "INIT")
        bad_stage = df.filter(col("noticeStage") != lit(expected_stage)).count()
        out["noticeStage_mismatch_rows"] = int(bad_stage)
        _warn_if_positive(target_date, f"{notice_type or '__NULL__'}.noticeStage_mismatch_rows", out["noticeStage_mismatch_rows"])

    if "cpvCodes" in df.columns:
        cpv_invalid = (
            df.selectExpr("explode_outer(cpvCodes) as cpv")
            .filter(col("cpv").isNotNull() & (~col("cpv").rlike(CPV_REGEX)))
            .count()
        )
        out["cpv_invalid_rows"] = int(cpv_invalid)
        _warn_if_positive(target_date, f"{notice_type or '__NULL__'}.cpv_invalid_rows", out["cpv_invalid_rows"])

    if "biddingWindowDays" in df.columns:
        neg_bidding = df.filter(col("biddingWindowDays").isNotNull() & (col("biddingWindowDays") < lit(0))).count()
        out["negative_biddingWindowDays_rows"] = int(neg_bidding)
        _warn_if_positive(
            target_date,
            f"{notice_type or '__NULL__'}.negative_biddingWindowDays_rows",
            out["negative_biddingWindowDays_rows"],
        )

    if "submittingOffersDate" in df.columns:
        invalid_submit = df.filter(
            col("submittingOffersDate").isNotNull() & to_timestamp(col("submittingOffersDate")).isNull()
        ).count()
        out["invalid_submittingOffersDate_rows"] = int(invalid_submit)
        _warn_if_positive(
            target_date,
            f"{notice_type or '__NULL__'}.invalid_submittingOffersDate_rows",
            out["invalid_submittingOffersDate_rows"],
        )

    if notice_type == "TenderResultNotice":
        if "procedureResultParsed" in df.columns:
            missing_procedure_parsed = df.filter(
                col("procedureResultParsed").isNull() | (size(col("procedureResultParsed")) == lit(0))
            ).count()
            out["missing_procedureResultParsed_rows"] = int(missing_procedure_parsed)
            _warn_if_positive(
                target_date,
                "TenderResultNotice.missing_procedureResultParsed_rows",
                out["missing_procedureResultParsed_rows"],
            )
    return out
