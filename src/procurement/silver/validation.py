"""Spark-side validation checks for Silver outputs."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import array, array_contains, col, count, lit, size, sum, to_date, to_timestamp, trim, when, expr

log = logging.getLogger(__name__)

POSTAL_CODE_REGEX = r"^[0-9]{2}-[0-9]{3}$"

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
    """Validate one transformed Silver notice batch."""
    df_with_errors, rules = with_notice_validation_errors(df, target_date=target_date, notice_type=notice_type)
    return summarize_notice_validation(
        df_with_errors=df_with_errors,
        target_date=target_date,
        notice_type=notice_type,
        rules=rules,
    )


def with_notice_validation_errors(
    df: DataFrame,
    target_date: str,
    notice_type: str | None,
) -> tuple[DataFrame, list[str]]:
    """Attach row-level validation errors as `__validation_errors` array."""
    required = ("objectId", "publicationDate", "noticeType", "organizationId", "caseId")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Silver notice batch missing required columns: {missing}")

    rule_names: list[str] = []
    error_exprs = []

    def add_rule(rule_name: str, cond: "Column") -> None:
        rule_names.append(rule_name)
        error_exprs.append(when(cond, lit(rule_name)))

    add_rule("null_objectId_rows", col("objectId").isNull() | (trim(col("objectId")) == lit("")))
    add_rule("null_organizationId_rows", col("organizationId").isNull() | (trim(col("organizationId")) == lit("")))
    add_rule("null_caseId_rows", col("caseId").isNull() | (trim(col("caseId")) == lit("")))
    add_rule("invalid_publicationDate_rows", to_date(col("publicationDate")).isNull())
    add_rule(
        "publicationDate_off_partition_rows",
        (to_date(col("publicationDate")).isNotNull())
        & (to_date(col("publicationDate")).cast("string") != lit(target_date)),
    )

    if "noticeStage" in df.columns:
        expected_stage = NOTICE_STAGE_BY_TYPE.get(notice_type, "INIT")
        add_rule("noticeStage_mismatch_rows", col("noticeStage") != lit(expected_stage))

    if "cpvCodes" in df.columns:
        add_rule(
            "cpv_invalid_rows",
            col("cpvCodes").isNotNull()
            & (size(col("cpvCodes")) > lit(0))
            & (~col("cpvCodes").cast("string").rlike(r"\[\d{8}-\d(,\s*\d{8}-\d)*\]")),
        )

    if "biddingWindowDays" in df.columns:
        add_rule(
            "negative_biddingWindowDays_rows",
            col("biddingWindowDays").isNotNull() & (col("biddingWindowDays") < lit(0)),
        )

    if "submittingOffersDate" in df.columns:
        add_rule(
            "invalid_submittingOffersDate_rows",
            col("submittingOffersDate").isNotNull() & to_timestamp(col("submittingOffersDate")).isNull(),
        )

    if notice_type == "TenderResultNotice" and "procedureResultParsed" in df.columns:
        add_rule(
            "missing_procedureResultParsed_rows",
            col("procedureResultParsed").isNull() | (size(col("procedureResultParsed")) == lit(0)),
        )

    if "street" in df.columns:
        add_rule("street_missing_rows", col("street").isNull() | (trim(col("street")) == lit("")))

    if "postal_code" in df.columns:
        add_rule(
            "postal_invalid_rows",
            col("postal_code").isNotNull()
            & (trim(col("postal_code")) != lit(""))
            & (~col("postal_code").rlike(POSTAL_CODE_REGEX)),
        )

    out = df
    if "objectId" in df.columns:
        dup_ids = (
            df.groupBy("objectId")
            .count()
            .filter(col("objectId").isNotNull() & (col("count") > lit(1)))
            .select(col("objectId").alias("__dup_objectId"))
        )
        out = out.join(dup_ids, out.objectId == col("__dup_objectId"), "left")
        add_rule("duplicate_objectId_rows", col("__dup_objectId").isNotNull())

    out = out.withColumn("__validation_errors_tmp", array(*error_exprs))
    out = out.withColumn(
        "__validation_errors",
        expr("filter(__validation_errors_tmp, x -> x is not null)"),
    ).drop("__validation_errors_tmp", "__dup_objectId")
    return out, rule_names


def summarize_notice_validation(
    df_with_errors: DataFrame,
    target_date: str,
    notice_type: str | None,
    rules: list[str],
) -> dict[str, int]:
    """Summarize row-level validation errors to per-batch metrics."""
    metrics = (
        df_with_errors.agg(
            count(lit(1)).alias("total_rows"),
            sum(when(size(col("__validation_errors")) > lit(0), lit(1)).otherwise(lit(0))).alias("invalid_rows"),
        )
        .collect()[0]
        .asDict()
    )
    out = {k: int(v or 0) for k, v in metrics.items()}

    for rule in rules:
        cnt = (
            df_with_errors.agg(
                sum(when(array_contains(col("__validation_errors"), lit(rule)), lit(1)).otherwise(lit(0))).alias("c")
            )
            .collect()[0]
            .c
        )
        out[rule] = int(cnt or 0)
        _warn_if_positive(
            target_date,
            f"{notice_type or '__NULL__'}.{rule}",
            out[rule],
        )

    return out
