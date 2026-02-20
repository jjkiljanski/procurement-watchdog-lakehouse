"""Spark-side validation checks for Silver outputs."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, sum, trim, when

log = logging.getLogger(__name__)

POSTAL_CODE_REGEX = r"^[0-9]{2}-[0-9]{3}$"


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

