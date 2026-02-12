"""PySpark transforms for the BZP silver layer."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from procurement.silver.html_parser import parse_cpv_codes, parse_html

log = logging.getLogger(__name__)

EVAL_CRITERION_SCHEMA = StructType(
    [
        StructField("name", StringType()),
        StructField("weight", IntegerType()),
    ]
)

HTML_EXTRACTED_SCHEMA = StructType(
    [
        StructField("ulica", StringType()),
        StructField("kod_pocztowy", StringType()),
        StructField("nuts3_code", StringType()),
        StructField("nuts3_name", StringType()),
        StructField("opis", StringType()),
        StructField("kryteria_oceny", ArrayType(EVAL_CRITERION_SCHEMA)),
        StructField("wartosc_umowy_pln", DoubleType()),
    ]
)


def _parse_html_safe(html: str | None) -> dict | None:
    if not html:
        return None
    try:
        return parse_html(html).model_dump()
    except Exception:
        log.warning("Failed to parse HTML (len=%d)", len(html), exc_info=True)
        return None


def _parse_cpv_safe(cpv_raw: str | None) -> list[str]:
    if not cpv_raw:
        return []
    return parse_cpv_codes(cpv_raw)


parse_html_udf = udf(_parse_html_safe, HTML_EXTRACTED_SCHEMA)
parse_cpv_udf = udf(_parse_cpv_safe, ArrayType(StringType()))


def build_silver(df: DataFrame) -> DataFrame:
    """Transform a raw BZP DataFrame into the silver layer.

    Filters out records with truncated HTML, parses HTML via UDF,
    splits CPV codes, and drops the raw htmlBody column.
    """
    return (
        df.filter(col("htmlBody").endswith("</html>"))
        .withColumn("htmlExtracted", parse_html_udf(col("htmlBody")))
        .withColumn("cpvCodes", parse_cpv_udf(col("cpvCode")))
        .drop("htmlBody", "cpvCode")
    )
