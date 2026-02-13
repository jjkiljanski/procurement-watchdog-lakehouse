"""Helpers for schema-safe Spark gold transforms."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql.functions import col, lit
from pyspark.sql.types import ArrayType, DataType, StructType


def has_field(df: DataFrame, field_path: str) -> bool:
    """Return True if a dotted field path exists in a DataFrame schema."""
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


def safe_col(df: DataFrame, field_path: str, cast_to: str | None = None) -> Column:
    """Return an existing column or a typed NULL literal when missing."""
    if has_field(df, field_path):
        return col(field_path)
    if cast_to is None:
        return lit(None)
    return lit(None).cast(cast_to)

