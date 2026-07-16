"""Shared helpers for Silver layer transformations."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession


def read_delta_table(spark: SparkSession, path: Path) -> DataFrame:
    """Read a Delta table from a local path."""
    return spark.read.format("delta").load(str(path))


def write_delta_table(df: DataFrame, path: Path) -> None:
    """Write a DataFrame as a Delta table."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(path))
    )


def collect_invalid_rows(
    df: DataFrame,
    invalid_condition,
    columns: tuple[str, ...],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Collect a small sample of rows that fail one validation rule."""
    rows = df.where(invalid_condition).select(*columns).limit(limit).collect()
    return [row.asDict() for row in rows]


def raise_if_invalid_rows_found(
    df: DataFrame,
    invalid_condition,
    error_message: str,
    sample_columns: tuple[str, ...],
) -> None:
    """Fail the pipeline when a validation rule finds invalid rows."""
    invalid_sample_rows = collect_invalid_rows(
        df=df,
        invalid_condition=invalid_condition,
        columns=sample_columns,
    )

    if invalid_sample_rows:
        raise ValueError(f"{error_message} Sample rows: {invalid_sample_rows}")
