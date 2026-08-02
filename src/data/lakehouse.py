"""Shared helpers for Delta Lake transformation layers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


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


def collect_duplicate_key_rows(
    df: DataFrame,
    key_columns: tuple[str, ...],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Collect a small sample of duplicated key values."""
    duplicate_rows = (
        df.groupBy(*key_columns).count().where(col("count") > 1).limit(limit).collect()
    )

    return [row.asDict() for row in duplicate_rows]


def raise_if_duplicate_keys_found(
    df: DataFrame,
    key_columns: tuple[str, ...],
    table_name: str,
) -> None:
    """Fail the pipeline when a table contains duplicate key values."""
    duplicate_key_rows = collect_duplicate_key_rows(
        df=df,
        key_columns=key_columns,
    )

    if duplicate_key_rows:
        raise ValueError(
            f"{table_name} has duplicate key values for {key_columns}. "
            f"Sample rows: {duplicate_key_rows}"
        )


def collect_orphan_key_rows(
    child_df: DataFrame,
    parent_df: DataFrame,
    key_columns: tuple[str, ...],
    sample_columns: tuple[str, ...],
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Collect child rows whose key values do not exist in the parent table."""
    parent_keys = parent_df.select(*key_columns).distinct()

    orphan_rows = (
        child_df.join(parent_keys, on=list(key_columns), how="left_anti")
        .select(*sample_columns)
        .limit(limit)
        .collect()
    )

    return [row.asDict() for row in orphan_rows]


def raise_if_orphan_keys_found(
    child_df: DataFrame,
    parent_df: DataFrame,
    key_columns: tuple[str, ...],
    child_table_name: str,
    parent_table_name: str,
    sample_columns: tuple[str, ...],
) -> None:
    """Fail the pipeline when child rows reference missing parent keys."""
    orphan_key_rows = collect_orphan_key_rows(
        child_df=child_df,
        parent_df=parent_df,
        key_columns=key_columns,
        sample_columns=sample_columns,
    )

    if orphan_key_rows:
        raise ValueError(
            f"{child_table_name} contains keys that do not exist in "
            f"{parent_table_name} for {key_columns}. "
            f"Sample rows: {orphan_key_rows}"
        )
