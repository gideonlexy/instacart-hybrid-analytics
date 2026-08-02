"""Shared helpers for Gold feature marts."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.data.lakehouse import raise_if_invalid_rows_found


def validate_columns(
    df: DataFrame,
    expected_columns: tuple[str, ...],
    table_name: str,
) -> None:
    """Validate that a Gold table exposes exactly the expected columns."""
    observed_columns = tuple(df.columns)

    if observed_columns != expected_columns:
        raise ValueError(
            f"{table_name} has unexpected columns. "
            f"Expected {expected_columns}, observed {observed_columns}."
        )


def validate_required_columns(
    df: DataFrame,
    required_columns: tuple[str, ...],
    sample_columns: tuple[str, ...],
    table_name: str,
) -> None:
    """Validate that required Gold columns do not contain nulls."""
    for column_name in required_columns:
        null_check = col(column_name).isNull()

        raise_if_invalid_rows_found(
            df=df,
            invalid_condition=null_check,
            error_message=f"{table_name} has nulls in required column: {column_name}.",
            sample_columns=sample_columns,
        )
