"""Silver layer transformations for Instacart Bronze Delta tables.

Reads Bronze Delta tables, applies explicit typing and quality checks, then
writes cleaned Silver Delta tables. This first Silver slice handles orders.

Run from the repository root:
    .venv/bin/python databricks/02_silver_transformations.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from databricks.utilities.spark_session import get_spark, stop_spark  # noqa: E402


BRONZE_PATH = Path(os.getenv("BRONZE_DATA_PATH", PROJECT_ROOT / "data" / "bronze"))
SILVER_PATH = Path(os.getenv("SILVER_DATA_PATH", PROJECT_ROOT / "data" / "silver"))

ACCEPTED_EVAL_SETS = ("prior", "train", "test")
ORDERS_SILVER_COLUMNS = (
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "days_since_prior_order",
    "_ingestion_timestamp",
    "_source_file",
)
ORDERS_REQUIRED_COLUMNS = (
    "order_id",
    "user_id",
    "eval_set",
    "order_number",
    "order_dow",
    "order_hour_of_day",
    "_ingestion_timestamp",
    "_source_file",
)


def bronze_table_path(table_name: str) -> Path:
    """Return the local path for one Bronze Delta table."""
    return BRONZE_PATH / table_name


def silver_table_path(table_name: str) -> Path:
    """Return the local path for one Silver Delta table."""
    return SILVER_PATH / table_name


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


def build_orders_silver(bronze_orders: DataFrame) -> DataFrame:
    """Transform Bronze orders into a typed Silver orders DataFrame."""
    return bronze_orders.select(
        col("order_id").cast("long").alias("order_id"),
        col("user_id").cast("long").alias("user_id"),
        col("eval_set"),
        col("order_number").cast("int").alias("order_number"),
        col("order_dow").cast("int").alias("order_dow"),
        col("order_hour_of_day").cast("int").alias("order_hour_of_day"),
        col("days_since_prior_order").cast("double").alias("days_since_prior_order"),
        col("_ingestion_timestamp"),
        col("_source_file"),
    )


def collect_invalid_rows(
    df: DataFrame,
    invalid_condition,
    *,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Collect a small sample of rows that fail one validation rule."""
    rows = (
        df.where(invalid_condition)
        .select(*ORDERS_SILVER_COLUMNS)
        .limit(limit)
        .collect()
    )
    return [row.asDict() for row in rows]


def fail_if_invalid(df: DataFrame, invalid_condition, message: str) -> None:
    """Raise a helpful error when a validation rule finds invalid rows."""
    invalid_rows = collect_invalid_rows(df, invalid_condition)
    if invalid_rows:
        raise ValueError(f"{message} Sample rows: {invalid_rows}")


def validate_orders_silver(df: DataFrame) -> None:
    """Validate the typed Silver orders table before writing it."""
    observed_columns = tuple(df.columns)
    if observed_columns != ORDERS_SILVER_COLUMNS:
        raise ValueError(
            "silver.orders has unexpected columns. "
            f"Expected {ORDERS_SILVER_COLUMNS}, observed {observed_columns}."
        )

    required_null_condition = None
    for column_name in ORDERS_REQUIRED_COLUMNS:
        column_check = col(column_name).isNull()
        required_null_condition = (
            column_check
            if required_null_condition is None
            else required_null_condition | column_check
        )

    fail_if_invalid(
        df,
        required_null_condition,
        "silver.orders has null values in required columns.",
    )
    fail_if_invalid(
        df,
        ~col("eval_set").isin(*ACCEPTED_EVAL_SETS),
        "silver.orders has unexpected eval_set values.",
    )
    fail_if_invalid(
        df,
        col("order_number") < 1,
        "silver.orders has order_number values below 1.",
    )
    fail_if_invalid(
        df,
        ~col("order_dow").between(0, 6),
        "silver.orders has order_dow values outside 0-6.",
    )
    fail_if_invalid(
        df,
        ~col("order_hour_of_day").between(0, 23),
        "silver.orders has order_hour_of_day values outside 0-23.",
    )
    fail_if_invalid(
        df,
        col("days_since_prior_order").isNotNull()
        & ~col("days_since_prior_order").between(0, 30),
        "silver.orders has days_since_prior_order values outside 0-30.",
    )
    fail_if_invalid(
        df,
        col("days_since_prior_order").isNull() & (col("order_number") != 1),
        "silver.orders has missing days_since_prior_order after the first order.",
    )


def build_silver_orders_table(spark: SparkSession) -> Path:
    """Read Bronze orders, validate typed data, and write Silver orders."""
    bronze_orders = read_delta_table(spark, bronze_table_path("orders"))
    silver_orders = build_orders_silver(bronze_orders)

    validate_orders_silver(silver_orders)
    output_path = silver_table_path("orders")
    write_delta_table(silver_orders, output_path)

    return output_path


def main() -> int:
    """Run Silver transformations."""
    spark = get_spark("instacart_silver_transformations")

    try:
        print("[SILVER] Starting transformations")
        print(f"[SILVER] BRONZE Path: {BRONZE_PATH}")
        print(f"[SILVER] SILVER Path: {SILVER_PATH}")

        orders_path = build_silver_orders_table(spark)
        print(f"[SILVER] orders -> {orders_path}")

        print("[SILVER] Transformations complete")
        return 0
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
