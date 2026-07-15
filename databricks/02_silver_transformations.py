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

ORDER_PRODUCTS_SILVER_COLUMNS = (
    "order_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "_ingestion_timestamp",
    "_source_file",
)

ORDER_PRODUCTS_REQUIRED_COLUMNS = ORDER_PRODUCTS_SILVER_COLUMNS


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


def build_order_products_silver(bronze_order_products: DataFrame) -> DataFrame:
    """Transform Bronze order_products into a typed Silver order_products DataFrame."""
    return bronze_order_products.select(
        col("order_id").cast("long").alias("order_id"),
        col("product_id").cast("long").alias("product_id"),
        col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
        col("reordered").cast("int").alias("reordered"),
        col("_ingestion_timestamp"),
        col("_source_file"),
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

        if required_null_condition is None:
            required_null_condition = column_check
        else:
            required_null_condition = required_null_condition | column_check

    raise_if_invalid_rows_found(
        df,
        required_null_condition,
        "silver.orders has null values in required columns.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        ~col("eval_set").isin(*ACCEPTED_EVAL_SETS),
        "silver.orders has unexpected eval_set values.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        col("order_number") < 1,
        "silver.orders has order_number values below 1.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        ~col("order_dow").between(0, 6),
        "silver.orders has order_dow values outside 0-6.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        ~col("order_hour_of_day").between(0, 23),
        "silver.orders has order_hour_of_day values outside 0-23.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        col("days_since_prior_order").isNotNull()
        & ~col("days_since_prior_order").between(0, 30),
        "silver.orders has days_since_prior_order values outside 0-30.",
        ORDERS_SILVER_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df,
        col("days_since_prior_order").isNull() & (col("order_number") != 1),
        "silver.orders has missing days_since_prior_order after the first order.",
        ORDERS_SILVER_COLUMNS,
    )


def validate_order_products_silver(df: DataFrame) -> None:
    """Validate the typed Silver order_products table before writing it."""
    observed_columns = tuple(df.columns)
    if observed_columns != ORDER_PRODUCTS_SILVER_COLUMNS:
        raise ValueError(
            "silver.order_products has unexpected columns. "
            f"Expected {ORDER_PRODUCTS_SILVER_COLUMNS}, observed {observed_columns}."
        )

    required_null_condition = None
    for column_name in ORDER_PRODUCTS_REQUIRED_COLUMNS:
        column_check = col(column_name).isNull()

        if required_null_condition is None:
            required_null_condition = column_check
        else:
            required_null_condition = required_null_condition | column_check

    raise_if_invalid_rows_found(
        df,
        required_null_condition,
        "silver.order_products has null values in required columns.",
        ORDER_PRODUCTS_SILVER_COLUMNS,
    )

    raise_if_invalid_rows_found(
        df,
        col("add_to_cart_order") < 1,
        "silver.order_products has add_to_cart_order values below 1.",
        ORDER_PRODUCTS_SILVER_COLUMNS,
    )

    raise_if_invalid_rows_found(
        df,
        ~col("reordered").isin(0, 1),
        "silver.order_products has reordered values outside 0-1.",
        ORDER_PRODUCTS_SILVER_COLUMNS,
    )


def build_silver_orders_table(spark: SparkSession) -> Path:
    """Read Bronze orders, validate typed data, and write Silver orders."""
    bronze_orders = read_delta_table(spark, bronze_table_path("orders"))
    silver_orders = build_orders_silver(bronze_orders)

    validate_orders_silver(silver_orders)
    output_path = silver_table_path("orders")
    write_delta_table(silver_orders, output_path)

    return output_path


def build_silver_order_products_table(
    spark: SparkSession, bronze_table_name: str, silver_table_name: str
) -> Path:
    """Read Bronze order_products, validate typed data, and write Silver order_products."""
    bronze_order_products = read_delta_table(
        spark, bronze_table_path(bronze_table_name)
    )
    silver_order_products = build_order_products_silver(bronze_order_products)

    validate_order_products_silver(silver_order_products)
    output_path = silver_table_path(silver_table_name)
    write_delta_table(silver_order_products, output_path)

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

        order_products_prior_path = build_silver_order_products_table(
            spark,
            bronze_table_name="order_products_prior",
            silver_table_name="order_products_prior",
        )
        print(f"[SILVER] order_products_prior -> {order_products_prior_path}")

        order_products_train_path = build_silver_order_products_table(
            spark,
            bronze_table_name="order_products_train",
            silver_table_name="order_products_train",
        )
        print(f"[SILVER] order_products_train -> {order_products_train_path}")

        print("[SILVER] Transformations complete")
        return 0
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
