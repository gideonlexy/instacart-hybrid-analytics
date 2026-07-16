"""Silver transformations and validations for order-product line items."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.data.silver.common import raise_if_invalid_rows_found


ORDER_PRODUCTS_SILVER_COLUMNS = (
    "order_id",
    "product_id",
    "add_to_cart_order",
    "reordered",
    "_ingestion_timestamp",
    "_source_file",
)
ORDER_PRODUCTS_REQUIRED_COLUMNS = ORDER_PRODUCTS_SILVER_COLUMNS


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
