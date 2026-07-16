"""Silver transformations and validations for orders."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.data.silver.common import raise_if_invalid_rows_found


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
