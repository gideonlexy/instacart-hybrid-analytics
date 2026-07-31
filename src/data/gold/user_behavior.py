"""Gold user behavior feature mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    lit,
    when,
)

from src.data.gold.common import validate_columns, validate_required_columns
from src.data.lakehouse import (
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
)


USER_BEHAVIOR_GOLD_COLUMNS = (
    "user_id",
    "total_orders",
    "total_products",
    "unique_products",
    "reorder_rate",
    "avg_days_between_orders",
    "avg_order_dow",
    "avg_order_hour",
    "weekend_order_ratio",
    "avg_basket_size",
    "_ingestion_timestamp",
)
USER_BEHAVIOR_REQUIRED_COLUMNS = USER_BEHAVIOR_GOLD_COLUMNS


def build_user_behavior_gold(
    orders: DataFrame,
    order_products: DataFrame,
) -> DataFrame:
    """Build user-level behavioral features from Silver orders and line items.

    Grain: one row per user_id.
    """
    orders_with_products = order_products.select("order_id").distinct()
    active_orders = orders.join(orders_with_products, on="order_id", how="inner")

    order_metrics = active_orders.groupBy("user_id").agg(
        countDistinct("order_id").alias("total_orders"),
        avg("days_since_prior_order").alias("avg_days_between_orders"),
        avg("order_dow").alias("avg_order_dow"),
        avg("order_hour_of_day").alias("avg_order_hour"),
        avg(when(col("order_dow").isin(0, 1), lit(1.0)).otherwise(lit(0.0))).alias(
            "weekend_order_ratio"
        ),
    )

    order_products_with_users = orders.select("order_id", "user_id").join(
        order_products,
        on="order_id",
        how="inner",
    )

    product_metrics = order_products_with_users.groupBy("user_id").agg(
        count("product_id").alias("total_products"),
        countDistinct("product_id").alias("unique_products"),
        avg("reordered").alias("reorder_rate"),
    )

    user_behavior = (
        order_metrics.join(product_metrics, on="user_id", how="inner")
        .withColumn("avg_basket_size", col("total_products") / col("total_orders"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .select(*USER_BEHAVIOR_GOLD_COLUMNS)
    )

    return user_behavior


def validate_user_behavior_gold(df: DataFrame) -> None:
    """Validate the user behavior feature mart before writing it."""
    validate_columns(
        df=df,
        expected_columns=USER_BEHAVIOR_GOLD_COLUMNS,
        table_name="gold.user_behavior",
    )
    validate_required_columns(
        df=df,
        required_columns=USER_BEHAVIOR_REQUIRED_COLUMNS,
        sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
        table_name="gold.user_behavior",
    )
    raise_if_duplicate_keys_found(
        df=df,
        key_columns=("user_id",),
        table_name="gold.user_behavior",
    )

    positive_columns = (
        "total_orders",
        "total_products",
        "unique_products",
        "avg_basket_size",
    )

    for column_name in positive_columns:
        invalid_positive_check = col(column_name) <= 0

        raise_if_invalid_rows_found(
            df=df,
            invalid_condition=invalid_positive_check,
            error_message=f"gold.user_behavior has non-positive values in {column_name}.",
            sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
        )

    ratio_columns = ("reorder_rate", "weekend_order_ratio")

    for column_name in ratio_columns:
        invalid_ratio_check = ~col(column_name).between(0, 1)

        raise_if_invalid_rows_found(
            df=df,
            invalid_condition=invalid_ratio_check,
            error_message=f"gold.user_behavior has ratio values outside 0-1 in {column_name}.",
            sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
        )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("avg_order_dow").between(0, 6),
        error_message="gold.user_behavior has avg_order_dow values outside 0-6.",
        sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("avg_order_hour").between(0, 23),
        error_message="gold.user_behavior has avg_order_hour values outside 0-23.",
        sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("avg_days_between_orders").between(0, 30),
        error_message="gold.user_behavior has avg_days_between_orders outside 0-30.",
        sample_columns=USER_BEHAVIOR_GOLD_COLUMNS,
    )
