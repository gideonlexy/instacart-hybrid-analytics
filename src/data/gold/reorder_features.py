"""Gold user-product reorder feature mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max as spark_max,
    sum as spark_sum,
)

from src.data.gold.common import validate_columns, validate_required_columns
from src.data.lakehouse import (
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
)


REORDER_FEATURES_GOLD_COLUMNS = (
    "user_id",
    "product_id",
    "times_ordered",
    "times_reordered",
    "avg_cart_position",
    "last_order_number",
    "reorder_ratio",
    "_ingestion_timestamp",
)
REORDER_FEATURES_REQUIRED_COLUMNS = REORDER_FEATURES_GOLD_COLUMNS
REORDER_FEATURES_FEATURE_SOURCE_SET = "prior"


def build_reorder_features_gold(
    orders: DataFrame,
    order_products: DataFrame,
) -> DataFrame:
    """Build user-product interaction features from historical Silver rows.

    Grain: one row per user_id and product_id.
    """
    historical_order_products = order_products.where(
        col("source_set") == REORDER_FEATURES_FEATURE_SOURCE_SET
    )

    order_context = orders.select("order_id", "user_id", "order_number")

    historical_order_products_with_users = order_context.join(
        historical_order_products,
        on="order_id",
        how="inner",
    )

    reorder_features = (
        historical_order_products_with_users.groupBy("user_id", "product_id")
        .agg(
            count("order_id").alias("times_ordered"),
            spark_sum("reordered").alias("times_reordered"),
            avg("add_to_cart_order").alias("avg_cart_position"),
            spark_max("order_number").alias("last_order_number"),
        )
        .withColumn("reorder_ratio", col("times_reordered") / col("times_ordered"))
        .withColumn("_ingestion_timestamp", current_timestamp())
        .select(*REORDER_FEATURES_GOLD_COLUMNS)
    )

    return reorder_features


def validate_reorder_features_gold(df: DataFrame) -> None:
    """Validate the reorder feature mart before writing it."""
    validate_columns(
        df=df,
        expected_columns=REORDER_FEATURES_GOLD_COLUMNS,
        table_name="gold.reorder_features",
    )
    validate_required_columns(
        df=df,
        required_columns=REORDER_FEATURES_REQUIRED_COLUMNS,
        sample_columns=REORDER_FEATURES_GOLD_COLUMNS,
        table_name="gold.reorder_features",
    )
    raise_if_duplicate_keys_found(
        df=df,
        key_columns=("user_id", "product_id"),
        table_name="gold.reorder_features",
    )

    positive_columns = (
        "times_ordered",
        "avg_cart_position",
        "last_order_number",
    )

    for column_name in positive_columns:
        invalid_positive_check = col(column_name) <= 0

        raise_if_invalid_rows_found(
            df=df,
            invalid_condition=invalid_positive_check,
            error_message=f"gold.reorder_features has non-positive values in {column_name}.",
            sample_columns=REORDER_FEATURES_GOLD_COLUMNS,
        )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("times_reordered") < 0,
        error_message="gold.reorder_features has negative times_reordered values.",
        sample_columns=REORDER_FEATURES_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("times_reordered") > col("times_ordered"),
        error_message="gold.reorder_features has times_reordered above times_ordered.",
        sample_columns=REORDER_FEATURES_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("reorder_ratio").between(0, 1),
        error_message="gold.reorder_features has reorder_ratio values outside 0-1.",
        sample_columns=REORDER_FEATURES_GOLD_COLUMNS,
    )
