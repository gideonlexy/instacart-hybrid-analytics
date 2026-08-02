"""Gold product performance feature mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, countDistinct, current_timestamp

from src.data.gold.common import validate_columns, validate_required_columns
from src.data.lakehouse import (
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
)


PRODUCT_PERFORMANCE_GOLD_COLUMNS = (
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "total_orders",
    "unique_orders",
    "reorder_rate",
    "avg_cart_position",
    "_ingestion_timestamp",
)
PRODUCT_PERFORMANCE_REQUIRED_COLUMNS = PRODUCT_PERFORMANCE_GOLD_COLUMNS
PRODUCT_PERFORMANCE_FEATURE_SOURCE_SET = "prior"


def build_product_performance_gold(
    order_products: DataFrame,
    product_catalog: DataFrame,
) -> DataFrame:
    """Build product-level performance KPIs from historical Silver rows.

    Grain: one row per product_id.
    """
    historical_order_products = order_products.where(
        col("source_set") == PRODUCT_PERFORMANCE_FEATURE_SOURCE_SET
    )

    product_metrics = historical_order_products.groupBy("product_id").agg(
        count("order_id").alias("total_orders"),
        countDistinct("order_id").alias("unique_orders"),
        avg("reordered").alias("reorder_rate"),
        avg("add_to_cart_order").alias("avg_cart_position"),
    )

    product_performance = (
        product_metrics.join(
            product_catalog,
            on="product_id",
            how="inner",
        )
        .withColumn("_ingestion_timestamp", current_timestamp())
        .select(*PRODUCT_PERFORMANCE_GOLD_COLUMNS)
    )

    return product_performance


def validate_product_performance_gold(df: DataFrame) -> None:
    """Validate the product performance mart before writing it."""
    validate_columns(
        df=df,
        expected_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
        table_name="gold.product_performance",
    )
    validate_required_columns(
        df=df,
        required_columns=PRODUCT_PERFORMANCE_REQUIRED_COLUMNS,
        sample_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
        table_name="gold.product_performance",
    )
    raise_if_duplicate_keys_found(
        df=df,
        key_columns=("product_id",),
        table_name="gold.product_performance",
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("total_orders") <= 0,
        error_message="gold.product_performance has non-positive total_orders",
        sample_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("unique_orders") <= 0,
        error_message="gold.product_performance has non-positive unique_orders",
        sample_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("avg_cart_position") <= 0,
        error_message="gold.product_performance has non-positive avg_cart_position",
        sample_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
    )
    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("reorder_rate").between(0, 1),
        error_message="gold.product_performance has reorder_rate outside of [0, 1]",
        sample_columns=PRODUCT_PERFORMANCE_GOLD_COLUMNS,
    )
