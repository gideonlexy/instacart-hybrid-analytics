"""Gold customer retention metrics mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, current_timestamp, lit, when

from src.data.gold.common import validate_columns, validate_required_columns
from src.data.lakehouse import (
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
)


RETENTION_METRICS_GOLD_COLUMNS = (
    "user_id",
    "total_orders",
    "retention_segment",
    "_ingestion_timestamp",
)
RETENTION_METRICS_REQUIRED_COLUMNS = RETENTION_METRICS_GOLD_COLUMNS
RETENTION_METRICS_FEATURE_EVAL_SET = "prior"
ACCEPTED_RETENTION_SEGMENTS = (
    "one_and_done",
    "light_repeat",
    "regular",
    "power_user",
)


def build_retention_metrics_gold(orders: DataFrame) -> DataFrame:
    """Build historical customer retention segments from Silver orders.

    Grain: one row per user_id.
    """
    historical_orders = orders.where(
        col("eval_set") == RETENTION_METRICS_FEATURE_EVAL_SET
    )

    retention_metrics = (
        historical_orders.groupBy("user_id")
        .agg(count("order_id").alias("total_orders"))
        .withColumn(
            "retention_segment",
            when(col("total_orders") == 1, lit("one_and_done"))
            .when(col("total_orders").between(2, 4), lit("light_repeat"))
            .when(col("total_orders").between(5, 11), lit("regular"))
            .otherwise(lit("power_user")),
        )
        .withColumn("_ingestion_timestamp", current_timestamp())
        .select(*RETENTION_METRICS_GOLD_COLUMNS)
    )

    return retention_metrics


def validate_retention_metrics_gold(df: DataFrame) -> None:
    """Validate the retention metrics mart before writing it."""
    validate_columns(
        df=df,
        expected_columns=RETENTION_METRICS_GOLD_COLUMNS,
        table_name="gold.retention_metrics",
    )
    validate_required_columns(
        df=df,
        required_columns=RETENTION_METRICS_REQUIRED_COLUMNS,
        sample_columns=RETENTION_METRICS_GOLD_COLUMNS,
        table_name="gold.retention_metrics",
    )
    raise_if_duplicate_keys_found(
        df=df,
        key_columns=("user_id",),
        table_name="gold.retention_metrics",
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("total_orders") <= 0,
        error_message="gold.retention_metrics has non-positive total_orders",
        sample_columns=RETENTION_METRICS_GOLD_COLUMNS,
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("retention_segment").isin(*ACCEPTED_RETENTION_SEGMENTS),
        error_message="gold.retention_metrics has invalid retention_segment values",
        sample_columns=RETENTION_METRICS_GOLD_COLUMNS,
    )
