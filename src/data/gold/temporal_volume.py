"""Gold temporal order-volume mart."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, current_timestamp

from src.data.gold.common import validate_columns, validate_required_columns
from src.data.lakehouse import (
    raise_if_duplicate_keys_found,
    raise_if_invalid_rows_found,
)


TEMPORAL_VOLUME_GOLD_COLUMNS = (
    "order_dow",
    "order_hour_of_day",
    "order_count",
    "_ingestion_timestamp",
)
TEMPORAL_VOLUME_REQUIRED_COLUMNS = TEMPORAL_VOLUME_GOLD_COLUMNS
TEMPORAL_VOLUME_SOURCE_EVAL_SETS = ("prior", "train", "test")


def build_temporal_volume_gold(orders: DataFrame) -> DataFrame:
    """Build order-volume counts by day-of-week and hour-of-day.

    Grain: one row per order_dow and order_hour_of_day.
    """
    observed_orders = orders.where(
        col("eval_set").isin(*TEMPORAL_VOLUME_SOURCE_EVAL_SETS)
    )

    temporal_volume = (
        observed_orders.groupBy("order_dow", "order_hour_of_day")
        .agg(count("order_id").alias("order_count"))
        .orderBy("order_dow", "order_hour_of_day")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .select(*TEMPORAL_VOLUME_GOLD_COLUMNS)
    )

    return temporal_volume


def validate_temporal_volume_gold(df: DataFrame) -> None:
    """Validate the temporal volume mart before writing it."""
    validate_columns(
        df=df,
        expected_columns=TEMPORAL_VOLUME_GOLD_COLUMNS,
        table_name="gold.temporal_volume",
    )
    validate_required_columns(
        df=df,
        required_columns=TEMPORAL_VOLUME_REQUIRED_COLUMNS,
        sample_columns=TEMPORAL_VOLUME_GOLD_COLUMNS,
        table_name="gold.temporal_volume",
    )
    raise_if_duplicate_keys_found(
        df=df,
        key_columns=("order_dow", "order_hour_of_day"),
        table_name="gold.temporal_volume",
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("order_dow").between(0, 6),
        error_message="gold.temporal_volume has order_dow outside of [0, 6].",
        sample_columns=TEMPORAL_VOLUME_GOLD_COLUMNS,
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=~col("order_hour_of_day").between(0, 23),
        error_message="gold.temporal_volume has order_hour_of_day outside of [0, 23].",
        sample_columns=TEMPORAL_VOLUME_GOLD_COLUMNS,
    )

    raise_if_invalid_rows_found(
        df=df,
        invalid_condition=col("order_count") <= 0,
        error_message="gold.temporal_volume has non-positive order_count.",
        sample_columns=TEMPORAL_VOLUME_GOLD_COLUMNS,
    )
