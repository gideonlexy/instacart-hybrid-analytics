"""Gold layer feature mart runner for validated Silver Delta tables.

The Gold layer creates analytics-ready features at explicit grains. These
outputs are used by ML workflows and later loaded into Snowflake/dbt for
warehouse-facing analytics.

Run from the repository root:
    .venv/bin/python databricks/03_gold_features.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.gold.reorder_features import (  # noqa: E402
    build_reorder_features_gold,
    validate_reorder_features_gold,
)
from src.data.gold.user_behavior import (  # noqa: E402
    build_user_behavior_gold,
    validate_user_behavior_gold,
)
from src.data.lakehouse import read_delta_table, write_delta_table  # noqa: E402
from src.utils.spark_session import get_spark, stop_spark  # noqa: E402


SILVER_PATH = Path(os.getenv("SILVER_DATA_PATH", PROJECT_ROOT / "data" / "silver"))
GOLD_PATH = Path(os.getenv("GOLD_DATA_PATH", PROJECT_ROOT / "data" / "gold"))


def silver_table_path(table_name: str) -> Path:
    """Return the local path for one Silver Delta table."""
    return SILVER_PATH / table_name


def gold_table_path(table_name: str) -> Path:
    """Return the local path for one Gold Delta table."""
    return GOLD_PATH / table_name


def build_gold_user_behavior_table(spark: SparkSession) -> Path:
    """Build, validate, and write the Gold user behavior mart."""
    silver_orders = read_delta_table(spark, silver_table_path("orders"))
    silver_order_products = read_delta_table(spark, silver_table_path("order_products"))

    user_behavior = build_user_behavior_gold(
        orders=silver_orders,
        order_products=silver_order_products,
    )
    user_behavior.cache()

    try:
        row_count = user_behavior.count()
        print(f"[GOLD] user_behavior rows: {row_count:,}")

        print("[GOLD] user_behavior preview")
        user_behavior.show(5, truncate=False)

        validate_user_behavior_gold(user_behavior)

        output_path = gold_table_path("user_behavior")
        write_delta_table(user_behavior, output_path)

        return output_path
    finally:
        user_behavior.unpersist()


def build_gold_reorder_features_table(spark: SparkSession) -> Path:
    """Build, validate, and write the Gold reorder features mart."""
    silver_orders = read_delta_table(spark, silver_table_path("orders"))
    silver_order_products = read_delta_table(spark, silver_table_path("order_products"))

    reorder_features = build_reorder_features_gold(
        orders=silver_orders,
        order_products=silver_order_products,
    )
    reorder_features.cache()

    try:
        row_count = reorder_features.count()
        print(f"[GOLD] reorder_features rows: {row_count:,}")

        print("[GOLD] reorder_features preview")
        reorder_features.show(5, truncate=False)

        validate_reorder_features_gold(reorder_features)

        output_path = gold_table_path("reorder_features")
        write_delta_table(reorder_features, output_path)

        return output_path
    finally:
        reorder_features.unpersist()


def main() -> int:
    """Run Gold feature mart transformations."""
    spark = get_spark("instacart_gold_features")

    try:
        print("[GOLD] Starting feature marts")
        print(f"[GOLD] SILVER Path: {SILVER_PATH}")
        print(f"[GOLD] GOLD Path: {GOLD_PATH}")

        user_behavior_path = build_gold_user_behavior_table(spark)
        print(f"[GOLD] user_behavior -> {user_behavior_path}")

        reorder_features_path = build_gold_reorder_features_table(spark)
        print(f"[GOLD] reorder_features -> {reorder_features_path}")

        print("[GOLD] Feature marts complete")
        return 0
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
