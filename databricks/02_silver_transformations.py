"""Silver layer job runner for Instacart Bronze Delta tables.

The transformation and validation logic lives in ``src.data.silver`` modules.
This script keeps Databricks/local job orchestration thin: read Bronze, call the
right Silver transformation, validate, and write Delta outputs.

Run from the repository root:
    .venv/bin/python databricks/02_silver_transformations.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.silver.common import read_delta_table, write_delta_table  # noqa: E402
from src.data.silver.order_products import (  # noqa: E402
    build_order_products_silver,
    validate_order_products_silver,
)
from src.data.silver.orders import (  # noqa: E402
    build_orders_silver,
    validate_orders_silver,
)
from src.data.silver.product_catalog import (  # noqa: E402
    build_product_catalog_silver,
    validate_product_catalog_silver,
)

from src.utils.spark_session import get_spark, stop_spark  # noqa: E402


BRONZE_PATH = Path(os.getenv("BRONZE_DATA_PATH", PROJECT_ROOT / "data" / "bronze"))
SILVER_PATH = Path(os.getenv("SILVER_DATA_PATH", PROJECT_ROOT / "data" / "silver"))


def bronze_table_path(table_name: str) -> Path:
    """Return the local path for one Bronze Delta table."""
    return BRONZE_PATH / table_name


def silver_table_path(table_name: str) -> Path:
    """Return the local path for one Silver Delta table."""
    return SILVER_PATH / table_name


def build_silver_orders_table(spark: SparkSession) -> Path:
    """Read Bronze orders, validate typed data, and write Silver orders."""
    bronze_orders = read_delta_table(spark, bronze_table_path("orders"))
    silver_orders = build_orders_silver(bronze_orders)

    validate_orders_silver(silver_orders)
    output_path = silver_table_path("orders")
    write_delta_table(silver_orders, output_path)

    return output_path


def build_silver_order_products_table(
    spark: SparkSession,
    bronze_table_name: str,
    silver_table_name: str,
) -> Path:
    """Read Bronze order-products, validate typed data, and write Silver output."""
    bronze_order_products = read_delta_table(
        spark,
        bronze_table_path(bronze_table_name),
    )
    silver_order_products = build_order_products_silver(bronze_order_products)

    validate_order_products_silver(silver_order_products)
    output_path = silver_table_path(silver_table_name)
    write_delta_table(silver_order_products, output_path)

    return output_path


def build_silver_product_catalog_table(
    spark: SparkSession,
) -> Path:
    """Read Bronze products lookup tables, validate, and write Silver product catalog."""
    bronze_products = read_delta_table(spark, bronze_table_path("products"))
    bronze_aisles = read_delta_table(spark, bronze_table_path("aisles"))
    bronze_departments = read_delta_table(spark, bronze_table_path("departments"))

    silver_product_catalog = build_product_catalog_silver(
        products=bronze_products,
        aisles=bronze_aisles,
        departments=bronze_departments,
    )

    print("[SILVER] product_catalog preview")
    silver_product_catalog.show(5, truncate=False)

    validate_product_catalog_silver(silver_product_catalog)

    output_path = silver_table_path("product_catalog")
    write_delta_table(silver_product_catalog, output_path)

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

        product_catalog_path = build_silver_product_catalog_table(spark)
        print(f"[SILVER] product_catalog -> {product_catalog_path}")

        print("[SILVER] Transformations complete")
        return 0
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
