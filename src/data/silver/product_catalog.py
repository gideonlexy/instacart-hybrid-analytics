"""Silver product catalog transformation scaffold.

This module is intentionally scaffolded for the next Core learning step:
joining products, aisles, and departments into one clean product dimension.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.data.silver.common import raise_if_invalid_rows_found


PRODUCT_CATALOG_SILVER_COLUMNS = (
    "product_id",
    "product_name",
    "aisle_id",
    "aisle",
    "department_id",
    "department",
    "_ingestion_timestamp",
    "_source_file",
)
PRODUCT_CATALOG_REQUIRED_COLUMNS = PRODUCT_CATALOG_SILVER_COLUMNS


def build_product_catalog_silver(
    products: DataFrame,
    aisles: DataFrame,
    departments: DataFrame,
) -> DataFrame:
    """Build a typed, joined Silver product catalog DataFrame."""
    products_typed = products.select(
        col("product_id").cast("long").alias("product_id"),
        col("product_name"),
        col("aisle_id").cast("long").alias("aisle_id"),
        col("department_id").cast("int").alias("department_id"),
        col("_ingestion_timestamp"),
        col("_source_file"),
    )

    aisles_typed = aisles.select(
        col("aisle_id").cast("long").alias("aisle_id"),
        col("aisle"),
    )

    departments_typed = departments.select(
        col("department_id").cast("int").alias("department_id"),
        col("department"),
    )

    # Join the DataFrames
    product_catalog = (
        products_typed.join(aisles_typed, on="aisle_id", how="left")
        .join(departments_typed, on="department_id", how="left")
        .select(*PRODUCT_CATALOG_SILVER_COLUMNS)
    )

    return product_catalog


def validate_product_catalog_silver(df: DataFrame) -> None:
    """Validate the typed Silver product catalog before writing it."""
    observed_columns = tuple(df.columns)
    if observed_columns != PRODUCT_CATALOG_SILVER_COLUMNS:
        raise ValueError(
            f"Product catalog parsed with unexpected columns. "
            f"Expected {PRODUCT_CATALOG_SILVER_COLUMNS}, observed {observed_columns}."
        )

    for column_name in PRODUCT_CATALOG_REQUIRED_COLUMNS:
        null_check = col(column_name).isNull()

        raise_if_invalid_rows_found(
            df,
            invalid_condition=null_check,
            error_message=(
                f"Product catalog has nulls in required column: '{column_name}'. "
                f"All required columns: {PRODUCT_CATALOG_REQUIRED_COLUMNS}."
            ),
            sample_columns=PRODUCT_CATALOG_REQUIRED_COLUMNS,
        )

    id_columns = ("product_id", "aisle_id", "department_id")

    for column_name in id_columns:
        invalid_id_check = col(column_name) < 1

        raise_if_invalid_rows_found(
            df,
            invalid_condition=invalid_id_check,
            error_message=(
                f"Product catalog has invalid IDs in column: '{column_name}'. "
                f"IDs must be greater than or equal to 1."
            ),
            sample_columns=PRODUCT_CATALOG_REQUIRED_COLUMNS,
        )
