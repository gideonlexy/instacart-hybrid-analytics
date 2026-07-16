"""Silver product catalog transformation scaffold.

This module is intentionally scaffolded for the next Core learning step:
joining products, aisles, and departments into one clean product dimension.
"""

from __future__ import annotations

from pyspark.sql import DataFrame


PRODUCT_CATALOG_SILVER_COLUMNS = (
    # TODO: add product catalog output columns here.
)
PRODUCT_CATALOG_REQUIRED_COLUMNS = PRODUCT_CATALOG_SILVER_COLUMNS


def build_product_catalog_silver(
    products: DataFrame,
    aisles: DataFrame,
    departments: DataFrame,
) -> DataFrame:
    """Build a typed, joined Silver product catalog DataFrame."""
    raise NotImplementedError(
        "Add product catalog join logic in the next guided implementation step."
    )


def validate_product_catalog_silver(df: DataFrame) -> None:
    """Validate the typed Silver product catalog before writing it."""
    raise NotImplementedError(
        "Add product catalog validation in the next guided implementation step."
    )
