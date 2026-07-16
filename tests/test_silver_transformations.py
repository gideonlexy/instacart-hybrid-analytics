"""Tests for Silver transformation configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from src.data.silver.order_products import (
    ORDER_PRODUCTS_REQUIRED_COLUMNS,
    ORDER_PRODUCTS_SILVER_COLUMNS,
)
from src.data.silver.orders import (
    ACCEPTED_EVAL_SETS,
    ORDERS_REQUIRED_COLUMNS,
    ORDERS_SILVER_COLUMNS,
)

from src.data.silver.product_catalog import (
    PRODUCT_CATALOG_REQUIRED_COLUMNS,
    PRODUCT_CATALOG_SILVER_COLUMNS,
)


SILVER_SCRIPT = (
    Path(__file__).resolve().parents[1] / "databricks" / "02_silver_transformations.py"
)


@pytest.fixture(scope="module")
def silver_runner():
    spec = importlib.util.spec_from_file_location(
        "silver_transformations", SILVER_SCRIPT
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_orders_silver_columns_are_explicit():
    assert ORDERS_SILVER_COLUMNS == (
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


def test_orders_required_columns_allow_days_since_prior_order_nulls():
    assert "days_since_prior_order" not in ORDERS_REQUIRED_COLUMNS
    assert set(ORDERS_REQUIRED_COLUMNS) == {
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "_ingestion_timestamp",
        "_source_file",
    }


def test_orders_eval_set_domain():
    assert ACCEPTED_EVAL_SETS == ("prior", "train", "test")


def test_order_products_silver_columns_are_explicit():
    assert ORDER_PRODUCTS_SILVER_COLUMNS == (
        "order_id",
        "product_id",
        "add_to_cart_order",
        "reordered",
        "_ingestion_timestamp",
        "_source_file",
    )


def test_product_catalog_silver_columns_are_explicit():
    assert PRODUCT_CATALOG_SILVER_COLUMNS == (
        "product_id",
        "product_name",
        "aisle_id",
        "aisle",
        "department_id",
        "department",
        "_ingestion_timestamp",
        "_source_file",
    )


def test_product_catalog_required_columns_match_output_columns():
    assert PRODUCT_CATALOG_REQUIRED_COLUMNS == PRODUCT_CATALOG_SILVER_COLUMNS


def test_order_products_required_columns_match_output_columns():
    assert ORDER_PRODUCTS_REQUIRED_COLUMNS == ORDER_PRODUCTS_SILVER_COLUMNS


def test_delta_table_path_helpers(silver_runner):
    assert silver_runner.bronze_table_path("orders").name == "orders"
    assert silver_runner.bronze_table_path("orders").parent.name == "bronze"
    assert silver_runner.silver_table_path("orders").name == "orders"
    assert silver_runner.silver_table_path("orders").parent.name == "silver"
