"""Tests for Silver transformation configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


SILVER_SCRIPT = (
    Path(__file__).resolve().parents[1] / "databricks" / "02_silver_transformations.py"
)


@pytest.fixture(scope="module")
def silver_module():
    spec = importlib.util.spec_from_file_location(
        "silver_transformations", SILVER_SCRIPT
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_orders_silver_columns_are_explicit(silver_module):
    assert silver_module.ORDERS_SILVER_COLUMNS == (
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


def test_orders_required_columns_allow_days_since_prior_order_nulls(silver_module):
    assert "days_since_prior_order" not in silver_module.ORDERS_REQUIRED_COLUMNS
    assert set(silver_module.ORDERS_REQUIRED_COLUMNS) == {
        "order_id",
        "user_id",
        "eval_set",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "_ingestion_timestamp",
        "_source_file",
    }


def test_orders_eval_set_domain(silver_module):
    assert silver_module.ACCEPTED_EVAL_SETS == ("prior", "train", "test")


def test_delta_table_path_helpers(silver_module):
    assert silver_module.bronze_table_path("orders").name == "orders"
    assert silver_module.bronze_table_path("orders").parent.name == "bronze"
    assert silver_module.silver_table_path("orders").name == "orders"
    assert silver_module.silver_table_path("orders").parent.name == "silver"
