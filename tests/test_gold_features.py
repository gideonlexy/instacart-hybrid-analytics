"""Tests for Gold feature mart configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from src.data.gold.user_behavior import (
    USER_BEHAVIOR_GOLD_COLUMNS,
    USER_BEHAVIOR_REQUIRED_COLUMNS,
)


GOLD_SCRIPT = Path(__file__).resolve().parents[1] / "databricks" / "03_gold_features.py"


@pytest.fixture(scope="module")
def gold_runner():
    spec = importlib.util.spec_from_file_location("gold_features", GOLD_SCRIPT)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_user_behavior_gold_columns_are_explicit():
    assert USER_BEHAVIOR_GOLD_COLUMNS == (
        "user_id",
        "total_orders",
        "total_products",
        "unique_products",
        "reorder_rate",
        "avg_days_between_orders",
        "avg_order_dow",
        "avg_order_hour",
        "weekend_order_ratio",
        "avg_basket_size",
        "_ingestion_timestamp",
    )


def test_user_behavior_required_columns_match_output_columns():
    assert USER_BEHAVIOR_REQUIRED_COLUMNS == USER_BEHAVIOR_GOLD_COLUMNS


def test_gold_table_path_helpers(gold_runner):
    assert gold_runner.silver_table_path("orders").name == "orders"
    assert gold_runner.silver_table_path("orders").parent.name == "silver"
    assert gold_runner.gold_table_path("user_behavior").name == "user_behavior"
    assert gold_runner.gold_table_path("user_behavior").parent.name == "gold"


def test_gold_runner_exposes_user_behavior_build_step(gold_runner):
    assert callable(gold_runner.build_gold_user_behavior_table)
