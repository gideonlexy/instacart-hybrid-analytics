"""Tests for Gold feature mart configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from src.data.gold.product_performance import (
    PRODUCT_PERFORMANCE_FEATURE_SOURCE_SET,
    PRODUCT_PERFORMANCE_GOLD_COLUMNS,
    PRODUCT_PERFORMANCE_REQUIRED_COLUMNS,
)
from src.data.gold.retention_metrics import (
    ACCEPTED_RETENTION_SEGMENTS,
    RETENTION_METRICS_FEATURE_EVAL_SET,
    RETENTION_METRICS_GOLD_COLUMNS,
    RETENTION_METRICS_REQUIRED_COLUMNS,
)
from src.data.gold.reorder_features import (
    REORDER_FEATURES_FEATURE_SOURCE_SET,
    REORDER_FEATURES_GOLD_COLUMNS,
    REORDER_FEATURES_REQUIRED_COLUMNS,
)
from src.data.gold.temporal_volume import (
    TEMPORAL_VOLUME_GOLD_COLUMNS,
    TEMPORAL_VOLUME_REQUIRED_COLUMNS,
    TEMPORAL_VOLUME_SOURCE_EVAL_SETS,
)
from src.data.gold.user_behavior import (
    USER_BEHAVIOR_FEATURE_SOURCE_SET,
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


def test_user_behavior_features_use_historical_source_set():
    assert USER_BEHAVIOR_FEATURE_SOURCE_SET == "prior"


def test_reorder_features_gold_columns_are_explicit():
    assert REORDER_FEATURES_GOLD_COLUMNS == (
        "user_id",
        "product_id",
        "times_ordered",
        "times_reordered",
        "avg_cart_position",
        "last_order_number",
        "reorder_ratio",
        "_ingestion_timestamp",
    )


def test_reorder_features_required_columns_match_output_columns():
    assert REORDER_FEATURES_REQUIRED_COLUMNS == REORDER_FEATURES_GOLD_COLUMNS


def test_reorder_features_use_historical_source_set():
    assert REORDER_FEATURES_FEATURE_SOURCE_SET == "prior"


def test_product_performance_gold_columns_are_explicit():
    assert PRODUCT_PERFORMANCE_GOLD_COLUMNS == (
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


def test_product_performance_required_columns_match_output_columns():
    assert PRODUCT_PERFORMANCE_REQUIRED_COLUMNS == PRODUCT_PERFORMANCE_GOLD_COLUMNS


def test_product_performance_uses_historical_source_set():
    assert PRODUCT_PERFORMANCE_FEATURE_SOURCE_SET == "prior"


def test_retention_metrics_gold_columns_are_explicit():
    assert RETENTION_METRICS_GOLD_COLUMNS == (
        "user_id",
        "total_orders",
        "retention_segment",
        "_ingestion_timestamp",
    )


def test_retention_metrics_required_columns_match_output_columns():
    assert RETENTION_METRICS_REQUIRED_COLUMNS == RETENTION_METRICS_GOLD_COLUMNS


def test_retention_metrics_use_historical_eval_set():
    assert RETENTION_METRICS_FEATURE_EVAL_SET == "prior"


def test_retention_segments_are_explicit():
    assert ACCEPTED_RETENTION_SEGMENTS == (
        "one_and_done",
        "light_repeat",
        "regular",
        "power_user",
    )


def test_temporal_volume_gold_columns_are_explicit():
    assert TEMPORAL_VOLUME_GOLD_COLUMNS == (
        "order_dow",
        "order_hour_of_day",
        "order_count",
        "_ingestion_timestamp",
    )


def test_temporal_volume_required_columns_match_output_columns():
    assert TEMPORAL_VOLUME_REQUIRED_COLUMNS == TEMPORAL_VOLUME_GOLD_COLUMNS


def test_temporal_volume_uses_all_validated_eval_sets():
    assert TEMPORAL_VOLUME_SOURCE_EVAL_SETS == ("prior", "train", "test")


def test_gold_table_path_helpers(gold_runner):
    assert gold_runner.silver_table_path("orders").name == "orders"
    assert gold_runner.silver_table_path("orders").parent.name == "silver"
    assert gold_runner.gold_table_path("user_behavior").name == "user_behavior"
    assert gold_runner.gold_table_path("user_behavior").parent.name == "gold"


def test_gold_runner_exposes_user_behavior_build_step(gold_runner):
    assert callable(gold_runner.build_gold_user_behavior_table)


def test_gold_runner_exposes_reorder_features_build_step(gold_runner):
    assert callable(gold_runner.build_gold_reorder_features_table)


def test_gold_runner_exposes_product_performance_build_step(gold_runner):
    assert callable(gold_runner.build_gold_product_performance_table)


def test_gold_runner_exposes_retention_metrics_build_step(gold_runner):
    assert callable(gold_runner.build_gold_retention_metrics_table)


def test_gold_runner_exposes_temporal_volume_build_step(gold_runner):
    assert callable(gold_runner.build_gold_temporal_volume_table)
