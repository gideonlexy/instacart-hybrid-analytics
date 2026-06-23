"""Tests for Bronze ingestion configuration."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


BRONZE_SCRIPT = (
    Path(__file__).resolve().parents[1] / "databricks" / "01_bronze_ingestion.py"
)


@pytest.fixture(scope="module")
def bronze_module():
    spec = importlib.util.spec_from_file_location("bronze_ingestion", BRONZE_SCRIPT)
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_table_name_from_filename(bronze_module):
    assert (
        bronze_module.table_name_from_filename("order_products__prior.csv")
        == "order_products_prior"
    )
    assert bronze_module.table_name_from_filename("products.csv") == "products"


def test_csv_reader_options_are_strict_and_handle_instacart_quotes(bronze_module):
    assert bronze_module.CSV_READER_OPTIONS["header"] == "true"
    assert bronze_module.CSV_READER_OPTIONS["inferSchema"] == "false"
    assert bronze_module.CSV_READER_OPTIONS["mode"] == "FAILFAST"
    assert bronze_module.CSV_READER_OPTIONS["quote"] == '"'
    assert bronze_module.CSV_READER_OPTIONS["escape"] == '"'
    assert (
        bronze_module.CSV_READER_OPTIONS["unescapedQuoteHandling"]
        == "STOP_AT_CLOSING_QUOTE"
    )


def test_products_numeric_columns_are_validated(bronze_module):
    assert bronze_module.NUMERIC_COLUMNS_BY_FILE["products.csv"] == (
        "product_id",
        "aisle_id",
        "department_id",
    )
