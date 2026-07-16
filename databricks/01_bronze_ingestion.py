"""Bronze layer ingestion for Instacart raw CSV files.

Reads source CSV files from the local raw landing zone and writes one Delta
table per source file. Bronze preserves source columns as strings and adds
ingestion metadata only.

Run from the repository root:
    .venv/bin/python databricks/01_bronze_ingestion.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.data.raw_contract import RAW_FILE_CONTRACTS, RawFileContract  # noqa: E402
from src.utils.spark_session import get_spark, stop_spark  # noqa: E402


RAW_PATH = Path(os.getenv("RAW_DATA_PATH", PROJECT_ROOT / "data" / "raw"))
BRONZE_PATH = Path(os.getenv("BRONZE_DATA_PATH", PROJECT_ROOT / "data" / "bronze"))
CSV_READER_OPTIONS: dict[str, str] = {
    "header": "true",
    "inferSchema": "false",
    "mode": "FAILFAST",
    "quote": '"',
    "escape": '"',
    "unescapedQuoteHandling": "STOP_AT_CLOSING_QUOTE",
}
NON_NEGATIVE_NUMBER_PATTERN = r"^\d+(\.\d+)?$"
NUMERIC_COLUMNS_BY_FILE: dict[str, tuple[str, ...]] = {
    "orders.csv": (
        "order_id",
        "user_id",
        "order_number",
        "order_dow",
        "order_hour_of_day",
        "days_since_prior_order",
    ),
    "order_products__prior.csv": (
        "order_id",
        "product_id",
        "add_to_cart_order",
        "reordered",
    ),
    "order_products__train.csv": (
        "order_id",
        "product_id",
        "add_to_cart_order",
        "reordered",
    ),
    "products.csv": ("product_id", "aisle_id", "department_id"),
    "aisles.csv": ("aisle_id",),
    "departments.csv": ("department_id",),
}


def table_name_from_filename(filename: str) -> str:
    """Convert a raw CSV filename into a Bronze table directory name."""
    return filename.removesuffix(".csv").replace("__", "_")


def read_raw_csv(spark: SparkSession, contract: RawFileContract) -> DataFrame:
    """Read one raw CSV file with all source columns preserved as strings."""
    source_path = RAW_PATH / contract.filename
    reader = spark.read

    for option_name, option_value in CSV_READER_OPTIONS.items():
        reader = reader.option(option_name, option_value)

    return reader.csv(str(source_path))


def validate_raw_frame(df: DataFrame, contract: RawFileContract) -> None:
    """Validate parsed raw data before writing it into Bronze."""
    observed_columns = tuple(df.columns)
    if observed_columns != contract.columns:
        raise ValueError(
            f"{contract.filename} parsed with unexpected columns. "
            f"Expected {contract.columns}, observed {observed_columns}."
        )

    numeric_columns = NUMERIC_COLUMNS_BY_FILE.get(contract.filename, ())
    invalid_condition = None
    for column_name in numeric_columns:
        column_check = col(column_name).isNotNull() & ~col(column_name).rlike(
            NON_NEGATIVE_NUMBER_PATTERN
        )
        invalid_condition = (
            column_check
            if invalid_condition is None
            else invalid_condition | column_check
        )

    if invalid_condition is None:
        return

    invalid_rows = (
        df.where(invalid_condition).select(*contract.columns).limit(5).collect()
    )
    if invalid_rows:
        sample_rows = [row.asDict() for row in invalid_rows]
        raise ValueError(
            f"{contract.filename} has non-numeric values in numeric columns. "
            f"Sample rows: {sample_rows}"
        )


def add_ingestion_metadata(df: DataFrame, source_file: str) -> DataFrame:
    """Add audit metadata required for the Bronze layer."""
    return df.withColumn("_ingestion_timestamp", current_timestamp()).withColumn(
        "_source_file", lit(source_file)
    )


def write_bronze_table(df: DataFrame, table_name: str) -> None:
    """Write one DataFrame as a Bronze Delta table."""
    output_path = BRONZE_PATH / table_name
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(output_path))
    )


def ingest_bronze_table(
    spark: SparkSession, contract: RawFileContract
) -> tuple[str, str]:
    """Read one raw CSV file and write it as a Bronze Delta table."""
    table_name = table_name_from_filename(contract.filename)

    raw_df = read_raw_csv(spark, contract)
    validate_raw_frame(raw_df, contract)
    bronze_df = add_ingestion_metadata(raw_df, contract.filename)
    write_bronze_table(bronze_df, table_name)

    return contract.filename, table_name


def main() -> int:
    """Run Bronze ingestion for all raw Instacart source files."""
    spark = get_spark("instacart_bronze_ingestion")

    try:
        print("[BRONZE] Starting Ingestion")
        print(f"[BRONZE] RAW Path: {RAW_PATH}")
        print(f"[BRONZE] BRONZE Path: {BRONZE_PATH}")

        for contract in RAW_FILE_CONTRACTS:
            source_file, table_name = ingest_bronze_table(spark, contract)
            print(f"[BRONZE] {source_file} -> {BRONZE_PATH / table_name}")

        print("[BRONZE] Ingestion Complete")
        return 0
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    raise SystemExit(main())
