"""Smoke test for the local Spark + Delta setup.

Run from the repository root:
    .venv/bin/python scripts/verify_local_spark.py
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from databricks.utilities.spark_session import get_spark, stop_spark  # noqa: E402


def main() -> None:
    output_path = Path(
        os.getenv(
            "SPARK_SMOKE_TEST_PATH",
            str(PROJECT_ROOT / "data" / "processed" / "spark_smoke_delta"),
        )
    )

    if output_path.exists():
        shutil.rmtree(output_path)

    spark = get_spark("instacart_local_spark_smoke_test")
    try:
        sample = spark.createDataFrame(
            [(1, "local-spark"), (2, "delta-lake")],
            ["id", "check_name"],
        )
        sample.write.format("delta").mode("overwrite").save(str(output_path))

        result = spark.read.format("delta").load(str(output_path))
        row_count = result.count()

        if row_count != 2:
            raise AssertionError(f"Expected 2 rows, got {row_count}")

        print(f"Spark version: {spark.version}")
        print(f"Delta smoke test passed: wrote and read {row_count} rows")
        print(f"Output path: {output_path}")
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
