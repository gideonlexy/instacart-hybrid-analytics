"""Spark session factory for local development and Databricks runtime.

The project develops Spark jobs locally first, then runs the same job modules in
Databricks by changing environment configuration rather than rewriting code.
"""

from __future__ import annotations

import os
from pathlib import Path

from pyspark.errors.exceptions.base import PySparkRuntimeError
from pyspark.sql import SparkSession


def is_databricks_runtime() -> bool:
    """Return True when code is running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _default_warehouse_dir() -> str:
    return str(Path.cwd() / "data" / "processed" / "spark-warehouse")


def build_local_spark_builder(
    app_name: str = "instacart",
    *,
    master: str | None = None,
    warehouse_dir: str | None = None,
) -> SparkSession.Builder:
    """Build a local Spark builder without starting the JVM.

    Keeping this separate from ``get_spark`` lets unit tests validate our Spark
    configuration even on machines where Java is not installed yet.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master or os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.warehouse.dir",
            warehouse_dir or os.getenv("SPARK_WAREHOUSE_DIR", _default_warehouse_dir()),
        )
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
        )
        .config("spark.sql.session.timeZone", os.getenv("SPARK_TIME_ZONE", "UTC"))
        .config("spark.sql.adaptive.enabled", "true")
    )


def get_spark(app_name: str = "instacart") -> SparkSession:
    """Return a Spark session for Databricks or local Delta Lake development."""
    if is_databricks_runtime():
        return SparkSession.builder.getOrCreate()

    builder = build_local_spark_builder(app_name)

    try:
        from delta import configure_spark_with_delta_pip

        return configure_spark_with_delta_pip(builder).getOrCreate()
    except PySparkRuntimeError as exc:
        if "JAVA_GATEWAY_EXITED" in str(exc):
            raise RuntimeError(
                "Spark could not start because Java is not available. "
                "Install a Spark-compatible Java runtime, preferably Java 17, "
                "then rerun the Spark smoke test."
            ) from exc
        raise
    except ImportError:
        return builder.getOrCreate()


def stop_spark(spark: SparkSession) -> None:
    """Stop Spark locally, but leave Databricks-managed sessions alone."""
    if not is_databricks_runtime():
        spark.stop()
