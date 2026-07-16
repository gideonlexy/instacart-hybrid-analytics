"""Unit tests for the Spark session factory.

These tests intentionally avoid starting Spark so they can run before Java is
installed on a new development machine.
"""

from src.utils.spark_session import (
    build_local_spark_builder,
    is_databricks_runtime,
)


def test_databricks_detection_false_when_runtime_env_missing(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)

    assert is_databricks_runtime() is False


def test_databricks_detection_true_when_runtime_env_present(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3")

    assert is_databricks_runtime() is True


def test_local_builder_sets_expected_defaults(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_MASTER", "local[1]")

    builder = build_local_spark_builder(
        "unit-test",
        warehouse_dir=str(tmp_path / "spark-warehouse"),
    )

    options = builder._options
    assert options["spark.app.name"] == "unit-test"
    assert options["spark.master"] == "local[1]"
    assert options["spark.sql.session.timeZone"] == "UTC"
    assert options["spark.sql.shuffle.partitions"] == "4"
    assert options["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert (
        options["spark.sql.catalog.spark_catalog"]
        == "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
