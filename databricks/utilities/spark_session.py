"""Compatibility shim for the Spark session factory.

New code should import from ``src.utils.spark_session``. This module remains so
older local scripts or notebooks do not break while the project evolves.
"""

from src.utils.spark_session import (  # noqa: F401
    build_local_spark_builder,
    get_spark,
    is_databricks_runtime,
    stop_spark,
)
