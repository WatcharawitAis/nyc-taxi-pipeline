"""Spark session utilities shared by every pipeline layer."""

import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

SPARK: SparkSession | None = SparkSession.getActiveSession()
if SPARK is None:
    raise RuntimeError(
        "No active SparkSession found. This module must be imported from within "
        "a Databricks runtime or Spark Declarative Pipeline context, where a "
        "session is already active."
    )


def get_required_conf(key: str) -> str:
    """Read a Spark/pipeline configuration value, failing fast if it's missing.

    Args:
        key: Spark configuration key, as set in the pipeline's
            Configuration block (e.g. "catalog", "bronze_schema").

    Returns:
        The configuration value.

    Raises:
        ValueError: If the key is unset or resolves to a blank string.
    """
    value = SPARK.conf.get(key, None)
    if not value or not value.strip():
        raise ValueError(
            f"Required pipeline configuration '{key}' is missing or blank. "
            "Check the pipeline's Configuration block in "
            "resources/nyc_taxi_pipeline.pipeline.yml and the corresponding "
            "bundle variable in databricks.yml."
        )
    return value
