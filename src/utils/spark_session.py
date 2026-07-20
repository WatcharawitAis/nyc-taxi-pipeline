"""Spark session utilities shared by every pipeline layer."""

import logging

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


def get_spark_session() -> SparkSession:
    """Return the active SparkSession.

    Returns:
        The active SparkSession.
    """
    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError(
            "No active SparkSession found. This module must be imported from within "
            "a Databricks runtime or Spark Declarative Pipeline context, where a "
            "session is already active."
        )
    return spark_session


def get_required_conf(key: str, spark_session: SparkSession = None) -> str:
    """Read a Spark/pipeline configuration value, returning None if it's missing.

    Args:
        key: Spark configuration key, as set in the pipeline's
            Configuration block (e.g. "catalog", "bronze_schema").
        spark_session: SparkSession instance. If None, gets the active session.

    Returns:
        The configuration value, or None if the key doesn't exist.
    """
    if spark_session is None:
        spark_session = get_spark_session()

    try:
        value = spark_session.conf.get(key)
        return value
    except AnalysisException:
        return None
