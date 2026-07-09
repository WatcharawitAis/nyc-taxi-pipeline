"""Pipeline for gold layer"""

from pyspark import pipelines as dp
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.engine import DQEngine

from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.gold.gold_pipelines import gold_pipeline


GOLD_SCHEMA_NAME = spark.conf.get("gold_schema")
SILVER_SCHEMA_NAME = spark.conf.get("silver_schema")

dq_engine = DQEngine(WorkspaceClient())
CHECKS = dq_engine.load_checks(
    config=FileChecksStorageConfig(
        location="../checks/day_of_week_metrics_checks.yml"
    )
)

@dp.table(
    name=f"{GOLD_SCHEMA_NAME}.day_of_week_metrics",
    comment="Daily aggregated metrics for the number of rides, "
    "average distance, average fare, and average speed for each day of the week.",
)
def day_of_week_metrics():
    """Gold Layer: Aggregated metrics by day of week

    Provides business-ready metrics grouped by day of week with readable day names.
    """
    df = dp.read(f"{SILVER_SCHEMA_NAME}.silver_nyc_taxi_trips")
    transformed_df = gold_pipeline(df)

    cleaned_df = dq_engine.apply_checks_by_metadata(transformed_df, CHECKS)
    valid_df = cleaned_df.drop("_errors", "_warnings")
    return valid_df
