"""Pipeline for silver layer"""

from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine

from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.silver.silver_pipelines import silver_pipeline


SILVER_SCHEMA_NAME = spark.conf.get("silver_schema")
BRONZE_SCHEMA_NAME = spark.conf.get("bronze_schema")

dq_engine = DQEngine(WorkspaceClient())


CHECKS = dq_engine.load_checks(
    config=FileChecksStorageConfig(
        location=f"../checks/silver_nyc_taxi_checks.yml"
    )
)

@dp.table(
    name=f"{SILVER_SCHEMA_NAME}.silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
)
def silver_nyc_taxi_trips():
    """Silver Layer: Cleaned and enriched NYC taxi trip data
    """
    df = dp.read_stream(f"{BRONZE_SCHEMA_NAME}.bronze_nyc_taxi_trips")
    transformed_df = silver_pipeline(df)
    cleaned_df = dq_engine.apply_checks_by_metadata(transformed_df, CHECKS)
    return cleaned_df
