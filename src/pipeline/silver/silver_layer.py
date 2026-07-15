"""Pipeline for silver layer"""

from pyspark import pipelines as dp
from databricks.sdk import WorkspaceClient
from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine

from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.silver.silver_pipelines import silver_pipeline

CATALOG = spark.conf.get("catalog")
SILVER_SCHEMA_NAME = spark.conf.get("silver_schema")
BRONZE_SCHEMA_NAME = spark.conf.get("bronze_schema")

DQ_ENGINE = DQEngine(WorkspaceClient())

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
)
def silver_nyc_taxi_trips():
    """Silver Layer: Cleaned and enriched NYC taxi trip data
    """
    df = dp.read_stream(f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_nyc_taxi_trips")
    transformed_df = silver_pipeline(df)
    checks = DQ_ENGINE.load_checks(
    config=FileChecksStorageConfig(
        location="../checks/silver_nyc_taxi_checks.yml"
        )
    )
    cleaned_df = DQ_ENGINE.apply_checks_by_metadata(transformed_df, checks)
    return cleaned_df
