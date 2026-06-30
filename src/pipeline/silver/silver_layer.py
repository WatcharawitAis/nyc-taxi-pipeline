"""Pipeline for silver layer"""

from pyspark import pipelines as dp

from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.silver.silver_pipelines import silver_pipeline

SILVER_SCHEMA_NAME = spark.conf.get("silver_schema")


@dp.table(
    name=f"{SILVER_SCHEMA_NAME}.silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
)
def silver_nyc_taxi_trips():
    """Silver Layer: Cleaned and enriched NYC taxi trip data

    Output: {catalog}.silver.silver_nyc_taxi_trips

    Data Quality Rules:
    - VALIDATE: DateTime must be parseable timestamps
    - REMOVE: Negative fares, zero distance, zero fares (invalid data)
    - ADD: Derived metrics (trip duration, average speed, time of day)
    - CAST: Zip codes to string (preserve leading zeros)
    """
    df = dp.read_stream("bronze.bronze_nyc_taxi_trips")
    clean_df = silver_pipeline(df)
    return clean_df
