"""Pipeline for gold layer"""

from pyspark import pipelines as dp

from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.gold.gold_pipelines import gold_pipeline


GOLD_SCHEMA_NAME = spark.conf.get("gold_schema")
SILVER_SCHEMA_NAME = spark.conf.get("silver_schema")


@dp.table(
    name=f"{GOLD_SCHEMA_NAME}.day_of_week_metrics",
    comment="Daily aggregated metrics for the number of rides, "
    "average distance, average fare, and average speed for each day of the week.",
)
def day_of_week_metrics():
    """Gold Layer: Aggregated metrics by day of week

    Output: {catalog}.gold.day_of_week_metrics

    Provides business-ready metrics grouped by day of week with readable day names.

    Transformations applied:
    1. Aggregate trips by day of week
    2. Convert day numbers to names
    3. Round metrics to 2 decimal places
    4. Sort by day of week
    """
    df = dp.read(f"{SILVER_SCHEMA_NAME}.silver_nyc_taxi_trips")
    df = gold_pipeline(df)
    return df
