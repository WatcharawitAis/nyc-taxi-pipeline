"""Pipeline for bronze layer"""

from pyspark import pipelines as dp
from src.pipeline.utils.spark_session import SPARK as spark


BRONZE_SCHEMA_NAME = spark.conf.get("bronze_schema")


@dp.table(
    name=f"{BRONZE_SCHEMA_NAME}.bronze_nyc_taxi_trips",
    comment="Raw NYC taxi trip data ingested from workspace.default.my_nyctaxi_trips",
)
def bronze_nyc_taxi_trips():
    """Bronze Layer: Raw NYC taxi trip data

    Returns the raw NYC taxi trip data ingested from workspace.default.my_nyctaxi_trips.
    Output: {catalog}.bronze.bronze_nyc_taxi_trips
    """
    return spark.readStream.table("workspace.default.my_nyctaxi_trips")
