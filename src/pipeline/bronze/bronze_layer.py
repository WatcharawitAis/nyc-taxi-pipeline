# import dlt
from src.pipeline.utils.spark_session import spark
from pyspark import pipelines as dp

bronze_schema_name = spark.conf.get("bronze_schema")
@dp.table(
    name=f"{bronze_schema_name}.bronze_nyc_taxi_trips",
    comment="Raw NYC taxi trip data ingested from workspace.default.my_nyctaxi_trips",
    # schema is not specified - will use target schema (bronze) from pipeline config
)
def bronze_nyc_taxi_trips():
    """
    Bronze Layer: Raw NYC taxi trip data
    
    Returns the raw NYC taxi trip data ingested from workspace.default.my_nyctaxi_trips.
    Output: {catalog}.bronze.bronze_nyc_taxi_trips
    """
    return spark.readStream.table("workspace.default.my_nyctaxi_trips")  # noqa: F821
