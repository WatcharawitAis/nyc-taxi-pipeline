# import dlt
from pyspark import pipelines as dp

@dp.table(
    name="bronze.bronze_nyc_taxi_trips",
    comment="Raw NYC taxi trip data ingested from samples.nyctaxi.trips",
    # schema is not specified - will use target schema (bronze) from pipeline config
)
def bronze_nyc_taxi_trips():
    """
    Bronze Layer: Raw NYC taxi trip data
    
    Returns the raw NYC taxi trip data ingested from samples.nyctaxi.trips.
    Output: {catalog}.bronze.bronze_nyc_taxi_trips
    """
    return spark.readStream.table("samples.nyctaxi.trips")  # noqa: F821
