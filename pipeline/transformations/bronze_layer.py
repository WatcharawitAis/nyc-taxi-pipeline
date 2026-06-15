import dlt

@dlt.table(
    name="bronze_nyc_taxi_trips",
    comment="Raw NYC taxi trip data ingested from samples.nyctaxi.trips",
)
def bronze_nyc_taxi_trips():
    """
    Returns the raw NYC taxi trip data ingested from samples.nyctaxi.trips
    """
    return spark.read.table("samples.nyctaxi.trips")  # noqa: F821
