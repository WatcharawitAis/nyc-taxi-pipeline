from pyspark import pipelines as dp


@dp.table(
    name="biap.default.bronze_nyc_taxi_trips",
    comment="Raw NYC taxi trip data ingested from samples.nyctaxi.trips"
)
def bronze_nyc_taxi_trips():
    return spark.read.table("samples.nyctaxi.trips")  # noqa: F821
