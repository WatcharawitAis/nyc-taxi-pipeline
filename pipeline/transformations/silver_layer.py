from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="biap.default.silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics"
)
def silver_nyc_taxi_trips():
    """
    Silver layer: Clean invalid data
    
    Data Quality Rules:
    - REMOVE: Negative fares, zero distance, zero fares (invalid data)
    - ADD: Derived metrics (trip duration, average speed, time of day)
    """
    # Read from Bronze layer
    df = spark.read.table("biap.default.bronze_nyc_taxi_trips")
    
    # Calculate derived metrics
    df = df.withColumn(
        "trip_duration_minutes",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60
    ).withColumn(
        "avg_speed_mph",
        F.when(
            F.col("trip_duration_minutes") > 0,
            (F.col("trip_distance") / F.col("trip_duration_minutes")) * 60
        ).otherwise(None)
    ).withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    ).withColumn(
        "pickup_day_of_week",
        F.dayofweek("tpep_pickup_datetime")
    )
    # Filter out INVALID data only (keep outliers)
    clean_df = df.filter(
        (F.col("fare_amount") > 0) &                          # Remove negative/zero fares
        (F.col("trip_distance") > 0) &                        # Remove zero distance trips
        (F.col("trip_duration_minutes") > 0) &                # Remove invalid duration
        (F.col("tpep_pickup_datetime").isNotNull()) &         # Ensure timestamps exist
        (F.col("tpep_dropoff_datetime").isNotNull()) &
        (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))  # Logical order
    )

    return clean_df
