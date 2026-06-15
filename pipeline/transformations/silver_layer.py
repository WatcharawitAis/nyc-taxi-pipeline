from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
)
def silver_nyc_taxi_trips():
    """
    Silver layer: Clean invalid data

    Data Quality Rules:
    - VALIDATE: DateTime must be parseable timestamps
    - REMOVE: Negative fares, zero distance, zero fares (invalid data)
    - ADD: Derived metrics (trip duration, average speed, time of day)
    - CAST: Zip codes to string (preserve leading zeros)
    """
    # Read from Bronze layer
    df = spark.read.table("bronze_nyc_taxi_trips")  # noqa: F821

    # ⭐ เพิ่มการ validate datetime columns ก่อน
    df = df.withColumns(
        {
            # ลอง parse เป็น timestamp ถ้า parse ไม่ได้จะได้ NULL
            "valid_pickup_datetime": F.to_timestamp("tpep_pickup_datetime"),
            "valid_dropoff_datetime": F.to_timestamp("tpep_dropoff_datetime"),
        }
    )

    # Calculate derived metrics using withColumns for better performance
    df = df.withColumns(
        {
            # Cast zip codes to string to preserve leading zeros
            "pickup_zip": F.col("pickup_zip").cast("string"),
            "dropoff_zip": F.col("dropoff_zip").cast("string"),
            # Derived time metrics
            "trip_duration_minutes": (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60,
            "avg_speed_mph": F.when(
                F.col("trip_duration_minutes") > 0,
                (F.col("trip_distance") / F.col("trip_duration_minutes")) * 60,
            ).otherwise(None),
            "pickup_hour": F.hour("tpep_pickup_datetime"),
            "pickup_day_of_week": F.dayofweek("tpep_pickup_datetime"),
        }
    )
    # Filter out INVALID data only (keep outliers)
    clean_df = df.filter(
        (F.col("fare_amount") > 0)  # Remove negative/zero fares
        & (F.col("trip_distance") > 0)  # Remove zero distance trips
        & (F.col("trip_duration_minutes") > 0)  # Remove invalid duration
        & (F.col("tpep_pickup_datetime").isNotNull())  # Ensure timestamps exist
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (
            F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
        )  # Logical order
    )
    return clean_df
