from pyspark.sql import functions as F


try:
    import dlt
except ImportError:
    dlt = None  # For testing environments where dlt is not available

from src.pipeline.utils.validations import (
    apply_data_quality_filters,
    clean_and_validate_zip,
    validate_datetime_columns,
)
from src.pipeline.utils.calculations import calculate_avg_speed, calculate_trip_duration
from src.pipeline.utils.transformations import extract_time_features



if dlt is not None:

    @dlt.table(
        name="silver.silver_nyc_taxi_trips",
        comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
    )
    def silver_nyc_taxi_trips():
        """
        Silver Layer: Cleaned and enriched NYC taxi trip data
        
        Output: {catalog}.silver.silver_nyc_taxi_trips

        Data Quality Rules:
        - VALIDATE: DateTime must be parseable timestamps
        - REMOVE: Negative fares, zero distance, zero fares (invalid data)
        - ADD: Derived metrics (trip duration, average speed, time of day)
        - CAST: Zip codes to string (preserve leading zeros)
        """
        # Read from Bronze layer (bronze schema)
        df = dlt.readStream("bronze.bronze_nyc_taxi_trips")

        # Apply transformations using testable functions

        df = validate_datetime_columns(df)

        # Clean zip codes

        df = df.withColumns(
            {
                "pickup_zip": clean_and_validate_zip("pickup_zip"),
                "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
            }
        )

        # Calculate metrics

        df = calculate_trip_duration(df)
        df = calculate_avg_speed(df)
        df = extract_time_features(df)

        # Select and rename columns

        clean_df = df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "pickup_day_of_week",
        )

        # Apply filters

        clean_df = apply_data_quality_filters(clean_df)

        return clean_df
