"""Silver Pipeline"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.pipeline.utils.calculations import calculate_avg_speed, calculate_trip_duration
from src.pipeline.utils.transformations import extract_time_features


def silver_pipeline(df: DataFrame) -> DataFrame:
    """Silver Pipeline Logic for real TLC data (bronze_yellow_tripdata).

    Args:
        df: Bronze DataFrame (bronze_yellow_tripdata schema): must contain
            tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance,
            fare_amount.

    Returns:
        DataFrame with trip_duration_minutes, avg_speed_mph, pickup_hour,
        pickup_day_of_week, and _processed_at columns added, same row count
        as the input.
    """
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    df = df.withColumn("_processed_at", F.current_timestamp())
    return df
