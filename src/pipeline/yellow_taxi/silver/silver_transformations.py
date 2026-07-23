"""Silver Pipeline"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.calculations import calculate_avg_speed, calculate_trip_duration
from src.utils.transformations import (
    extract_time_features,
    extract_year_month_from_filename,
)

def yellow_taxi_silver_transformation(df: DataFrame) -> DataFrame:
    """Silver Pipeline Logic for real TLC data (bronze_yellow_tripdata).

    Args:
        df: Bronze DataFrame (bronze_yellow_tripdata schema): must contain
            tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance,
            fare_amount, and _source_file for year/month extraction.

    Returns:
        DataFrame with trip_year, trip_month, trip_duration_minutes,
        avg_speed_mph, pickup_hour, pickup_day_of_week, and _processed_at
        columns added, same row count as the input.
    """
    df = extract_year_month_from_filename(df)
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    df = df.withColumn("_processed_at", F.current_timestamp())
    return df
