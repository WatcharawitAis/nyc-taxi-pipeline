"""Calculation functions for derived metrics in NYC taxi data."""

from pyspark.sql import functions as F


def calculate_trip_duration(
    df, pickup_col="tpep_pickup_datetime", dropoff_col="tpep_dropoff_datetime"
):
    """Calculates trip duration in minutes."""
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()

    return df.withColumns(
        {
            "trip_duration_minutes": (
                F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)
            )
            / 60
        }
    )


def calculate_avg_speed(
    df, distance_col="trip_distance", duration_col="trip_duration_minutes"
):
    """
    Calculates average speed in miles per hour.

    Returns NULL only for zero duration (to avoid division by zero).
    Negative durations will produce negative speeds, which helps identify data quality issues.
    """
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()

    return df.withColumns(
        {
            "avg_speed_mph": F.when(
                F.col(duration_col) != 0,
                (F.col(distance_col) / F.col(duration_col)) * 60,
            ).otherwise(None)
        }
    )
