"""Calculation functions for derived metrics in NYC taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_trip_duration(
    df: DataFrame,
    pickup_col: str = "tpep_pickup_datetime",
    dropoff_col: str = "tpep_dropoff_datetime",
) -> DataFrame:
    """Calculates trip duration in minutes.

    Args:
        df: Input DataFrame containing pickup_col and dropoff_col.
        pickup_col: Name of the pickup timestamp column.
        dropoff_col: Name of the dropoff timestamp column.

    Returns:
        DataFrame with a trip_duration_minutes column added.
    """
    return df.withColumns(
        {
            "trip_duration_minutes": (
                F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)
            )
            / 60
        }
    )


def calculate_avg_speed(
    df: DataFrame,
    distance_col: str = "trip_distance",
    duration_col: str = "trip_duration_minutes",
) -> DataFrame:
    """Calculates average speed in miles per hour.

    Args:
        df: Input DataFrame containing distance_col and duration_col.
        distance_col: Name of the trip distance column (miles).
        duration_col: Name of the trip duration column (minutes).

    Returns:
        DataFrame with an avg_speed_mph column added. NULL when duration is 0.
    """
    return df.withColumns(
        {
            "avg_speed_mph": F.when(
                F.col(duration_col) != 0,
                (F.col(distance_col) / F.col(duration_col)) * 60,
            ).otherwise(None)
        }
    )
