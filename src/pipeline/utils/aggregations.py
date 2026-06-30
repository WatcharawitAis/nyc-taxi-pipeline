"""Aggregation and formatting functions for NYC taxi data."""

from pyspark.sql import functions as F


def aggregate_by_day_of_week(df, group_col="pickup_day_of_week"):
    """Aggregates trip metrics by day of week.

    Calculates:
    - total_rides: Count of trips
    - total_fare: Sum of all fares
    - avg_distance: Average trip distance
    - avg_fare: Average fare amount
    - avg_speed: Average speed

    Args:
        df: Input DataFrame from silver layer
        group_col: Column to group by

    Returns:
        Aggregated DataFrame
    """
    return df.groupBy(group_col).agg(
        F.count(F.lit(1)).alias("total_rides"),
        F.sum("fare_amount").alias("total_fare"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("avg_speed_mph").alias("avg_speed"),
    )


def round_metric_columns(df, precision=2):
    """
    Rounds numeric metric columns to specified precision.

    Args:
        df: Input DataFrame
        precision: Number of decimal places

    Returns:
        DataFrame with rounded metrics
    """
    return df.select(
        F.col("pickup_day_of_week"),
        F.col("day_name"),
        F.col("total_rides"),
        F.round(F.col("total_fare"), precision).alias("total_fare"),
        F.round(F.col("avg_distance"), precision).alias("avg_distance"),
        F.round(F.col("avg_fare"), precision).alias("avg_fare"),
        F.round(F.col("avg_speed"), precision).alias("avg_speed"),
    )


def sort_by_day_of_week(df, day_col="pickup_day_of_week"):
    """
    Sorts DataFrame by day of week (1=Sunday to 7=Saturday).

    Args:
        df: Input DataFrame
        day_col: Column name to sort by

    Returns:
        Sorted DataFrame
    """
    return df.orderBy(day_col)
