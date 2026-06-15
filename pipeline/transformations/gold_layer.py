from pyspark.sql import functions as F


try:
    import dlt
except ImportError:
    dlt = None  # For testing environments where dlt is not available


def convert_day_number_to_name(df, day_col="pickup_day_of_week"):
    """Converts numeric day of week (1-7) to day name.

    Spark's dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
    """
    # Use .withColumns() instead of .withColumn() for Spark Connect compatibility

    return df.withColumns(
        {
            "day_name": F.expr(
                f"""
            CASE {day_col}
                WHEN 1 THEN 'Sunday'
                WHEN 2 THEN 'Monday'
                WHEN 3 THEN 'Tuesday'
                WHEN 4 THEN 'Wednesday'
                WHEN 5 THEN 'Thursday'
                WHEN 6 THEN 'Friday'
                WHEN 7 THEN 'Saturday'
            END
        """
            )
        }
    )


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
        F.col("pickup_day_of_week").alias("day_of_week"),
        F.col("day_name"),
        F.col("total_rides"),
        F.col("total_fare"),
        F.round(F.col("avg_distance"), precision).alias("avg_distance"),
        F.round(F.col("avg_fare"), precision).alias("avg_fare"),
        F.round(F.col("avg_speed"), precision).alias("avg_speed"),
    )


def sort_by_day_of_week(df, day_col="day_of_week"):
    """
    Sorts DataFrame by day of week (1=Sunday to 7=Saturday).

    Args:
        df: Input DataFrame
        day_col: Column name to sort by

    Returns:
        Sorted DataFrame
    """
    return df.orderBy(day_col)


# ========================================
# DLT VIEW DEFINITION
# ========================================

# Only define DLT views when dlt module is available (Databricks Runtime)


if dlt is not None:

    @dlt.view(
        name="day_of_week_metrics",
        comment="Daily aggregated metrics for the number of rides, "
        "average distance, average fare, and average speed for each day of the week.",
    )
    def day_of_week_metrics():
        """
        Gold layer: Aggregated metrics by day of week

        Provides business-ready metrics grouped by day of week with readable day names.

        Transformations applied:
        1. Aggregate trips by day of week
        2. Convert day numbers to names
        3. Round metrics to 2 decimal places
        4. Sort by day of week
        """
        # Read from Silver layer

        df = dlt.read("silver_nyc_taxi_trips")

        # Apply transformations using testable functions

        df = aggregate_by_day_of_week(df)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        df = sort_by_day_of_week(df)

        return df
