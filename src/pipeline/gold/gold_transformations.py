"""Gold transformation logic for real NYC TLC (yellow_tripdata) data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.transformations import convert_day_number_to_name


def filter_verified_trips(df: DataFrame) -> DataFrame:
    """Filters silver_yellow_tripdata down to rows that passed every DQX check.

    Args:
        df: silver_yellow_tripdata, unfiltered - every row still carries its
            _errors/_warnings DQX check result columns.

    Returns:
        Only the rows with no _errors/_warnings, with those columns dropped.
        Applied inline by each gold aggregation rather than reading a
        separately materialized "verified_trips" table, so a passing DQX
        batch isn't duplicated into extra storage on top of
        silver_yellow_tripdata.
    """
    return df.where(df["_errors"].isNull() & df["_warnings"].isNull()).drop(
        "_errors", "_warnings"
    )


def monthly_trip_metrics_transformation(df: DataFrame) -> DataFrame:
    """Aggregates real trip data by trip_year/trip_month.

    Args:
        df: silver_yellow_tripdata (valid trips only).

    Returns:
        One row per trip_year/trip_month, sorted chronologically.
    """
    return (
        df.groupBy("trip_year", "trip_month")
        .agg(
            F.count(F.lit(1)).alias("total_rides"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed"),
            F.round(F.avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy("trip_year", "trip_month")
    )


def pickup_zone_metrics_transformation(df: DataFrame) -> DataFrame:
    """Aggregates real trip data by pickup zone (PULocationID).

    Args:
        df: silver_yellow_tripdata (valid trips only).

    Returns:
        One row per pickup_location_id, sorted by total_rides descending.
    """
    return (
        df.groupBy(F.col("PULocationID").alias("pickup_location_id"))
        .agg(
            F.count(F.lit(1)).alias("total_rides"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        )
        .orderBy(F.desc("total_rides"))
    )


def hourly_demand_heatmap_transformation(df: DataFrame) -> DataFrame:
    """Aggregates real trip data by pickup_day_of_week x pickup_hour.

    Args:
        df: silver_yellow_tripdata (valid trips only).

    Returns:
        One row per (pickup_day_of_week, pickup_hour) pair, with a readable
        day_name column, sorted chronologically.
    """
    return (
        df.groupBy("pickup_day_of_week", "pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("total_rides"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("avg_speed_mph"), 2).alias("avg_speed"),
        )
        .transform(convert_day_number_to_name)
        .orderBy("pickup_day_of_week", "pickup_hour")
    )


def data_quality_trend_transformation(bronze_df: DataFrame, silver_df: DataFrame) -> DataFrame:
    """Aggregates ingestion volume vs. DQX pass/fail rate by trip_year/trip_month.

    Args:
        bronze_df: bronze_yellow_tripdata (all ingested records).
        silver_df: silver_yellow_tripdata, unfiltered - every row still
            carries its _errors/_warnings DQX check result columns.

    Returns:
        One row per trip_year/trip_month with total_records, valid_count,
        quarantine_count, and quarantine_rate_pct.
    """
    bronze_counts = bronze_df.groupBy("trip_year", "trip_month").agg(
        F.count(F.lit(1)).alias("total_records")
    )

    is_valid = silver_df["_errors"].isNull() & silver_df["_warnings"].isNull()
    silver_counts = silver_df.groupBy("trip_year", "trip_month").agg(
        F.count(F.when(is_valid, 1)).alias("valid_count"),
        F.count(F.when(~is_valid, 1)).alias("quarantine_count"),
    )

    return (
        bronze_counts.join(silver_counts, ["trip_year", "trip_month"], "left")
        .fillna(0, subset=["valid_count", "quarantine_count"])
        .withColumn(
            "quarantine_rate_pct",
            F.round(F.col("quarantine_count") / F.col("total_records") * 100, 2),
        )
        .orderBy("trip_year", "trip_month")
    )
