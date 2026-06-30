"""Data transformation functions for NYC taxi data."""

from pyspark.sql import functions as F


def extract_time_features(df, datetime_col="tpep_pickup_datetime"):
    """
    Extracts hour and day of week from datetime column.

    Args:
        df: Input DataFrame
        datetime_col: Datetime column name

    Returns:
        DataFrame with pickup_hour and pickup_day_of_week columns
    """
    return df.withColumns(
        {
            "pickup_hour": F.hour(datetime_col),
            "pickup_day_of_week": F.dayofweek(datetime_col),
        }
    )


def convert_day_number_to_name(df, day_col="pickup_day_of_week"):
    """Converts numeric day of week (1-7) to day name.

    Spark's dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
    """
    return df.withColumns(
        {
            "day_name": F.when(F.col(day_col) == 1, "Sunday")
            .when(F.col(day_col) == 2, "Monday")
            .when(F.col(day_col) == 3, "Tuesday")
            .when(F.col(day_col) == 4, "Wednesday")
            .when(F.col(day_col) == 5, "Thursday")
            .when(F.col(day_col) == 6, "Friday")
            .when(F.col(day_col) == 7, "Saturday")
            .otherwise(None)
        }
    )
