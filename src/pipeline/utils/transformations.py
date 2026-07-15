"""Data transformation functions for NYC taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def extract_time_features(
    df: DataFrame, datetime_col: str = "tpep_pickup_datetime"
) -> DataFrame:
    """Extracts hour and day of week from a datetime column.

    Args:
        df: Input DataFrame containing datetime_col.
        datetime_col: Name of the source timestamp column.

    Returns:
        DataFrame with pickup_hour and pickup_day_of_week columns added.
    """
    return df.withColumns(
        {
            "pickup_hour": F.hour(datetime_col),
            "pickup_day_of_week": F.dayofweek(datetime_col),
        }
    )


def convert_day_number_to_name(
    df: DataFrame, day_col: str = "pickup_day_of_week"
) -> DataFrame:
    """Converts a numeric day of week (1=Sunday ... 7=Saturday) to its day name.

    Args:
        df: Input DataFrame containing day_col.
        day_col: Name of the numeric day-of-week column.

    Returns:
        DataFrame with a day_name column added. NULL for any value outside 1-7.
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
