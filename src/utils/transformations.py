"""Data transformation functions for NYC taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Regex pattern: matches "yellow_tripdata_YYYY-MM.parquet" anywhere in the file path
FILENAME_YEAR_MONTH_PATTERN = r"yellow_tripdata_(\d{4})-(\d{2})\.parquet"


def extract_year_month_from_filename(df: DataFrame) -> DataFrame:
    """Extracts trip_year and trip_month from the source file path.

    Args:
        df: Input DataFrame with a _source_file column (the bronze-persisted
            copy of Spark's _metadata.file_path - _metadata itself is only
            available while reading directly from files, not from a Delta
            table downstream, so it can't be read here).

    Returns:
        DataFrame with trip_year (int) and trip_month (int) columns added.
        Values are NULL if the filename doesn't match the expected pattern.
    """
    file_path = F.col("_source_file")
    year = F.regexp_extract(file_path, FILENAME_YEAR_MONTH_PATTERN, 1)
    month = F.regexp_extract(file_path, FILENAME_YEAR_MONTH_PATTERN, 2)

    return df.withColumns(
        {
            "trip_year": F.when(year == "", None).otherwise(year.cast("int")),
            "trip_month": F.when(month == "", None).otherwise(month.cast("int")),
        }
    )


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
