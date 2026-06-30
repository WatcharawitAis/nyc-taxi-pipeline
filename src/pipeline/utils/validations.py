"""Data validation and cleaning functions for NYC taxi data."""

from pyspark.sql import functions as F


def clean_and_validate_zip(col_name):
    """
    Cleans and validates zip codes in the specified column.

    Args:
        col_name (str): The name of the column containing zip codes.

    Returns:
        pyspark.sql.Column: A column with cleaned and validated zip codes.
    """
    # แปลงเป็น string และ trim ช่องว่างก่อน
    zip_pattern = r"^[0-9]+(\.0+)?$"
    cleaned_col = F.trim(F.col(col_name).cast("string"))

    return F.when(
        cleaned_col.rlike(zip_pattern),
        F.regexp_replace(cleaned_col, r"\.0+$", ""),
    ).otherwise(None)


def validate_datetime_columns(
    df, pickup_col="tpep_pickup_datetime", dropoff_col="tpep_dropoff_datetime"
):
    """
    Validates datetime columns by attempting to parse them as timestamps.
    Invalid strings become NULL.

    Args:
        df: Input DataFrame
        pickup_col: Name of pickup datetime column
        dropoff_col: Name of dropoff datetime column

    Returns:
        DataFrame with validated datetime columns (valid_pickup_datetime, valid_dropoff_datetime)
    """
    return df.withColumns(
        {
            "tpep_pickup_datetime": F.to_timestamp(pickup_col),
            "tpep_dropoff_datetime": F.to_timestamp(dropoff_col),
        }
    )


def apply_data_quality_filters(df):
    """
    Filters out invalid records based on business rules.

    Rules:
    - fare_amount > 0
    - trip_distance > 0
    - trip_duration_minutes > 0
    - Both datetime columns are not NULL
    - Dropoff is after pickup

    Args:
        df: Input DataFrame

    Returns:
        Filtered DataFrame
    """
    return df.filter(
        (F.col("fare_amount") > 0)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_duration_minutes") > 0)
        & (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    )
