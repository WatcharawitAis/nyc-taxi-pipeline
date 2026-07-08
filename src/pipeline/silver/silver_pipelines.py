"""Silver Pipeline"""
from pyspark.sql.functions import current_timestamp
from src.pipeline.utils.calculations import calculate_avg_speed, calculate_trip_duration
from src.pipeline.utils.transformations import extract_time_features
from src.pipeline.utils.validations import (
    apply_data_quality_filters,
    clean_and_validate_zip,
    validate_datetime_columns,
)
from src.pipeline.utils.constraints import SILVER_COLUMNS

def silver_pipeline(df):
    """Silver Pipeline Logic"""
    # df = validate_datetime_columns(df)
    # df = df.withColumns(
    #     {
    #         "pickup_zip": clean_and_validate_zip("pickup_zip"),
    #         "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
    #     }
    # )
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    df = df.withColumn("_processed_at", current_timestamp())
    # clean_df = df.select(SILVER_COLUMNS)
    
    # clean_df = apply_data_quality_filters(clean_df)
    return df
