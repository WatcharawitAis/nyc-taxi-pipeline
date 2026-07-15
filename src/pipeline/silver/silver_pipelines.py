"""Silver Pipeline"""
from pyspark.sql.functions import current_timestamp
from src.pipeline.utils.calculations import calculate_avg_speed, calculate_trip_duration
from src.pipeline.utils.transformations import extract_time_features

def silver_pipeline(df):
    """Silver Pipeline Logic"""
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    df = df.withColumn("_processed_at", current_timestamp())
    return df
