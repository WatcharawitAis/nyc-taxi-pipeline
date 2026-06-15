# Conditional import for dlt - only available in Databricks Runtime
try:
    import dlt
except ImportError:
    dlt = None  # For testing environments where dlt is not available

from pyspark.sql import functions as F


# ========================================
# PURE TRANSFORMATION FUNCTIONS (Testable)
# ========================================

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
        # ถ้าผ่านเงื่อนไข ให้ใช้ regex_replace ตัดจุดทศนิยมและเลข 0 ข้างหลังออก (เช่น "20023.0" -> "20023")
        # โครงสร้างนี้จะไม่ทำลายเลข 0 ตัวหน้า เช่น "01234.0" -> "01234"
        F.regexp_replace(cleaned_col, r"\.0+$", "")
    ).otherwise(None)


def validate_datetime_columns(df, pickup_col="tpep_pickup_datetime", dropoff_col="tpep_dropoff_datetime"):
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
    return df.withColumns({
        "valid_pickup_datetime": F.to_timestamp(pickup_col),
        "valid_dropoff_datetime": F.to_timestamp(dropoff_col),
    })


def calculate_trip_duration(df, pickup_col="valid_pickup_datetime", dropoff_col="valid_dropoff_datetime"):
    """Calculates trip duration in minutes."""
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()
    return df.withColumns({
        "trip_duration_minutes": (F.unix_timestamp(dropoff_col) - F.unix_timestamp(pickup_col)) / 60
    })


def calculate_avg_speed(df, distance_col="trip_distance", duration_col="trip_duration_minutes"):
    """Calculates average speed in miles per hour."""
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()
    return df.withColumns({
        "avg_speed_mph": F.when(
            F.col(duration_col) > 0,
            (F.col(distance_col) / F.col(duration_col)) * 60
        ).otherwise(None)
    })


def extract_time_features(df, datetime_col="valid_pickup_datetime"):
    """
    Extracts hour and day of week from datetime column.

    Args:
        df: Input DataFrame
        datetime_col: Datetime column name

    Returns:
        DataFrame with pickup_hour and pickup_day_of_week columns
    """
    return df.withColumns({
        "pickup_hour": F.hour(datetime_col),
        "pickup_day_of_week": F.dayofweek(datetime_col),
    })


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


# ========================================
# DLT TABLE DEFINITION
# ========================================

@dlt.table(
    name="silver_nyc_taxi_trips",
    comment="Cleaned NYC taxi trip data with quality flags and derived metrics",
)
def silver_nyc_taxi_trips():
    """
    Silver layer: Clean invalid data

    Data Quality Rules:
    - VALIDATE: DateTime must be parseable timestamps
    - REMOVE: Negative fares, zero distance, zero fares (invalid data)
    - ADD: Derived metrics (trip duration, average speed, time of day)
    - CAST: Zip codes to string (preserve leading zeros)
    """
    # Read from Bronze layer
    df = dlt.read("bronze_nyc_taxi_trips")

    # Apply transformations using testable functions
    df = validate_datetime_columns(df)
    
    # Clean zip codes
    df = df.withColumns({
        "pickup_zip": clean_and_validate_zip("pickup_zip"),
        "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
    })

    # Calculate metrics
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)

    # Select and rename columns
    clean_df = df.select(
        F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
        F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
        "pickup_zip",
        "dropoff_zip",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "total_amount",
        "trip_duration_minutes",
        "avg_speed_mph",
        "pickup_hour",
        "pickup_day_of_week",
    )

    # Apply filters
    clean_df = apply_data_quality_filters(clean_df)
    
    return clean_df
