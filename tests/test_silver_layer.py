"""
Unit Tests for Silver Layer Transformations

Test Coverage:
- clean_and_validate_zip()
- validate_datetime_columns()
- calculate_trip_duration()
- calculate_avg_speed()
- extract_time_features()
- apply_data_quality_filters()

วิธีรัน:
    pytest tests/test_silver_layer.py -v
    pytest tests/test_silver_layer.py::test_zip_with_decimal_point -v
    pytest tests/test_silver_layer.py -v -k "integration"  # รัน integration tests อย่างเดียว
"""

import pytest
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

from pipeline.transformations.silver_layer import (
    clean_and_validate_zip,
    validate_datetime_columns,
    calculate_trip_duration,
    calculate_avg_speed,
    extract_time_features,
    apply_data_quality_filters,
)


# ========================================
# TEST: clean_and_validate_zip()
# ========================================

def test_zip_with_decimal_point(spark):
    """Test: Remove decimal points from zip codes"""
    data = [("10001.0",), ("10002.00",), ("10003.000",)]
    df = spark.createDataFrame(data, ["zip_code"])
    
    result = df.select("*", clean_and_validate_zip("zip_code").alias("cleaned_zip_code"))
    
    clean_zips = [r.cleaned_zip_code for r in result.collect()]
    assert clean_zips == ["10001", "10002", "10003"]


def test_zip_preserve_leading_zeros(spark):
    """Test: Leading zeros must be preserved (e.g., '01234' not '1234')"""
    data = [("01001.0",), ("02002.00",), ("03003.0",)]
    df = spark.createDataFrame(data, ["zip_code"])
    
    result = df.select("*", clean_and_validate_zip("zip_code").alias("cleaned_zip_code"))
    
    clean_zips = [r.cleaned_zip_code for r in result.collect()]
    assert clean_zips == ["01001", "02002", "03003"]


def test_zip_invalid_values_become_null(spark):
    """Test: Invalid values ('N/A', empty string, etc.) should become NULL"""
    data = [("N/A",), ("invalid",), ("",), (None,), ("abc123",), ("10-001",)]
    df = spark.createDataFrame(data, ["zip_code"])
    
    result = df.select("*", clean_and_validate_zip("zip_code").alias("cleaned_zip_code"))
    
    clean_zips = [r.cleaned_zip_code for r in result.collect()]
    assert all(z is None for z in clean_zips), "All invalid zips should be NULL"


# ========================================
# TEST: validate_datetime_columns()
# ========================================

def test_datetime_valid_strings(spark):
    """Test: Valid datetime strings should parse to timestamps"""
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00"),
        ("2023-01-01 11:00:00", "2023-01-01 11:45:00"),
        ("2023-12-31 23:59:59", "2024-01-01 00:30:00"),
    ]
    df = spark.createDataFrame(data, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    result = validate_datetime_columns(df)
    
    # Check that validated columns are NOT NULL
    valid_count = result.filter(
        F.col("valid_pickup_datetime").isNotNull() &
        F.col("valid_dropoff_datetime").isNotNull()
    ).count()
    
    assert valid_count == 3, "All 3 valid datetimes should parse correctly"


def test_datetime_iso8601_format(spark):
    """Test: ISO 8601 format with timezone (e.g., 2016-01-22T22:39:39.000+00:00)"""
    data = [
        ("2016-01-22T22:39:39.000+00:00", "2016-01-22T23:09:39.000+00:00"),
        ("2023-01-01T10:00:00Z", "2023-01-01T10:30:00Z"),
    ]
    df = spark.createDataFrame(data, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    result = validate_datetime_columns(df)
    
    valid_count = result.filter(
        F.col("valid_pickup_datetime").isNotNull() &
        F.col("valid_dropoff_datetime").isNotNull()
    ).count()
    
    assert valid_count == 2, "ISO 8601 formats should parse correctly"


def test_datetime_invalid_strings_become_null(spark):
    """
    Test: Invalid datetime strings should become NULL
    
    Edge cases:
    - 'N/A' string
    - 'invalid' string
    - Empty strings
    - Bad format numbers
    """
    data = [
        ("N/A", "2023-01-01 10:30:00"),
        ("2023-01-01 10:00:00", "invalid"),
        ("", ""),
        ("99999999", "99999999"),
    ]
    df = spark.createDataFrame(data, ["tpep_pickup_datetime", "tpep_dropoff_datetime"])
    
    result = validate_datetime_columns(df)
    
    null_pickup_count = result.filter(F.col("valid_pickup_datetime").isNull()).count()
    null_dropoff_count = result.filter(F.col("valid_dropoff_datetime").isNull()).count()
    
    # Fixed: Row 2 has VALID pickup ("2023-01-01 10:00:00"), so only 3 pickups are NULL
    assert null_pickup_count == 3, "3 out of 4 invalid pickup times should be NULL"
    assert null_dropoff_count == 3, "3 out of 4 invalid dropoff times should be NULL"


# ========================================
# TEST: calculate_trip_duration()
# ========================================

def test_trip_duration_positive_values(spark):
    """Test: Calculate duration between pickup and dropoff"""
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00"),  # 30 minutes
        ("2023-01-01 11:00:00", "2023-01-01 12:00:00"),  # 60 minutes
        ("2023-01-01 09:00:00", "2023-01-01 09:15:00"),  # 15 minutes
    ]
    df = spark.createDataFrame(data, ["valid_pickup_datetime", "valid_dropoff_datetime"])
    df = df.withColumns({
        "valid_pickup_datetime": F.to_timestamp("valid_pickup_datetime"),
        "valid_dropoff_datetime": F.to_timestamp("valid_dropoff_datetime"),
    })
    
    result = calculate_trip_duration(df)
    
    durations = [r.trip_duration_minutes for r in result.collect()]
    assert durations == [30.0, 60.0, 15.0]


def test_trip_duration_null_handling(spark):
    """Test: NULL timestamp should result in NULL duration"""
    # Fixed: Define schema explicitly when using None values
    schema = StructType([
        StructField("valid_pickup_datetime", TimestampType(), True),
        StructField("valid_dropoff_datetime", StringType(), True),
    ])
    
    data = [(None, "2023-01-01 10:30:00")]
    df = spark.createDataFrame(data, schema)
    
    # Convert the dropoff string to timestamp
    df = df.withColumns({
        "valid_dropoff_datetime": F.to_timestamp("valid_dropoff_datetime")
    })
    
    result = calculate_trip_duration(df)
    
    duration = result.first().trip_duration_minutes
    assert duration is None, "Duration should be NULL when pickup is NULL"


def test_trip_duration_negative_when_reversed(spark):
    """Test: Dropoff before pickup results in negative duration"""
    data = [("2023-01-01 12:00:00", "2023-01-01 11:00:00")]
    df = spark.createDataFrame(data, ["valid_pickup_datetime", "valid_dropoff_datetime"])
    df = df.withColumns({
        "valid_pickup_datetime": F.to_timestamp("valid_pickup_datetime"),
        "valid_dropoff_datetime": F.to_timestamp("valid_dropoff_datetime"),
    })
    
    result = calculate_trip_duration(df)
    
    duration = result.first().trip_duration_minutes
    assert duration == -60.0, "Duration should be negative when dropoff < pickup"


# ========================================
# TEST: calculate_avg_speed()
# ========================================

def test_avg_speed_normal_cases(spark):
    """Test: Speed = (distance / time) * 60"""
    data = [
        (30.0, 30.0),  # 30 miles in 30 min = 60 mph
        (10.0, 60.0),  # 10 miles in 60 min = 10 mph
        (5.0, 15.0),   # 5 miles in 15 min = 20 mph
    ]
    df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])
    
    result = calculate_avg_speed(df)
    
    speeds = [r.avg_speed_mph for r in result.collect()]
    assert speeds == [60.0, 10.0, 20.0]


def test_avg_speed_zero_duration(spark):
    """Test: Zero duration should return NULL (not infinity)"""
    data = [(10.0, 0.0), (5.0, 0.0)]
    df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])
    
    result = calculate_avg_speed(df)
    
    speeds = [r.avg_speed_mph for r in result.collect()]
    assert all(s is None for s in speeds), "Speed should be NULL when duration is 0"


def test_avg_speed_negative_duration(spark):
    """Test: Negative duration should still calculate speed (for data quality checks)"""
    data = [(10.0, -30.0)]  # 10 miles in -30 min (invalid, but calculate anyway)
    df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])
    
    result = calculate_avg_speed(df)
    
    speed = result.first().avg_speed_mph
    assert speed == -20.0, "Speed calculation should work with negative duration"


# ========================================
# TEST: extract_time_features()
# ========================================

def test_extract_time_features_hour_and_day(spark):
    """Test: Extract hour and day of week from datetime"""
    data = [
        ("2023-01-01 10:30:00",),  # Sunday, hour 10
        ("2023-01-02 14:45:00",),  # Monday, hour 14
        ("2023-01-03 23:59:00",),  # Tuesday, hour 23
    ]
    df = spark.createDataFrame(data, ["valid_pickup_datetime"])
    df = df.withColumns({
        "valid_pickup_datetime": F.to_timestamp("valid_pickup_datetime")
    })
    
    result = extract_time_features(df)
    
    hours = [r.pickup_hour for r in result.collect()]
    days = [r.pickup_day_of_week for r in result.collect()]
    
    assert hours == [10, 14, 23]
    assert days == [1, 2, 3]  # Spark's dayofweek: 1=Sunday, 2=Monday, 3=Tuesday


def test_extract_time_features_null_handling(spark):
    """Test: NULL datetime should result in NULL features"""
    schema = StructType([
        StructField("valid_pickup_datetime", TimestampType(), True),
    ])
    
    data = [(None,)]
    df = spark.createDataFrame(data, schema)
    
    result = extract_time_features(df)
    
    row = result.first()
    assert row.pickup_hour is None, "Hour should be NULL when datetime is NULL"
    assert row.pickup_day_of_week is None, "Day should be NULL when datetime is NULL"


# ========================================
# TEST: apply_data_quality_filters()
# ========================================

def test_data_quality_filters_valid_records(spark):
    """Test: Valid records pass all filters"""
    schema = StructType([
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("trip_duration_minutes", DoubleType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
    ])
    
    # Fixed: Use datetime objects instead of Column expressions
    data = [
        (10.0, 2.5, 15.0, datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 15, 0)),
        (25.0, 5.0, 30.0, datetime(2023, 1, 1, 11, 0, 0), datetime(2023, 1, 1, 11, 30, 0)),
    ]
    df = spark.createDataFrame(data, schema)
    
    result = apply_data_quality_filters(df)
    
    count = result.count()
    assert count == 2, "Both valid records should pass filters"


def test_data_quality_filters_remove_invalid(spark):
    """Test: Invalid records are filtered out"""
    schema = StructType([
        StructField("fare_amount", DoubleType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("trip_duration_minutes", DoubleType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
    ])
    
    # Fixed: Use datetime objects instead of Column expressions
    data = [
        # Valid record
        (10.0, 2.5, 15.0, datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 15, 0)),
        # Invalid: zero fare
        (0.0, 2.5, 15.0, datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 15, 0)),
        # Invalid: zero distance
        (10.0, 0.0, 15.0, datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 15, 0)),
        # Invalid: zero duration
        (10.0, 2.5, 0.0, datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 15, 0)),
        # Invalid: NULL pickup
        (10.0, 2.5, 15.0, None, datetime(2023, 1, 1, 10, 15, 0)),
        # Invalid: dropoff before pickup
        (10.0, 2.5, 15.0, datetime(2023, 1, 1, 10, 15, 0), datetime(2023, 1, 1, 10, 0, 0)),
    ]
    df = spark.createDataFrame(data, schema)
    
    result = apply_data_quality_filters(df)
    
    count = result.count()
    assert count == 1, "Only 1 valid record should pass all filters"
