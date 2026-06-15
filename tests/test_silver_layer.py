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
from pyspark.sql import functions as F

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
    
    assert null_pickup_count == 4, "All 4 invalid pickup times should be NULL"
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
    data = [(None, "2023-01-01 10:30:00")]
    df = spark.createDataFrame(data, ["valid_pickup_datetime", "valid_dropoff_datetime"])
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()
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
    """Test: Negative duration should return NULL"""
    data = [(10.0, -5.0)]
    df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])
    
    result = calculate_avg_speed(df)
    
    speed = result.first().avg_speed_mph
    assert speed is None, "Speed should be NULL when duration is negative"


# ========================================
# TEST: extract_time_features()
# ========================================

def test_extract_hour_and_day_of_week(spark):
    """
    Test: Extract hour and day of week from timestamp
    
    Day of week: 1=Sunday, 2=Monday, 3=Tuesday, ..., 7=Saturday
    """
    data = [
        "2023-01-02 10:00:00",  # Monday, 10 AM (dayofweek=2)
        "2023-01-03 15:30:00",  # Tuesday, 3:30 PM (dayofweek=3)
        "2023-01-07 23:59:59",  # Saturday, 11:59 PM (dayofweek=7)
    ]
    df = spark.createDataFrame([(d,) for d in data], ["valid_pickup_datetime"])
    # แก้ lint: ใช้ .withColumns() แทน .withColumn()
    df = df.withColumns({
        "valid_pickup_datetime": F.to_timestamp("valid_pickup_datetime")
    })
    
    result = extract_time_features(df)
    
    rows = result.collect()
    
    # Monday 10 AM
    assert rows[0].pickup_hour == 10
    assert rows[0].pickup_day_of_week == 2
    
    # Tuesday 3:30 PM
    assert rows[1].pickup_hour == 15
    assert rows[1].pickup_day_of_week == 3
    
    # Saturday 11:59 PM
    assert rows[2].pickup_hour == 23
    assert rows[2].pickup_day_of_week == 7


# ========================================
# TEST: apply_data_quality_filters()
# ========================================

def test_data_quality_all_valid_records_pass(spark):
    """Test: Valid records should pass through filter"""
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", 2.5, 15.0, 30.0),
        ("2023-01-01 11:00:00", "2023-01-01 12:00:00", 5.0, 25.0, 60.0),
        ("2023-01-01 14:00:00", "2023-01-01 14:20:00", 1.0, 8.0, 20.0),
    ]
    df = spark.createDataFrame(
        data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "trip_duration_minutes"]
    )
    df = df.withColumns({
        "tpep_pickup_datetime": F.to_timestamp("tpep_pickup_datetime"),
        "tpep_dropoff_datetime": F.to_timestamp("tpep_dropoff_datetime"),
    })
    
    result = apply_data_quality_filters(df)
    
    assert result.count() == 3, "All 3 valid records should pass"


def test_data_quality_invalid_records_filtered(spark):
    """Test: Invalid records should be filtered out"""
    data = [
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", 2.5, 15.0, 30.0),   # Valid
        ("2023-01-01 11:00:00", "2023-01-01 12:00:00", 0.0, 25.0, 60.0),   # Zero distance
        ("2023-01-01 12:00:00", "2023-01-01 13:00:00", 5.0, 0.0, 60.0),    # Zero fare
        ("2023-01-01 13:00:00", "2023-01-01 14:00:00", 5.0, 25.0, -10.0),  # Negative duration
        (None, "2023-01-01 15:00:00", 5.0, 25.0, 60.0),                     # NULL pickup
    ]
    df = spark.createDataFrame(
        data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "trip_duration_minutes"]
    )
    df = df.withColumns({
        "tpep_pickup_datetime": F.to_timestamp("tpep_pickup_datetime"),
        "tpep_dropoff_datetime": F.to_timestamp("tpep_dropoff_datetime"),
    })
    
    result = apply_data_quality_filters(df)
    
    assert result.count() == 1, "Only 1 valid record should remain"
    
    row = result.first()
    assert row.trip_distance == 2.5
    assert row.fare_amount == 15.0


def test_data_quality_illogical_time_order(spark):
    """Test: Dropoff before pickup should be filtered out"""
    data = [
        ("2023-01-01 10:30:00", "2023-01-01 10:00:00", 2.5, 15.0, 30.0),  # Dropoff BEFORE pickup
    ]
    df = spark.createDataFrame(
        data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount", "trip_duration_minutes"]
    )
    df = df.withColumns({
        "tpep_pickup_datetime": F.to_timestamp("tpep_pickup_datetime"),
        "tpep_dropoff_datetime": F.to_timestamp("tpep_dropoff_datetime"),
    })
    
    result = apply_data_quality_filters(df)
    
    assert result.count() == 0, "Should filter out illogical time order"


# ========================================
# INTEGRATION TESTS
# ========================================

def test_integration_silver_pipeline_end_to_end(spark):
    """
    Integration Test: Complete silver transformation pipeline
    
    Simulates: bronze → silver transformations
    Tests all transformation functions working together
    """
    # Arrange: Create mock bronze data
    bronze_data = [
        # Valid record
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", "10001.0", "10002.0", 
         1, 2.5, 15.0, 3.0, 18.0),
        
        # Invalid datetime (should be filtered)
        ("N/A", "2023-01-01 11:00:00", "10003", "10004.0",
         2, 5.0, 25.0, 5.0, 30.0),
        
        # Zero distance (should be filtered)
        ("2023-01-01 12:00:00", "2023-01-01 13:00:00", "10005.00", "10006",
         1, 0.0, 10.0, 2.0, 12.0),
        
        # Valid record with decimal zip and leading zeros
        ("2023-01-02 08:00:00", "2023-01-02 09:00:00", "01234.0", "10007.00",
         3, 10.0, 50.0, 10.0, 60.0),
    ]
    
    bronze_df = spark.createDataFrame(
        bronze_data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_zip", "dropoff_zip",
         "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount"]
    )
    
    # Act: Apply all transformations (simulate silver_nyc_taxi_trips function)
    df = validate_datetime_columns(bronze_df)
    
    df = df.withColumns({
        "pickup_zip": clean_and_validate_zip("pickup_zip"),
        "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
    })
    
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    
    silver_df = df.select(
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
    
    result = apply_data_quality_filters(silver_df)
    
    # Assert: Check results
    assert result.count() == 2, "Should have 2 valid records after filtering"
    
    rows = result.collect()
    
    # Record 1: Verify transformations
    assert rows[0].pickup_zip == "10001"
    assert rows[0].dropoff_zip == "10002"
    assert rows[0].trip_duration_minutes == 30.0
    assert rows[0].pickup_hour == 10
    assert rows[0].avg_speed_mph == 5.0  # 2.5 miles / 30 min * 60
    
    # Record 2: Verify leading zeros preserved
    assert rows[1].pickup_zip == "01234"
    assert rows[1].dropoff_zip == "10007"
    assert rows[1].trip_duration_minutes == 60.0
    assert rows[1].avg_speed_mph == 10.0  # 10 miles / 60 min * 60


def test_integration_data_quality_rules(spark):
    """
    Integration Test: Data quality filtering rules
    
    Validates that all filter conditions work correctly together
    """
    # Test data with various quality issues
    data = [
        # Valid
        ("2023-01-01 10:00:00", "2023-01-01 10:30:00", "10001", "10002", 1, 2.5, 15.0, 3.0, 18.0),
        
        # Invalid: zero fare
        ("2023-01-01 11:00:00", "2023-01-01 12:00:00", "10003", "10004", 1, 5.0, 0.0, 0.0, 0.0),
        
        # Invalid: negative fare
        ("2023-01-01 13:00:00", "2023-01-01 14:00:00", "10005", "10006", 1, 3.0, -10.0, 0.0, -10.0),
        
        # Invalid: dropoff before pickup
        ("2023-01-01 15:00:00", "2023-01-01 14:00:00", "10007", "10008", 1, 2.0, 10.0, 2.0, 12.0),
        
        # Invalid: NULL pickup
        (None, "2023-01-01 16:00:00", "10009", "10010", 1, 2.0, 10.0, 2.0, 12.0),
    ]
    
    df = spark.createDataFrame(
        data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_zip", "dropoff_zip",
         "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount"]
    )
    
    # Apply transformations
    df = validate_datetime_columns(df)
    df = calculate_trip_duration(df)
    
    silver_df = df.select(
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
    )
    
    result = apply_data_quality_filters(silver_df)
    
    # Only 1 valid record should remain
    assert result.count() == 1, "Only 1 valid record should pass all filters"
    assert result.first().fare_amount == 15.0


def test_integration_iso8601_datetime_pipeline(spark):
    """
    Integration Test: ISO 8601 datetime format handling
    
    Tests that the pipeline handles ISO 8601 format correctly
    (e.g., 2016-01-22T22:39:39.000+00:00)
    """
    # Test data with ISO 8601 format
    data = [
        ("2016-01-22T22:39:39.000+00:00", "2016-01-22T23:09:39.000+00:00", "10001.0", "10002.0",
         1, 2.5, 15.0, 3.0, 18.0),
        ("2023-01-01T10:00:00Z", "2023-01-01T11:00:00Z", "10003", "10004",
         2, 5.0, 25.0, 5.0, 30.0),
    ]
    
    df = spark.createDataFrame(
        data,
        ["tpep_pickup_datetime", "tpep_dropoff_datetime", "pickup_zip", "dropoff_zip",
         "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount"]
    )
    
    # Apply pipeline
    df = validate_datetime_columns(df)
    df = df.withColumns({
        "pickup_zip": clean_and_validate_zip("pickup_zip"),
        "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
    })
    df = calculate_trip_duration(df)
    df = calculate_avg_speed(df)
    df = extract_time_features(df)
    
    silver_df = df.select(
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
    
    result = apply_data_quality_filters(silver_df)
    
    # Both records should pass
    assert result.count() == 2, "Both ISO 8601 records should be valid"
    
    rows = result.collect()
    
    # Verify first record (30 min duration)
    assert rows[0].trip_duration_minutes == 30.0
    assert rows[0].pickup_zip == "10001"
    
    # Verify second record (60 min duration)
    assert rows[1].trip_duration_minutes == 60.0
    assert rows[1].pickup_zip == "10003"
