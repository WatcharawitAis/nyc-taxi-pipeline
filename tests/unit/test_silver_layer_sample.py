"""
Unit tests for Silver layer transformation functions

Tests individual functions in isolation with synthetic data.
Run with: pytest tests/unit/test_silver_layer.py -v
"""

import pytest
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)
from tests.conftest import spark
from src.pipeline.utils.validations import (
    clean_and_validate_zip,
    validate_datetime_columns,
    apply_data_quality_filters,
)
from src.pipeline.utils.calculations import (
    calculate_trip_duration,
    calculate_avg_speed,
)
from src.pipeline.utils.transformations import extract_time_features


class TestCleanAndValidateZip:
    """Test zip code cleaning and validation"""

    def test_clean_zip_with_decimal(self, spark):
        """Zip codes with trailing .0 should be cleaned"""
        data = ["10001.0", "90210.00", "02134.000"]
        df = spark.createDataFrame(data, ["zip"])
        
        result = df.select("*", clean_and_validate_zip("zip").alias("clean_zip"))
        cleaned = [row.clean_zip for row in result.collect()]
        
        assert cleaned == ["10001", "90210", "02134"], f"The result is {cleaned}"

    def test_preserve_leading_zeros(self, spark):
        """Leading zeros in zip codes should be preserved"""
        data = [("01234.0",), ("00501",), ("00901.00",)]
        df = spark.createDataFrame(data, ["zip"])
        
        result = df.select("*", clean_and_validate_zip("zip").alias("clean_zip"))
        cleaned = [row.clean_zip for row in result.collect()]
        
        assert cleaned == ["01234", "00501", "00901"], f"The result is {cleaned}"

    def test_invalid_zip_becomes_null(self, spark):
        """Invalid zip codes should become NULL"""
        data = [("ABC12",), ("",), (None,), ("12.34",)]
        df = spark.createDataFrame(data, ["zip"])
        
        result = df.select("*", clean_and_validate_zip("zip").alias("clean_zip"))
        cleaned = [row.clean_zip for row in result.collect()]
        
        assert cleaned == [None, None, None, None]


class TestValidateDatetimeColumns:
    """Test datetime validation logic"""

    def test_valid_timestamps(self, spark):
        """Valid timestamp strings should parse correctly"""
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = validate_datetime_columns(df)
        
        # Compute columns once to avoid multiple Analyze RPCs
        result_columns = result.columns
        assert "valid_pickup_datetime" in result_columns
        assert "valid_dropoff_datetime" in result_columns
        assert result.filter(F.col("valid_pickup_datetime").isNotNull()).count() == 1

    def test_null_timestamps(self, spark):
        """NULL timestamps should remain NULL"""
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
        ])
        data = [(None, None)]
        df = spark.createDataFrame(data, schema)
        
        result = validate_datetime_columns(df)
        
        assert result.filter(F.col("valid_pickup_datetime").isNull()).count() == 1
        assert result.filter(F.col("valid_dropoff_datetime").isNull()).count() == 1


class TestCalculateTripDuration:
    """Test trip duration calculation"""

    def test_positive_duration(self, spark):
        """Normal trip should have positive duration"""
        schema = StructType([
            StructField("valid_pickup_datetime", TimestampType(), True),
            StructField("valid_dropoff_datetime", TimestampType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = calculate_trip_duration(df)
        duration = result.select("trip_duration_minutes").collect()[0][0]
        
        assert duration == 30.0

    def test_zero_duration(self, spark):
        """Same pickup and dropoff time should give zero duration"""
        schema = StructType([
            StructField("valid_pickup_datetime", TimestampType(), True),
            StructField("valid_dropoff_datetime", TimestampType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = calculate_trip_duration(df)
        duration = result.select("trip_duration_minutes").collect()[0][0]
        
        assert duration == 0.0

    def test_negative_duration(self, spark):
        """Dropoff before pickup should give negative duration (data quality issue)"""
        schema = StructType([
            StructField("valid_pickup_datetime", TimestampType(), True),
            StructField("valid_dropoff_datetime", TimestampType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 30, 0), datetime(2023, 1, 1, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = calculate_trip_duration(df)
        duration = result.select("trip_duration_minutes").collect()[0][0]
        
        assert duration == -30.0


class TestCalculateAvgSpeed:
    """Test average speed calculation"""

    def test_normal_speed(self, spark):
        """Normal trip should calculate correct speed"""
        data = [(10.0, 30.0)]  # 10 miles in 30 minutes = 20 mph
        df = spark.createDataFrame(
            data, ["trip_distance", "trip_duration_minutes"]
        )
        
        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]
        
        assert speed == 20.0

    def test_zero_duration_returns_null(self, spark):
        """Zero duration should return NULL to avoid division by zero"""
        data = [(10.0, 0.0)]
        df = spark.createDataFrame(
            data, ["trip_distance", "trip_duration_minutes"]
        )
        
        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]
        
        assert speed is None

    def test_negative_duration_gives_negative_speed(self, spark):
        """Negative duration should give negative speed (for data quality detection)"""
        data = [(10.0, -30.0)]
        df = spark.createDataFrame(
            data, ["trip_distance", "trip_duration_minutes"]
        )
        
        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]
        
        assert speed == -20.0


class TestExtractTimeFeatures:
    """Test time feature extraction"""

    def test_extract_hour_and_day(self, spark):
        """Should extract correct hour and day of week"""
        schema = StructType([
            StructField("valid_pickup_datetime", TimestampType(), True),
        ])
        # January 1, 2023 is Sunday (day 1), at 14:30 (hour 14)
        data = [(datetime(2023, 1, 1, 14, 30, 0),)]
        df = spark.createDataFrame(data, schema)
        
        result = extract_time_features(df)
        row = result.collect()[0]
        
        assert row.pickup_hour == 14
        assert row.pickup_day_of_week == 1  # Sunday

    def test_midnight_hour(self, spark):
        """Midnight should be hour 0"""
        schema = StructType([
            StructField("valid_pickup_datetime", TimestampType(), True),
        ])
        data = [(datetime(2023, 1, 1, 0, 0, 0),)]
        df = spark.createDataFrame(data, schema)
        
        result = extract_time_features(df)
        hour = result.select("pickup_hour").collect()[0][0]
        
        assert hour == 0


class TestApplyDataQualityFilters:
    """Test data quality filtering logic"""

    def test_valid_records_pass(self, spark, sample_taxi_data):
        """Valid records should pass all filters"""
        # Add required columns for filtering
        df = validate_datetime_columns(sample_taxi_data)
        df = calculate_trip_duration(df)
        
        result = apply_data_quality_filters(df)
        
        # Should keep only 2 valid records (first two in sample data)
        assert result.count() == 2

    def test_negative_fare_filtered(self, spark):
        """Negative fares should be filtered out"""
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), 
             datetime(2023, 1, 1, 10, 30, 0), 
             5.0, -10.0, 30.0),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = apply_data_quality_filters(df)
        
        assert result.count() == 0

    def test_zero_distance_filtered(self, spark):
        """Zero distance trips should be filtered out"""
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
        ])
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), 
             datetime(2023, 1, 1, 10, 30, 0), 
             0.0, 10.0, 30.0),
        ]
        df = spark.createDataFrame(data, schema)
        
        result = apply_data_quality_filters(df)
        
        assert result.count() == 0
