"""
Integration tests for NYC Taxi Pipeline

Tests end-to-end pipeline flow from bronze to gold layer.
These tests use Databricks Connect to run against a real cluster.

Run with: pytest tests/integration/test_pipeline.py -v --tb=short
"""

import pytest
from pyspark.sql import functions as F
from datetime import datetime

from src.pipeline.silver.silver_layer import (
    validate_datetime_columns,
    clean_and_validate_zip,
    calculate_trip_duration,
    calculate_avg_speed,
    extract_time_features,
    apply_data_quality_filters,
)

from src.pipeline.bronze.gold_layer import (
    convert_day_number_to_name,
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)


class TestBronzeToSilverIntegration:
    """Test bronze to silver layer transformation"""

    def test_full_silver_transformation(self, databricks_spark, sample_taxi_data):
        """Test complete silver layer transformation pipeline"""
        
        # Apply all transformations in sequence (mimics DLT pipeline)
        df = validate_datetime_columns(sample_taxi_data)
        
        # Clean zip codes
        df = df.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        
        # Calculate metrics
        df = calculate_trip_duration(df)
        df = calculate_avg_speed(df)
        df = extract_time_features(df)
        
        # Select columns
        clean_df = df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "pickup_day_of_week",
        )
        
        # Apply filters
        final_df = apply_data_quality_filters(clean_df)
        
        # Assertions
        assert final_df.count() == 2, "Should keep only 2 valid records"
        
        # Check that all expected columns exist
        expected_cols = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount",
            "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"
        ]
        for col in expected_cols:
            assert col in final_df.columns
        
        # Verify data quality
        results = final_df.collect()
        for row in results:
            assert row.fare_amount > 0, "All fares should be positive"
            assert row.trip_distance > 0, "All distances should be positive"
            assert row.trip_duration_minutes > 0, "All durations should be positive"

    def test_zip_code_cleaning_integration(self, databricks_spark, sample_taxi_data):
        """Test that zip codes are properly cleaned in the pipeline"""
        
        df = sample_taxi_data.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        
        results = df.filter(F.col("pickup_zip").isNotNull()).collect()
        
        # Check that decimal points are removed
        for row in results:
            assert "." not in row.pickup_zip, "Zip codes should not contain decimals"
            if row.dropoff_zip:
                assert "." not in row.dropoff_zip, "Zip codes should not contain decimals"

    def test_datetime_validation_integration(self, databricks_spark, sample_taxi_data):
        """Test datetime validation in pipeline context"""
        
        df = validate_datetime_columns(sample_taxi_data)
        
        # Count valid timestamps
        valid_count = df.filter(
            F.col("valid_pickup_datetime").isNotNull() &
            F.col("valid_dropoff_datetime").isNotNull()
        ).count()
        
        assert valid_count == 5, "All 5 records should have valid timestamps"


class TestSilverToGoldIntegration:
    """Test silver to gold layer aggregation"""

    def test_full_gold_transformation(self, databricks_spark, sample_silver_data):
        """Test complete gold layer transformation pipeline"""
        
        # Apply all transformations in sequence (mimics DLT pipeline)
        df = aggregate_by_day_of_week(sample_silver_data)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        df = sort_by_day_of_week(df)
        
        # Assertions
        assert df.count() == 3, "Should have 3 days of week"
        
        # Check that all expected columns exist
        expected_cols = [
            "day_of_week", "day_name", "total_rides",
            "total_fare", "avg_distance", "avg_fare", "avg_speed"
        ]
        for col in expected_cols:
            assert col in df.columns
        
        # Verify sorting (Sunday=1 to Saturday=7)
        days = [row.day_of_week for row in df.collect()]
        assert days == sorted(days), "Results should be sorted by day of week"
        
        # Verify day names are present
        day_names = [row.day_name for row in df.collect()]
        assert all(name is not None for name in day_names), "All day names should be present"

    def test_aggregation_accuracy(self, databricks_spark, sample_silver_data):
        """Test that aggregations calculate correct values"""
        
        df = aggregate_by_day_of_week(sample_silver_data)
        
        # Get Sunday (day 1) metrics
        sunday_row = df.filter(F.col("pickup_day_of_week") == 1).collect()[0]
        
        assert sunday_row.total_rides == 1
        assert sunday_row.total_fare == 15.0
        assert sunday_row.avg_distance == 5.5

    def test_metric_rounding_integration(self, databricks_spark, sample_silver_data):
        """Test that metrics are properly rounded"""
        
        df = aggregate_by_day_of_week(sample_silver_data)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        
        results = df.collect()
        
        # Check that all numeric columns have at most 2 decimal places
        for row in results:
            # Convert to string and check decimal places
            if row.total_fare is not None:
                fare_str = str(row.total_fare)
                if "." in fare_str:
                    decimals = len(fare_str.split(".")[1])
                    assert decimals <= 2, f"Fare should have at most 2 decimals, got {decimals}"


class TestEndToEndPipeline:
    """Test complete pipeline from bronze to gold"""

    def test_full_pipeline_flow(self, databricks_spark, sample_taxi_data, expected_columns):
        """Test the entire pipeline transformation flow"""
        
        # BRONZE to SILVER
        silver_df = validate_datetime_columns(sample_taxi_data)
        silver_df = silver_df.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        silver_df = calculate_trip_duration(silver_df)
        silver_df = calculate_avg_speed(silver_df)
        silver_df = extract_time_features(silver_df)
        
        silver_df = silver_df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount",
            "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"
        )
        
        silver_df = apply_data_quality_filters(silver_df)
        
        # Verify silver layer
        assert silver_df.count() == 2, "Silver layer should have 2 valid records"
        assert set(silver_df.columns) == set(expected_columns["silver"])
        
        # SILVER to GOLD
        gold_df = aggregate_by_day_of_week(silver_df)
        gold_df = convert_day_number_to_name(gold_df)
        gold_df = round_metric_columns(gold_df)
        gold_df = sort_by_day_of_week(gold_df)
        
        # Verify gold layer
        assert gold_df.count() >= 1, "Gold layer should have at least 1 day"
        assert set(gold_df.columns) == set(expected_columns["gold"])
        
        # Verify business metrics exist
        first_row = gold_df.collect()[0]
        assert first_row.total_rides > 0
        assert first_row.total_fare > 0
        assert first_row.avg_distance > 0
        assert first_row.day_name is not None

    def test_data_quality_enforcement(self, databricks_spark):
        """Test that data quality rules are enforced throughout pipeline"""
        
        # Create a dataset with various quality issues
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
        
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ])
        
        data = [
            # Valid record
            (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "10001", "10002", 5.0, 15.0),
            # Negative fare - should be filtered
            (datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 11, 30), "10001", "10002", 5.0, -10.0),
            # Zero distance - should be filtered
            (datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 12, 30), "10001", "10002", 0.0, 15.0),
            # Dropoff before pickup - should be filtered
            (datetime(2023, 1, 1, 14, 0), datetime(2023, 1, 1, 13, 0), "10001", "10002", 5.0, 15.0),
        ]
        
        df = databricks_spark.createDataFrame(data, schema)
        
        # Run through silver pipeline
        silver_df = validate_datetime_columns(df)
        silver_df = calculate_trip_duration(silver_df)
        silver_df = silver_df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount", "trip_duration_minutes"
        )
        clean_df = apply_data_quality_filters(silver_df)
        
        # Only 1 valid record should remain
        assert clean_df.count() == 1, "Data quality filters should remove 3 invalid records"
        
        valid_row = clean_df.collect()[0]
        assert valid_row.fare_amount == 15.0
        assert valid_row.trip_distance == 5.0
