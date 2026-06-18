"""
Integration tests for NYC Taxi Pipeline

Tests end-to-end pipeline with REAL Delta tables in Unity Catalog.
These tests write to test catalog, run transformations, and verify results.

Prerequisites:
- Test catalog 'dev_test' with CREATE/WRITE permissions
- Databricks cluster with Unity Catalog enabled

Run with: pytest tests/integration/test_pipeline.py -v --tb=short
"""

import pytest
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


# Test configuration
TEST_CATALOG = "dev_test"
TEST_SCHEMA = "taxi_pipeline_tests"
BRONZE_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.bronze_taxi"
SILVER_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.silver_taxi"
GOLD_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.gold_taxi_daily"


@pytest.fixture(scope="module")
def setup_test_tables(databricks_spark):
    """Create test schema and cleanup before/after tests"""
    
    # Setup: Create schema
    databricks_spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
    databricks_spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")
    
    # Cleanup existing tables
    databricks_spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
    databricks_spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
    databricks_spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")
    
    yield
    
    # Teardown: Cleanup after all tests (optional, comment if want to inspect)
    # databricks_spark.sql(f"DROP SCHEMA IF EXISTS {TEST_CATALOG}.{TEST_SCHEMA} CASCADE")


class TestBronzeToSilverIntegration:
    """Test bronze to silver layer with real tables"""

    def test_full_silver_transformation_with_real_tables(self, databricks_spark, setup_test_tables):
        """Test complete silver transformation with real Delta tables"""
        
        # 1. SETUP: Write test data to bronze table
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ])
        
        bronze_data = [
            # Valid records
            (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "10001.0", "10002.0", 5.5, 15.0),
            (datetime(2023, 1, 2, 11, 0), datetime(2023, 1, 2, 11, 45), "10003.0", "10004.0", 8.2, 25.5),
            # Invalid records (should be filtered in silver)
            (datetime(2023, 1, 3, 12, 0), datetime(2023, 1, 3, 12, 30), "10001.0", "10002.0", -5.0, 15.0),  # Negative distance
            (datetime(2023, 1, 4, 13, 0), datetime(2023, 1, 4, 13, 30), "10001.0", "10002.0", 5.0, -10.0),  # Negative fare
            (datetime(2023, 1, 5, 14, 0), datetime(2023, 1, 5, 13, 0), "10001.0", "10002.0", 5.0, 15.0),    # Dropoff before pickup
        ]
        
        bronze_df = databricks_spark.createDataFrame(bronze_data, schema)
        bronze_df.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
        
        # 2. RUN TRANSFORMATION: Apply silver layer logic
        from src.pipeline.silver.silver_layer import (
            validate_datetime_columns, clean_and_validate_zip,
            calculate_trip_duration, calculate_avg_speed,
            extract_time_features, apply_data_quality_filters
        )
        
        # Read from bronze table (real table!)
        df = databricks_spark.table(BRONZE_TABLE)
        
        # Apply transformations
        df = validate_datetime_columns(df)
        df = df.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        df = calculate_trip_duration(df)
        df = calculate_avg_speed(df)
        df = extract_time_features(df)
        
        df = df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount",
            "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"
        )
        
        df = apply_data_quality_filters(df)
        
        # Write to silver table (real table!)
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
        
        # 3. VERIFY: Read back from silver table and assert
        silver_result = databricks_spark.table(SILVER_TABLE)
        
        assert silver_result.count() == 2, "Silver should have 2 valid records (3 filtered out)"
        
        # Verify columns exist (compute once to avoid repeated RPC calls)
        silver_columns = silver_result.columns
        expected_cols = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount",
            "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"
        ]
        assert set(silver_columns) == set(expected_cols), "Silver table should have all expected columns"
        
        # Verify data quality
        results = silver_result.collect()
        for row in results:
            assert row.fare_amount > 0, "All fares should be positive"
            assert row.trip_distance > 0, "All distances should be positive"
            assert row.trip_duration_minutes > 0, "All durations should be positive"
            assert "." not in row.pickup_zip, "Zip codes should not have decimals"

    def test_zip_code_cleaning_in_real_table(self, databricks_spark, setup_test_tables):
        """Test zip code cleaning writes correctly to Delta table"""
        
        # Write data with decimal zip codes
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ])
        
        data = [(datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), 
                 "10001.0", "10002.0", 5.0, 15.0)]
        
        df = databricks_spark.createDataFrame(data, schema)
        df.write.mode("overwrite").saveAsTable(BRONZE_TABLE)
        
        # Transform and write to silver
        from src.pipeline.silver.silver_layer import clean_and_validate_zip
        
        df = databricks_spark.table(BRONZE_TABLE)
        df = df.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
        
        # Verify persisted data
        result = databricks_spark.table(SILVER_TABLE).collect()[0]
        assert result.pickup_zip == "10001"
        assert result.dropoff_zip == "10002"


class TestSilverToGoldIntegration:
    """Test silver to gold layer with real tables"""

    def test_full_gold_aggregation_with_real_tables(self, databricks_spark, setup_test_tables):
        """Test complete gold aggregation with real Delta tables"""
        
        # 1. SETUP: Write test data to silver table
        # In TestSilverToGoldIntegration class, around line 170-179
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("avg_speed_mph", DoubleType(), True),
            StructField("pickup_hour", IntegerType(), True),      # Changed from DoubleType
            StructField("pickup_day_of_week", IntegerType(), True),  # Changed from DoubleType
        ])
        
        silver_data = [
            # Sunday (day 1)
            (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "10001", "10002", 5.5, 15.0, 30.0, 11.0, 10, 1),
            # Monday (day 2)
            (datetime(2023, 1, 2, 11, 0), datetime(2023, 1, 2, 11, 45), "10003", "10004", 8.2, 25.5, 45.0, 10.9, 11, 2),
            (datetime(2023, 1, 2, 14, 0), datetime(2023, 1, 2, 14, 30), "10005", "10006", 3.0, 12.0, 30.0, 6.0, 14, 2),
            # Tuesday (day 3)
            (datetime(2023, 1, 3, 12, 0), datetime(2023, 1, 3, 12, 30), "10001", "10002", 6.5, 18.5, 30.0, 13.0, 12, 3),
        ]
        
        silver_df = databricks_spark.createDataFrame(silver_data, schema)
        silver_df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
        
        # 2. RUN TRANSFORMATION: Apply gold layer logic
        from src.pipeline.gold.gold_layer import (
            aggregate_by_day_of_week, convert_day_number_to_name,
            round_metric_columns, sort_by_day_of_week
        )
        
        # Read from silver table (real table!)
        df = databricks_spark.table(SILVER_TABLE)
        
        # Apply aggregations
        df = aggregate_by_day_of_week(df)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        df = sort_by_day_of_week(df)
        
        # Write to gold table (real table!)
        df.write.mode("overwrite").saveAsTable(GOLD_TABLE)
        
        # 3. VERIFY: Read back from gold table and assert
        gold_result = databricks_spark.table(GOLD_TABLE)
        
        assert gold_result.count() == 3, "Gold should have 3 days of week"
        
        # Verify columns (compute once)
        gold_columns = gold_result.columns
        expected_cols = [
            "day_of_week", "day_name", "total_rides",
            "total_fare", "avg_distance", "avg_fare", "avg_speed"
        ]
        assert set(gold_columns) == set(expected_cols), "Gold table should have all expected columns"
        
        # Verify sorting
        days = [row.day_of_week for row in gold_result.collect()]
        assert days == sorted(days), "Results should be sorted by day of week"
        
        # Verify specific aggregations
        monday_row = gold_result.filter(F.col("day_of_week") == 2).collect()[0]
        assert monday_row.total_rides == 2
        assert monday_row.day_name == "Monday"


class TestEndToEndPipeline:
    """Test complete pipeline from bronze to gold with real tables"""

    def test_full_pipeline_bronze_to_gold(self, databricks_spark, setup_test_tables):
        """Test entire pipeline: bronze → silver → gold using real Delta tables"""
        
        # 1. BRONZE: Write raw data
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ])
        
        bronze_data = [
            (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "10001.0", "10002.0", 5.5, 15.0),
            (datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 11, 45), "10003.0", "10004.0", 8.2, 25.5),
            (datetime(2023, 1, 2, 12, 0), datetime(2023, 1, 2, 12, 30), "10005.0", "10006.0", 3.0, 12.0),
        ]
        
        databricks_spark.createDataFrame(bronze_data, schema).write.mode("overwrite").saveAsTable(BRONZE_TABLE)
        
        # 2. SILVER: Transform bronze → silver
        from src.pipeline.silver.silver_layer import (
            validate_datetime_columns, clean_and_validate_zip,
            calculate_trip_duration, calculate_avg_speed,
            extract_time_features, apply_data_quality_filters
        )
        
        df = databricks_spark.table(BRONZE_TABLE)
        df = validate_datetime_columns(df)
        df = df.withColumns({
            "pickup_zip": clean_and_validate_zip("pickup_zip"),
            "dropoff_zip": clean_and_validate_zip("dropoff_zip"),
        })
        df = calculate_trip_duration(df)
        df = calculate_avg_speed(df)
        df = extract_time_features(df)
        df = df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount",
            "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"
        )
        df = apply_data_quality_filters(df)
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
        
        # 3. GOLD: Transform silver → gold
        from src.pipeline.gold.gold_layer import (
            aggregate_by_day_of_week, convert_day_number_to_name,
            round_metric_columns, sort_by_day_of_week
        )
        
        df = databricks_spark.table(SILVER_TABLE)
        df = aggregate_by_day_of_week(df)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        df = sort_by_day_of_week(df)
        df.write.mode("overwrite").saveAsTable(GOLD_TABLE)
        
        # 4. VERIFY: End-to-end data flow
        bronze_count = databricks_spark.table(BRONZE_TABLE).count()
        silver_count = databricks_spark.table(SILVER_TABLE).count()
        gold_count = databricks_spark.table(GOLD_TABLE).count()
        
        assert bronze_count == 3, "Bronze should have 3 raw records"
        assert silver_count == 3, "Silver should have 3 cleaned records"
        assert gold_count >= 1, "Gold should have at least 1 aggregated day"
        
        # Verify gold metrics
        gold_row = databricks_spark.table(GOLD_TABLE).collect()[0]
        assert gold_row.total_rides > 0
        assert gold_row.total_fare > 0
        assert gold_row.day_name is not None
        
        print(f"✅ Pipeline test passed: {bronze_count} bronze → {silver_count} silver → {gold_count} gold")

    def test_data_quality_enforcement_across_layers(self, databricks_spark, setup_test_tables):
        """Test data quality rules enforced when writing to real tables"""
        
        # Write data with quality issues to bronze
        schema = StructType([
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ])
        
        data = [
            (datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 30), "10001", "10002", 5.0, 15.0),  # Valid
            (datetime(2023, 1, 1, 11, 0), datetime(2023, 1, 1, 11, 30), "10001", "10002", 5.0, -10.0), # Negative fare
            (datetime(2023, 1, 1, 12, 0), datetime(2023, 1, 1, 12, 30), "10001", "10002", 0.0, 15.0),  # Zero distance
        ]
        
        databricks_spark.createDataFrame(data, schema).write.mode("overwrite").saveAsTable(BRONZE_TABLE)
        
        # Transform with quality filters
        from src.pipeline.silver.silver_layer import (
            validate_datetime_columns, calculate_trip_duration, apply_data_quality_filters
        )
        
        df = databricks_spark.table(BRONZE_TABLE)
        df = validate_datetime_columns(df)
        df = calculate_trip_duration(df)
        df = df.select(
            F.col("valid_pickup_datetime").alias("tpep_pickup_datetime"),
            F.col("valid_dropoff_datetime").alias("tpep_dropoff_datetime"),
            "pickup_zip", "dropoff_zip", "trip_distance", "fare_amount", "trip_duration_minutes"
        )
        df = apply_data_quality_filters(df)
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)
        
        # Verify only valid record persisted
        silver_count = databricks_spark.table(SILVER_TABLE).count()
        assert silver_count == 1, "Only 1 valid record should be written to silver"
        
        valid_row = databricks_spark.table(SILVER_TABLE).collect()[0]
        assert valid_row.fare_amount == 15.0
        assert valid_row.trip_distance == 5.0