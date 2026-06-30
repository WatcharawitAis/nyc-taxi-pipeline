"""Integration tests for NYC Taxi Pipeline"""

from datetime import datetime

import pytest
from pyspark.sql import functions as F

from src.pipeline.silver.silver_pipelines import silver_pipeline
from src.pipeline.gold.gold_pipelines import gold_pipeline
from src.pipeline.utils.constraints import SILVER_COLUMNS, GOLD_COLUMNS
from tests.conftest import sample_taxi_data, sample_taxi_schema, sample_silver_data, spark # pylint: disable=unused-import

# Test configuration
TEST_CATALOG = "dev_test"
TEST_SCHEMA = "taxi_pipeline_tests"
BRONZE_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.bronze_taxi"
SILVER_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.silver_taxi"
GOLD_TABLE = f"{TEST_CATALOG}.{TEST_SCHEMA}.gold_taxi_daily"


@pytest.fixture
def setup_test_tables(spark):
    """Create test schema and cleanup before/after tests"""
    
    # Setup: Create schema
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {TEST_CATALOG}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TEST_CATALOG}.{TEST_SCHEMA}")

    # Cleanup existing tables
    spark.sql(f"DROP TABLE IF EXISTS {BRONZE_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {SILVER_TABLE}")
    spark.sql(f"DROP TABLE IF EXISTS {GOLD_TABLE}")


class TestBronzeToSilverIntegration:
    """Test bronze to silver layer with real tables"""

    def test_full_silver_transformation_with_real_tables(
        self, spark, setup_test_tables, sample_taxi_data
    ):
        """Test complete silver transformation with real Delta tables"""

        bronze_df = sample_taxi_data
        bronze_df.write.mode("overwrite").saveAsTable(BRONZE_TABLE)

        # 2. RUN TRANSFORMATION: Apply silver layer logic

        # Read from bronze table (real table!)
        df = spark.table(BRONZE_TABLE)

        # Apply transformations
        df = silver_pipeline(df)
        # Write to silver table (real table!)
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

        # 3. VERIFY: Read back from silver table and assert
        silver_result = spark.table(SILVER_TABLE)

        assert silver_result.count() == 2, (
            "Silver should have 2 valid records (3 filtered out)"
        )
        # Verify columns exist (compute once to avoid repeated RPC calls)
        silver_columns = silver_result.columns
        expected_cols = SILVER_COLUMNS
        assert set(silver_columns) == set(expected_cols), (
            "Silver table should have all expected columns"
        )

        # Verify data quality
        results = silver_result.collect()
        for row in results:
            assert row.fare_amount > 0, "All fares should be positive"
            assert row.trip_distance > 0, "All distances should be positive"
            assert row.trip_duration_minutes > 0, "All durations should be positive"
            assert "." not in row.pickup_zip, "Zip codes should not have decimals"

class TestSilverToGoldIntegration:
    """Test silver to gold layer with real tables"""

    def test_full_gold_aggregation_with_real_tables(self, spark, setup_test_tables, sample_silver_data):
        """Test complete gold aggregation with real Delta tables"""
        silver_df = sample_silver_data
        silver_df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

        # Read from silver table (real table!)
        df = spark.table(SILVER_TABLE)

        # Apply aggregations
        df = gold_pipeline(df)

        # Write to gold table (real table!)
        df.write.mode("overwrite").saveAsTable(GOLD_TABLE)

        # 3. VERIFY: Read back from gold table and assert
        gold_result = spark.table(GOLD_TABLE)

        assert gold_result.count() == 3, "Gold should have 3 days of week"

        # Verify columns (compute once)
        gold_columns = gold_result.columns
        expected_cols = GOLD_COLUMNS

        assert set(gold_columns) == set(expected_cols), (
            "Gold table should have all expected columns"
        )

        # Verify sorting
        days = [row.pickup_day_of_week for row in gold_result.collect()]
        assert days == sorted(days), "Results should be sorted by day of week"

        # Verify specific aggregations
        monday_row = gold_result.filter(F.col("pickup_day_of_week") == 2).collect()[0]
        assert monday_row.total_rides == 2
        assert monday_row.day_name == "Monday"


class TestEndToEndPipeline:
    """Test complete pipeline from bronze to gold with real tables"""

    def test_full_pipeline_bronze_to_gold(self, spark, setup_test_tables, sample_taxi_schema):
        """Test entire pipeline: bronze → silver → gold using real Delta tables"""

        # 1. BRONZE: Write raw data
        schema = sample_taxi_schema

        bronze_data = [
            (
                datetime(2023, 1, 1, 10, 0),
                datetime(2023, 1, 1, 10, 30),
                "10001.0",
                "10002.0",
                5.5,
                15.0,
            ),
            (
                datetime(2023, 1, 1, 11, 0),
                datetime(2023, 1, 1, 11, 45),
                "10003.0",
                "10004.0",
                8.2,
                25.5,
            ),
            (
                datetime(2023, 1, 2, 12, 0),
                datetime(2023, 1, 2, 12, 30),
                "10005.0",
                "10006.0",
                3.0,
                12.0,
            ),
        ]

        spark.createDataFrame(bronze_data, schema).write.mode("overwrite").saveAsTable(
            BRONZE_TABLE
        )

        df = spark.table(BRONZE_TABLE)
        df = silver_pipeline(df)
        df.write.mode("overwrite").saveAsTable(SILVER_TABLE)

        df = spark.table(SILVER_TABLE)
        df = gold_pipeline(df)
        df.write.mode("overwrite").saveAsTable(GOLD_TABLE)

        # 4. VERIFY: End-to-end data flow
        bronze_count = spark.table(BRONZE_TABLE).count()
        silver_count = spark.table(SILVER_TABLE).count()
        gold_count = spark.table(GOLD_TABLE).count()

        assert bronze_count == 3, "Bronze should have 3 raw records"
        assert silver_count == 3, "Silver should have 3 cleaned records"
        assert gold_count >= 1, "Gold should have at least 1 aggregated day"

        # Verify gold metrics
        gold_row = spark.table(GOLD_TABLE).collect()[0]
        assert gold_row.total_rides > 0
        assert gold_row.total_fare > 0
        assert gold_row.day_name is not None

        print(
            f"✅ Pipeline test passed: {bronze_count} bronze → {silver_count}\
             silver → {gold_count} gold"
        )
