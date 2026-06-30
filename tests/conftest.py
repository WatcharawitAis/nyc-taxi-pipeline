"""This is a pytest configuration file"""

from datetime import datetime

import pytest
from databricks.connect import DatabricksSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

from src.pipeline.utils.constraints import BRONZE_COLUMNS, SILVER_COLUMNS, GOLD_COLUMNS


@pytest.fixture
def spark():
    """Create a SparkSession (the entry point to Spark functionality)"""
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture
def sample_taxi_schema():
    """Schema for NYC taxi data"""
    return StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
        ]
    )

@pytest.fixture
def sample_taxi_silver_schema():
    """Silver Schema for NYC taxi data"""
    return StructType(
        [
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
            StructField("pickup_zip", StringType(), True),
            StructField("dropoff_zip", StringType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("trip_duration_minutes", DoubleType(), True),
            StructField("avg_speed_mph", DoubleType(), True),
            StructField("pickup_hour", IntegerType(), True),
            StructField("pickup_day_of_week", IntegerType(), True),
        ]
    )


@pytest.fixture
def sample_taxi_data(spark, sample_taxi_schema):
    """Sample data for unit tests"""
    data = [
        # Valid record
        (
            datetime(2023, 1, 1, 10, 0, 0),
            datetime(2023, 1, 1, 10, 30, 0),
            "10001",
            "10002",
            5.5,
            15.0,
        ),
        # Valid record with decimal zip codes
        (
            datetime(2023, 1, 1, 11, 0, 0),
            datetime(2023, 1, 1, 11, 15, 0),
            "10003.0",
            "10004.00",
            2.3,
            8.5,
        ),
        # Invalid record - negative fare
        (
            datetime(2023, 1, 1, 12, 0, 0),
            datetime(2023, 1, 1, 12, 20, 0),
            "10005",
            "10006",
            3.0,
            -5.0,
        ),
        # Invalid record - zero distance
        (
            datetime(2023, 1, 1, 13, 0, 0),
            datetime(2023, 1, 1, 13, 10, 0),
            "10007",
            "10008",
            0.0,
            10.0,
        ),
        # Invalid record - dropoff before pickup
        (
            datetime(2023, 1, 1, 14, 30, 0),
            datetime(2023, 1, 1, 14, 0, 0),
            "10009",
            "10010",
            4.0,
            12.0,
        ),
    ]
    return spark.createDataFrame(data, schema=sample_taxi_schema)


@pytest.fixture
def sample_silver_data(spark, sample_taxi_silver_schema):
    """silver layer data for integration tests"""
    data =  [
            # Sunday (day 1)
            (
                datetime(2023, 1, 1, 10, 0),
                datetime(2023, 1, 1, 10, 30),
                "10001",
                "10002",
                5.5,
                15.0,
                30.0,
                11.0,
                10,
                1,
            ),
            # Monday (day 2)
            (
                datetime(2023, 1, 2, 11, 0),
                datetime(2023, 1, 2, 11, 45),
                "10003",
                "10004",
                8.2,
                25.5,
                45.0,
                10.9,
                11,
                2,
            ),
            (
                datetime(2023, 1, 2, 14, 0),
                datetime(2023, 1, 2, 14, 30),
                "10005",
                "10006",
                3.0,
                12.0,
                30.0,
                6.0,
                14,
                2,
            ),
            # Tuesday (day 3)
            (
                datetime(2023, 1, 3, 12, 0),
                datetime(2023, 1, 3, 12, 30),
                "10001",
                "10002",
                6.5,
                18.5,
                30.0,
                13.0,
                12,
                3,
            ),
        ]
    return spark.createDataFrame(data, schema=sample_taxi_silver_schema)


@pytest.fixture
def expected_columns():
    """Expected column names for each layer"""
    return {
        "bronze": BRONZE_COLUMNS,
        "silver": SILVER_COLUMNS,
        "gold": GOLD_COLUMNS,
    }
