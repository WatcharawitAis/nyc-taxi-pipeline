import pytest
from pyspark.sql import SparkSession
from databricks.connect import DatabricksSession
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

# @pytest.fixture(scope="session")
# def databricks_spark():
#     """Local Spark session for unit tests — runs on CI runner"""
#     spark = (
#         SparkSession.builder
#         .remote("local")
#         .appName("unit-tests")
#         .config("spark.sql.shuffle.partitions", "1")
#         .getOrCreate()
#     )
#     yield spark
#     spark.stop()



@pytest.fixture(scope="session")
def databricks_spark():
    """Databricks Spark session for integration tests — connects to cluster"""
    spark = (
        DatabricksSession.builder
        .serverless(True)
        .getOrCreate()
        )
    yield spark
    # ไม่ต้อง stop() เพราะ databricks-connect จัดการเอง


@pytest.fixture
def sample_taxi_schema():
    """Schema สำหรับ NYC taxi data"""
    return StructType([
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("pickup_zip", StringType(), True),
        StructField("dropoff_zip", StringType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
    ])


@pytest.fixture
def sample_taxi_data(databricks_spark, sample_taxi_schema):
    """ข้อมูลตัวอย่างสำหรับ unit tests"""
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
    return databricks_spark.createDataFrame(data, schema=sample_taxi_schema)


@pytest.fixture
def sample_silver_data(databricks_spark):
    """ข้อมูล silver layer สำหรับ integration tests"""
    schema = StructType([
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
    ])
    
    data = [
        # Sunday
        (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 30, 0), 
         "10001", "10002", 5.5, 15.0, 30.0, 11.0, 10, 1),
        # Monday
        (datetime(2023, 1, 2, 11, 0, 0), datetime(2023, 1, 2, 11, 15, 0), 
         "10003", "10004", 2.3, 8.5, 15.0, 9.2, 11, 2),
        # Tuesday
        (datetime(2023, 1, 3, 12, 0, 0), datetime(2023, 1, 3, 12, 20, 0), 
         "10005", "10006", 3.0, 10.0, 20.0, 9.0, 12, 3),
    ]
    return databricks_spark.createDataFrame(data, schema=schema)


@pytest.fixture
def expected_columns():
    """Expected column names สำหรับแต่ละ layer"""
    return {
        "bronze": [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime", 
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
        ],
        "silver": [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
            "trip_duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "pickup_day_of_week",
        ],
        "gold": [
            "day_of_week",
            "day_name",
            "total_rides",
            "total_fare",
            "avg_distance",
            "avg_fare",
            "avg_speed",
        ],
    }
