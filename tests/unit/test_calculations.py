"""Unit tests for src/pipeline/utils/calculations.py"""

from datetime import datetime

from pyspark.sql.types import StructField, StructType, TimestampType

from src.utils.calculations import (
    calculate_avg_speed,
    calculate_trip_duration,
)


class TestCalculateTripDuration:
    """Test trip duration calculation"""

    def test_positive_duration(self, spark):
        """Normal trip should have positive duration"""
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
            ]
        )
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 30, 0)),
        ]
        df = spark.createDataFrame(data, schema)

        result = calculate_trip_duration(df)
        duration = result.select("trip_duration_minutes").collect()[0][0]

        assert duration == 30.0

    def test_zero_duration(self, spark):
        """Same pickup and dropoff time should give zero duration"""
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
            ]
        )
        data = [
            (datetime(2023, 1, 1, 10, 0, 0), datetime(2023, 1, 1, 10, 0, 0)),
        ]
        df = spark.createDataFrame(data, schema)

        result = calculate_trip_duration(df)
        duration = result.select("trip_duration_minutes").collect()[0][0]

        assert duration == 0.0

    def test_negative_duration(self, spark):
        """Dropoff before pickup should give negative duration (data quality issue)"""
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
            ]
        )
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
        df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])

        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]

        assert speed == 20.0

    def test_zero_duration_returns_null(self, spark):
        """Zero duration should return NULL to avoid division by zero"""
        data = [(10.0, 0.0)]
        df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])

        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]

        assert speed is None

    def test_negative_duration_gives_negative_speed(self, spark):
        """Negative duration should give negative speed (for data quality detection)"""
        data = [(10.0, -30.0)]
        df = spark.createDataFrame(data, ["trip_distance", "trip_duration_minutes"])

        result = calculate_avg_speed(df)
        speed = result.select("avg_speed_mph").collect()[0][0]

        assert speed == -20.0
