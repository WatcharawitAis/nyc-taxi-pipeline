"""Unit tests for src/pipeline/utils/transformations.py"""

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, TimestampType

from src.pipeline.utils.transformations import (
    convert_day_number_to_name,
    extract_time_features,
    extract_year_month_from_filename,
)


class TestExtractYearMonthFromFilename:
    """Test year/month extraction from file path"""

    def test_extract_from_valid_filename(self, spark):
        """Should extract year and month from valid filename pattern"""
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn(
            "_metadata",
            F.struct(
                F.lit(
                    "/Volumes/biap_dev/landing/nyc-yellow-taxi-files/2026/yellow_tripdata_2026-03.parquet"
                ).alias("file_path")
            ),
        )

        result = extract_year_month_from_filename(df)
        row = result.collect()[0]

        assert row.trip_year == 2026
        assert row.trip_month == 3

    def test_different_year_month(self, spark):
        """Should extract different year/month combinations correctly"""
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn(
            "_metadata",
            F.struct(
                F.lit(
                    "/some/other/path/yellow_tripdata_2025-12.parquet"
                ).alias("file_path")
            ),
        )

        result = extract_year_month_from_filename(df)
        row = result.collect()[0]

        assert row.trip_year == 2025
        assert row.trip_month == 12

    def test_invalid_filename_returns_null(self, spark):
        """Should return NULL for files that don't match the pattern"""
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn(
            "_metadata",
            F.struct(F.lit("/some/invalid/file.parquet").alias("file_path")),
        )

        result = extract_year_month_from_filename(df)
        row = result.collect()[0]

        assert row.trip_year is None
        assert row.trip_month is None


class TestExtractTimeFeatures:
    """Test time feature extraction"""

    def test_extract_hour_and_day(self, spark):
        """Should extract correct hour and day of week"""
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
            ]
        )
        # January 1, 2023 is Sunday (day 1), at 14:30 (hour 14). Pinned to UTC
        # explicitly - a naive datetime here would be encoded in the client's
        # local timezone but decoded against the cluster's session timezone,
        # silently drifting the hour depending on where tests are run from.
        data = [(datetime(2023, 1, 1, 14, 30, 0, tzinfo=timezone.utc),)]
        df = spark.createDataFrame(data, schema)

        result = extract_time_features(df)
        row = result.collect()[0]

        assert row.pickup_hour == 14
        assert row.pickup_day_of_week == 1  # Sunday

    def test_midnight_hour(self, spark):
        """Midnight should be hour 0"""
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
            ]
        )
        data = [(datetime(2023, 1, 1, 0, 0, 0, tzinfo=timezone.utc),)]
        df = spark.createDataFrame(data, schema)

        result = extract_time_features(df)
        hour = result.select("pickup_hour").collect()[0][0]

        assert hour == 0


class TestConvertDayNumberToName:
    """Test day number to name conversion"""

    def test_all_days_conversion(self, spark):
        """All day numbers should convert correctly"""
        data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]
        df = spark.createDataFrame(data, ["pickup_day_of_week"])

        result = convert_day_number_to_name(df)
        day_names = [
            row.day_name for row in result.orderBy("pickup_day_of_week").collect()
        ]

        expected = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        assert day_names == expected

    def test_invalid_day_number(self, spark):
        """Invalid day numbers should return None"""
        data = [(0,), (8,), (None,)]
        df = spark.createDataFrame(data, ["pickup_day_of_week"])

        result = convert_day_number_to_name(df)
        day_names = [row.day_name for row in result.collect()]

        assert day_names == [None, None, None]
