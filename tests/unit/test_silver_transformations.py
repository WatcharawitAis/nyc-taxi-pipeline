"""Unit tests for the composed yellow_taxi_silver_transformation function in
src/pipeline/yellow_taxi/silver/silver_transformations.py.

The individual utility functions it composes (calculate_trip_duration,
calculate_avg_speed, extract_time_features, extract_year_month_from_filename)
are unit-tested on their own in test_calculations.py/test_transformations.py.
This test instead checks that the composed transformation wires them together
correctly, end to end.

Business-rule filtering (fare/distance/duration > 0, dropoff after pickup)
is enforced by DQX checks at the layer level (silver_yellow_tripdata_checks.yml),
not by this function - so it does not drop any rows itself.
"""

from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from src.pipeline.yellow_taxi.silver.silver_transformations import (
    yellow_taxi_silver_transformation,
)


class TestYellowTaxiSilverTransformation:
    """Test the composed real-TLC-data silver transformation (silver_yellow_tripdata)"""

    def _sample_df(self, spark):
        schema = StructType(
            [
                StructField("tpep_pickup_datetime", TimestampType(), True),
                StructField("tpep_dropoff_datetime", TimestampType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("fare_amount", DoubleType(), True),
            ]
        )
        data = [
            # valid trip
            (
                datetime(2026, 1, 1, 10, 0, 0),
                datetime(2026, 1, 1, 10, 30, 0),
                5.5,
                15.0,
            ),
            # negative fare - not filtered here, DQX catches it downstream
            (
                datetime(2026, 1, 1, 12, 0, 0),
                datetime(2026, 1, 1, 12, 20, 0),
                3.0,
                -5.0,
            ),
        ]
        df = spark.createDataFrame(data, schema)
        # Add _source_file for year/month extraction (the bronze-persisted
        # copy of _metadata.file_path - see bronze_transformations.py)
        df = df.withColumn(
            "_source_file",
            F.lit(
                "/Volumes/biap_dev/landing/nyc-yellow-taxi-files/2026/yellow_tripdata_2026-01.parquet"
            ),
        )
        return df

    def test_end_to_end_shape(self, spark):
        """Should derive metrics and extract time features for every row,
        tag _processed_at, and not drop any rows itself"""
        df = self._sample_df(spark)

        result = yellow_taxi_silver_transformation(df)

        assert result.count() == 2  # no filtering happens in this function
        for col in (
            "trip_year",
            "trip_month",
            "trip_duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "pickup_day_of_week",
            "_processed_at",
        ):
            assert col in result.columns

    def test_extracts_year_and_month_from_filename(self, spark):
        """trip_year/trip_month should be parsed from the source file name"""
        df = spark.createDataFrame(
            [
                (
                    datetime(2026, 3, 15, 10, 0, 0),
                    datetime(2026, 3, 15, 10, 30, 0),
                    5.5,
                    15.0,
                )
            ],
            ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance", "fare_amount"],
        )
        df = df.withColumn(
            "_source_file",
            F.lit(
                "/Volumes/biap_dev/landing/nyc-yellow-taxi-files/2026/yellow_tripdata_2026-03.parquet"
            ),
        )

        result = yellow_taxi_silver_transformation(df)
        row = result.collect()[0]

        assert row.trip_year == 2026
        assert row.trip_month == 3
