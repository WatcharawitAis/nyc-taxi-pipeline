"""Unit tests for the composed silver_pipeline function in
src/pipeline/silver/silver_pipelines.py.

The individual utility functions it composes (calculate_trip_duration,
calculate_avg_speed, extract_time_features) are unit-tested on their own in
test_calculations.py/test_transformations.py. This test instead checks that
the composed pipeline wires them together correctly, end to end.

Business-rule filtering (fare/distance/duration > 0, dropoff after pickup)
is enforced by DQX checks at the layer level (silver_yellow_tripdata_checks.yml),
not by this function - so it does not drop any rows itself.
"""

from datetime import datetime

from pyspark.sql.types import (
    DoubleType,
    StructField,
    StructType,
    TimestampType,
)

from src.pipeline.silver.silver_pipelines import silver_pipeline


class TestSilverPipeline:
    """Test the composed real-TLC-data silver pipeline (silver_yellow_tripdata)"""

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
        return spark.createDataFrame(data, schema)

    def test_end_to_end_shape(self, spark):
        """Should derive metrics and extract time features for every row,
        tag _processed_at, and not drop any rows itself"""
        df = self._sample_df(spark)

        result = silver_pipeline(df)

        assert result.count() == 2  # no filtering happens in this function
        for col in (
            "trip_duration_minutes",
            "avg_speed_mph",
            "pickup_hour",
            "pickup_day_of_week",
            "_processed_at",
        ):
            assert col in result.columns
