"""Unit tests for src/pipeline/gold/gold_pipelines.py"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.pipeline.gold.gold_pipelines import (
    data_quality_trend_pipeline,
    hourly_demand_heatmap_pipeline,
    monthly_trip_metrics_pipeline,
    pickup_zone_metrics_pipeline,
)


class TestMonthlyTripMetricsPipeline:
    """Test monthly_trip_metrics (timeline view)"""

    def test_aggregates_by_year_month(self, spark):
        schema = StructType(
            [
                StructField("trip_year", IntegerType(), True),
                StructField("trip_month", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("avg_speed_mph", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
            ]
        )
        data = [
            (2026, 1, 10.0, 5.0, 20.0, 2.0),
            (2026, 1, 20.0, 7.0, 30.0, 4.0),
            (2026, 2, 15.0, 6.0, 25.0, 3.0),
        ]
        df = spark.createDataFrame(data, schema)

        result = monthly_trip_metrics_pipeline(df).collect()

        assert len(result) == 2
        jan = result[0]
        assert jan.trip_year == 2026
        assert jan.trip_month == 1
        assert jan.total_rides == 2
        assert jan.total_fare == 30.0
        assert jan.avg_fare == 15.0
        assert jan.avg_tip == 3.0

    def test_sorted_chronologically(self, spark):
        schema = StructType(
            [
                StructField("trip_year", IntegerType(), True),
                StructField("trip_month", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("avg_speed_mph", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
            ]
        )
        data = [
            (2026, 5, 10.0, 5.0, 20.0, 2.0),
            (2026, 1, 10.0, 5.0, 20.0, 2.0),
            (2026, 3, 10.0, 5.0, 20.0, 2.0),
        ]
        df = spark.createDataFrame(data, schema)

        months = [row.trip_month for row in monthly_trip_metrics_pipeline(df).collect()]

        assert months == [1, 3, 5]


class TestPickupZoneMetricsPipeline:
    """Test pickup_zone_metrics (ride demand by zone)"""

    def test_aggregates_by_zone_sorted_by_rides_desc(self, spark):
        schema = StructType(
            [
                StructField("PULocationID", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
            ]
        )
        data = [
            (100, 10.0, 2.0),
            (100, 20.0, 4.0),
            (200, 15.0, 3.0),
        ]
        df = spark.createDataFrame(data, schema)

        result = pickup_zone_metrics_pipeline(df).collect()

        assert result[0].pickup_location_id == 100  # 2 rides, sorted first
        assert result[0].total_rides == 2
        assert result[0].avg_fare == 15.0
        assert result[1].pickup_location_id == 200
        assert result[1].total_rides == 1


class TestHourlyDemandHeatmapPipeline:
    """Test hourly_demand_heatmap (day-of-week x hour grid)"""

    def test_aggregates_by_day_and_hour_with_readable_name(self, spark):
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("pickup_hour", IntegerType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("avg_speed_mph", DoubleType(), True),
            ]
        )
        data = [
            (1, 8, 10.0, 20.0),  # Sunday, 8am
            (1, 8, 20.0, 30.0),  # Sunday, 8am
            (2, 9, 15.0, 25.0),  # Monday, 9am
        ]
        df = spark.createDataFrame(data, schema)

        result = hourly_demand_heatmap_pipeline(df).collect()

        assert result[0].pickup_day_of_week == 1
        assert result[0].pickup_hour == 8
        assert result[0].day_name == "Sunday"
        assert result[0].total_rides == 2
        assert result[0].avg_fare == 15.0


class TestDataQualityTrendPipeline:
    """Test data_quality_trend (bronze volume vs. silver valid/quarantine)"""

    def _bronze_df(self, spark, rows):
        schema = StructType(
            [
                StructField("trip_year", IntegerType(), True),
                StructField("trip_month", IntegerType(), True),
            ]
        )
        return spark.createDataFrame(rows, schema)

    def _silver_df(self, spark, rows):
        """rows: list of (trip_year, trip_month, errors) where errors is
        None for a valid row or a non-null string standing in for DQX's
        _errors column (only its nullness matters here)."""
        schema = StructType(
            [
                StructField("trip_year", IntegerType(), True),
                StructField("trip_month", IntegerType(), True),
                StructField("_errors", StringType(), True),
                StructField("_warnings", StringType(), True),
            ]
        )
        return spark.createDataFrame(
            [(year, month, errors, None) for year, month, errors in rows], schema
        )

    def test_computes_quarantine_rate(self, spark):
        bronze_df = self._bronze_df(spark, [(2026, 1)] * 10)
        silver_df = self._silver_df(
            spark, [(2026, 1, None)] * 8 + [(2026, 1, "fare_amount_is_positive")] * 2
        )

        result = data_quality_trend_pipeline(bronze_df, silver_df).collect()[0]

        assert result.total_records == 10
        assert result.valid_count == 8
        assert result.quarantine_count == 2
        assert result.quarantine_rate_pct == 20.0

    def test_month_with_no_quarantine_rows(self, spark):
        """A month with no failing rows should show a 0 quarantine_count."""
        bronze_df = self._bronze_df(spark, [(2026, 1)] * 5)
        silver_df = self._silver_df(spark, [(2026, 1, None)] * 5)

        result = data_quality_trend_pipeline(bronze_df, silver_df).collect()[0]

        assert result.quarantine_count == 0
        assert result.quarantine_rate_pct == 0.0
