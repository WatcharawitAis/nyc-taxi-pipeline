"""
Unit tests for Gold layer transformation functions

Tests individual aggregation and transformation functions in isolation.
Run with: pytest tests/unit/test_gold_layer.py -v
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    IntegerType,
    StringType,
)

from src.pipeline.utils.transformations import convert_day_number_to_name
from src.pipeline.utils.aggregations import (
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)
from tests.conftest import spark  

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

        expected = ["Sunday","Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        assert day_names == expected

    def test_invalid_day_number(self, spark):
        """Invalid day numbers should return None"""
        data = [(0,), (8,), (None,)]
        df = spark.createDataFrame(data, ["pickup_day_of_week"])

        result = convert_day_number_to_name(df)
        day_names = [row.day_name for row in result.collect()]

        assert day_names == [None, None, None]


class TestAggregateByDayOfWeek:
    """Test day of week aggregation"""

    def test_aggregation_columns_exist(self, spark, sample_silver_data):
        """Aggregated result should have all expected columns"""
        result = aggregate_by_day_of_week(sample_silver_data)

        expected_cols = ['pickup_day_of_week', 'total_rides', \
                         'total_fare', 'avg_distance', 'avg_fare', 'avg_speed']

        # Compute columns once to avoid multiple Analyze RPCs
        result_columns = result.columns
        for col in expected_cols:
            assert col in result_columns

    def test_correct_aggregation_values(self, spark):
        """Aggregation should calculate correct values"""
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("avg_speed_mph", DoubleType(), True),
            ]
        )

        # Two trips on Monday
        data = [
            (2, 5.0, 10.0, 20.0),
            (2, 3.0, 8.0, 15.0),
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_day_of_week(df).collect()[0]

        assert result.pickup_day_of_week == 2
        assert result.total_rides == 2
        assert result.total_fare == 18.0
        assert result.avg_distance == 4.0  # (5.0 + 3.0) / 2
        assert result.avg_fare == 9.0  # (10.0 + 8.0) / 2
        assert result.avg_speed == 17.5  # (20.0 + 15.0) / 2

    def test_group_by_multiple_days(self, spark):
        """Should create separate rows for each day of week"""
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("avg_speed_mph", DoubleType(), True),
            ]
        )

        data = [
            (1, 5.0, 10.0, 20.0),  # Sunday
            (2, 3.0, 8.0, 15.0),  # Monday
            (3, 4.0, 9.0, 18.0),  # Tuesday
        ]
        df = spark.createDataFrame(data, schema)

        result = aggregate_by_day_of_week(df)

        assert result.count() == 3


class TestRoundMetricColumns:
    """Test metric rounding"""

    def test_rounding_to_two_decimals(self, spark):
        """Metrics should be rounded to 2 decimal places by default"""
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("day_name", StringType(), True),
                StructField("total_rides", IntegerType(), True),
                StructField("total_fare", DoubleType(), True),
                StructField("avg_distance", DoubleType(), True),
                StructField("avg_fare", DoubleType(), True),
                StructField("avg_speed", DoubleType(), True),
            ]
        )

        data = [
            (1, "Sunday", 10, 123.456789, 4.567891, 12.345678, 18.912345),
        ]
        df = spark.createDataFrame(data, schema)

        result = round_metric_columns(df).collect()[0]

        assert result.total_fare == 123.46
        assert result.avg_distance == 4.57
        assert result.avg_fare == 12.35
        assert result.avg_speed == 18.91


class TestSortByDayOfWeek:
    """Test sorting by day of week"""

    def test_sort_order(self, spark):
        """Results should be sorted from Sunday (1) to Saturday (7)"""
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("day_name", StringType(), True),
            ]
        )

        # Create data in reverse order
        data = [
            (7, "Saturday"),
            (3, "Tuesday"),
            (1, "Sunday"),
            (5, "Thursday"),
        ]
        df = spark.createDataFrame(data, schema)

        result = sort_by_day_of_week(df)
        sorted_days = [row.pickup_day_of_week for row in result.collect()]

        assert sorted_days == [1, 3, 5, 7]

    def test_sort_maintains_all_columns(self, spark):
        """Sorting should maintain all columns"""
        schema = StructType(
            [
                StructField("pickup_day_of_week", IntegerType(), True),
                StructField("day_name", StringType(), True),
                StructField("total_rides", IntegerType(), True),
            ]
        )

        data = [
            (2, "Monday", 100),
            (1, "Sunday", 80),
        ]
        df = spark.createDataFrame(data, schema)

        result = sort_by_day_of_week(df)

        # Check that all columns are preserved
        # Compute columns once to avoid multiple Analyze RPCs
        result_columns = result.columns
        assert "pickup_day_of_week" in result_columns
        assert "day_name" in result_columns
        assert "total_rides" in result_columns

        # Check that data is sorted correctly
        first_row = result.collect()[0]
        assert first_row.pickup_day_of_week == 1
        assert first_row.day_name == "Sunday"
        assert first_row.total_rides == 80
