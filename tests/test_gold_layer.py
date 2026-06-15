"""
Unit Tests for Gold Layer Transformations

Test Coverage:
- convert_day_number_to_name()
- aggregate_by_day_of_week()
- round_metric_columns()
- sort_by_day_of_week()

วิธีรัน:
    pytest tests/test_gold_layer.py -v
    pytest tests/test_gold_layer.py::test_convert_day_to_name -v
    pytest tests/test_gold_layer.py -v -k "integration"  # รัน integration tests อย่างเดียว
"""

import pytest
from pyspark.sql import functions as F

from pipeline.transformations.gold_layer import (
    convert_day_number_to_name,
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)


# ========================================
# TEST: convert_day_number_to_name()
# ========================================

def test_convert_day_to_name_all_days(spark):
    """Test: Convert day numbers (1-7) to day names"""
    data = [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]
    df = spark.createDataFrame(data, ["pickup_day_of_week"])
    
    result = convert_day_number_to_name(df)
    
    day_names = [r.day_name for r in result.collect()]
    expected = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    assert day_names == expected


def test_convert_day_to_name_weekdays_only(spark):
    """Test: Convert weekday numbers to names"""
    data = [(2,), (3,), (4,), (5,), (6,)]  # Monday-Friday
    df = spark.createDataFrame(data, ["pickup_day_of_week"])
    
    result = convert_day_number_to_name(df)
    
    day_names = [r.day_name for r in result.collect()]
    assert day_names == ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def test_convert_day_to_name_weekends_only(spark):
    """Test: Convert weekend numbers to names"""
    data = [(1,), (7,)]  # Sunday, Saturday
    df = spark.createDataFrame(data, ["pickup_day_of_week"])
    
    result = convert_day_number_to_name(df)
    
    day_names = [r.day_name for r in result.collect()]
    assert day_names == ["Sunday", "Saturday"]


def test_convert_day_to_name_null_handling(spark):
    """Test: NULL day number should result in NULL day name"""
    data = [(None,), (2,), (None,)]
    df = spark.createDataFrame(data, ["pickup_day_of_week"])
    
    result = convert_day_number_to_name(df)
    
    rows = result.collect()
    assert rows[0].day_name is None
    assert rows[1].day_name == "Monday"
    assert rows[2].day_name is None


def test_convert_day_to_name_invalid_numbers(spark):
    """Test: Invalid day numbers (0, 8, 9) should result in NULL"""
    data = [(0,), (8,), (9,), (100,)]
    df = spark.createDataFrame(data, ["pickup_day_of_week"])
    
    result = convert_day_number_to_name(df)
    
    day_names = [r.day_name for r in result.collect()]
    assert all(name is None for name in day_names), "Invalid day numbers should be NULL"


# ========================================
# TEST: aggregate_by_day_of_week()
# ========================================

def test_aggregate_single_day(spark):
    """Test: Aggregate trips for a single day"""
    data = [
        (2, 10.0, 15.0, 50.0),  # Monday: trip 1
        (2, 20.0, 25.0, 60.0),  # Monday: trip 2
        (2, 15.0, 30.0, 55.0),  # Monday: trip 3
    ]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    result = aggregate_by_day_of_week(df)
    
    row = result.first()
    assert row.total_rides == 3
    assert row.total_fare == 70.0  # 15 + 25 + 30
    assert row.avg_distance == 15.0  # (10 + 20 + 15) / 3
    assert row.avg_fare == 23.333333333333332  # (15 + 25 + 30) / 3
    assert row.avg_speed == 55.0  # (50 + 60 + 55) / 3


def test_aggregate_multiple_days(spark):
    """Test: Aggregate trips across multiple days"""
    data = [
        (1, 10.0, 15.0, 50.0),  # Sunday
        (1, 20.0, 25.0, 60.0),  # Sunday
        (2, 15.0, 30.0, 55.0),  # Monday
        (7, 5.0, 10.0, 45.0),   # Saturday
    ]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    result = aggregate_by_day_of_week(df)
    
    rows = result.collect()
    assert len(rows) == 3, "Should have 3 different days"
    
    # Sunday (2 trips)
    sunday = [r for r in rows if r.pickup_day_of_week == 1][0]
    assert sunday.total_rides == 2
    assert sunday.total_fare == 40.0  # 15 + 25
    
    # Monday (1 trip)
    monday = [r for r in rows if r.pickup_day_of_week == 2][0]
    assert monday.total_rides == 1
    assert monday.avg_fare == 30.0


def test_aggregate_empty_dataframe(spark):
    """Test: Aggregate empty DataFrame should return empty result"""
    schema = "pickup_day_of_week int, trip_distance double, fare_amount double, avg_speed_mph double"
    df = spark.createDataFrame([], schema)
    
    result = aggregate_by_day_of_week(df)
    
    assert result.count() == 0


def test_aggregate_null_values(spark):
    """Test: NULL values should be handled correctly in aggregations"""
    data = [
        (2, 10.0, 15.0, None),    # NULL speed
        (2, None, 25.0, 60.0),    # NULL distance
        (2, 20.0, None, 55.0),    # NULL fare
    ]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    result = aggregate_by_day_of_week(df)
    
    row = result.first()
    assert row.total_rides == 3
    # avg() ignores NULLs
    assert row.avg_distance == 15.0  # (10 + 20) / 2
    assert row.avg_fare == 20.0      # (15 + 25) / 2
    assert row.avg_speed == 57.5     # (60 + 55) / 2


# ========================================
# TEST: round_metric_columns()
# ========================================

def test_round_metrics_default_precision(spark):
    """Test: Round metrics to 2 decimal places (default)"""
    data = [(1, "Sunday", 100, 1234.567, 12.3456, 45.6789, 55.5555)]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "day_name", "total_rides", "total_fare",
         "avg_distance", "avg_fare", "avg_speed"]
    )
    
    result = round_metric_columns(df, precision=2)
    
    row = result.first()
    assert row.avg_distance == 12.35
    assert row.avg_fare == 45.68
    assert row.avg_speed == 55.56


def test_round_metrics_custom_precision(spark):
    """Test: Round metrics to custom precision (1 decimal)"""
    data = [(1, "Sunday", 100, 1234.567, 12.3456, 45.6789, 55.5555)]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "day_name", "total_rides", "total_fare",
         "avg_distance", "avg_fare", "avg_speed"]
    )
    
    result = round_metric_columns(df, precision=1)
    
    row = result.first()
    assert row.avg_distance == 12.3
    assert row.avg_fare == 45.7
    assert row.avg_speed == 55.6


def test_round_metrics_zero_precision(spark):
    """Test: Round metrics to whole numbers (precision=0)"""
    data = [(1, "Sunday", 100, 1234.567, 12.6, 45.4, 55.5)]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "day_name", "total_rides", "total_fare",
         "avg_distance", "avg_fare", "avg_speed"]
    )
    
    result = round_metric_columns(df, precision=0)
    
    row = result.first()
    assert row.avg_distance == 13.0
    assert row.avg_fare == 45.0
    assert row.avg_speed == 56.0


def test_round_metrics_preserves_non_rounded_columns(spark):
    """Test: Non-rounded columns should remain unchanged"""
    data = [(1, "Sunday", 100, 1234.567, 12.3456, 45.6789, 55.5555)]
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "day_name", "total_rides", "total_fare",
         "avg_distance", "avg_fare", "avg_speed"]
    )
    
    result = round_metric_columns(df, precision=2)
    
    row = result.first()
    # These columns should NOT be rounded
    assert row.day_of_week == 1
    assert row.day_name == "Sunday"
    assert row.total_rides == 100
    # total_fare IS rounded to 2 decimals
    assert row.total_fare == 1234.57


# ========================================
# TEST: sort_by_day_of_week()
# ========================================

def test_sort_ascending_order(spark):
    """Test: Sort by day of week in ascending order (1-7)"""
    data = [(7,), (3,), (1,), (5,), (2,)]
    df = spark.createDataFrame(data, ["day_of_week"])
    
    result = sort_by_day_of_week(df)
    
    days = [r.day_of_week for r in result.collect()]
    assert days == [1, 2, 3, 5, 7], "Days should be sorted 1-7"


def test_sort_all_seven_days(spark):
    """Test: Sort all 7 days of week"""
    data = [(7,), (6,), (5,), (4,), (3,), (2,), (1,)]
    df = spark.createDataFrame(data, ["day_of_week"])
    
    result = sort_by_day_of_week(df)
    
    days = [r.day_of_week for r in result.collect()]
    assert days == [1, 2, 3, 4, 5, 6, 7]


def test_sort_with_additional_columns(spark):
    """Test: Sort preserves other columns"""
    data = [
        (7, "Saturday", 100),
        (2, "Monday", 200),
        (1, "Sunday", 150),
    ]
    df = spark.createDataFrame(data, ["day_of_week", "day_name", "total_rides"])
    
    result = sort_by_day_of_week(df)
    
    rows = result.collect()
    assert rows[0].day_name == "Sunday"
    assert rows[1].day_name == "Monday"
    assert rows[2].day_name == "Saturday"


# ========================================
# INTEGRATION TESTS
# ========================================

def test_integration_gold_pipeline_end_to_end(spark):
    """
    Integration Test: Complete gold transformation pipeline
    
    Simulates: silver → gold transformations
    Tests all transformation functions working together
    """
    # Arrange: Create mock silver data
    silver_data = [
        # Sunday (2 trips)
        (1, 10.0, 15.0, 2.5, 18.0, 30.0, 50.0, 10, 1),
        (1, 20.0, 25.0, 5.0, 30.0, 60.0, 60.0, 14, 1),
        
        # Monday (3 trips)
        (2, 15.0, 30.0, 3.0, 33.0, 45.0, 55.0, 8, 2),
        (2, 25.0, 35.0, 4.0, 39.0, 50.0, 58.0, 12, 2),
        (2, 12.5, 22.5, 2.0, 24.5, 40.0, 52.0, 16, 2),
        
        # Saturday (1 trip)
        (7, 5.0, 10.0, 1.5, 11.5, 25.0, 45.0, 20, 7),
    ]
    
    silver_df = spark.createDataFrame(
        silver_data,
        ["passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount",
         "trip_duration_minutes", "avg_speed_mph", "pickup_hour", "pickup_day_of_week"]
    )
    
    # Act: Apply all transformations (simulate day_of_week_metrics function)
    df = aggregate_by_day_of_week(silver_df)
    df = convert_day_number_to_name(df)
    df = round_metric_columns(df)
    result = sort_by_day_of_week(df)
    
    # Assert: Check results
    rows = result.collect()
    assert len(rows) == 3, "Should have 3 days (Sunday, Monday, Saturday)"
    
    # Sunday
    sunday = rows[0]
    assert sunday.day_of_week == 1
    assert sunday.day_name == "Sunday"
    assert sunday.total_rides == 2
    assert sunday.total_fare == 40.0  # 15 + 25
    assert sunday.avg_distance == 15.0  # (10 + 20) / 2
    assert sunday.avg_fare == 20.0  # (15 + 25) / 2
    assert sunday.avg_speed == 55.0  # (50 + 60) / 2
    
    # Monday
    monday = rows[1]
    assert monday.day_of_week == 2
    assert monday.day_name == "Monday"
    assert monday.total_rides == 3
    assert monday.total_fare == 87.5  # 30 + 35 + 22.5
    assert monday.avg_distance == 17.5  # (15 + 25 + 12.5) / 3
    assert monday.avg_fare == 29.17  # Rounded to 2 decimals
    assert monday.avg_speed == 55.0  # (55 + 58 + 52) / 3
    
    # Saturday
    saturday = rows[2]
    assert saturday.day_of_week == 7
    assert saturday.day_name == "Saturday"
    assert saturday.total_rides == 1
    assert saturday.avg_distance == 5.0


def test_integration_gold_weekday_vs_weekend_comparison(spark):
    """
    Integration Test: Compare weekday vs weekend metrics
    
    Business use case: Analyze differences between weekday and weekend patterns
    """
    # Weekday data (Monday-Friday)
    weekday_data = [
        (2, 5.0, 20.0, 60.0),   # Monday
        (3, 6.0, 25.0, 65.0),   # Tuesday
        (4, 5.5, 22.0, 62.0),   # Wednesday
        (5, 5.8, 23.0, 63.0),   # Thursday
        (6, 6.2, 24.0, 64.0),   # Friday
    ]
    
    # Weekend data (Saturday-Sunday)
    weekend_data = [
        (1, 8.0, 30.0, 55.0),   # Sunday
        (7, 9.0, 35.0, 58.0),   # Saturday
    ]
    
    all_data = weekday_data + weekend_data
    
    df = spark.createDataFrame(
        all_data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    # Apply pipeline
    result = aggregate_by_day_of_week(df)
    result = convert_day_number_to_name(result)
    result = round_metric_columns(result)
    result = sort_by_day_of_week(result)
    
    rows = result.collect()
    assert len(rows) == 7
    
    # Weekday average distance should be lower than weekend
    weekday_rows = rows[1:6]  # Monday-Friday
    weekend_rows = [rows[0], rows[6]]  # Sunday, Saturday
    
    weekday_avg_distance = sum(r.avg_distance for r in weekday_rows) / len(weekday_rows)
    weekend_avg_distance = sum(r.avg_distance for r in weekend_rows) / len(weekend_rows)
    
    assert weekend_avg_distance > weekday_avg_distance, "Weekend trips should be longer on average"


def test_integration_gold_empty_days_handling(spark):
    """
    Integration Test: Handle days with no trips
    
    Tests that pipeline handles sparse data correctly
    """
    # Only Monday and Friday have data
    data = [
        (2, 5.0, 20.0, 60.0),   # Monday
        (2, 6.0, 25.0, 65.0),   # Monday
        (6, 8.0, 30.0, 55.0),   # Friday
    ]
    
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    # Apply pipeline
    result = aggregate_by_day_of_week(df)
    result = convert_day_number_to_name(result)
    result = round_metric_columns(result)
    result = sort_by_day_of_week(result)
    
    rows = result.collect()
    
    # Should only have 2 days (not 7)
    assert len(rows) == 2
    assert rows[0].day_name == "Monday"
    assert rows[1].day_name == "Friday"
    
    # Monday should have 2 trips aggregated
    assert rows[0].total_rides == 2


def test_integration_gold_high_precision_rounding(spark):
    """
    Integration Test: Verify rounding precision in final output
    
    Tests that very precise calculations are rounded correctly
    """
    # Create data that will produce precise decimal results
    data = [
        (2, 10.123456789, 15.987654321, 50.111111111),
        (2, 20.987654321, 25.123456789, 60.999999999),
        (2, 15.555555555, 30.444444444, 55.777777777),
    ]
    
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    # Apply pipeline with default precision (2 decimals)
    result = aggregate_by_day_of_week(df)
    result = convert_day_number_to_name(result)
    result = round_metric_columns(result, precision=2)
    result = sort_by_day_of_week(result)
    
    row = result.first()
    
    # Verify all metrics are rounded to 2 decimals
    avg_distance_str = str(row.avg_distance)
    avg_fare_str = str(row.avg_fare)
    avg_speed_str = str(row.avg_speed)
    
    # Check that decimals don't exceed 2 places
    if '.' in avg_distance_str:
        assert len(avg_distance_str.split('.')[1]) <= 2
    if '.' in avg_fare_str:
        assert len(avg_fare_str.split('.')[1]) <= 2
    if '.' in avg_speed_str:
        assert len(avg_speed_str.split('.')[1]) <= 2


def test_integration_gold_all_days_complete_week(spark):
    """
    Integration Test: Complete week with all 7 days
    
    Tests full week aggregation and sorting
    """
    # Create data for all 7 days
    data = [
        (1, 8.0, 30.0, 55.0),   # Sunday
        (2, 5.0, 20.0, 60.0),   # Monday
        (3, 6.0, 22.0, 62.0),   # Tuesday
        (4, 5.5, 21.0, 61.0),   # Wednesday
        (5, 6.5, 23.0, 63.0),   # Thursday
        (6, 7.0, 25.0, 64.0),   # Friday
        (7, 9.0, 35.0, 58.0),   # Saturday
    ]
    
    df = spark.createDataFrame(
        data,
        ["pickup_day_of_week", "trip_distance", "fare_amount", "avg_speed_mph"]
    )
    
    # Apply pipeline
    result = aggregate_by_day_of_week(df)
    result = convert_day_number_to_name(result)
    result = round_metric_columns(result)
    result = sort_by_day_of_week(result)
    
    rows = result.collect()
    
    # Should have all 7 days
    assert len(rows) == 7
    
    # Verify correct order and names
    expected_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    actual_names = [r.day_name for r in rows]
    assert actual_names == expected_names
    
    # Verify each day has exactly 1 ride
    for row in rows:
        assert row.total_rides == 1
