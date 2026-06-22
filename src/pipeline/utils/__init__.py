"""
Utility functions for NYC Taxi Pipeline.

This package contains reusable transformation, validation, calculation,
and aggregation functions used across bronze, silver, and gold layers.
"""

# Import all utility functions for easy access
from utils.aggregations import (
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)
from utils.calculations import calculate_avg_speed, calculate_trip_duration
from utils.transformations import convert_day_number_to_name, extract_time_features
from utils.validations import (
    apply_data_quality_filters,
    clean_and_validate_zip,
    validate_datetime_columns,
)

__all__ = [
    # Validations
    "clean_and_validate_zip",
    "validate_datetime_columns",
    "apply_data_quality_filters",
    # Calculations
    "calculate_trip_duration",
    "calculate_avg_speed",
    # Transformations
    "extract_time_features",
    "convert_day_number_to_name",
    # Aggregations
    "aggregate_by_day_of_week",
    "round_metric_columns",
    "sort_by_day_of_week",
]
