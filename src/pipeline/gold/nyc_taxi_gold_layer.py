"""Pipeline for gold layer"""


from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.gold.gold_transformations import (
    data_quality_trend_transformation,
    filter_verified_trips,
    hourly_demand_heatmap_transformation,
    monthly_trip_metrics_transformation,
    pickup_zone_metrics_transformation,
)
from src.utils.spark_session import get_required_conf, get_spark_session
from src.utils.transformations import extract_year_month_from_filename

SPARK = get_spark_session()
CATALOG: str = get_required_conf("catalog", SPARK)
GOLD_SCHEMA_NAME: str = get_required_conf("gold_schema", SPARK)
SILVER_SCHEMA_NAME: str = get_required_conf("silver_schema", SPARK)
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema", SPARK)


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.monthly_trip_metrics",
    comment="Monthly aggregated metrics (rides, fare, distance, speed, tips) for real "
    "NYC TLC trip data - a timeline view for quality/trend dashboards. "
    "Filters silver_yellow_tripdata to rows that passed all DQX checks.",
)
def monthly_trip_metrics() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by trip_year/trip_month"""
    df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return monthly_trip_metrics_transformation(filter_verified_trips(df))

@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.pickup_zone_metrics",
    comment="Aggregated metrics for real NYC TLC trip data by pickup zone (PULocationID) "
    "- ride demand by zone. Filters silver_yellow_tripdata to rows that passed "
    "all DQX checks.",
)
def pickup_zone_metrics() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by pickup zone"""
    df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return pickup_zone_metrics_transformation(filter_verified_trips(df))

@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.hourly_demand_heatmap",
    comment="Ride demand heatmap (rides, fare, speed) by pickup day-of-week x hour, "
    "for real NYC TLC trip data. Filters silver_yellow_tripdata to rows that "
    "passed all DQX checks.",
)
def hourly_demand_heatmap() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by day-of-week x hour"""
    df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return hourly_demand_heatmap_transformation(filter_verified_trips(df))
