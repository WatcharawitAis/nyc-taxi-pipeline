"""Pipeline for gold layer"""

import logging

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.gold.gold_pipelines import (
    data_quality_trend_pipeline,
    hourly_demand_heatmap_pipeline,
    monthly_trip_metrics_pipeline,
    pickup_zone_metrics_pipeline,
)
from src.pipeline.utils.spark_session import get_required_conf

logger = logging.getLogger(__name__)

CATALOG: str = get_required_conf("catalog")
GOLD_SCHEMA_NAME: str = get_required_conf("gold_schema")
SILVER_SCHEMA_NAME: str = get_required_conf("silver_schema")
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema")

logger.info(
    "Gold layer configured: catalog=%s gold_schema=%s", CATALOG, GOLD_SCHEMA_NAME
)


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.monthly_trip_metrics",
    comment="Monthly aggregated metrics (rides, fare, distance, speed, tips) for real "
    "NYC TLC trip data - a timeline view for quality/trend dashboards. "
    "Reads verified_trips, so only rows that passed all DQX checks are included.",
)
def monthly_trip_metrics() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by trip_year/trip_month"""
    df = dp.read("verified_trips")
    return monthly_trip_metrics_pipeline(df)


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.pickup_zone_metrics",
    comment="Aggregated metrics for real NYC TLC trip data by pickup zone (PULocationID) "
    "- ride demand by zone. Reads verified_trips, so only rows that passed all "
    "DQX checks are included.",
)
def pickup_zone_metrics() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by pickup zone"""
    df = dp.read("verified_trips")
    return pickup_zone_metrics_pipeline(df)


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.hourly_demand_heatmap",
    comment="Ride demand heatmap (rides, fare, speed) by pickup day-of-week x hour, "
    "for real NYC TLC trip data. Reads verified_trips, so only rows that passed "
    "all DQX checks are included.",
)
def hourly_demand_heatmap() -> DataFrame:
    """Gold Layer: real NYC TLC trip data aggregated by day-of-week x hour"""
    df = dp.read("verified_trips")
    return hourly_demand_heatmap_pipeline(df)


@dp.table(
    name=f"{CATALOG}.{GOLD_SCHEMA_NAME}.data_quality_trend",
    comment="Ingestion volume vs. DQX pass/fail rate by trip_year/trip_month for real "
    "NYC TLC trip data - the quality-timeline view.",
)
def data_quality_trend() -> DataFrame:
    """Gold Layer: bronze volume vs. silver valid/quarantine counts, by month"""
    bronze_df = dp.read(f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata")
    silver_df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return data_quality_trend_pipeline(bronze_df, silver_df)
