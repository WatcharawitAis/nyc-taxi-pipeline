"""Pipeline for bronze layer"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.yellow_taxi.bronze.bronze_transformations import (
    yellow_taxi_bronze_transformation,
)
from src.utils.spark_session import get_required_conf, get_spark_session

SPARK = get_spark_session()
CATALOG: str = get_required_conf("catalog", SPARK)
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema", SPARK)
LANDING_SCHEMA: str = get_required_conf("landing_schema", SPARK)
LANDING_VOLUME: str = get_required_conf("landing_volume", SPARK)
LANDING_VOLUME_PATH: str = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"


@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata",
    comment=f"Real NYC TLC Yellow Taxi monthly files landed from {LANDING_VOLUME_PATH}. "
    "Real TLC schema (PULocationID/DOLocationID).",
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def bronze_yellow_tripdata() -> DataFrame:
    """Incrementally reads new yellow_tripdata_YYYY-MM.parquet files from
    LANDING_VOLUME_PATH via Auto Loader."""
    df = (
        SPARK.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(LANDING_VOLUME_PATH)
    )
    return yellow_taxi_bronze_transformation(df)
