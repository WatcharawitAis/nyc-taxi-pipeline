"""Pipeline for bronze layer"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.bronze.bronze_pipelines import bronze_pipeline
from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.utils.spark_session import get_required_conf

CATALOG: str = get_required_conf("catalog")
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema")
LANDING_SCHEMA: str = get_required_conf("landing_schema")
LANDING_VOLUME: str = get_required_conf("landing_volume")
LANDING_VOLUME_PATH: str = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{LANDING_VOLUME}"

@dp.table(
    name=f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata",
    comment=f"Real NYC TLC Yellow Taxi monthly files landed from {LANDING_VOLUME_PATH}. "
    "Real TLC schema (PULocationID/DOLocationID).",
    # tpep_pickup_datetime/tpep_dropoff_datetime are TIMESTAMP_NTZ in the real
    # TLC files; Delta requires this feature explicitly enabled on the table.
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def bronze_yellow_tripdata() -> DataFrame:
    """Incrementally reads new yellow_tripdata_YYYY-MM.parquet files from
    LANDING_VOLUME_PATH via Auto Loader."""
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(LANDING_VOLUME_PATH)
    )
    return bronze_pipeline(df)
