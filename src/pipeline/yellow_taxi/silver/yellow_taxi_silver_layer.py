"""Pipeline for silver layer"""

from importlib.resources import files

from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.yellow_taxi.silver.silver_transformations import (
    yellow_taxi_silver_transformation,
)
from src.utils.spark_session import get_required_conf, get_spark_session

SPARK = get_spark_session()
CATALOG: str = get_required_conf("catalog", SPARK)
SILVER_SCHEMA_NAME: str = get_required_conf("silver_schema", SPARK)
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema", SPARK)

CHECKS_FILE = files("src").joinpath("checks", "silver_yellow_tripdata_checks.yml")
if not CHECKS_FILE.is_file():
    raise FileNotFoundError(
        f"DQX checks file not found at {CHECKS_FILE}. Expected it to ship "
    )

DQ_ENGINE = DQEngine(WorkspaceClient())

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata",
    comment="Cleaned real NYC TLC trip data with DQX check",
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def silver_yellow_tripdata() -> DataFrame:
    """Transforms bronze_yellow_tripdata and annotates every row with DQX
    check results (_errors/_warnings columns)."""

    df = SPARK.readStream.table(
        f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata"
    )
    transformed_df = yellow_taxi_silver_transformation(df)
    checks = DQ_ENGINE.load_checks(
        config=FileChecksStorageConfig(location=str(CHECKS_FILE))
    )
    return DQ_ENGINE.apply_checks_by_metadata(transformed_df, checks)

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA_NAME}.validated_silver_yellow_tripdata",
    comment="Verified silver_yellow_tripdata row only",
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def validated_silver_yellow_tripdata() -> DataFrame:
    """Returns silver_yellow_tripdata with only rows that passed all DQX checks."""
    df = SPARK.readStream.table(
        f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata"
    )
    return df.filter(df["_errors"].isNull() & df["_warnings"].isNull())

@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA_NAME}.quarantine_silver_yellow_tripdata",
    comment="Quarantined silver_yellow_tripdata rows that failed DQX checks",
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def quarantine_silver_yellow_tripdata() -> DataFrame:
    """Returns silver_yellow_tripdata with only rows that not passed all DQX checks."""
    df = SPARK.readStream.table(
        f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata"
    )
    return df.filter(df["_errors"].isNotNull() | df["_warnings"].isNotNull())


