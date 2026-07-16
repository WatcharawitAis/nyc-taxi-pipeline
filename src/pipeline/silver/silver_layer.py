"""Pipeline for silver layer"""

from importlib.resources import files

from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.silver.silver_pipelines import silver_pipeline
from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.utils.spark_session import get_required_conf

CATALOG: str = get_required_conf("catalog")
SILVER_SCHEMA_NAME: str = get_required_conf("silver_schema")
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema")

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

    df = spark.readStream.table(
        f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata"
    )
    transformed_df = silver_pipeline(df)
    checks = DQ_ENGINE.load_checks(
        config=FileChecksStorageConfig(location=str(CHECKS_FILE))
    )
    return DQ_ENGINE.apply_checks_by_metadata(transformed_df, checks)


@dp.temporary_view(
    name="verified_trips",
    comment="silver_yellow_tripdata rows that passed all DQX checks "
)
def verified_trips() -> DataFrame:
    """silver_yellow_tripdata filtered to rows with no DQX errors/warnings"""
    df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return df.where(df["_errors"].isNull() & df["_warnings"].isNull()).drop(
        "_errors", "_warnings"
    )

@dp.temporary_view(
    name="quarantined_trips",
    comment="silver_yellow_tripdata rows that failed at least one DQX check.",
)
def quarantined_trips() -> DataFrame:
    """silver_yellow_tripdata filtered to rows with a DQX error or warning"""
    df = dp.read(f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata")
    return df.where(df["_errors"].isNotNull() | df["_warnings"].isNotNull())
