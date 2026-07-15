"""Pipeline for silver layer"""

import logging
from importlib.resources import files

from databricks.labs.dqx.config import FileChecksStorageConfig
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

from src.pipeline.silver.silver_pipelines import silver_pipeline
from src.pipeline.utils.spark_session import SPARK as spark
from src.pipeline.utils.spark_session import get_required_conf

logger = logging.getLogger(__name__)

CATALOG: str = get_required_conf("catalog")
SILVER_SCHEMA_NAME: str = get_required_conf("silver_schema")
BRONZE_SCHEMA_NAME: str = get_required_conf("bronze_schema")

# Resolves correctly whether this code runs from the raw source tree (how
# the Lakeflow pipeline itself runs, via a `library: file:` reference) or
# from the installed wheel (how the test job runs it) - unlike a
# Path(__file__).resolve().parents[N] approach, this doesn't break if this
# file's depth relative to the package root ever changes.
CHECKS_FILE = files("src").joinpath("checks", "silver_yellow_tripdata_checks.yml")
if not CHECKS_FILE.is_file():
    raise FileNotFoundError(
        f"DQX checks file not found at {CHECKS_FILE}. Expected it to ship "
        "alongside the pipeline source under src/checks/ - check the bundle "
        "deploy actually uploaded it."
    )

logger.info("Silver layer configured: catalog=%s checks_file=%s", CATALOG, CHECKS_FILE)

DQ_ENGINE = DQEngine(WorkspaceClient())


@dp.table(
    name=f"{CATALOG}.{SILVER_SCHEMA_NAME}.silver_yellow_tripdata",
    comment="Cleaned real NYC TLC trip data with DQX check results kept as "
    "_errors/_warnings columns (silver_yellow_tripdata_checks.yml - "
    "PULocationID/DOLocationID based, not zip). Consumers must filter on "
    "_errors IS NULL (and _warnings IS NULL) to get valid-only rows - see "
    "gold_layer.py.",
    # tpep_pickup_datetime/tpep_dropoff_datetime are TIMESTAMP_NTZ in the real
    # TLC files; Delta requires this feature explicitly enabled on the table.
    table_properties={"delta.feature.timestampNtz": "supported"},
)
def silver_yellow_tripdata() -> DataFrame:
    """Transforms bronze_yellow_tripdata and annotates every row with DQX
    check results (_errors/_warnings columns)."""
    # bronze_yellow_tripdata gets its _ingested_at mocked via a one-off UPDATE
    # (see explorations/mock_historical_timeline.py). dp.read_stream() can't
    # take extra options, so this uses spark.readStream directly with
    # skipChangeCommits: without it, that UPDATE trips
    # DELTA_SOURCE_TABLE_IGNORE_CHANGES on the next pipeline run and
    # reprocesses everything with current_timestamp(), wiping the mock.
    df = spark.readStream.option("skipChangeCommits", "true").table(
        f"{CATALOG}.{BRONZE_SCHEMA_NAME}.bronze_yellow_tripdata"
    )
    transformed_df = silver_pipeline(df)
    checks = DQ_ENGINE.load_checks(
        config=FileChecksStorageConfig(location=str(CHECKS_FILE))
    )
    return DQ_ENGINE.apply_checks_by_metadata(transformed_df, checks)


@dp.temporary_view(
    name="verified_trips",
    comment="silver_yellow_tripdata rows that passed all DQX checks, with "
    "_errors/_warnings dropped. A view (no extra storage) so consumers "
    "always see current data without needing to know the _errors/_warnings "
    "filtering convention themselves.",
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
