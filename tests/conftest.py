"""This is a pytest configuration file"""

import pytest
from databricks.connect import DatabricksSession


@pytest.fixture
def spark():
    """Create a SparkSession (the entry point to Spark functionality)

    Pins the session timezone to UTC: tests construct timestamps as UTC-aware
    datetimes, and without a fixed session timezone, functions like F.hour()
    would extract against whatever the cluster's default is instead,
    silently drifting results depending on where/when tests run.
    """
    session = DatabricksSession.builder.serverless(True).getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session
