import pytest
import os
import sys
from pyspark.sql import SparkSession

# Add the project root to sys.path (works both locally and in CI).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    
    This session will be reused across all tests in the session
    to improve performance.
    """
    spark_session = (
        SparkSession.builder
        .master("local[1]")
        .appName("nyc-taxi-unit-tests")
        .config("spark.sql.shuffle.partitions", "1")  # Reduce partitions for testing
        .getOrCreate()
    )
    
    yield spark_session
    
    # Cleanup
    spark_session.stop()
