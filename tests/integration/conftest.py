from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """
    Returns the SparkSession object.
    """
    return SparkSession.getActiveSession()
