"""Bronze Pipeline

Bronze layer transformation logic for raw data ingestion.
No transformations applied - preserves original data format.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def bronze_yellow_taxi_transformation(df: DataFrame) -> DataFrame:
    """Adds _ingested_at timestamp to raw TLC files.

    Args:
        df: Raw input DataFrame from TLC parquet files.

    Returns:
        DataFrame with _ingested_at (timestamp) column added.
        Preserves all original columns without transformation.
    """
    return df.withColumns({"_ingested_at": F.current_timestamp()})
