"""Bronze Pipeline

Bronze layer transformation logic for raw data ingestion.
No transformations applied - preserves original data format.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def bronze_pipeline(df: DataFrame) -> DataFrame:
    """Bronze Pipeline Logic
    
    Args:
        df: Raw input DataFrame
        
    Returns:
        DataFrame with ingestion timestamp added
    """
    # Add ingestion timestamp for lineage tracking
    df = df.withColumn("_ingested_at", current_timestamp())
    return df
