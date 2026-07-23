"""Bronze Pipeline

Bronze layer transformation logic for raw data ingestion.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def yellow_taxi_bronze_transformation(df: DataFrame) -> DataFrame:
    """Adds _source_file and _ingested_at columns to raw TLC files.

    Args:
        df: Raw input DataFrame from TLC parquet files, with Spark's
            _metadata.file_path column available (file-based source).

    Returns:
        DataFrame with _source_file (string) and _ingested_at (timestamp)
        columns added. _metadata is only available while reading directly
        from files, not once persisted to a Delta table, so _source_file
        captures it here for downstream layers (year/month parsing in
        silver_pipeline) to read instead.
    """
    return df.withColumns(
        {
            "_source_file": F.col("_metadata.file_path"),
            "_ingested_at": F.current_timestamp(),
        }
    )
