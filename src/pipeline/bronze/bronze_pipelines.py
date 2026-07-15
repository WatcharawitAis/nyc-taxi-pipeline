"""Bronze Pipeline

Bronze layer transformation logic for raw data ingestion.
No transformations applied - preserves original data format.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FILENAME_YEAR_MONTH_PATTERN = r"yellow_tripdata_(\d{4})-(\d{2})\.parquet"


def bronze_pipeline(df: DataFrame) -> DataFrame:
    """Adds trip_year, trip_month, and _ingested_at columns to raw TLC files.

    Args:
        df: Raw input DataFrame, read from files named e.g.
            yellow_tripdata_2026-01.parquet, with Spark's hidden _metadata
            column available (file-based source).

    Returns:
        DataFrame with trip_year (int), trip_month (int), and _ingested_at
        (timestamp) columns added. trip_year/trip_month are NULL if the file
        name doesn't match FILENAME_YEAR_MONTH_PATTERN.
    """
    file_path = F.col("_metadata.file_path")
    year = F.regexp_extract(file_path, FILENAME_YEAR_MONTH_PATTERN, 1)
    month = F.regexp_extract(file_path, FILENAME_YEAR_MONTH_PATTERN, 2)

    return df.withColumns(
        {
            "trip_year": year.cast("int"),
            "trip_month": month.cast("int"),
            "_ingested_at": F.current_timestamp(),
        }
    )
