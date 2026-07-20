"""Unit tests for src/pipeline/yellow_taxi/bronze/bronze_transformations.py"""

from pyspark.sql import functions as F

from src.pipeline.yellow_taxi.bronze.bronze_transformations import (
    yellow_taxi_bronze_transformation,
)


def _with_metadata(df, file_path):
    """Simulates Spark's file-source _metadata.file_path column, which is
    only available while reading directly from files (e.g. Auto Loader) -
    the real input this transformation runs against."""
    return df.withColumn(
        "_metadata", F.struct(F.lit(file_path).alias("file_path"))
    )


class TestYellowTaxiBronzeTransformation:
    """Test the real-TLC-data bronze transformation (bronze_yellow_tripdata)"""

    def test_adds_ingested_at_column(self, spark):
        """Should tag every row with a non-null _ingested_at timestamp"""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        df = _with_metadata(df, "/some/path/yellow_tripdata_2026-01.parquet")

        result = yellow_taxi_bronze_transformation(df)

        assert "_ingested_at" in result.columns
        assert result.filter(F.col("_ingested_at").isNull()).count() == 0

    def test_adds_source_file_column(self, spark):
        """Should persist _metadata.file_path as a real _source_file column,
        since _metadata itself isn't available once read back from the
        bronze Delta table downstream."""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])
        df = _with_metadata(df, "/some/path/yellow_tripdata_2026-01.parquet")

        result = yellow_taxi_bronze_transformation(df)

        assert result.collect()[0]._source_file == "/some/path/yellow_tripdata_2026-01.parquet"

    def test_preserves_all_original_columns(self, spark):
        """Should not drop or modify any original columns"""
        df = spark.createDataFrame([(1, "test", 100.5)], ["id", "name", "value"])
        df = _with_metadata(df, "/some/path/yellow_tripdata_2026-01.parquet")

        result = yellow_taxi_bronze_transformation(df)

        assert "id" in result.columns
        assert "name" in result.columns
        assert "value" in result.columns
        assert result.count() == 1
