"""Unit tests for src/pipeline/bronze/bronze_pipelines.py"""

from pyspark.sql import functions as F

from src.pipeline.bronze.bronze_pipelines import bronze_pipeline


class TestBronzePipeline:
    """Test the real-TLC-data bronze pipeline (bronze_yellow_tripdata)"""

    def test_adds_ingested_at_column(self, spark):
        """Should tag every row with a non-null _ingested_at timestamp"""
        df = spark.createDataFrame([(1, "test")], ["id", "name"])

        result = bronze_pipeline(df)

        assert "_ingested_at" in result.columns
        assert result.filter(F.col("_ingested_at").isNull()).count() == 0

    def test_preserves_all_original_columns(self, spark):
        """Should not drop or modify any original columns"""
        df = spark.createDataFrame([(1, "test", 100.5)], ["id", "name", "value"])

        result = bronze_pipeline(df)

        assert "id" in result.columns
        assert "name" in result.columns
        assert "value" in result.columns
        assert result.count() == 1
