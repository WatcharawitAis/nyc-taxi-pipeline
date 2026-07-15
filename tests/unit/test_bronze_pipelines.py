"""Unit tests for src/pipeline/bronze/bronze_pipelines.py"""

from pyspark.sql import functions as F

from src.pipeline.bronze.bronze_pipelines import bronze_pipeline


class TestBronzePipeline:
    """Test the real-TLC-data bronze pipeline (bronze_yellow_tripdata)"""

    def test_extracts_year_and_month_from_filename(self, spark):
        """trip_year/trip_month should be parsed from the source file name"""
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn(
            "_metadata",
            F.struct(
                F.lit(
                    "/Volumes/biap_dev/landing/nyc-yellow-taxi-files/2026/yellow_tripdata_2026-03.parquet"
                ).alias("file_path")
            ),
        )

        result = bronze_pipeline(df)
        row = result.collect()[0]

        assert row.trip_year == 2026
        assert row.trip_month == 3

    def test_adds_ingested_at_column(self, spark):
        """Should tag every row with a non-null _ingested_at timestamp"""
        df = spark.createDataFrame([(1,)], ["id"])
        df = df.withColumn(
            "_metadata",
            F.struct(
                F.lit(
                    "/Volumes/biap_dev/landing/nyc-yellow-taxi-files/2026/yellow_tripdata_2026-01.parquet"
                ).alias("file_path")
            ),
        )

        result = bronze_pipeline(df)

        assert "_ingested_at" in result.columns
        assert result.filter(F.col("_ingested_at").isNull()).count() == 0
