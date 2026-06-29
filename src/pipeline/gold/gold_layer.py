try:
    from pyspark import pipelines as dp
except ImportError:
    dp = None  # For testing environments where dlt is not available

# Import utility functions using relative imports
from src.pipeline.utils.spark_session import spark
from src.pipeline.utils.aggregations import (
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)
from src.pipeline.utils.transformations import convert_day_number_to_name

gold_schema_name = spark.conf.get("gold_schema")
silver_schema_name = spark.conf.get("silver_schema")
# ========================================
# DLT VIEW DEFINITION
# ========================================

# Only define DLT views when dlt module is available (Databricks Runtime)

if dp is not None:

    @dp.table(
        name=f"{gold_schema_name}.day_of_week_metrics",
        comment="Daily aggregated metrics for the number of rides, "
        "average distance, average fare, and average speed for each day of the week.",
    )
    def day_of_week_metrics():
        """
        Gold Layer: Aggregated metrics by day of week
        
        Output: {catalog}.gold.day_of_week_metrics

        Provides business-ready metrics grouped by day of week with readable day names.

        Transformations applied:
        1. Aggregate trips by day of week
        2. Convert day numbers to names
        3. Round metrics to 2 decimal places
        4. Sort by day of week
        """
        # Read from Silver layer (silver schema)
        df = dp.read(f"{silver_schema_name}.silver_nyc_taxi_trips")  # noqa: F821

        # Apply transformations using testable functions

        df = aggregate_by_day_of_week(df)
        df = convert_day_number_to_name(df)
        df = round_metric_columns(df)
        df = sort_by_day_of_week(df)

        return df
