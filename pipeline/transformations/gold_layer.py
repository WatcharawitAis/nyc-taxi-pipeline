from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ==========================================================================
# MATERIALIZED VIEW: Day of Week Metrics
# ==========================================================================
@dp.materialized_view(
    name="biap.default.day_of_week_metrics",
    comment="Daily aggregated metrics for the number of rides, "
    "average distance, average fare, and average speed for each day of the week.",
)
def day_of_week_metrics():
    # Aggregate by numeric day of week
    sliver = (
        spark.read.table("biap.default.silver_nyc_taxi_trips")
        .groupBy("pickup_day_of_week")
        .agg(  # noqa: F821, E501
            F.count(F.lit(1)).alias("total_rides"),
            F.sum("fare_amount").alias("total_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("avg_speed_mph").alias("avg_speed"),
        )
    )

    # Convert numeric day (1=Sunday, 2=Monday, ..., 7=Saturday) to day names
    result = sliver.withColumn(
        "day_name",
        F.when(F.col("pickup_day_of_week") == 1, "Sunday")
        .when(F.col("pickup_day_of_week") == 2, "Monday")
        .when(F.col("pickup_day_of_week") == 3, "Tuesday")
        .when(F.col("pickup_day_of_week") == 4, "Wednesday")
        .when(F.col("pickup_day_of_week") == 5, "Thursday")
        .when(F.col("pickup_day_of_week") == 6, "Friday")
        .when(F.col("pickup_day_of_week") == 7, "Saturday"),
    )

    return result.select(
        F.col("pickup_day_of_week").alias("day_of_week"),
        F.col("day_name"),
        F.col("total_rides"),
        F.col("total_fare"),
        F.round(F.col("avg_distance"), 2).alias("avg_distance"),
        F.round(F.col("avg_fare"), 2).alias("avg_fare"),
        F.round(F.col("avg_speed"), 2).alias("avg_speed"),
    ).orderBy("day_of_week")
