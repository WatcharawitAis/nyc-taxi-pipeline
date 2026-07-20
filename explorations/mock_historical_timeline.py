# Databricks notebook source
# Mocks a believable historical ingestion/processing timeline on
# bronze_yellow_tripdata / silver_yellow_tripdata for quality-dashboard demo
# purposes only. Run this once after backfilling the real TLC months via
# yellow_taxi_pipeline. Deliberately NOT part
# of the pipeline itself: production ingestion should always use the real
# current_timestamp() (see yellow_taxi_bronze_transformation()/
# yellow_taxi_silver_transformation()); faking a spread-out timeline
# is a one-off demo concern, not something the pipeline should fabricate on
# every run.

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")

INGEST_LAG_DAYS = 5
PROCESSING_LAG_DAYS = 1

# COMMAND ----------

# Mock _ingested_at per row from its own trip_year/trip_month: the month's
# last day plus a fixed lag, approximating when a real monthly TLC drop
# would have landed.
spark.sql(f"""
    UPDATE {catalog}.{bronze_schema}.bronze_yellow_tripdata
    SET _ingested_at = last_day(make_date(trip_year, trip_month, 1)) + INTERVAL {INGEST_LAG_DAYS} DAYS
""")

# COMMAND ----------

# silver_yellow_tripdata carries trip_year/trip_month through from bronze
# (the calculation utils only add columns, none of them drop existing ones),
# so _processed_at can be derived the same way instead of joining back to bronze.
spark.sql(f"""
    UPDATE {catalog}.{silver_schema}.silver_yellow_tripdata
    SET _processed_at = last_day(make_date(trip_year, trip_month, 1))
        + INTERVAL {INGEST_LAG_DAYS + PROCESSING_LAG_DAYS} DAYS
""")

# COMMAND ----------

display(spark.sql(f"""
    SELECT trip_year, trip_month, min(_ingested_at) AS min_ingested_at, max(_ingested_at) AS max_ingested_at
    FROM {catalog}.{bronze_schema}.bronze_yellow_tripdata
    GROUP BY trip_year, trip_month
    ORDER BY trip_year, trip_month
"""))
