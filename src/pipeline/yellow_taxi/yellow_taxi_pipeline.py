"""Pipeline entry point - imports register bronze + silver DLT tables.
"""
import src.pipeline.yellow_taxi.bronze.yellow_taxi_bronze_layer  # noqa: E402, F401
import src.pipeline.yellow_taxi.silver.yellow_taxi_silver_layer  # noqa: E402, F401

