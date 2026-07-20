"""Pipeline entry point - imports register DLT tables"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Registering nyc_taxi_pipeline layers: bronze, silver")

import src.pipeline.yellow_taxi.bronze.bronze_yellow_taxi_pipeline # noqa: E402, F401
import src.pipeline.yellow_taxi.silver.silver_yellow_taxi_pipeline  # noqa: E402, F401

logger.info("All pipeline layers registered")
