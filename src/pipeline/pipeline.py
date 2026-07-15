"""Pipeline entry point - imports register DLT tables"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Registering nyc_taxi_pipeline layers: bronze, silver, gold")

import src.pipeline.bronze.bronze_layer  # noqa: E402, F401
import src.pipeline.gold.gold_layer  # noqa: E402, F401
import src.pipeline.silver.silver_layer  # noqa: E402, F401

logger.info("All pipeline layers registered")
