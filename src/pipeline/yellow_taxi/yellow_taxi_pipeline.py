"""Pipeline entry point - imports register bronze + silver DLT tables.

Bronze and silver stay one deployable pipeline: both layers of this source
are owned and released together by the same team, so splitting them into
separate Lakeflow pipelines would only add orchestration overhead (an extra
job task, an extra Unity Catalog hop) without a real ownership boundary to
justify it. Gold stays a separate pipeline since it's cross-source and
consumed by a different team.
"""

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Registering yellow_taxi_pipeline layers: bronze, silver")

import src.pipeline.yellow_taxi.bronze.yellow_taxi_bronze_layer  # noqa: E402, F401
import src.pipeline.yellow_taxi.silver.yellow_taxi_silver_layer  # noqa: E402, F401

logger.info("All yellow_taxi_pipeline layers registered")
