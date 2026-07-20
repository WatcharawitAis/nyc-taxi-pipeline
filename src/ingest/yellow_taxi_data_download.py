"""Download missing NYC Yellow Taxi trip data files into a Databricks Volume.

Scans the target volume for existing (year, month) parquet files, computes
the gap up to the current month, and downloads whatever is missing.
"""
print("start")
import logging
import os
import random
import time
from datetime import datetime
from typing import Optional

import requests
from dateutil.relativedelta import relativedelta
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

from src.utils.spark_session import get_required_conf, get_spark_session


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SPARK = get_spark_session()

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
CATALOG = get_required_conf("catalog", SPARK) or "biap_dev"
SCHEMA = get_required_conf("landing_schema", SPARK) or "landing"
VOLUMN_FOLDER = get_required_conf("landing_volume", SPARK) or "nyc-yellow-taxi-files"

VOLUME_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUMN_FOLDER}"
START_YEAR_MONTH = (2026, 1)


YearMonth = tuple[int, int]

def scan_existing_year_months(dbutils: DBUtils, volume_base_path: str) -> set[YearMonth]:
    """Return the set of (year, month) pairs with a valid parquet file already present."""
    now = datetime.now()

    try:
        year_dirs = dbutils.fs.ls(volume_base_path)
    except Exception as e:
        logger.warning("Could not list volume %s: %s", volume_base_path, e)
        return set()

    existing: set[YearMonth] = set()
    for year_entry in year_dirs:
        year_name = year_entry.name.rstrip("/")
        if not (year_entry.isDir() and year_name.isdigit() and len(year_name) == 4):
            continue

        try:
            files = dbutils.fs.ls(year_entry.path)
        except Exception:
            continue

        for f in files:
            if f.size <= 0:
                continue
            stem = f.name.removesuffix(".parquet")
            if not stem.startswith("yellow_tripdata_"):
                continue
            year_str, _, month_str = stem.removeprefix("yellow_tripdata_").partition("-")
            if not (year_str.isdigit() and month_str.isdigit()):
                continue
            year, month = int(year_str), int(month_str)
            if (year, month) <= (now.year, now.month):
                existing.add((year, month))

    logger.info("Found %d existing file(s) in volume", len(existing))
    return existing


def compute_missing_year_months(
    existing: set[YearMonth],
    start: YearMonth = START_YEAR_MONTH,
) -> list[YearMonth]:
    """List every (year, month) from the earliest known point through the current month not in `existing`.

    The lower bound is the older of `start` and the oldest file actually found in `existing`,
    so a file older than `start` (e.g. from a misconfigured constant) is never silently ignored.
    Always walks the full range rather than resuming from the latest file, so months that
    failed a previous download (404/403 at the time, timeout, manual deletion, etc.) are
    caught again. `existing` is already fully scanned, so this costs no extra I/O.
    """
    now = datetime.now()
    lower_bound = min(start, min(existing)) if existing else start
    cursor = datetime(*lower_bound, 1)

    missing = []
    while cursor <= now:
        year_month = (cursor.year, cursor.month)
        if year_month not in existing:
            missing.append(year_month)
        cursor += relativedelta(months=1)

    return missing


def download_month(dbutils: DBUtils, volume_base_path: str, year: int, month: int) -> bool:
    """Download a single year-month parquet file into the volume. Returns True on success or if already present."""
    dest_dir = f"{volume_base_path}/{year}"
    dest_path = f"{dest_dir}/yellow_tripdata_{year}-{month:02d}.parquet"

    try:
        existing = dbutils.fs.ls(dest_path)
        if existing and existing[0].size > 0:
            logger.info("⊙ %s-%02d already exists, skipping", year, month)
            return True
    except Exception:
        pass

    url = BASE_URL.format(year=year, month=month)
    try:
        head = requests.head(url, timeout=10)
        if head.status_code in (403, 404):
            # CloudFront returns 403 (not 404) for objects that don't exist yet
            # when the origin bucket disallows ListBucket -- treat both as "not published yet"
            logger.warning("✗ %s-%02d not available yet (HTTP %s), skipping", year, month, head.status_code)
            return False
        if head.status_code != 200:
            logger.error("✗ %s-%02d HTTP %s", year, month, head.status_code)
            return False

        response = requests.get(url, timeout=300)
        os.makedirs(dest_dir, exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(response.content)

        logger.info("✓ %s-%02d downloaded (%s bytes)", year, month, f"{os.path.getsize(dest_path):,}")
        return True

    except requests.exceptions.Timeout:
        logger.error("✗ %s-%02d timed out", year, month)
        return False
    except Exception as e:
        logger.error("✗ %s-%02d failed: %s", year, month, e)
        return False


def main() -> None:
    spark = SPARK
    dbutils = DBUtils(spark)

    logger.info("Scanning %s for existing files...", VOLUME_BASE_PATH)
    existing = scan_existing_year_months(dbutils, VOLUME_BASE_PATH)

    missing = compute_missing_year_months(existing)
    if not missing:
        logger.info("No missing months, nothing to do.")
        return
    logger.info("Missing %d month(s): %s-%02d to %s-%02d", len(missing), *missing[0], *missing[-1])

    success, failed = 0, 0
    for year, month in missing:
        time.sleep(random.uniform(3, 15))
        if download_month(dbutils, VOLUME_BASE_PATH, year, month):
            success += 1
        else:
            failed += 1

    logger.info("Done. Success: %d, Failed: %d", success, failed)

if __name__ == "__main__":
    main()