"""Shared logic for downloading missing NYC Taxi trip data files into a Databricks Volume.

Scans the target volume for existing (year, month) parquet files, computes
the gap up to the current month, and downloads whatever is missing. Used by
the per-color entry points in src/ingest/ (e.g. yellow, green).
"""
import logging
import os
import random
import time
from datetime import datetime

import requests
from dateutil.relativedelta import relativedelta
from pyspark.dbutils import DBUtils

logger = logging.getLogger(__name__)

YearMonth = tuple[int, int]

DOWNLOADED = "downloaded"
ALREADY_EXISTS = "already_exists"
NOT_PUBLISHED = "not_published"
FAILED = "failed"


def scan_existing_year_months(
    dbutils: DBUtils, volume_base_path: str, file_prefix: str
) -> set[YearMonth]:
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
            if not stem.startswith(file_prefix):
                continue
            year_str, _, month_str = stem.removeprefix(file_prefix).partition("-")
            if not (year_str.isdigit() and month_str.isdigit()):
                continue
            year, month = int(year_str), int(month_str)
            if (year, month) <= (now.year, now.month):
                existing.add((year, month))

    logger.info("Found %d existing file(s) in volume", len(existing))
    return existing


def compute_missing_year_months(
    existing: set[YearMonth],
    start: YearMonth,
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


def download_month(
    dbutils: DBUtils,
    volume_base_path: str,
    year: int,
    month: int,
    base_url: str,
    file_prefix: str,
) -> str:
    """Download a single year-month parquet file into the volume.

    Returns one of DOWNLOADED, ALREADY_EXISTS, NOT_PUBLISHED (soft skip -
    file simply isn't published yet, expected for recent months), or FAILED
    (a real error - timeout, unexpected HTTP status, network exception).
    """
    dest_dir = f"{volume_base_path}/{year}"
    dest_path = f"{dest_dir}/{file_prefix}{year}-{month:02d}.parquet"

    try:
        existing = dbutils.fs.ls(dest_path)
        if existing and existing[0].size > 0:
            logger.info("⊙ %s-%02d already exists, skipping", year, month)
            return ALREADY_EXISTS
    except Exception:
        pass

    url = base_url.format(year=year, month=month)
    try:
        head = requests.head(url, timeout=10)
        if head.status_code in (403, 404):
            # CloudFront returns 403 (not 404) for objects that don't exist yet
            # when the origin bucket disallows ListBucket -- treat both as "not published yet"
            logger.warning("✗ %s-%02d not available yet (HTTP %s), skipping", year, month, head.status_code)
            return NOT_PUBLISHED
        if head.status_code != 200:
            logger.error("✗ %s-%02d HTTP %s", year, month, head.status_code)
            return FAILED

        response = requests.get(url, timeout=300)
        os.makedirs(dest_dir, exist_ok=True)
        with open(dest_path, "wb") as f:
            f.write(response.content)

        logger.info("✓ %s-%02d downloaded (%s bytes)", year, month, f"{os.path.getsize(dest_path):,}")
        return DOWNLOADED

    except requests.exceptions.Timeout:
        logger.error("✗ %s-%02d timed out", year, month)
        return FAILED
    except Exception as e:
        logger.error("✗ %s-%02d failed: %s", year, month, e)
        return FAILED


def run_download(
    spark,
    volume_base_path: str,
    base_url: str,
    file_prefix: str,
    start_year_month: YearMonth,
) -> None:
    """Scan a volume for missing (year, month) files and download them all.

    Shared entry point for every taxi-color ingest script; only the volume
    path, source URL template, and file prefix differ between colors.
    """
    dbutils = DBUtils(spark)

    logger.info("Scanning %s for existing files...", volume_base_path)
    existing = scan_existing_year_months(dbutils, volume_base_path, file_prefix)

    missing = compute_missing_year_months(existing, start_year_month)
    if not missing:
        logger.info("No missing months, nothing to do.")
        return
    logger.info("Missing %d month(s): %s-%02d to %s-%02d", len(missing), *missing[0], *missing[-1])

    success, not_published, failed = 0, 0, 0
    for year, month in missing:
        time.sleep(random.uniform(3, 15))
        result = download_month(dbutils, volume_base_path, year, month, base_url, file_prefix)
        if result in (DOWNLOADED, ALREADY_EXISTS):
            success += 1
        elif result == NOT_PUBLISHED:
            not_published += 1
        else:
            failed += 1

    logger.info(
        "Done. Success: %d, Not yet published: %d, Failed: %d",
        success,
        not_published,
        failed,
    )
    if failed:
        raise RuntimeError(
            f"{failed} month(s) failed to download (network/HTTP errors) - "
            "see logs above for details"
        )
