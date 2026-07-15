import urllib.request

import dbutils


def download_taxi_month(year: int, month: int) -> str:
    """Downloads one month of NYC TLC Yellow Taxi data into the landing volume.

    Args:
        year: Trip data year, e.g. 2026.
        month: Trip data month (1-12).

    Returns:
        The destination path in the landing volume.
    """
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    source_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
    dest_dir = f"/Volumes/biap_dev/landing/nyc-yellow-taxi-files/{year}"
    dest_path = f"{dest_dir}/{file_name}"

    dbutils.fs.mkdirs(dest_dir)
    urllib.request.urlretrieve(source_url, dest_path)

    return dest_path


download_taxi_month(2025, 1)
