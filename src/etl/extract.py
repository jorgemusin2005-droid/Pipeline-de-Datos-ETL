"""Download source datasets for the NYC Yellow Taxi ETL pipeline."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import requests
from requests import Response
from tqdm import tqdm

from src.config.settings import BRONZE_DIR, TAXI_ZONES_DIR


YELLOW_TAXI_2024_01_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2024-01.parquet"
)
TAXI_ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)
REQUEST_TIMEOUT_SECONDS = 30
CHUNK_SIZE_BYTES = 1024 * 1024


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Download the sample trip parquet file and taxi zone lookup CSV."""
    args = _parse_args()

    try:
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        TAXI_ZONES_DIR.mkdir(parents=True, exist_ok=True)

        downloads = (
            (
                YELLOW_TAXI_2024_01_URL,
                BRONZE_DIR / "yellow_tripdata_2024-01.parquet",
            ),
            (
                TAXI_ZONE_LOOKUP_URL,
                TAXI_ZONES_DIR / "taxi_zone_lookup.csv",
            ),
        )

        for url, destination_path in downloads:
            download_file(url, destination_path, force=args.force)

        logger.info("Source data extraction completed successfully")
        return 0
    except Exception:
        logger.exception("Source data extraction failed")
        return 1


def download_file(url: str, destination_path: Path, force: bool = False) -> None:
    """Download a file with progress reporting and atomic local replacement.

    Args:
        url: Public HTTP URL to download.
        destination_path: Local path where the completed file will be stored.
        force: Whether to overwrite an existing local file.

    Raises:
        RuntimeError: If the HTTP request fails or the file cannot be written.
    """
    if destination_path.exists() and not force:
        logger.info("Skipping existing file: %s", destination_path)
        return

    temporary_path = destination_path.with_suffix(f"{destination_path.suffix}.tmp")

    logger.info("Downloading %s", url)
    try:
        with requests.get(
            url,
            stream=True,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            _raise_for_bad_response(response, url)
            total_size = int(response.headers.get("content-length", 0))

            with temporary_path.open("wb") as output_file:
                progress_bar = tqdm(
                    total=total_size or None,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=destination_path.name,
                )
                with progress_bar:
                    for chunk in response.iter_content(
                        chunk_size=CHUNK_SIZE_BYTES
                    ):
                        if chunk:
                            output_file.write(chunk)
                            progress_bar.update(len(chunk))

        temporary_path.replace(destination_path)
        logger.info("Saved file to %s", destination_path)
    except Exception as exc:
        if temporary_path.exists():
            temporary_path.unlink()
        raise RuntimeError(f"Unable to download {url}") from exc


def _raise_for_bad_response(response: Response, url: str) -> None:
    """Raise a clear exception for failed HTTP responses."""
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        status_code = response.status_code
        raise RuntimeError(f"HTTP {status_code} while downloading {url}") from exc


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download sample NYC TLC Yellow Taxi source datasets.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite files that already exist locally.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())

