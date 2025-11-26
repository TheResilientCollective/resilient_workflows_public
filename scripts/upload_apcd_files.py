#!/usr/bin/env python3
"""
Script to upload APCD files listed in CSV to S3 bucket at tijuana/sd_apcd_air/raw

This script:
1. Reads the CSV file containing file URLs and metadata
2. Downloads each file from the source URL
3. Uploads it to the S3 bucket at the specified path structure
4. Preserves the original filename and organizes by year only

Usage:
    python upload_apcd_files.py [--csv-path PATH] [--dry-run] [--start-from N] [--limit N]

Environment variables required:
    S3_BUCKET, S3_ADDRESS, S3_PORT, S3_USE_SSL, S3_ACCESS_KEY, S3_SECRET_KEY
"""

import argparse
import csv
import os
import sys
import time
import requests
from pathlib import Path
from typing import Optional
import logging
from urllib.parse import urlparse
import io

from minio import Minio


def setup_logging(level=logging.INFO):
    """Setup logging configuration"""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)


def load_env_from_file(env_path: str = None):
    """Load environment variables from .env file"""
    if env_path is None:
        env_path = Path(__file__).parent.parent / "workflows" / ".env"

    if not os.path.exists(env_path):
        raise FileNotFoundError(f"Environment file not found at {env_path}")

    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value


def create_minio_client() -> Minio:
    """Create Minio client from environment variables"""
    required_vars = ['S3_BUCKET', 'S3_ADDRESS', 'S3_ACCESS_KEY', 'S3_SECRET_KEY']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    # Construct endpoint
    address = os.environ['S3_ADDRESS']
    port = os.environ.get('S3_PORT', '443')
    if port and port != '443' and port != '80':
        endpoint = f"{address}:{port}"
    else:
        endpoint = address

    secure = os.environ.get('S3_USE_SSL', 'true').lower() == 'true'

    return Minio(
        endpoint=endpoint,
        access_key=os.environ['S3_ACCESS_KEY'],
        secret_key=os.environ['S3_SECRET_KEY'],
        secure=secure
    )


def download_file(url: str, timeout: int = 30) -> bytes:
    """Download file from URL and return content as bytes"""
    # Chrome-like headers to mimic a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

    try:
        response = requests.get(url, headers=headers, timeout=timeout, stream=True)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        raise Exception(f"Failed to download {url}: {e}")


def upload_apcd_file(minio_client: Minio, bucket_name: str, url: str, filename: str,
                     year: str, month: str, logger, dry_run: bool = False) -> bool:
    """
    Download and upload a single APCD file

    Args:
        minio_client: Configured Minio client
        bucket_name: S3 bucket name
        url: Source URL to download from
        filename: Original filename
        year: Year folder (e.g., "2023")
        month: Month folder (e.g., "Jan") - not used in path, kept for compatibility
        logger: Logger instance
        dry_run: If True, don't actually upload

    Returns:
        True if successful, False otherwise
    """
    # Construct S3 path: tijuana/sd_apcd_air/raw/year/filename (no month subfolder)
    s3_path = f"tijuana/sd_apcd_air/raw/{year}/{filename}"

    logger.info(f"Processing: {filename} -> s3://{bucket_name}/{s3_path}")

    if dry_run:
        logger.info("DRY RUN: Would upload to S3")
        return True

    try:
        # Download file
        logger.debug(f"Downloading from: {url}")
        file_content = download_file(url)
        logger.debug(f"Downloaded {len(file_content)} bytes")

        # Upload to S3 using Minio
        result = minio_client.put_object(
            bucket_name=bucket_name,
            object_name=s3_path,
            data=io.BytesIO(file_content),
            length=len(file_content),
            content_type='text/csv'  # APCD files are CSV
        )

        logger.info(f"✓ Successfully uploaded: {result.object_name}")
        return True

    except Exception as e:
        logger.error(f"✗ Failed to process {filename}: {e}")
        return False


def read_csv_files(csv_path: str, start_from: int = 0, limit: Optional[int] = None):
    """Read and yield file records from CSV"""
    with open(csv_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader):
            if i < start_from:
                continue
            if limit and i >= start_from + limit:
                break

            yield i, row


def main():
    parser = argparse.ArgumentParser(description='Upload APCD files to S3')
    parser.add_argument('--csv-path',
                       default='data/apcd_sd/apcd_files_list_smart.csv',
                       help='Path to CSV file with file list')
    parser.add_argument('--dry-run', action='store_true',
                       help='Simulate upload without actually uploading')
    parser.add_argument('--start-from', type=int, default=0,
                       help='Start processing from row N (0-based)')
    parser.add_argument('--limit', type=int,
                       help='Limit processing to N files')
    parser.add_argument('--delay', type=float, default=1.0,
                       help='Delay between uploads in seconds')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    # Setup logging
    logger = setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    # Load environment
    try:
        load_env_from_file()
        logger.info("Environment variables loaded from .env file")
    except FileNotFoundError as e:
        logger.warning(f"Could not load .env file: {e}")
        logger.info("Using existing environment variables")

    # Create Minio client
    try:
        minio_client = create_minio_client()
        bucket_name = os.environ['S3_BUCKET']
        logger.info(f"Connected to S3: {os.environ['S3_ADDRESS']}")
    except Exception as e:
        logger.error(f"Failed to create Minio client: {e}")
        return 1

    # Resolve CSV path
    csv_path = Path(args.csv_path)
    if not csv_path.is_absolute():
        csv_path = Path(__file__).parent.parent / csv_path

    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        return 1

    logger.info(f"Reading file list from: {csv_path}")

    # Process files
    success_count = 0
    failure_count = 0

    try:
        for row_num, row in read_csv_files(str(csv_path), args.start_from, args.limit):
            year = row['Year']
            month = row['Month']
            filename = row['filename']
            url = row['url']

            logger.info(f"[{row_num + 1}] Processing {year}/{month}/{filename}")

            success = upload_apcd_file(
                minio_client=minio_client,
                bucket_name=bucket_name,
                url=url,
                filename=filename,
                year=year,
                month=month,
                logger=logger,
                dry_run=args.dry_run
            )

            if success:
                success_count += 1
            else:
                failure_count += 1

            # Rate limiting
            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        logger.info("Upload interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

    # Summary
    total = success_count + failure_count
    logger.info(f"\nUpload complete:")
    logger.info(f"  Total processed: {total}")
    logger.info(f"  Successful: {success_count}")
    logger.info(f"  Failed: {failure_count}")

    if args.dry_run:
        logger.info("  (DRY RUN - no actual uploads performed)")

    return 0 if failure_count == 0 else 1


if __name__ == '__main__':
    sys.exit(main())