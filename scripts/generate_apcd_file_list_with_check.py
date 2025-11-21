#!/usr/bin/env python3
"""
Script to generate a list of APCD data files from http://jtimmer.digitalspacemail17.net/data/
for files with naming convention: yesterday_{YYYYmmdd}.CSV

This script generates file URLs and can optionally check if they're accessible.
Since the website may have access restrictions, it provides multiple modes:

1. Generate theoretical file list (fast)
2. Check actual file availability (slower, tests each URL)
3. Generate with smart URL logic for split data layout

The latest data may be split:
- Latest data is at the top level: http://domain/data/yesterday_YYYYMMDD.CSV
- Current year data is partially in yearly directories: http://domain/data/data/YYYY/MMM/yesterday_YYYYMMDD.CSV

Output format: Year,Month,filename,url,[status]
"""

import csv
import sys
import re
import requests
from datetime import datetime, timedelta
from calendar import month_abbr
import argparse
import concurrent.futures
from urllib.parse import urljoin

def check_file_exists(url, session=None, timeout=5):
    """
    Check if a specific file exists at the given URL

    Args:
        url (str): URL to check
        session (requests.Session): Optional session for connection reuse
        timeout (int): Request timeout in seconds

    Returns:
        str: 'OK' if accessible, 'FORBIDDEN' if 403, 'NOT_FOUND' if 404, 'ERROR' for other issues
    """
    if session is None:
        session = requests.Session()

    try:
        response = session.head(url, timeout=timeout)
        if response.status_code == 200:
            return 'OK'
        elif response.status_code == 403:
            return 'FORBIDDEN'
        elif response.status_code == 404:
            return 'NOT_FOUND'
        else:
            return f'HTTP_{response.status_code}'
    except requests.Timeout:
        return 'TIMEOUT'
    except requests.RequestException as e:
        return 'ERROR'

def check_urls_batch(urls, max_workers=10, timeout=5):
    """
    Check multiple URLs concurrently

    Args:
        urls (list): List of URLs to check
        max_workers (int): Maximum concurrent requests
        timeout (int): Request timeout per URL

    Returns:
        dict: URL -> status mapping
    """
    results = {}
    session = requests.Session()

    def check_single_url(url):
        return url, check_file_exists(url, session, timeout)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(check_single_url, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url)):
            url, status = future.result()
            results[url] = status

            if (i + 1) % 50 == 0:
                print(f"Checked {i + 1}/{len(urls)} URLs...", file=sys.stderr)

    return results

def determine_file_location(date_obj, base_url, current_year=None):
    """
    Determine where a file should be located based on date and current year logic

    Args:
        date_obj (datetime): Date of the file
        base_url (str): Base URL
        current_year (int): Current year (for determining split logic)

    Returns:
        tuple: (url, location_type) where location_type is 'top_level' or 'yearly'
    """
    if current_year is None:
        current_year = datetime.now().year

    date_str = date_obj.strftime('%Y%m%d')
    filename = f"yesterday_{date_str}.CSV"
    month_name = month_abbr[date_obj.month]

    # Logic for where files should be:
    # 1. Recent files (last 30 days) are likely at top level
    # 2. Current year files might be split between top level and yearly dirs
    # 3. Historical files are in yearly/monthly directories

    days_from_now = (datetime.now().date() - date_obj.date()).days

    if days_from_now <= 30:
        # Very recent files - likely at top level
        url = f"{base_url}/{filename}"
        location_type = 'top_level'
    elif date_obj.year == current_year:
        # Current year - could be either location, prefer top level for recent
        if days_from_now <= 90:
            url = f"{base_url}/{filename}"
            location_type = 'top_level'
        else:
            url = f"{base_url}/data/{date_obj.year}/{month_name}/{filename}"
            location_type = 'yearly'
    else:
        # Historical data - in yearly/monthly directories
        url = f"{base_url}/data/{date_obj.year}/{month_name}/{filename}"
        location_type = 'yearly'

    return url, location_type

def generate_apcd_file_list(start_year=2023, end_year=None, output_file=None,
                           check_urls=False, max_workers=10, include_status=False,
                           non_top_level_only=False):
    """
    Generate a list of APCD files with smart URL logic

    Args:
        start_year (int): Starting year (default: 2023)
        end_year (int): Ending year (default: current year)
        output_file (str): Output CSV file path (default: stdout)
        check_urls (bool): Whether to check URL accessibility
        max_workers (int): Concurrent requests for URL checking
        include_status (bool): Include status column in output
        non_top_level_only (bool): Only include files NOT in top level directory
    """

    if end_year is None:
        end_year = datetime.now().year

    base_url = "http://jtimmer.digitalspacemail17.net/data"

    # Generate all file entries
    file_entries = []
    all_urls = []

    for year in range(start_year, end_year + 1):
        for month_num in range(1, 13):
            # Skip future months in current year
            if year == datetime.now().year and month_num > datetime.now().month:
                continue

            month_name = month_abbr[month_num]  # Jan, Feb, Mar, etc.

            # Calculate number of days in month
            if month_num == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month_num + 1, 1)

            current_month = datetime(year, month_num, 1)
            days_in_month = (next_month - current_month).days

            # Generate entry for each day in the month
            for day in range(1, days_in_month + 1):
                date_obj = datetime(year, month_num, day)

                # Skip future dates
                if date_obj > datetime.now():
                    continue

                date_str = date_obj.strftime('%Y%m%d')
                filename = f"yesterday_{date_str}.CSV"

                # Determine smart URL based on date and split logic
                url, location_type = determine_file_location(date_obj, base_url)

                # Filter for non-top-level files if requested
                if non_top_level_only and location_type != 'yearly':
                    continue

                file_entries.append({
                    'year': year,
                    'month': month_name,
                    'filename': filename,
                    'url': url,
                    'date': date_obj,
                    'location_type': location_type
                })

                all_urls.append(url)

    # Check URLs if requested
    url_status = {}
    if check_urls:
        print(f"Checking {len(all_urls)} URLs with {max_workers} workers...", file=sys.stderr)
        url_status = check_urls_batch(all_urls, max_workers=max_workers)

        # Summary statistics
        status_counts = {}
        for status in url_status.values():
            status_counts[status] = status_counts.get(status, 0) + 1

        print("URL Check Results:", file=sys.stderr)
        for status, count in sorted(status_counts.items()):
            print(f"  {status}: {count}", file=sys.stderr)

    # Write output
    if output_file:
        output = open(output_file, 'w', newline='')
    else:
        output = sys.stdout

    writer = csv.writer(output)

    # Write header
    header = ['Year', 'Month', 'filename', 'url']
    if include_status or check_urls:
        header.append('status')
    writer.writerow(header)

    # Write file entries
    for entry in file_entries:
        row = [entry['year'], entry['month'], entry['filename'], entry['url']]

        if include_status or check_urls:
            status = url_status.get(entry['url'], 'UNKNOWN') if check_urls else 'THEORETICAL'
            row.append(status)

        writer.writerow(row)

    if output_file:
        output.close()
        print(f"Generated {len(file_entries)} entries, saved to: {output_file}", file=sys.stderr)

    return len(file_entries)

def main():
    parser = argparse.ArgumentParser(
        description='Generate APCD file list with smart URL logic and optional availability checking'
    )
    parser.add_argument('--start-year', type=int, default=2023,
                       help='Starting year (default: 2023)')
    parser.add_argument('--end-year', type=int, default=None,
                       help='Ending year (default: current year)')
    parser.add_argument('--output', type=str, default=None,
                       help='Output CSV file (default: stdout)')
    parser.add_argument('--check-urls', action='store_true',
                       help='Check URL accessibility (slower)')
    parser.add_argument('--workers', type=int, default=10,
                       help='Concurrent workers for URL checking (default: 10)')
    parser.add_argument('--include-status', action='store_true',
                       help='Include status column even without URL checking')
    parser.add_argument('--non-top-level-only', action='store_true',
                       help='Only include files NOT in the top level directory (yearly/monthly structure)')

    args = parser.parse_args()

    generate_apcd_file_list(
        start_year=args.start_year,
        end_year=args.end_year,
        output_file=args.output,
        check_urls=args.check_urls,
        max_workers=args.workers,
        include_status=args.include_status,
        non_top_level_only=args.non_top_level_only
    )

if __name__ == "__main__":
    main()