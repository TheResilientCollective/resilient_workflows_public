"""
dagster_wahis_job.py
====================
Dagster job that downloads the latest OIE-WAHIS infur file from SharePoint.

Schedule: Weekly (Monday 6am, matching WAHIS release cadence).

Setup:
    1. pip install dagster dagster-webserver msal requests openpyxl
    2. Place sharepoint_auth.py and sharepoint_downloader.py alongside this file
    3. Run: dagster dev -f dagster_wahis_job.py
    4. First time: trigger manually, complete the device code auth in your terminal
    5. After that: fully unattended on schedule

Environment variables (optional, override defaults):
    WAHIS_DOWNLOAD_DIR   — where to save files (default: ./downloads)
"""

import os
from pathlib import Path

from dagster import (
    OpExecutionContext,
    ScheduleDefinition,
    asset,
    define_asset_job,
    job,
    op,
    repository,
    schedule,
    Definitions,
    RunRequest,
    sensor,
    AssetExecutionContext,
)

# ── Import our downloader ─────────────────────────────────────────────────────
# Assumes sharepoint_auth.py and sharepoint_downloader.py are in the same dir.
import sys
sys.path.insert(0, str(Path(__file__).parent))
from sharepoint_downloader import run as sp_run

# ── Configuration ─────────────────────────────────────────────────────────────

DOWNLOAD_DIR = Path(os.environ.get("WAHIS_DOWNLOAD_DIR", "./downloads"))


# ── Ops ───────────────────────────────────────────────────────────────────────

@op
def download_wahis_infur(context: OpExecutionContext) -> list[str]:
    """
    Downloads the latest infur_*.xlsx from the OIE-WAHIS SharePoint.
    Returns list of local file paths that were downloaded.
    """
    context.log.info(f"Starting OIE-WAHIS SharePoint download → {DOWNLOAD_DIR}")

    # NOTE: If the token cache is expired (>90 days) this will raise an error
    # with instructions to re-authenticate. Run manually with --reauth to refresh.
    try:
        paths = sp_run(
            download_dir=DOWNLOAD_DIR,
            force_reauth=False,
            list_only=False,
            also_metadata=True,  # grab metadata file too
        )
    except PermissionError as e:
        context.log.error(str(e))
        raise

    str_paths = [str(p) for p in paths]

    if str_paths:
        context.log.info(f"Downloaded {len(str_paths)} file(s):")
        for p in str_paths:
            context.log.info(f"  {p}")
    else:
        context.log.info("No new files to download (already up to date).")

    return str_paths


@op
def validate_download(context: OpExecutionContext, file_paths: list[str]) -> None:
    """
    Basic validation: check files exist and are non-empty.
    Extend this op to parse the xlsx and run data quality checks.
    """
    import openpyxl

    for path_str in file_paths:
        p = Path(path_str)
        if not p.exists():
            raise FileNotFoundError(f"Expected file not found: {p}")
        size_kb = p.stat().st_size / 1024
        if size_kb < 1:
            raise ValueError(f"File suspiciously small ({size_kb:.1f} KB): {p}")
        context.log.info(f"  ✓ {p.name}  ({size_kb:.1f} KB)")

        # Try opening as xlsx to confirm it's valid
        try:
            wb = openpyxl.load_workbook(p, read_only=True)
            sheet_names = wb.sheetnames
            context.log.info(f"    Sheets: {sheet_names}")
            wb.close()
        except Exception as e:
            raise ValueError(f"Could not open {p.name} as Excel file: {e}")


# ── Job ───────────────────────────────────────────────────────────────────────

@job
def wahis_download_job():
    """Download latest OIE-WAHIS data from SharePoint."""
    paths = download_wahis_infur()
    validate_download(paths)


# ── Schedule: Weekly on Monday at 06:00 ──────────────────────────────────────

wahis_weekly_schedule = ScheduleDefinition(
    job=wahis_download_job,
    cron_schedule="0 6 * * 1",   # Monday 06:00 (server local time)
    execution_timezone="America/Los_Angeles",
)


# ── Definitions (Dagster 1.x style) ──────────────────────────────────────────

defs = Definitions(
    jobs=[wahis_download_job],
    schedules=[wahis_weekly_schedule],
)
