"""
San Diego County Epidemiology Tableau Data Assets

This module extracts data from San Diego County epidemiology Tableau workbooks
and processes them for environmental health surveillance.
"""

import requests
import os
from dagster import (
    asset,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    RunRequest,
    schedule,
    AutomationCondition
)
import pandas as pd
import geopandas as gpd
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from ..utils.constants import ICONS
from ..utils import store_assets
from ..utils.tableau_extractor import TableauExtractor



# Configuration - easily changeable for production
TABLEAU_DASHBOARD_URL = os.environ.get(
    'SANDIEGO_EPIDEMIOLOGY_DASHBOARD_URL',
    'https://public.tableau.com/views/DraftRespDash/RespDash'
)
TABLEAU_WORKBOOK_URL = os.environ.get(
    'SANDIEGO_EPIDEMIOLOGY_WORKBOOK_URL',

    'https://public.tableau.com/workbooks/DraftRespDash.twb'
)
TABLEAU_DOCUMENT_ID = os.environ.get(
    'SANDIEGO_EPIDEMIOLOGY_DOCUMENT_ID',
    '{6F324CCD-B1F2-4F80-AA86-8AD270C97348}'
)

s3_output_path = 'health/sandiego_epidemiology'


def extract_sandiego_epidemiology_data(
        dashboard_url: str = "https://public.tableau.com/views/DraftRespDash/RespDash",
        workbook_url: str = "https://public.tableau.com/workbooks/DraftRespDash.twb",
        document_id: str = "{6F324CCD-B1F2-4F80-AA86-8AD270C97348}",
        output_dir: str = "../../../data/sandiego_epideimilogy"
) -> Tuple[Dict[str, pd.DataFrame], List[Path]]:
    """
    Main function to extract San Diego epidemiology data from Tableau.

    Args:
        dashboard_url: URL to the Tableau dashboard
        workbook_url: Direct URL to download the workbook
        document_id: Document ID for the specific data sheet
        output_dir: Directory to save extracted data

    Returns:
        Tuple of (extracted DataFrames, list of saved file paths)
    """
    extractor = TableauExtractor()
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    all_data = {}
    saved_files = []

    # Try session-based extraction first (most reliable method)
    extractor.logger.info("Attempting session-based extraction...")
    session_data = extractor.extract_tableau_session_data(dashboard_url, document_id)

    if session_data:
        all_data.update(session_data)

        # Save session data as CSV files
        for name, df in session_data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                csv_path = output_path / f"{name}.csv"
                df.to_csv(csv_path, index=False)
                saved_files.append(csv_path)
                extractor.logger.info(f"Saved session data: {csv_path}")

    # If session extraction failed or didn't get data, try direct workbook download
    if not session_data:
        extractor.logger.info("Session extraction failed, trying direct workbook download...")
        downloaded_file = extractor.download_direct_workbook(workbook_url, str(output_path))

        if downloaded_file:
            extractor.logger.info(f"Successfully downloaded workbook: {downloaded_file}")
            saved_files.append(downloaded_file)

            # If it's a TWBX file, extract data from it
            if downloaded_file.suffix == '.twbx':
                twbx_data = extractor.extract_twbx_data(str(downloaded_file))
                all_data.update(twbx_data)

                # Save extracted data as individual CSV files
                for name, df in twbx_data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        csv_path = output_path / f"{name}.csv"
                        df.to_csv(csv_path, index=False)
                        saved_files.append(csv_path)
                        extractor.logger.info(f"Saved extracted data: {csv_path}")

            # If it's a Hyper file, extract data using tableauhyperapi
            elif downloaded_file.suffix == '.hyper':
                hyper_data = extractor.extract_hyper_data(str(downloaded_file))
                all_data.update(hyper_data)

                # Save extracted data
                for name, df in hyper_data.items():
                    if isinstance(df, pd.DataFrame) and not df.empty:
                        csv_path = output_path / f"{name}.csv"
                        df.to_csv(csv_path, index=False)
                        saved_files.append(csv_path)
                        extractor.logger.info(f"Saved extracted data: {csv_path}")

        # If direct download failed, try alternative methods
        if not downloaded_file:
            extractor.logger.info("Direct download failed, trying alternative methods...")

            # Try the dashboard page for embedded data
            webpage_data = extractor.extract_data_from_webpage(dashboard_url)
            if webpage_data:
                # Save webpage content for inspection
                webpage_file = output_path / "dashboard_webpage.html"
                with open(webpage_file, 'w', encoding='utf-8') as f:
                    f.write(webpage_data.get('html_content', ''))
                saved_files.append(webpage_file)

                # Process any CSV data found in the webpage
                csv_data_list = webpage_data.get('csv_data', [])
                for i, csv_data in enumerate(csv_data_list):
                    csv_file = output_path / f"webpage_csv_{i}.csv"
                    with open(csv_file, 'w') as f:
                        f.write(csv_data)
                    saved_files.append(csv_file)

                    # Try to parse as DataFrame
                    try:
                        from io import StringIO
                        df = pd.read_csv(StringIO(csv_data))
                        all_data[f"webpage_csv_{i}"] = df
                        extractor.logger.info(f"Parsed CSV data: {df.shape}")
                    except Exception as e:
                        extractor.logger.warning(f"Could not parse CSV data {i}: {e}")

    return all_data, saved_files

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sd_epidemiology_tableau_raw",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager()
)
def epidemiology_tableau_raw(context):
    """
    Extract raw data from San Diego County epidemiology Tableau workbooks.

    This asset downloads and extracts data from all available workbooks
    on the San Diego County epidemiology Tableau Public profile.
    """
    logger = get_dagster_logger()
    logger.info("Starting San Diego epidemiology Tableau data extraction")

    try:
        s3_resource = context.resources.s3
        at_resource = context.resources.airtable
        slack_resource = context.resources.slack
        logger.info("Successfully initialized resources")
    except Exception as e:
        logger.error(f"Failed to initialize resources: {e}")
        raise

    try:
        # Create temporary directory for extraction
        import tempfile
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Extracting data from {TABLEAU_DASHBOARD_URL}")

            # Extract data using utility function
            extracted_data, saved_files = extract_sandiego_epidemiology_data(
                dashboard_url=TABLEAU_DASHBOARD_URL,
                workbook_url=TABLEAU_WORKBOOK_URL,
                document_id=TABLEAU_DOCUMENT_ID,
                output_dir=temp_dir
            )

            if not extracted_data and not saved_files:
                logger.warning("No data extracted from Tableau workbooks")
                return {"status": "no_data", "workbooks_processed": 0}

            # Process each extracted dataset
            processed_count = 0
            for name, df in extracted_data.items():
                if isinstance(df, pd.DataFrame) and not df.empty:
                    logger.info(f"Processing dataset: {name} ({len(df)} rows)")

                    # Convert to GeoDataFrame if it has lat/lon columns
                    if 'lat' in df.columns and 'lon' in df.columns:
                        # Remove rows with invalid coordinates
                        df = df.dropna(subset=['lat', 'lon'])
                        df = df[(df['lat'] >= -90) & (df['lat'] <= 90)]
                        df = df[(df['lon'] >= -180) & (df['lon'] <= 180)]

                        if not df.empty:
                            gdf = gpd.GeoDataFrame(
                                df,
                                geometry=gpd.points_from_xy(df.lon, df.lat),
                                crs='EPSG:4326'
                            )
                        else:
                            gdf = gpd.GeoDataFrame(df)
                    else:
                        gdf = gpd.GeoDataFrame(df)

                    # Add standard fields
                    gdf['extraction_date'] = datetime.now().isoformat()
                    gdf['source'] = 'sandiego_epidemiology_tableau'
                    gdf['icon'] = ICONS.get('health', 'health')

                    # Store in S3
                    filename = f'{s3_output_path}/raw/{name.lower().replace(" ", "_")}'
                    store_assets.geodataframe_to_s3(gdf, filename, s3_resource)
                    processed_count += 1

                    logger.info(f"Stored {name} with {len(gdf)} records")

            # Store raw files that were downloaded
            for file_path in saved_files:
                if file_path.exists():
                    # Upload raw files to S3 for archival
                    with open(file_path, 'rb') as f:
                        s3_key = f'{s3_output_path}/raw_files/{file_path.name}'
                        s3_resource.put_object(
                            Bucket=s3_resource._get_bucket_name(),
                            Key=s3_key,
                            Body=f.read()
                        )
                    logger.info(f"Archived raw file: {file_path.name}")

            # Create metadata
            metadata = store_assets.objectMetadata(
                name="San Diego Epidemiology Tableau Data",
                description="Epidemiological surveillance data from San Diego County health department Tableau dashboards",
                url=TABLEAU_DASHBOARD_URL,
                dateModified=datetime.now().isoformat(),
                temporalCoverage="varies by dataset",
                spatialCoverage="San Diego County, CA",
                publisher="San Diego County Health Department",
                keyword=["epidemiology", "public health", "disease surveillance", "San Diego"]
            )

            # Store metadata
            metadata_filename = f'{s3_output_path}/raw/metadata'
            store_assets.dict_to_s3(metadata, metadata_filename, s3_resource)

            # Notification
            message = f"San Diego Epidemiology Tableau extraction completed: {processed_count} datasets processed"
            slack_resource.send_message(message)

            return {
                "status": "success",
                "datasets_extracted": len(extracted_data),
                "workbooks_processed": processed_count,
                "files_archived": len(saved_files),
                "extraction_timestamp": datetime.now().isoformat()
            }

    except Exception as e:
        error_msg = f"Error extracting San Diego epidemiology data: {str(e)}"
        logger.error(error_msg)
        slack_resource.send_message(f"❌ {error_msg}")
        raise


@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sd_epidemiology_current_conditions",
    required_resource_keys={"s3",},
    deps=[AssetKey(["sandiego", "sd_epidemiology_tableau_raw"])],
    automation_condition=AutomationCondition.eager()
)
def epidemiology_current_conditions(context):
    """
    Process the most recent epidemiological conditions data.

    This asset processes the raw Tableau data to create a standardized
    view of current disease surveillance conditions in San Diego County.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    try:
        # List available raw data files
        bucket_name = s3_resource._get_bucket_name()
        raw_prefix = f'{s3_output_path}/raw/'

        response = s3_resource.list_objects_v2(
            Bucket=bucket_name,
            Prefix=raw_prefix
        )

        if 'Contents' not in response:
            logger.warning("No raw epidemiology data found")
            return {"status": "no_data"}

        # Process available datasets
        combined_data = []
        processed_files = 0

        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('.csv') and 'metadata' not in key and 'raw_files' not in key:
                logger.info(f"Processing file: {key}")

                try:
                    # Read data from S3
                    csv_obj = s3_resource.get_object(Bucket=bucket_name, Key=key)
                    df = pd.read_csv(csv_obj['Body'])

                    if not df.empty:
                        # Add source information
                        df['data_source'] = key.split('/')[-1].replace('.csv', '')
                        df['processing_date'] = datetime.now().isoformat()

                        # Standardize date columns if present
                        date_columns = ['date', 'Date', 'DATE', 'report_date', 'collection_date']
                        for col in date_columns:
                            if col in df.columns:
                                try:
                                    df[col] = pd.to_datetime(df[col])
                                except:
                                    logger.warning(f"Could not parse date column {col}")

                        combined_data.append(df)
                        processed_files += 1

                except Exception as e:
                    logger.warning(f"Error processing {key}: {str(e)}")
                    continue

        if not combined_data:
            logger.warning("No valid data files processed")
            return {"status": "no_valid_data"}

        # Combine all data
        logger.info(f"Combining {len(combined_data)} datasets")
        combined_df = pd.concat(combined_data, ignore_index=True, sort=False)

        # Create GeoDataFrame if coordinates exist
        if 'lat' in combined_df.columns and 'lon' in combined_df.columns:
            # Clean coordinates
            combined_df = combined_df.dropna(subset=['lat', 'lon'])
            combined_df = combined_df[
                (combined_df['lat'] >= 32.0) & (combined_df['lat'] <= 33.5) &  # San Diego County bounds
                (combined_df['lon'] >= -117.6) & (combined_df['lon'] <= -116.0)
            ]

            if not combined_df.empty:
                gdf = gpd.GeoDataFrame(
                    combined_df,
                    geometry=gpd.points_from_xy(combined_df.lon, combined_df.lat),
                    crs='EPSG:4326'
                )
            else:
                gdf = gpd.GeoDataFrame(combined_df)
        else:
            gdf = gpd.GeoDataFrame(combined_df)

        # Add standard fields
        gdf['icon'] = ICONS.get('health', 'health')
        gdf['last_updated'] = datetime.now().isoformat()

        # Store processed data
        filename = f'{s3_output_path}/output/epidemiology_current_conditions'
        store_assets.geodataframe_to_s3(gdf, filename, s3_resource)

        # Update Airtable if configured
        if len(gdf) > 0:
            try:
                # Create a summary record for Airtable
                summary_df = pd.DataFrame([{
                    'dataset': 'sandiego_epidemiology_current',
                    'record_count': len(gdf),
                    'last_updated': datetime.now().isoformat(),
                    'data_sources': ', '.join(gdf['data_source'].unique()) if 'data_source' in gdf.columns else 'unknown',
                    'geographical_coverage': 'San Diego County',
                    'status': 'active'
                }])


            except Exception as e:
                logger.warning(f"Could not update Airtable: {str(e)}")

        logger.info(f"Processed {len(gdf)} total records from {processed_files} files")

        return {
            "status": "success",
            "total_records": len(gdf),
            "files_processed": processed_files,
            "has_geography": 'geometry' in gdf.columns and not gdf.geometry.isnull().all(),
            "processing_timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        error_msg = f"Error processing epidemiology current conditions: {str(e)}"
        logger.error(error_msg)
        raise


# Jobs and Schedules
sandiego_epidemiology_job = define_asset_job(
    "sandiego_epidemiology_extraction",
    selection=[
        AssetKey(["sandiego", "sd_epidemiology_tableau_raw"]),
        AssetKey(["sandiego", "sd_epidemiology_current_conditions"])
    ]
)


@schedule(
    job=sandiego_epidemiology_job,
    cron_schedule="0 8 * * *",  # Daily at 8 AM
    name="sandiego_epidemiology_daily"
)
def sandiego_epidemiology_schedule(context):
    """Daily extraction of San Diego epidemiology data."""
    return RunRequest()
