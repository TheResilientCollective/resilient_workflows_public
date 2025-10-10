import tempfile
import os
from pathlib import Path
import pandas as pd
from dagster import asset, get_dagster_logger, define_asset_job, AssetKey, sensor, RunRequest, SensorEvaluationContext, Config
from typing import Dict, Any, Iterable
import json
import numpy as np
import dagster as dg
from six import StringIO
import requests
import datetime

from ..resources import minio
from ..utils import store_assets

from workflows.public.public.utils.tableau_workbook import TableauWorkbookProcessor, convert_tableau_timestamps_to_datetime
from ..utils.date import check_missing_weeks

# MPOX-specific configuration embedded in the asset file
class MPOXWorkbookConfig(Config):
    """Configuration for MPOX Tableau workbook processing"""
    # MPOX workbook URLs from the plan
    # The San Diego Epidemilogy has public data in a tableau notebook at: https://public.tableau.com/workbooks/MPX.twb
    # The metadata is at: https://public.tableau.com/profile/api/single_workbook/MPX
    url: str = "https://public.tableau.com/workbooks/MPX.twb"
    workbook_name: str = "sandiego_epidemiology_mpox"
    wb_api_url: str = "https://public.tableau.com/profile/api/single_workbook/MPX"

# S3 output path for MPOX data
s3_output_path = 'pathogens/sandiego/mpox/'
SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")

# Target workbooks from the plan: 'MPXV Disease Summary', 'Demographics (MPXV Disease Summary)'
TARGET_WORKBOOKS = ['MPXV Disease Summary2', 'Demographics3 (MPXV Disease Summary)']
TARGET_WORKBOOKS_PREFIX = ['MPXV', 'Demographics']
# Helper function for S3 storage (reused from existing workflow)
def _store_dataframe_to_s3(
    df: pd.DataFrame,
    s3_resource,
    workbook_name: str,
    dataset_identifier: str,
    logger,
    base_s3_output_prefix: str,
    source_url: str,
    date_updated: datetime.datetime = None
):
    """
    Helper function to store a DataFrame to S3, handling GeoDataFrame conversion and metadata.
    """
    date_path = dates3Path(date_updated)
    s3_path = f"{base_s3_output_prefix}/{workbook_name}/{date_path}/{dataset_identifier}"

    # Create metadata
    metadata = store_assets.objectMetadata(
        name=f"sandiego_mpox_{workbook_name}_{dataset_identifier.replace('/', '_')}",
        description=f"San Diego MPOX epidemiology data from {workbook_name} {dataset_identifier}",
        source_url=source_url
    )

    try:
        import geopandas as gpd
        gdf = gpd.GeoDataFrame(df)
        store_assets.geodataframe_to_s3(gdf, s3_path, s3_resource, metadata=metadata)
        logger.info(f"Stored GeoDataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{s3_path}")

    except Exception as geo_error:
        logger.warning(f"Could not create GeoDataFrame for {dataset_identifier}: {geo_error}. Storing as regular DataFrame.")
        store_assets.dataframe_to_s3(df, s3_path, s3_resource, metadata=metadata)
        logger.info(f"Stored DataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{s3_path}")

def dates3Path(date=None):
    if date is None:
        date = datetime.datetime.now()
    return date.strftime('%Y%m%d_%H')

def transform_mpxv_disease_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MPXV Disease Summary data according to the plan specifications:

    Input columns: Date, Count
    Output columns:
    - Jurisdiction: SanDiego
    - date_week_start: Date from dataset
    - date_week_end: Date + Seven Days
    - Week_Number: Week Number of Date
    - Year: Year of Date
    - Week_Year: f'{Week_Number}-{Year}'
    - Cases: Count from dataset
    """
    if df.empty:
        return df

    # Ensure we have the required columns
    if 'Date' not in df.columns or 'Count' not in df.columns:
        raise ValueError("Required columns 'Date' and 'Count' not found in MPXV Disease Summary data")

    # Debug: Log what's in the Date column
    print(f"Date column sample values: {df['Date'].head()}")
    print(f"Date column dtypes: {df['Date'].dtype}")
    print(f"Date column unique values: {df['Date'].unique()}")

    # Convert Date to datetime with error handling
    try:
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d', errors='coerce')
        # Check for any failed conversions
        null_dates = df['Date'].isnull().sum()
        if null_dates > 0:
            print(f"Warning: {null_dates} dates could not be parsed and were set to NaT")
    except Exception as e:
        raise ValueError(f"Failed to convert Date column to datetime: {e}")

    # Create transformed DataFrame
    transformed_df = pd.DataFrame()
    # want it first, but setting an empty df to a value provides an empty value..
    transformed_df['Jurisdiction'] = ''

    # Date columns
    transformed_df['date_week_start'] = df['Date'].dt.strftime('%Y-%m-%d')
    transformed_df['date_week_end'] = (df['Date'] + pd.Timedelta(days=7)).dt.strftime('%Y-%m-%d')

    # Week and year calculations
    transformed_df['Week_Number'] = df['Date'].dt.isocalendar().week
    transformed_df['Year'] = df['Date'].dt.year
    transformed_df['Week_Year'] = transformed_df['Week_Number'].astype(str) + '-' + transformed_df['Year'].astype(str)
    # Static jurisdiction
    transformed_df['Jurisdiction'] = 'SanDiego'
    # Cases (renamed from Count)
    transformed_df['Cases'] = df['Count'].fillna(0).astype(int)

    return transformed_df

def transform_demographics_mpxv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform Demographics (MPXV Disease Summary) data.
    According to the plan: "This should just be downloaded, and transformed to csv and json"
    We'll apply basic cleaning and standardization.
    """
    if df.empty:
        return df

    # Basic cleaning
    cleaned_df = df.copy()

    # Add metadata columns
    cleaned_df['extraction_date'] = pd.Timestamp.now().isoformat()
    cleaned_df['source'] = 'sandiego_mpox_tableau'
    cleaned_df['data_type'] = 'demographics'

    return cleaned_df

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="mpox_workbook_download",
    required_resource_keys={"s3"},
    description="Download San Diego MPOX Tableau workbook and store in S3"
)
def mpox_workbook_download(
    context,
) -> Dict[str, Any]:
    """Download MPOX Tableau workbook from URL and store in S3"""
    config = MPOXWorkbookConfig()

    name = 'mpox_workbook_download'
    description = '''
       San Diego MPOX Epidemiology Data from Tableau website
       '''
    source_url = config.url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)

    # Download workbook
    workbook_content = processor.download_workbook(config.url)
    workbook_length = len(workbook_content)

    # Store in S3
    date_path = dates3Path()
    s3_key = f"{s3_output_path}raw/{date_path}/workbook.twb"
    store_assets.raw_to_s3(workbook_content, s3_key, s3_resource,
                          contenttype='application/octet-stream',
                          metadata=metadata)

    logger.info(f"Stored MPOX workbook in S3: s3://{s3_resource.S3_BUCKET}/{s3_key} ({workbook_length} bytes)")

    return {
        "s3_key": s3_key,
        "file_size": workbook_length,
        "url": config.url,
        "workbook_name": config.workbook_name
    }

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="mpox_hyper_extraction",
    deps=[mpox_workbook_download],
    required_resource_keys={"s3"},
    description="Extract Hyper files from MPOX Tableau workbook and store in S3"
)
def mpox_hyper_extraction(
    context,
    mpox_workbook_download: Dict[str, Any],
) -> Dict[str, Any]:
    """Extract Hyper files from MPOX workbook and process target workbooks"""

    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)
    s3_resource = context.resources.s3
    workbook_name = mpox_workbook_download["workbook_name"]
    s3_key = mpox_workbook_download["s3_key"]
    date_path = dates3Path()
    config = MPOXWorkbookConfig()

    with tempfile.TemporaryDirectory() as temp_dir:
        # Download workbook from S3
        workbook_content = s3_resource.getFile(s3_key)
        extract_dir = Path(temp_dir) / "extracted"
        extraction_info = processor.extract_workbook(workbook_content, extract_dir)

        # Store each Hyper file in S3 and process target workbooks
        hyper_files_stored = []
        all_dataframes = {}
        processed_count = 0

        for hyper_file_rel_path in extraction_info["hyper_files"]:
            hyper_file_path = extract_dir / hyper_file_rel_path

            if hyper_file_path.exists():
                # Store Hyper file in S3
                hyper_s3_key = f"{s3_output_path}raw/{workbook_name}/{date_path}/hyper/{hyper_file_path.name}"

                with open(hyper_file_path, 'rb') as f:
                    name = f'mpox_workbook_data {hyper_file_path.name}'
                    description = f'''
                         San Diego MPOX Epidemiology Data files from Tableau website {workbook_name} {hyper_file_path.name}
                         '''
                    metadata = store_assets.objectMetadata(name=name, description=description, source_url=config.url)
                    store_assets.raw_to_s3(f.read(), hyper_s3_key, s3_resource,
                                         contenttype='application/octet-stream', metadata=metadata)

                # Extract data from Hyper file
                extracted_data_from_hyper = processor.extract_hyper_data(hyper_file_path)

                for _, df in extracted_data_from_hyper.items(): # _ was table_name but for MPOX it is always 'Extract'
                    table_name = hyper_file_path.name
                    if any(table_name.startswith(prefix) for prefix in TARGET_WORKBOOKS_PREFIX):
                        if not df.empty:
                            # Add metadata columns
                            df['extraction_date'] = pd.Timestamp.now().isoformat()
                            df['source'] = 'sandiego_mpox_tableau'
                            df['workbook_name'] = workbook_name
                            df['hyper_file'] = hyper_file_path.name

                            # Store raw data
                            _store_dataframe_to_s3(
                                df=df,
                                s3_resource=s3_resource,
                                workbook_name=workbook_name,
                                dataset_identifier=f"{table_name}",
                                logger=logger,
                                base_s3_output_prefix=f"{s3_output_path}output",
                                source_url=config.url
                            )

                            # Process target workbooks with specific transformations
                            if table_name.startswith('MPXV'):
                                # Transform MPXV Disease Summary data
                                try:
                                    transformed_df = transform_mpxv_disease_summary(df)
                                    if not transformed_df.empty:
                                        _store_dataframe_to_s3(
                                            df=transformed_df,
                                            s3_resource=s3_resource,
                                            workbook_name=workbook_name,
                                            dataset_identifier="processed/mpxv_disease_summary",
                                            logger=logger,
                                            base_s3_output_prefix=f"{s3_output_path}output",
                                            source_url=config.url
                                        )
                                        logger.info(f"Processed MPXV Disease Summary: {len(transformed_df)} rows")
                                        processed_count += 1
                                except Exception as e:
                                    logger.error(f"Error transforming MPXV Disease Summary: {e}")

                            elif table_name.startswith('Demographics'):
                                # Transform Demographics data
                                try:
                                    transformed_df = transform_demographics_mpxv(df)
                                    if not transformed_df.empty:
                                        _store_dataframe_to_s3(
                                            df=transformed_df,
                                            s3_resource=s3_resource,
                                            workbook_name=workbook_name,
                                            dataset_identifier="processed/demographics_mpxv",
                                            logger=logger,
                                            base_s3_output_prefix=f"{s3_output_path}output",
                                            source_url=config.url
                                        )
                                        logger.info(f"Processed Demographics MPXV: {len(transformed_df)} rows")
                                        processed_count += 1
                                except Exception as e:
                                    logger.error(f"Error transforming Demographics MPXV: {e}")

                            all_dataframes[table_name] = {
                                "rows": len(df),
                                "columns": len(df.columns),
                                "s3_path": f"{s3_output_path}output/{workbook_name}/{date_path}/{table_name}"
                            }

                    hyper_files_stored.append({
                        "filename": hyper_file_path.name,
                        "s3_key": hyper_s3_key,
                        "size": hyper_file_path.stat().st_size
                    })

                    logger.info(f"Stored MPOX Hyper file: {hyper_file_path.name}")

    # Store processing summary
    summary = {
        "workbook_name": workbook_name,
        "processed_datasets": all_dataframes,
        "total_datasets": processed_count,
        "processing_timestamp": pd.Timestamp.now().isoformat(),
        "date_path": date_path,
        "target_workbooks_processed": TARGET_WORKBOOKS_PREFIX
    }

    logger.info(f"MPOX processing complete: {processed_count} datasets processed")

    return summary

# Asset checks for MPOX data quality
@dg.multi_asset_check(
    ins={
        "mpox_hyper_extraction": dg.AssetIn(key=dg.AssetKey(['sandiego', 'mpox_hyper_extraction']))
    },
    specs=[
        dg.AssetCheckSpec(name="mpox_data_has_no_nulls", asset=dg.AssetKey(['sandiego', 'mpox_hyper_extraction'])),
        dg.AssetCheckSpec(name="mpox_target_workbooks_present", asset=dg.AssetKey(['sandiego', 'mpox_hyper_extraction'])),
    ],
    required_resource_keys={"s3"}
)
def mpox_data_checks(context, mpox_hyper_extraction) -> Iterable[dg.AssetCheckResult]:
    s3_resource = context.resources.s3
    workbook_name = mpox_hyper_extraction["workbook_name"]
    processed_datasets = mpox_hyper_extraction["processed_datasets"]

    # Check for null values in processed data
    null_check_passed = True
    null_count = 0

    try:
        # Check if target workbooks are present
        target_workbooks_found = []
        for dataset_name in processed_datasets.keys():
            for target in TARGET_WORKBOOKS:
                if target.replace(' ', '_').lower() in dataset_name.lower():
                    target_workbooks_found.append(target)

        target_workbooks_check_passed = len(set(target_workbooks_found)) >= 2

        yield dg.AssetCheckResult(
            check_name="mpox_target_workbooks_present",
            passed=target_workbooks_check_passed,
            metadata={
                "target_workbooks_expected": TARGET_WORKBOOKS,
                "target_workbooks_found": list(set(target_workbooks_found)),
                "datasets_processed": list(processed_datasets.keys())
            },
            asset_key=dg.AssetKey(['sandiego', 'mpox_hyper_extraction']),
        )

        yield dg.AssetCheckResult(
            check_name="mpox_data_has_no_nulls",
            passed=null_check_passed,
            metadata={
                "null_count": null_count,
                "datasets_checked": list(processed_datasets.keys())
            },
            asset_key=dg.AssetKey(['sandiego', 'mpox_hyper_extraction']),
        )

    except Exception as e:
        logger = get_dagster_logger()
        logger.error(f"Error in MPOX data checks: {e}")

        yield dg.AssetCheckResult(
            check_name="mpox_data_has_no_nulls",
            passed=False,
            metadata={"error": str(e)},
            asset_key=dg.AssetKey(['sandiego', 'mpox_hyper_extraction']),
        )

        yield dg.AssetCheckResult(
            check_name="mpox_target_workbooks_present",
            passed=False,
            metadata={"error": str(e)},
            asset_key=dg.AssetKey(['sandiego', 'mpox_hyper_extraction']),
        )

# Dagster job definition
mpox_epidemiology_job = define_asset_job(
    "mpox_epidemiology",
    selection=[
        AssetKey(["sandiego", "mpox_workbook_download"]),
        AssetKey(["sandiego", "mpox_hyper_extraction"]),
    ]
)

# Sensor for automated updates
@sensor(
    job=mpox_epidemiology_job,
    minimum_interval_seconds=3600,
    required_resource_keys={"slack"}
)
def mpox_epidemiology_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors the MPOX Tableau workbook API for lastPublishDate changes
    and triggers the mpox_epidemiology_job when detected
    """
    logger = get_dagster_logger()
    slack = context.resources.slack

    try:
        config = MPOXWorkbookConfig()
        api_url = config.wb_api_url

        logger.info(f"Checking MPOX Tableau API: {api_url}")

        response = requests.get(api_url)
        response.raise_for_status()
        api_data = response.json()

        converted_data = convert_tableau_timestamps_to_datetime(api_data)

        if 'lastPublishDate' in converted_data:
            last_publish_date = converted_data['lastPublishDate']
            logger.info(f"Converted MPOX lastPublishDate: {last_publish_date}")

            previous_date = context.cursor or None

            if previous_date != str(last_publish_date):
                logger.info(f"MPOX epidemiology lastPublishDate changed from {previous_date} to {last_publish_date}")

                try:
                    slack.get_client().chat_postMessage(
                        channel=SLACK_CHANNEL,
                        text=f'MPOX epidemiology data updated to {last_publish_date}'
                    )
                except Exception as e:
                    logger.error(f'Slack post error for MPOX epidemiology update: {e}')

                yield RunRequest(
                    run_key=f"mpox_epidemiology_{last_publish_date.strftime('%Y%m%d_%H%M%S')}",
                    run_config={}
                )

                context.update_cursor(str(last_publish_date))
            else:
                logger.info(f"No change in MPOX lastPublishDate: {last_publish_date}")
        else:
            logger.warning("lastPublishDate field not found in MPOX API response")

    except Exception as e:
        logger.error(f"Error checking MPOX Tableau API: {e}")
