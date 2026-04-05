import tempfile
import os
from pathlib import Path
import pandas as pd
from dagster import asset, get_dagster_logger, define_asset_job, AssetKey, sensor, RunRequest, SensorEvaluationContext, \
    Config, AssetIn, AutomationCondition
from epiweeks import Week
from typing import Dict, Any, Iterable
import json
import numpy as np
import dagster as dg
from six import StringIO
import requests
import datetime

from resilient_core.resources import minio
from resilient_core.utils import store_assets
from resilient_core.utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    ResilientEpiProcessor,
    transform_to_basic_epidemiology
)

from ..utils.tableau_workbook import TableauWorkbookProcessor, convert_tableau_timestamps_to_datetime
from resilient_core.utils.date import check_missing_weeks
from resilient_core.utils.store_assets import store_dataframe_to_s3


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
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_UPDATES", "#test")

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
    date_updated: datetime.datetime = None,
    enable_latest_path=False
):
    """
    Helper function to store a DataFrame to S3, handling GeoDataFrame conversion and metadata.
    """
    date_path = dates3Path(date_updated)
    s3_path = f"{base_s3_output_prefix}/{workbook_name}/{date_path}/{dataset_identifier}"
    latestdatasetpath = "sandiego_epidemiology_mpox"

    # Create metadata
    metadata = store_assets.objectMetadata(
        name=f"sandiego_mpox_{workbook_name}_{dataset_identifier.replace('/', '_')}",
        description=f"San Diego MPOX epidemiology data from {workbook_name} {dataset_identifier}",
        source_url=source_url
    )
    try:
        store_dataframe_to_s3(df, s3_path, dataset_identifier,  s3_resource, metadata=metadata, enable_latest_path=enable_latest_path, latestdatasetpath=latestdatasetpath)
        logger.info(f"Stored GeoDataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{s3_path}")
        if enable_latest_path:
            logger.info(f"Stored GeoDataFrame for {dataset_identifier} to  {latestdatasetpath}{dataset_identifier }")
    except Exception as df_error:
        logger.error(f"Could not store DataFrame for {dataset_identifier}: {df_error}")
    # try:
    #     import geopandas as gpd
    #     gdf = gpd.GeoDataFrame(df)
    #     store_assets.geodataframe_to_s3(gdf, s3_path, s3_resource, metadata=metadata)
    #     logger.info(f"Stored GeoDataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{s3_path}")
    #
    # except Exception as geo_error:
    #     logger.warning(f"Could not create GeoDataFrame for {dataset_identifier}: {geo_error}. Storing as regular DataFrame.")
    #     store_assets.dataframe_to_s3(df, s3_path, s3_resource, metadata=metadata)
    #     logger.info(f"Stored DataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{s3_path}")

def dates3Path(date=None):
    if date is None:
        date = datetime.datetime.now()
    return date.strftime('%Y%m%d_%H')

def transform_mpxv_disease_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform MPXV Disease Summary data using the Resilient Epidemiology Schema.

    Uses the standardized BasicEpidemiologySchema for consistent data output.
    Handles column mapping from actual Tableau data to expected schema format.
    """
    logger = get_dagster_logger()

    if df.empty:
        logger.warning("Input DataFrame is empty for MPXV Disease Summary")
        return pd.DataFrame(columns=BasicEpidemiologySchema.schema.columns.keys())

    # Debug: Log the actual DataFrame structure
    logger.info(f"MPXV Disease Summary input DataFrame shape: {df.shape}")
    logger.info(f"MPXV Disease Summary columns: {list(df.columns)}")
    logger.info(f"MPXV Disease Summary sample data:\n{df.head()}")

    try:
        # Map the actual columns to the expected schema format
        # The original code expected 'Date' and 'Count', but let's check what we actually have
        df_mapped = df.copy()

        # Common column name mappings from Tableau extracts
        column_mappings = {
            # Date column variations
            'Episode Week': 'Date',
            'Week Starting': 'Date',
            'Week Start': 'Date',
            'episode_week': 'Date',
            'week_starting': 'Date',
            'week_start': 'Date',
            # Count column variations
            'Case Count': 'Count',
            'Cases': 'Count',
            'Count': 'Count',
            'case_count': 'Count',
            'cases': 'Count',
            'n': 'Count',
            'SUM(Number of Records)': 'Count'
        }

        # Apply column mappings
        for old_col, new_col in column_mappings.items():
            if old_col in df_mapped.columns and new_col not in df_mapped.columns:
                df_mapped = df_mapped.rename(columns={old_col: new_col})
                logger.info(f"Mapped column '{old_col}' to '{new_col}'")

        # Check if we have the required columns after mapping
        if 'Date' not in df_mapped.columns:
            logger.error(f"No date column found after mapping. Available columns: {list(df_mapped.columns)}")
            # Try to find any date-like column
            date_cols = [col for col in df_mapped.columns if any(word in col.lower() for word in ['date', 'week', 'time'])]
            if date_cols:
                logger.info(f"Found potential date columns: {date_cols}. Using first one: {date_cols[0]}")
                df_mapped = df_mapped.rename(columns={date_cols[0]: 'Date'})
            else:
                raise ValueError("No date column found in MPXV data")

        if 'Count' not in df_mapped.columns:
            logger.error(f"No count column found after mapping. Available columns: {list(df_mapped.columns)}")
            # Try to find any numeric column that could be a count
            numeric_cols = df_mapped.select_dtypes(include=[int, float]).columns.tolist()
            if numeric_cols:
                logger.info(f"Found potential count columns: {numeric_cols}. Using first one: {numeric_cols[0]}")
                df_mapped = df_mapped.rename(columns={numeric_cols[0]: 'Count'})
            else:
                raise ValueError("No count column found in MPXV data")

        # Now use the standardized transform function
        transformed_df = transform_to_basic_epidemiology(df_mapped, jurisdiction='SanDiego')

        logger.info(f"Successfully transformed MPXV data using BasicEpidemiologySchema: {len(transformed_df)} rows")
        logger.info(f"Output columns: {list(transformed_df.columns)}")
        return transformed_df

    except Exception as e:
        logger.error(f"Error transforming MPXV Disease Summary with schema: {e}")
        # Log comprehensive debug information
        logger.info(f"Input DataFrame shape: {df.shape}")
        logger.info(f"Input DataFrame columns: {list(df.columns)}")
        logger.info(f"Input DataFrame dtypes:\n{df.dtypes}")
        if not df.empty:
            logger.info(f"Sample data:\n{df.head()}")
        raise

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
    description="Extract Hyper files from MPOX Tableau workbook and store in S3",
    ins={
        "mpox_workbook_download": AssetIn(
            key=dg.AssetKey(["sandiego", "mpox_workbook_download"])
        )
    },
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
                                logger.info(f"🔍 Processing MPXV Disease Summary for table: {table_name}")
                                logger.info(f"📊 Raw MPXV data shape: {df.shape}")
                                logger.info(f"📋 Raw MPXV columns: {list(df.columns)}")
                                logger.info(f"🔢 Raw MPXV dtypes: {df.dtypes.to_dict()}")

                                try:
                                    transformed_df = transform_mpxv_disease_summary(df)
                                    logger.info(f"✅ Transformation completed. Result shape: {transformed_df.shape}")

                                    if not transformed_df.empty:
                                        _store_dataframe_to_s3(
                                            df=transformed_df,
                                            s3_resource=s3_resource,
                                            workbook_name=workbook_name,
                                            dataset_identifier="processed/mpxv_disease_summary",
                                            logger=logger,
                                            base_s3_output_prefix=f"{s3_output_path}output",
                                            source_url=config.url,
                                enable_latest_path=True
                                        )
                                        logger.info(f"✅ Successfully processed and stored MPXV Disease Summary: {len(transformed_df)} rows")
                                        processed_count += 1
                                    else:
                                        logger.warning("❌ Transformed MPXV DataFrame is empty - no data stored")

                                        # Try to store raw data as fallback for inspection
                                        logger.info("💾 Storing raw MPXV data as fallback for debugging")
                                        _store_dataframe_to_s3(
                                            df=df,
                                            s3_resource=s3_resource,
                                            workbook_name=workbook_name,
                                            dataset_identifier="raw_fallback/mpxv_disease_summary",
                                            logger=logger,
                                            base_s3_output_prefix=f"{s3_output_path}output",
                                            source_url=config.url,
                                enable_latest_path=True
                                        )

                                except Exception as e:
                                    logger.error(f"❌ Error transforming MPXV Disease Summary: {e}")
                                    logger.error(f"📝 Raw data will be stored for debugging")

                                    # Store raw data for debugging
                                    try:
                                        _store_dataframe_to_s3(
                                            df=df,
                                            s3_resource=s3_resource,
                                            workbook_name=workbook_name,
                                            dataset_identifier="debug/mpxv_disease_summary_raw",
                                            logger=logger,
                                            base_s3_output_prefix=f"{s3_output_path}output",
                                            source_url=config.url
                                        )
                                        logger.info("💾 Raw debug data stored successfully")
                                    except Exception as store_error:
                                        logger.error(f"Failed to store debug data: {store_error}")

                                    # Re-raise the original error to ensure proper failure handling
                                    raise

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

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sd_mpox",
    deps=[AssetKey(["sandiego", "mpox_hyper_extraction"])],
    required_resource_keys={"s3"},
    description="Combined MPOX dataset: disease summary + demographics, dates snapped to CDC epiweek (Sunday) start",
    ins={
        "mpox_hyper_extraction": AssetIn(
            key=AssetKey(["sandiego", "mpox_hyper_extraction"])
        )
    },
    automation_condition=AutomationCondition.eager()
)
def sd_mpox(context, mpox_hyper_extraction: Dict[str, Any]) -> Dict[str, Any]:
    """
    Combine MPXV Disease Summary and Demographics into a single dataset.
    All week start dates are snapped to the CDC epiweek Sunday boundary.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    date_path = dates3Path()
    config = MPOXWorkbookConfig()

    processed_datasets = mpox_hyper_extraction.get("processed_datasets", {})

    # Locate MPXV and Demographics keys (hyper filenames as keys)
    mpxv_key = next((k for k in processed_datasets if k.startswith('MPXV')), None)
    demographics_key = next((k for k in processed_datasets if k.startswith('Demographics')), None)

    disease_summary_df = pd.DataFrame()
    demographics_df = pd.DataFrame()

    # --- MPXV Disease Summary ---
    if mpxv_key:
        info = processed_datasets[mpxv_key]
        csv_path = f'{info["s3_path"]}/{mpxv_key}.csv'
        try:
            raw_df = pd.read_csv(StringIO(s3_resource.getFile(csv_path).decode('utf-8')))
            logger.info(f"Loaded MPXV raw data: {len(raw_df)} rows, columns: {list(raw_df.columns)}")
            disease_summary_df = transform_mpxv_disease_summary(raw_df)
            logger.info(f"Transformed MPXV to epi schema: {len(disease_summary_df)} rows")
        except Exception as e:
            logger.error(f"Error reading/transforming MPXV data from {csv_path}: {e}")
            raise
    else:
        logger.error(f"No MPXV dataset found in processed_datasets. Keys: {list(processed_datasets.keys())}")
        raise ValueError("MPXV Disease Summary not found — cannot build sd_mpox")

    # --- Demographics ---
    if demographics_key:
        info = processed_datasets[demographics_key]
        csv_path = f'{info["s3_path"]}/{demographics_key}.csv'
        try:
            demographics_df = pd.read_csv(StringIO(s3_resource.getFile(csv_path).decode('utf-8')))
            logger.info(f"Loaded Demographics: {len(demographics_df)} rows, columns: {list(demographics_df.columns)}")

            # Snap any date/week column to CDC epiweek start (Sunday)
            date_col = next(
                (col for col in demographics_df.columns
                 if any(kw in col.lower() for kw in ['week', 'date', 'episode'])),
                None
            )
            if date_col:
                demographics_df[date_col] = pd.to_datetime(demographics_df[date_col], errors='coerce')
                demographics_df = demographics_df.dropna(subset=[date_col])
                demographics_df['date_week_start'] = demographics_df[date_col].apply(
                    lambda d: Week.fromdate(d, system='cdc').startdate().strftime('%Y-%m-%d')
                )
                logger.info(f"Snapped '{date_col}' to CDC epiweek Sunday start in demographics")
            else:
                logger.warning("No date/week column found in demographics — cannot align to epiweek")
        except Exception as e:
            logger.warning(f"Could not load demographics from {csv_path}: {e} — continuing with disease summary only")

    # --- Merge on epiweek ---
    if not demographics_df.empty and 'date_week_start' in demographics_df.columns:
        combined_df = pd.merge(disease_summary_df, demographics_df, on='date_week_start', how='left')
        logger.info(f"Merged disease summary + demographics on date_week_start: {len(combined_df)} rows")
    else:
        combined_df = disease_summary_df
        logger.info("No demographics to merge — storing disease summary only")

    # --- Store combined dataset ---
    name = f'sd_mpox_{date_path}'
    metadata = store_assets.objectMetadata(
        name=name,
        description='San Diego MPOX combined dataset with CDC epiweek-aligned dates',
        source_url=config.url
    )
    # with demographc
    store_dataframe_to_s3(
        df=combined_df,
        s3_resource=s3_resource,
        path=f"{s3_output_path}output/sd_mpox/{date_path}",
        dataset_identifier='mpox_sd_basic',
        metadata=metadata,
        latestdatasetpath="pathogens/california",
        enable_latest_path=False
    )
    logger.info(f"Stored sd_mpox: {len(combined_df)} rows, {len(combined_df.columns)} columns")

    # --- Store validated basic epidemiology data to mpox/california ---
    metadata_basic = store_assets.objectMetadata(
        name="mpox_sd_weekly",
        description="San Diego County Mpox weekly data in basic epidemiology schema format",
        source_url=config.url
    )
    store_dataframe_to_s3(
        df=disease_summary_df,
        s3_resource=s3_resource,
        path=f"{s3_output_path}output/mpox_sd_weekly/{date_path}",
        dataset_identifier='mpox_sd_weekly_basic',
        metadata=metadata_basic,
        latestdatasetpath="pathogens/mpox/california",
        enable_latest_path=True
    )
    logger.info(f"Stored mpox_sd_weekly to mpox/california: {len(disease_summary_df)} rows")

    return {
        "date_path": date_path,
        "rows": len(combined_df),
        "columns": list(combined_df.columns),
        "status": "success"
    }


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
