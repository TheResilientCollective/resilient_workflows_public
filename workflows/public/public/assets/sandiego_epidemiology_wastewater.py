import tempfile
import os
from pathlib import Path
import pandas as pd
from dagster import asset, get_dagster_logger, define_asset_job, AssetKey, sensor, RunRequest, SensorEvaluationContext, \
    AssetIn
from typing import Dict, Any, Iterable
import json
import numpy as np
import dagster as dg
from six import StringIO
import requests
import datetime

from ..resources import minio
from ..utils import store_assets

from ..utils.tableau_workbook import TableauWastewaterConfig, TableauWorkbookProcessor, convert_tableau_timestamps_to_datetime
from ..utils.date import check_missing_weeks
from ..utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    transform_to_basic_epidemiology
)
from ..utils.store_assets import store_dataframe_to_s3

s3_output_path = 'pathogens/sandiego/sandiego_wastewater/'
# configure notebook url in utils/tableau_workbook
SLACK_CHANNEL =  os.environ.get("SLACK_SIMS_CHANNEL", "#test")

config= TableauWastewaterConfig()
TimeSeriesTablePrefix="Time_Series"


# New helper function to encapsulate dataframe storage logic
def _store_dataframe_to_s3(
    df: pd.DataFrame,
    s3_resource,
    workbook_name: str,
    dataset_identifier: str, # Use a more generic name for the last part of the path/metadata
    logger,
    base_s3_output_prefix: str, # e.g., "health/sandiego_wastewater/output"
    source_url: str,
    date_updated: datetime.datetime = None,
    enable_latest_path = False
):
    """
    Helper function to store a DataFrame to S3, handling GeoDataFrame conversion and metadata.
    """
    date_path = dates3Path(date_updated)
    #s3_path = f"{base_s3_output_prefix}/{workbook_name}/{date_path}/{dataset_identifier}"
    s3_path = f"{base_s3_output_prefix}/{workbook_name}/{date_path}/"
    latestdatasetpath="sandiego_epidemiology_wastewater"
    # Create metadata
    metadata = store_assets.objectMetadata(
        name=f"sandiego_wastewater_{workbook_name}_{dataset_identifier.replace('/', '_')} date:{date_path}" , # Replace '/' for metadata name
        description=f"San Diego epidemiology wastewater data from {workbook_name} {dataset_identifier} on {date_path}",
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
    #     import geopandas as gp
    #     # Try to convert to GeoDataFrame if geometry columns exist
    #     # geo_columns = [col for col in df.columns if
    #     #                'geom' in col.lower() or ('lat' in col.lower() and 'lon' in col.lower())]
    #
    #     # if geo_columns:
    #     #     gdf = gpd.GeoDataFrame(df)
    #     # else:
    #     #     gdf = gpd.GeoDataFrame(df)
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


@asset( group_name="health",
    key_prefix="sandiego",
    name="sandiego_wastewater_workbook_download",
    required_resource_keys={"s3"},
    description="Download San Diego epidemiology wastewater Tableau workbook and store in S3"
)
def sandiego_wastewater_workbook_download(
    context,
    config: TableauWastewaterConfig,

) -> Dict[str, Any]:
    date_path = dates3Path()
    """Download Tableau workbook from URL and store in S3"""
    name = f'sandiego_wastewater_workbook_download_{date_path}'
    description = f'''
       San Diego Epidemiology wastewater Data from Tableau website on {date_path}
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

    s3_key = f"{s3_output_path}raw/{date_path}/workbook.twbx"
    store_assets.raw_to_s3(workbook_content, s3_key, s3_resource
                          ,contenttype='application/octet-stream',
                          metadata=metadata
                          )

    logger.info(f"Stored workbook in S3: s3://{s3_resource.S3_BUCKET}/{s3_key} ({workbook_length} bytes)")

    return {
        "s3_key": s3_key,
        "file_size": workbook_length,
        "url": config.url,
        "workbook_name": config.workbook_name
    }


@asset( group_name="health",
    key_prefix="sandiego",
    name="sandiego_wastewater_hyper_extraction",
    deps=[AssetKey(['sandiego','sandiego_wastewater_workbook_download'])],
    required_resource_keys={"s3"},
    description="Extract Hyper files from Tableau workbook and store in S3",
        ins={
            "sandiego_wastewater_workbook_download": AssetIn(
                key=AssetKey(['sandiego','sandiego_wastewater_workbook_download'])
            )
        },
)
def sandiego_wastewater_hyper_extraction(
    context,
    sandiego_wastewater_workbook_download: Dict[str, Any],

) -> Dict[str, Any]:
    """Extract Hyper files from workbook and store in S3"""
    date_path = dates3Path()
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)
    s3_resource= context.resources.s3
    workbook_name = sandiego_wastewater_workbook_download["workbook_name"]
    s3_key = sandiego_wastewater_workbook_download["s3_key"]

    # The 'config' object is available globally due to its initialization outside the asset functions.

    with tempfile.TemporaryDirectory() as temp_dir:
        # Download workbook from S3
        workbook_path = Path(temp_dir) / "workbook.twb"

        workbook_content = s3_resource.getFile(s3_key)
        extract_dir = Path(temp_dir) / "extracted" # Define extract_dir
        extraction_info = processor.extract_workbook(workbook_content, extract_dir)

        # Store each Hyper file in S3
        tmp_files_stored = []

        all_dataframes = {} # Initialize all_dataframes here
        processed_count = 0 # Initialize processed_count here

        for hyper_file_rel_path in extraction_info["tmp_files"]:
            hyper_file_path = extract_dir / hyper_file_rel_path

            if hyper_file_path.exists():
                # Store Hyper file in S3
                hyper_s3_key = f"{s3_output_path}raw/{workbook_name}/{date_path}/hyper/{hyper_file_path.name}"

                with open(hyper_file_path, 'rb') as f:
                    name = f'sandiego_wastewater_workbook_data {hyper_file_path.name}'
                    description = f'''
                         San Diego Epidemiology wastewater Data files from Tableau website  {workbook_name} {hyper_file_path.name} on {date_path}
                         '''
                    source_url = config.url
                    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
                    store_assets.raw_to_s3(f.read(), hyper_s3_key, s3_resource, contenttype='application/octet-stream', metadata=metadata)
                    """Convert Hyper files to processed DataFrames and store using utility functions"""

                extracted_data_from_hyper = processor.extract_hyper_data(hyper_file_path) # Renamed to avoid conflict

                for table_name, df in extracted_data_from_hyper.items():
                    if not df.empty:
                        # Add metadata columns
                        df['extraction_date'] = pd.Timestamp.now().isoformat()
                        df['source'] = 'sandiego_wastewater_tableau'
                        df['workbook_name'] = workbook_name
                        df['hyper_file'] = hyper_file_path.name
                        table_name = fixTableNames(table_name)

                        _store_dataframe_to_s3(
                            df=df,
                            s3_resource=s3_resource,
                            workbook_name=workbook_name,
                            dataset_identifier=table_name,
                            logger=logger,
                            base_s3_output_prefix=f"{s3_output_path}output",
                            source_url=config.url
                        )

                        all_dataframes[table_name] = {
                            "rows": len(df),
                            "columns": len(df.columns),
                            "s3_path": f"{s3_output_path}output/{workbook_name}/{date_path}/{table_name}" # Update path based on helper
                        }
                        if TimeSeriesTablePrefix in table_name:
                            df = df.dropna(subset=['Disease', 'Metric'])

                            # Call reformatDf to process and prepare data by disease
                            processed_dfs_by_disease = reformatDf(df)

                            for disease, disease_data in processed_dfs_by_disease.items():
                                disease_safe_name = disease.replace(' ', '_').replace('/', '_').upper()

                                # Store original format data
                                original_df = disease_data['original_format']
                                if not original_df.empty:
                                    _store_dataframe_to_s3(
                                        df=original_df,
                                        s3_resource=s3_resource,
                                        workbook_name=workbook_name,
                                        dataset_identifier=f"processed_by_disease/{disease_safe_name}",
                                        logger=logger,
                                        base_s3_output_prefix=f"{s3_output_path}output",
                                        source_url=config.url,
                                        enable_latest_path=True
                                    )

                                    logger.info(f"📊 Stored original format data for {disease}: {len(original_df)} rows")

                                # Store validated epidemiology schema data
                                validated_df = disease_data['basic_epi_schema']
                                validation_status = disease_data['validation_status']

                                if not validated_df.empty and validation_status == 'success':
                                    _store_dataframe_to_s3(
                                        df=validated_df,
                                        s3_resource=s3_resource,
                                        workbook_name=workbook_name,
                                        dataset_identifier=f"validated_epi_schema/{disease_safe_name}",
                                        logger=logger,
                                        base_s3_output_prefix=f"{s3_output_path}output",
                                        source_url=config.url,
                                        enable_latest_path=True
                                    )

                                    logger.info(f"✅ Stored validated epidemiology schema for {disease}: {len(validated_df)} rows")

                                elif validation_status != 'success':
                                    logger.error(f"❌ Validation failed for {disease}: {validation_status}")
                                    if 'error_message' in disease_data:
                                        logger.error(f"Error details: {disease_data['error_message']}")

                                # Update tracking metadata
                                all_dataframes[f"{table_name}_{disease}_original"] = {
                                    "rows": len(original_df),
                                    "columns": len(original_df.columns),
                                    "s3_path": f"{s3_output_path}output/{workbook_name}/processed_by_disease/{disease_safe_name}",
                                    "format": "original"
                                }

                                if not validated_df.empty:
                                    all_dataframes[f"{table_name}_{disease}_validated"] = {
                                        "rows": len(validated_df),
                                        "columns": len(validated_df.columns),
                                        "s3_path": f"{s3_output_path}output/{workbook_name}/validated_epi_schema/{disease_safe_name}",
                                        "format": "basic_epidemiology_schema",
                                        "validation_status": validation_status
                                    }

                                processed_count += 1
                                logger.info(f"🦠 Completed processing {disease}: Original({len(original_df)} rows), Validated({len(validated_df)} rows), Status: {validation_status}")

                tmp_files_stored.append({
                    "filename": hyper_file_path.name,
                    "s3_key": hyper_s3_key,
                    "size": hyper_file_path.stat().st_size
                })

                logger.info(f"Stored Hyper file: {hyper_file_path.name}")


    # Store processing summary
    summary = {
        "workbook_name": workbook_name,
        "processed_datasets": all_dataframes,
        "total_datasets": processed_count,
        "processing_timestamp": pd.Timestamp.now().isoformat(),
        "date_path":date_path
    }


    logger.info(f"Processing complete: {processed_count} datasets processed")

    return summary



def fixTableNames(name):
    if TimeSeriesTablePrefix in name:
       return TimeSeriesTablePrefix
        # Find the first underscore after "Time_Series"
    else:
        parts = name.split('_')

        if len(parts) >= 2 and parts[0] == "Time" and parts[1] == "Series":
            return f"{parts[0]}_{parts[1]}"
        elif len(parts) > 1:
            return parts[0]
        else:
            return name



def reformatDf(df):
    """
    Reformats the input DataFrame, pivots metrics into columns, renames columns,
    and returns a dictionary of disease-specific DataFrames using resilient epi schemas.

    Args:
        df (pd.DataFrame): The input DataFrame from Hyper extraction,
                           expected to contain 'FY', 'CDCWk', 'WkStart',
                           'Disease', 'Metric', 'Count' columns.

    Returns:
        dict: A dictionary where keys are disease names (str) and values are
              the processed pandas DataFrames for that disease, validated using
              resilient epi schemas. Returns an empty dictionary if essential
              columns are missing or no relevant data.
    """
    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🔄 Starting reformatDf with {len(df)} rows")

    # Ensure necessary columns exist
    required_columns = ['FY', 'CDCWk', 'WkStrtActual', 'WkEndActual', 'Disease', 'Metric', 'Count']
    if not all(col in df.columns for col in required_columns):
        logger.error(f"❌ Input DataFrame missing required columns. Expected: {required_columns}, Got: {list(df.columns)}")
        return {}

    # Prepare for pivoting: ensure 'epiweek_start' is derived from 'WkStart'
    df_processed = df.copy()
    df_processed['FY_original'] = df_processed['FY']

    if 'WkStrtActual' in df_processed.columns:
        logger.info(f"📅 Processing dates from WkStrtActual column")
        df_processed['epiweek_start'] = df_processed['WkStrtActual'].apply(lambda x: pd.Timestamp(str(x)))
        df_processed['epiweek_end'] = (df_processed['epiweek_start'] + pd.Timedelta(days=6)).dt.strftime('%Y-%m-%d')
        # WkStrtActual is actually a tableauhyper timestamp
        df_processed['FY'] = df_processed['epiweek_start'].dt.year
        df_processed['epiweek_start'] = df_processed['epiweek_start'].dt.strftime('%Y-%m-%d')

    else:
        logger.error("❌ 'WkStrtActual' column not found in DataFrame, cannot derive 'epiweek_start'.")
        return {}

    # Replace 'Hospitalizations' with 'weekly_admissions' for easier column naming
    df_processed['Metric'] = df_processed['Metric'].replace('Hospitalizations', 'weekly_admissions')

    # Filter out rows where 'Metric' is not one of the target types
    target_metrics = ['Cases', 'weekly_admissions', 'Deaths']
    df_filtered = df_processed[df_processed['Metric'].isin(target_metrics)]

    if df_filtered.empty:
        logger.error("❌ No relevant metrics (Cases, Hospitalizations, Deaths) found in the DataFrame after filtering.")
        return {}

    logger.info(f"📊 Pivoting data with {len(df_filtered)} filtered rows")

    # Pivot the DataFrame
    pivoted_df = df_filtered.pivot_table(
        index=['Disease', 'epiweek_start', 'epiweek_end', 'FY', 'CDCWk'],
        columns='Metric',
        values='Count',
        aggfunc='sum'
    ).reset_index()

    # Rename columns
    pivoted_df = pivoted_df.rename(columns={
        'FY': 'disease_year',
        'CDCWk': 'disease_week',
        'Cases': 'cases',
        'weekly_admissions': 'weekly_admissions',
        'Deaths': 'deaths'
    })

    # Ensure all target columns for metrics exist, fill NaN if a metric was not present for a given row
    for col in ['cases', 'weekly_admissions', 'deaths']:
        if col not in pivoted_df.columns:
            pivoted_df[col] = np.nan

    # Select and reorder the final columns, keeping 'Disease' for splitting
    final_output_cols = ['disease_year', 'disease_week', 'epiweek_start', 'epiweek_end','cases', 'weekly_admissions', 'deaths', 'Disease']
    processed_df = pivoted_df[final_output_cols].copy()

    # Convert numeric columns to appropriate types, filling NaNs with 0
    for col in ['cases', 'weekly_admissions', 'deaths']:
        processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce').fillna(0).astype(int)

    logger.info(f"📋 Processing {len(processed_df.groupby('Disease'))} diseases")

    # Group by 'Disease' and prepare the dictionary of DataFrames with validation
    processed_dfs_by_disease = {}

    for disease, group_df in processed_df.groupby('Disease'):
        logger.info(f"🦠 Processing disease: {disease} ({len(group_df)} rows)")

        # Drop the 'Disease' column from the individual DataFrames
        group_df_clean = group_df.drop(columns=['Disease']).copy()

        # Transform to basic epidemiology schema format for cases data
        cases_data = group_df_clean[['epiweek_start', 'cases']].copy()
        cases_data = cases_data.rename(columns={'epiweek_start': 'Date', 'cases': 'Count'})

        try:
            # Use resilient epi schema to transform and validate
            jurisdiction = f"SanDiego{disease.replace(' ', '').replace('/', '')}"
            validated_epi_data = epi_processor.process_basic_epidemiology_data(
                cases_data,
                jurisdiction=jurisdiction,
                validate=True
            )

            if not validated_epi_data.empty:
                logger.info(f"✅ Successfully validated {len(validated_epi_data)} rows for {disease}")

                # Store both original processed format and validated schema format
                processed_dfs_by_disease[disease] = {
                    'original_format': group_df_clean,
                    'basic_epi_schema': validated_epi_data,
                    'validation_status': 'success',
                    'row_count': len(validated_epi_data)
                }
            else:
                logger.warning(f"⚠️  Validation produced empty result for {disease}")
                processed_dfs_by_disease[disease] = {
                    'original_format': group_df_clean,
                    'basic_epi_schema': pd.DataFrame(),
                    'validation_status': 'empty_result',
                    'row_count': 0
                }

        except EpidemiologyValidationError as ve:
            logger.error(f"❌ Validation failed for {disease}: {ve}")
            processed_dfs_by_disease[disease] = {
                'original_format': group_df_clean,
                'basic_epi_schema': pd.DataFrame(),
                'validation_status': 'validation_error',
                'error_message': str(ve),
                'row_count': 0
            }
        except Exception as e:
            logger.error(f"❌ Unexpected error processing {disease}: {e}")
            processed_dfs_by_disease[disease] = {
                'original_format': group_df_clean,
                'basic_epi_schema': pd.DataFrame(),
                'validation_status': 'processing_error',
                'error_message': str(e),
                'row_count': 0
            }

    logger.info(f"✅ Completed reformatDf processing for {len(processed_dfs_by_disease)} diseases")
    return processed_dfs_by_disease

@dg.multi_asset_check(
    # Map checks to targeted assets
    #ins=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction']),
ins={
            "sandiego_wastewater_hyper_extraction": dg.AssetIn(key=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction']))
        },
    specs=[
        dg.AssetCheckSpec(name="sde_timeseries_has_no_nulls", asset=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction'])),
        dg.AssetCheckSpec(name="sde_timeseries_has_allweeks", asset=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction'])),
    ],
    required_resource_keys={"s3"}
)
def sde_timeseries_checks(context,sandiego_wastewater_hyper_extraction)-> Iterable[dg.AssetCheckResult]:
    s3_resource = context.resources.s3
    workbook_name = sandiego_wastewater_hyper_extraction["workbook_name"]
    processed_datasets = sandiego_wastewater_hyper_extraction["processed_datasets"]
    s3_path= f'{processed_datasets[TimeSeriesTablePrefix]["s3_path"]}.csv'
    ts_content = s3_resource.getFile(s3_path)
    timeseries_dataframe = pd.read_csv(StringIO(ts_content.decode('utf-8')))
    fields_checked = ['FY','CDCWk', 'WkNum', 'WkStart', 'Disease', 'Metric']
    num_null_rows = timeseries_dataframe[fields_checked].isna().any(axis=1).sum()


    # Return the result of the check
    yield dg.AssetCheckResult(
        check_name="sde_timeseries_has_no_nulls",
        passed=bool(num_null_rows == 0),
        metadata={"null_row_count": int(num_null_rows),
                  'fields_checked':fields_checked},
        asset_key=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction']),
    )
    issues = check_missing_weeks(timeseries_dataframe,  date_column='WkStrtActual')

    # check weeks
    yield dg.AssetCheckResult(
        check_name="sde_timeseries_has_allweeks",
        passed=bool(issues['missing_weeks_count'] == 0),
        metadata={"missing_weeks_count":issues['missing_weeks_count'],
                  "missing_weeks": str(issues['missing_weeks']),
                  "issues":str(issues)},
        asset_key=dg.AssetKey(['sandiego', 'sandiego_wastewater_hyper_extraction']),
    )

sandiego_wastewater_job = define_asset_job(
    "sandiego_wastewater",
    selection=[
        AssetKey(["sandiego", "sandiego_wastewater_workbook_download"]),
        AssetKey(["sandiego", "sandiego_wastewater_hyper_extraction"]),
    ]
)

@sensor(
    job=sandiego_wastewater_job,
    minimum_interval_seconds=3600,
    required_resource_keys={"slack"}
)
def sandiego_wastewater_sensor(context: SensorEvaluationContext):
    """
    Sensor that monitors the Tableau workbook API for lastPublishDate changes
    and triggers the sandiego_wastewater_job when detected
    """
    logger = get_dagster_logger()
    slack = context.resources.slack
    try:
        workbook_config = TableauWastewaterConfig()
        api_url = workbook_config.wb_api_url

        logger.info(f"Checking Tableau API: {api_url}")

        response = requests.get(api_url)
        response.raise_for_status()
        api_data = response.json()

        converted_data = convert_tableau_timestamps_to_datetime(api_data)

        if 'lastPublishDate' in converted_data:
            last_publish_date = converted_data['lastPublishDate']
            logger.info(f"Converted lastPublishDate: {last_publish_date}")

            previous_date = context.cursor or None

            if previous_date != str(last_publish_date):
                logger.info(f"sandiego_wastewatery lastPublishDate changed from {previous_date} to {last_publish_date}")

                for field_name, field_value in converted_data.items():
                    if isinstance(field_value, datetime.datetime):
                        logger.info(f"Converted datetime field '{field_name}': {field_value}")
                try:
                    slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                        text=f'sandiego_wastewater updated to {last_publish_date}')
                except Exception as e:
                    get_dagster_logger().error('Slack post error for sandiego_wastewater updated')
                yield RunRequest(
                    run_key=f"sandiego_wastewater_{last_publish_date.strftime('%Y%m%d_%H%M%S')}",
                    run_config={}
                )

                context.update_cursor(str(last_publish_date))
            else:
                logger.info(f"No change in lastPublishDate: {last_publish_date}")
        else:
            logger.warning("lastPublishDate field not found in API response")

    except Exception as e:
        logger.error(f"Error checking Tableau API: {e}")

# @dg.definitions
# def asset_checks():
#     return dg.Definitions(
#         asset_checks=[sde_timeseries_checks()],
#     )
