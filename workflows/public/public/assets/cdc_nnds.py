import requests
import os
from dagster import ( asset,
                     get_dagster_logger ,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      TimeWindowPartitionsDefinition,
WeeklyPartitionsDefinition,
                      asset_check,
                      AssetCheckResult,
                      AssetCheckSeverity
                      )


import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from datetime import datetime, timedelta, date
import re
from ..utils.constants import ICONS
from ..utils import store_assets
from ..utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    StatisticalExtensionSchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    transform_to_basic_epidemiology,
    create_statistical_extension_record
)
from epiweeks import Week, Year


def calculate_correct_count(df, cumulative_col='current_YTD__cummulative', group_cols=None):
    """
    Calculate CorrectCount column by computing the weekly difference from cumulative values.

    For the first week of each year (week 1), the CorrectCount equals the cumulative value.
    For subsequent weeks, CorrectCount = current_cumulative - previous_week_cumulative.

    Args:
        df: pandas DataFrame or GeoDataFrame with disease surveillance data
        cumulative_col: Name of the cumulative count column (default: 'current_YTD__cummulative')
        group_cols: List of columns to group by (default: ['label', 'location1', 'year'])

    Returns:
        DataFrame with CorrectCount column added

    Raises:
        ValueError: If required columns are missing
    """
    logger = get_dagster_logger()

    # Set default grouping columns
    if group_cols is None:
        group_cols = ['label', 'location1', 'year']

    # Validate required columns exist
    required_cols = group_cols + ['week', cumulative_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Ensure cumulative column is numeric
    df[cumulative_col] = pd.to_numeric(df[cumulative_col], errors='coerce').fillna(0)

    # Create temporary integer columns for sorting to ensure proper chronological order
    # Critical: year and week must be sorted as integers, not strings
    df['_sort_year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)
    df['_sort_week'] = pd.to_numeric(df['week'], errors='coerce').fillna(0).astype(int)

    # Build sort columns: replace 'year' with '_sort_year' in group_cols if present
    sort_cols = []
    for col in group_cols:
        if col == 'year':
            sort_cols.append('_sort_year')
        else:
            sort_cols.append(col)
    sort_cols.append('_sort_week')

    # Sort by grouping columns and week to ensure proper chronological ordering
    df = df.sort_values(sort_cols).reset_index(drop=True)

    # Remove temporary sorting columns
    df = df.drop(columns=['_sort_year', '_sort_week'])

    # Calculate difference within each group (disease, location, year)
    df['Raw_Difference'] = df.groupby(group_cols)[cumulative_col].diff()

    # For the first row of each group (NaN after diff), use the cumulative value
    df['Raw_Difference'] = df['Raw_Difference'].fillna(df[cumulative_col])

    # Explicitly set week 1 values to use cumulative (handles edge cases)
    week_1_mask = df['week'].astype(int) == 1
    df.loc[week_1_mask, 'Raw_Difference'] = df.loc[week_1_mask, cumulative_col]

    df['Cases_Added'] = df['Raw_Difference'].clip(lower=0)  # Use this for case counts!
    df['Cases_Removed'] = df['Raw_Difference'].clip(upper=0)  # Track corrections
    df['Week_Type'] = 'Normal'
    df.loc[df['Cases_Added'] != df['current_week'], 'Week_Type'] = 'Adjustment'
    df.loc[df['Cases_Removed'] < 0, 'Week_Type'] = 'Adjustment_Cases_Removed'

    # Ensure CorrectCount is non-negative (data quality check)
    negative_counts = df[df['Raw_Difference'] < 0]
    if len(negative_counts) > 0:
        logger.warning(f"⚠️  Found {len(negative_counts)} negative CorrectCount values. This may indicate data quality issues.")
        logger.warning(f"Sample records with negative counts:\n{negative_counts[['label', 'location1', 'year', 'week', cumulative_col, 'Raw_Difference']].head()}")

    logger.info(f"✅ Calculated CorrectCount for {len(df)} records")

    return df


yearly_partitions = TimeWindowPartitionsDefinition(
    cron_schedule="0 0 1 1 *",
                  start="2022",
timezone="America/Los_Angeles",
fmt="%Y")

weekly_partitions = WeeklyPartitionsDefinition(
    start_date="2022-01-01",
timezone="America/Los_Angeles",
)
s3_output_path = 'pathogens/cdc/nndss'
s3_latest_mpox = 'mpox/cdc'
s3_latest_measles = 'measles/cdc'


AIRTABLE_TABLE_ID = os.environ.get('AIRTABLE_MPOX_TABLE_ID')
#appSv8IBMvMUGt9tW
# #tblaXEDEH1TZSB4Zx

@asset(group_name="pathogens", key_prefix="cdc",
       name="mpox_weekly", required_resource_keys={"s3", "airtable"}

       )
def mpox_weekly(context):
    s3_resource = context.resources.s3
    at_resource = context.resources.airtable
    #mpox_url = "https://data.cdc.gov/resource/x9gk-5huc.geojson?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_one_of(%60label%60%2C%20%22Mpox%22)%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    #query="$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_one_of(%60label%60%2C%20%22Mpox%22)%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    query="$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_one_of(%60label%60%2C%20%22Mpox%22)%0AORDER%20BY%20%60sort_order%60%20ASC"
    count_query="$select=count(label)&label=Mpox"
    query = "$select=*&label=Mpox"
    base_url="https://data.cdc.gov/resource/x9gk-5huc.geojson?"
    #response = requests.get(mpox_url)
    # get count
    response = requests.get(f"{base_url}{count_query}")
    if response.status_code == 200:
        count_json = response.json()
        count = int(count_json["features"][0]["properties"]["count_label"])
    else:
        raise Exception(f"access failed: {response.status_code} {response.text}")
    limit =1000
    offset = 0
    mpox_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}"
        get_dagster_logger().info(f"url :{mpox_url} ")
        try:
            this_df = gpd.read_file(mpox_url)
            if mpox_df is None:
                mpox_df = this_df
            else:
                mpox_df = pd.concat( [mpox_df, this_df],ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{ mpox_url} {e} ")
    # store raw
    filename = f'{s3_output_path}/raw/mpox/mpox_raw'
    store_assets.dataframe_to_s3(mpox_df, filename, s3_resource )

    mpox_df["lat"] = mpox_df.geometry.y
    mpox_df["lon"] = mpox_df.geometry.x
    mpox_df['date'] = mpox_df.apply(lambda row: Week(int(row['year']), int(row['week'])).startdate(), axis=1)
    mpox_df['date'] = pd.to_datetime(mpox_df['date'])

    mpox_df.rename(columns={"m1": "current_week",
                            "m2": "previous_52_weeks__max",
                            "m3": "current_YTD__cummulative",
             "m4": "previous_YTD__cummulative",
                            "m1_flag": "current_week1_flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)
    mpox_df['current_week']= mpox_df['current_week'].fillna(0)
    mpox_df['previous_52_weeks__max']=mpox_df['previous_52_weeks__max'].fillna(0)
    mpox_df['current_YTD__cummulative']=mpox_df['current_YTD__cummulative'].fillna(0)
    mpox_df['previous_YTD__cummulative']=mpox_df['previous_YTD__cummulative'].fillna(0)
    mpox_df["key"] = mpox_df["label"] + '_' + mpox_df["year"] + '_' + mpox_df["week"] + '_' + mpox_df["location1"]
    mpox_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    mpox_df.drop(columns=["sort_order"], inplace=True)

    # Add CorrectCount column using the centralized function
    mpox_df = calculate_correct_count(mpox_df, cumulative_col='current_YTD__cummulative')

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🦠 Processing {len(mpox_df)} Mpox records with resilient epi schemas")

    # Store original format
    filename = f'{s3_output_path}/output/mpox_weekly'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )
    logger.info(f"📊 Stored original Mpox data: {len(mpox_df)} rows")

    # Ensure proper data types before processing
    mpox_df['date'] = pd.to_datetime(mpox_df['date'], errors='coerce')
    mpox_df['current_week'] = pd.to_numeric(mpox_df['current_week'], errors='coerce').fillna(0)
    mpox_df['previous_52_weeks__max'] = pd.to_numeric(mpox_df['previous_52_weeks__max'], errors='coerce').fillna(0)
    mpox_df['current_YTD__cummulative'] = pd.to_numeric(mpox_df['current_YTD__cummulative'], errors='coerce').fillna(0)
    mpox_df['previous_YTD__cummulative'] = pd.to_numeric(mpox_df['previous_YTD__cummulative'], errors='coerce').fillna(0)

    # Remove rows with invalid dates
    mpox_df = mpox_df.dropna(subset=['date'])
    logger.info(f"🔧 After type conversion and cleaning: {len(mpox_df)} records")

    # Process through resilient epi schemas by state/location
    validated_basic_records = []
    statistical_extension_records = []

    for _, row in mpox_df.iterrows():
        location = row['location1'] if pd.notna(row['location1']) else 'Unknown'
        state = row['states'] if pd.notna(row['states']) else 'Unknown'

        # Create proper camel case jurisdiction from state name
        if state != 'Unknown':
            # Convert state name to proper camel case (e.g., "OKLAHOMA" -> "Oklahoma", "NEW YORK" -> "NewYork")
            jurisdiction = ''.join(word.capitalize() for word in state.split())
        else:
            jurisdiction = 'Unknown'

        try:
            # Create basic epidemiology record for current week cases (including zero counts)
            if pd.notna(row['Cases_Added']):
                basic_data = pd.DataFrame({
                    'Date': [row['date'].strftime('%Y-%m-%d')],
                    'Count': [int(row['Cases_Added'])],
                    'Week_Type': [int(row['Week_Type'])]
                })

                validated_basic = epi_processor.process_basic_epidemiology_data(
                    basic_data,
                    jurisdiction=jurisdiction,
                    validate=True
                )

                if not validated_basic.empty:
                    validated_basic['original_location'] = location
                    validated_basic['original_state'] = state
                    validated_basic['disease'] = 'Mpox'
                    validated_basic_records.append(validated_basic)

            # Create statistical extension records for all metrics
            date_str = row['date'].strftime('%Y-%m-%d')

            metrics_data = [
                ('cases', 'current_week', row['current_week']),
                ('cases', 'net_cases', row['Raw_Difference']),
                ('cases', 'cases_added', row['Cases_Added']),
                ('cases', 'cases_removed', row['Cases_Removed']),

                ('cases', 'previous_52_weeks__max', row['previous_52_weeks__max']),
                ('cases', 'current_YTD__cummulative', row['current_YTD__cummulative']),
                ('cases', 'previous_YTD__cummulative', row['previous_YTD__cummulative'])
            ]

            for metric_type, observation_prefix, value in metrics_data:
                if pd.notna(value) and value >= 0:
                    observation_type = 'actual' if 'cases_added' in observation_prefix else 'partial-data estimate'

                    stat_record = create_statistical_extension_record(
                        jurisdiction=jurisdiction,
                        date=date_str,
                        disease='Mpox',
                        metric=metric_type,
                        observation_type=observation_type,
                        count=int(value) if value == int(value) else value
                    )

                    if not stat_record.empty:
                        stat_record['original_location'] = location
                        stat_record['original_state'] = state
                        stat_record['cdc_week'] = row['week']
                        stat_record['cdc_year'] = row['year']
                        stat_record['metric_category'] = observation_prefix
                        statistical_extension_records.append(stat_record)

        except EpidemiologyValidationError as ve:
            logger.warning(f"⚠️  Validation error for {jurisdiction} on {row['date']}: {ve}")
        except Exception as e:
            logger.error(f"❌ Error processing {jurisdiction} on {row['date']}: {e}")

    # Combine and store validated basic epidemiology data
    if validated_basic_records:
        combined_basic = pd.concat(validated_basic_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_basic)} validated basic epidemiology records")
        logger.info(f"🔍 Basic epidemiology schema validation passed for {len(validated_basic_records)} record batches")

        filename_basic = f'{s3_output_path}/output/validated_epi_schema/'
        metadata_basic = store_assets.objectMetadata(
            name="mpox_weekly_basic_epidemiology",
            description="CDC Mpox weekly data in basic epidemiology schema format",
            source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson"
        )

        store_assets.store_dataframe_to_s3(combined_basic, filename_basic, "mpox_weekly_basic", s3_resource,
                                               metadata=metadata_basic, formats=['csv', 'json'],
                                               enable_latest_path=True, latestdatasetpath=s3_latest_mpox)


    # Combine and store statistical extension data
    if statistical_extension_records:
        combined_statistical = pd.concat(statistical_extension_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_statistical)} statistical extension records")
        logger.info(f"🔍 Statistical extension schema validation passed for {len(statistical_extension_records)} record batches")

        filename_statistical = f'{s3_output_path}/output/validated_epi_schema/mpox_weekly_statistical'
        metadata_statistical = store_assets.objectMetadata(
            name="mpox_weekly_statistical_extension",
            description="CDC Mpox weekly data in statistical extension schema format",
            source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson"
        )
        try:
            gdf_statistical = gpd.GeoDataFrame(combined_statistical)
            store_assets.geodataframe_to_s3(gdf_statistical, filename_statistical, s3_resource, metadata=metadata_statistical)
            logger.info(f"📊 Stored statistical extension data: {len(combined_statistical)} rows")
        except Exception as e:
            logger.warning(f"⚠️  Storing as DataFrame instead of GeoDataFrame: {e}")
            store_assets.dataframe_to_s3(combined_statistical, filename_statistical, s3_resource, metadata=metadata_statistical)

    mpox_df=mpox_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/mpox_weekly_states'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    mpox_df.drop('geometry', axis=1, inplace=True)
    try:
        at_resource.upsert2Table(AIRTABLE_TABLE_ID, mpox_df, keyfields=['key'])
    except Exception as e:
        get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


@asset(group_name="pathogens", key_prefix="cdc",
       name="measles_weekly", required_resource_keys={"s3", "airtable"}

       )
def measles_weekly(context):
    s3_resource = context.resources.s3
    at_resource = context.resources.airtable
#     https: // data.cdc.gov / resource / x9gk - 5
#     huc.json?$query = SELECT
#     `states`,
#     `year`,
#     `week`,
#     `label`,
#     `m1`,
#     `m1_flag`,
#     `m2`,
#     `m2_flag`,
#     `m3`,
#     `m3_flag`,
#     `m4`,
#     `m4_flag`,
#     `location1`,
#     `location2`,
#     `sort_order`,
#     `geocode`
#
#
# WHERE
# caseless_one_of(`label`, "Measles, Indigenous")
# OR
# caseless_one_of(`label`, "Measles, Imported")
# ORDER
# BY
# `sort_order`
# ASC
# NULL
# LAST
# https://data.cdc.gov/resource/x9gk-5huc.json?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%0A%20%20caseless_one_of(%60label%60%2C%20%22Measles%2C%20Indigenous%22)%0A%20%20%20%20OR%20caseless_one_of(%60label%60%2C%20%22Measles%2C%20Imported%22)%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST
    query="$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_one_of(%60label%60%2C%20%22Mpox%22)%0AORDER%20BY%20%60sort_order%60%20ASC"
    count_query="$select=count(label)&$where=label='Measles, Indigenous' OR label='Measles, Imported'"
    query = "$select=*&$where=label='Measles, Indigenous' OR label='Measles, Imported' "
    query = "$select=*"
    where = "&$where=label='Measles,%20Indigenous'%20OR%20label='Measles,%20Imported'"
    base_url="https://data.cdc.gov/resource/x9gk-5huc.geojson?"
    #response = requests.get(mpox_url)
    # get count
    response = requests.get(f"{base_url}{count_query}")
    if response.status_code == 200:
        count_json = response.json()
        count = int(count_json["features"][0]["properties"]["count_label"])
    else:
        raise Exception(f"access failed: {response.status_code} {response.text}")
    limit =1000
    offset = 0
    mpox_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}&{where}"
        get_dagster_logger().info(f"url :{mpox_url} ")
        try:
            this_df = gpd.read_file(mpox_url)
            if mpox_df is None:
                mpox_df = this_df
            else:
                mpox_df = pd.concat( [mpox_df, this_df],ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{ mpox_url} {e} ")
    # store raw
    filename = f'{s3_output_path}/raw/measles/measles_raw'
    store_assets.dataframe_to_s3(mpox_df, filename, s3_resource )
    mpox_df["lat"] = mpox_df.geometry.y
    mpox_df["lon"] = mpox_df.geometry.x
    mpox_df['date'] = mpox_df.apply(lambda row: Week(int(row['year']), int(row['week'])).startdate(), axis=1)
    mpox_df['date'] = pd.to_datetime(mpox_df['date'])

    mpox_df.rename(columns={"m1": "current_week",
                           "m2": "previous_52_weeks__max",
                           "m3": "current_YTD__cummulative",
   "m4": "previous_YTD__cummulative",
                            "m1_flag": "current_week1_flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)

    mpox_df['current_week']= mpox_df['current_week'].fillna(0)
    mpox_df['previous_52_weeks__max']=mpox_df['previous_52_weeks__max'].fillna(0)
    mpox_df['current_YTD__cummulative']=mpox_df['current_YTD__cummulative'].fillna(0)
    mpox_df['previous_YTD__cummulative']=mpox_df['previous_YTD__cummulative'].fillna(0)
    mpox_df["key"] = mpox_df["label"] + '_' + mpox_df["year"] + '_' + mpox_df["week"] + '_' + mpox_df["location1"]
    mpox_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    mpox_df.drop(columns=["sort_order"], inplace=True)

    # Add CorrectCount column using the centralized function
    mpox_df = calculate_correct_count(mpox_df, cumulative_col='current_YTD__cummulative')

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🦠 Processing {len(mpox_df)} Measles records with resilient epi schemas")

    # Store original format
    filename = f'{s3_output_path}/output/measles_weekly'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )
    logger.info(f"📊 Stored original Measles data: {len(mpox_df)} rows")

    # Ensure proper data types before processing
    mpox_df['date'] = pd.to_datetime(mpox_df['date'], errors='coerce')
    mpox_df['current_week'] = pd.to_numeric(mpox_df['current_week'], errors='coerce').fillna(0)
    mpox_df['previous_52_weeks__max'] = pd.to_numeric(mpox_df['previous_52_weeks__max'], errors='coerce').fillna(0)
    mpox_df['current_YTD__cummulative'] = pd.to_numeric(mpox_df['current_YTD__cummulative'], errors='coerce').fillna(0)
    mpox_df['previous_YTD__cummulative'] = pd.to_numeric(mpox_df['previous_YTD__cummulative'], errors='coerce').fillna(0)

    # Remove rows with invalid dates
    mpox_df = mpox_df.dropna(subset=['date'])
    logger.info(f"🔧 After type conversion and cleaning: {len(mpox_df)} records")

    # Process through resilient epi schemas by state/location
    validated_basic_records = []
    statistical_extension_records = []

    for _, row in mpox_df.iterrows():
        location = row['location1'] if pd.notna(row['location1']) else 'Unknown'
        state = row['states'] if pd.notna(row['states']) else 'Unknown'

        # Create proper camel case jurisdiction from state name
        if state != 'Unknown':
            # Convert state name to proper camel case (e.g., "OKLAHOMA" -> "Oklahoma", "NEW YORK" -> "NewYork")
            jurisdiction = ''.join(word.capitalize() for word in state.split())
        else:
            jurisdiction = 'Unknown'

        # Determine disease type based on label
        disease_name = 'Measles'
        if 'Indigenous' in str(row.get('label', '')):
            disease_name = 'Measles (Indigenous)'
        elif 'Imported' in str(row.get('label', '')):
            disease_name = 'Measles (Imported)'

        try:
            # Create basic epidemiology record for current week cases (including zero counts)
            if pd.notna(row['cases_added']):
                basic_data = pd.DataFrame({
                    'Date': [row['date'].strftime('%Y-%m-%d')],
                    'Count': [int(row['cases_added'])],
                    'Week_Type': [int(row['Week_Type'])]
                })

                validated_basic = epi_processor.process_basic_epidemiology_data(
                    basic_data,
                    jurisdiction=jurisdiction,
                    validate=True
                )

                if not validated_basic.empty:
                    validated_basic['original_location'] = location
                    validated_basic['original_state'] = state
                    validated_basic['disease'] = disease_name
                    validated_basic['disease_label'] = str(row.get('label', ''))
                    validated_basic_records.append(validated_basic)

            # Create statistical extension records for all metrics
            date_str = row['date'].strftime('%Y-%m-%d')

            metrics_data = [
                ('cases', 'current_week', row['current_week']),
                ('cases', 'net_cases', row['Raw_Difference']),
                ('cases', 'cases_added', row['Cases_Added']),
                ('cases', 'cases_removed', row['Cases_Removed']),

                ('cases', 'previous_52_weeks__max', row['previous_52_weeks__max']),
                ('cases', 'current_YTD__cummulative', row['current_YTD__cummulative']),
                ('cases', 'previous_YTD__cummulative', row['previous_YTD__cummulative'])
            ]

            for metric_type, observation_prefix, value in metrics_data:
                if pd.notna(value) and value >= 0:
                    observation_type = 'actual' if 'cases_added' in observation_prefix else 'partial-data estimate'

                    stat_record = create_statistical_extension_record(
                        jurisdiction=jurisdiction,
                        date=date_str,
                        disease=disease_name,
                        metric=metric_type,
                        observation_type=observation_type,
                        count=int(value) if value == int(value) else value
                    )

                    if not stat_record.empty:
                        stat_record['original_location'] = location
                        stat_record['original_state'] = state
                        stat_record['disease_label'] = str(row.get('label', ''))
                        stat_record['cdc_week'] = row['week']
                        stat_record['cdc_year'] = row['year']
                        stat_record['metric_category'] = observation_prefix
                        statistical_extension_records.append(stat_record)

        except EpidemiologyValidationError as ve:
            logger.warning(f"⚠️  Validation error for {jurisdiction} on {row['date']}: {ve}")
        except Exception as e:
            logger.error(f"❌ Error processing {jurisdiction} on {row['date']}: {e}")

    # Combine and store validated basic epidemiology data
    if validated_basic_records:
        combined_basic = pd.concat(validated_basic_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_basic)} validated basic epidemiology records")
        logger.info(f"🔍 Basic epidemiology schema validation passed for {len(validated_basic_records)} record batches")

        filename_basic = f'{s3_output_path}/output/validated_epi_schema/'
        metadata_basic = store_assets.objectMetadata(
            name="measles_weekly_basic_epidemiology",
            description="CDC Measles weekly data in basic epidemiology schema format",
            source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson"
        )
        store_assets.store_dataframe_to_s3(combined_basic, filename_basic, "measles_weekly_basic", s3_resource,
                                               metadata=metadata_basic, formats=['csv', 'json'],
                                               enable_latest_path=True, latestdatasetpath=s3_latest_measles)

    # Combine and store statistical extension data
    if statistical_extension_records:
        combined_statistical = pd.concat(statistical_extension_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_statistical)} statistical extension records")
        logger.info(f"🔍 Statistical extension schema validation passed for {len(statistical_extension_records)} record batches")

        filename_statistical = f'{s3_output_path}/output/validated_epi_schema/measles_weekly_statistical'
        metadata_statistical = store_assets.objectMetadata(
            name="measles_weekly_statistical_extension",
            description="CDC Measles weekly data in statistical extension schema format",
            source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson"
        )
        try:
            gdf_statistical = gpd.GeoDataFrame(combined_statistical)
            store_assets.geodataframe_to_s3(gdf_statistical, filename_statistical, s3_resource, metadata=metadata_statistical)
            logger.info(f"📊 Stored statistical extension data: {len(combined_statistical)} rows")
        except Exception as e:
            logger.warning(f"⚠️  Storing as DataFrame instead of GeoDataFrame: {e}")
            store_assets.dataframe_to_s3(combined_statistical, filename_statistical, s3_resource, metadata=metadata_statistical)

    mpox_df=mpox_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/measles_weekly_states'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    mpox_df.drop('geometry', axis=1, inplace=True)
    try:
        at_resource.upsert2Table(AIRTABLE_TABLE_ID, mpox_df, keyfields=['key'])
    except Exception as e:
        get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


@asset_check(asset=AssetKey(["cdc", "mpox_weekly"]), description="Validates that no count columns contain negative values", required_resource_keys={"s3"})
def check_mpox_weekly_no_negative_counts(context):
    """
    Validates that count columns in mpox_weekly data do not contain negative values.
    Checks: CorrectCount, current_week, current_YTD__cummulative, previous_YTD__cummulative
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    try:
        # Read the most recent mpox_weekly data from S3 (CSV format)
        bucket_name = s3_resource.bucket
        file_path = f'{s3_output_path}/output/mpox_weekly_states.csv'

        # Get object from S3
        obj = s3_resource.get_client().get_object(Bucket=bucket_name, Key=file_path)
        df = pd.read_csv(obj['Body'])

        if df is None or df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Could not read mpox_weekly data from S3 or data is empty"
            )

        # Define count columns to check
        count_columns = ['Raw_Difference', 'current_week', 'current_YTD__cummulative', 'previous_YTD__cummulative']

        # Check for negative values in each column
        negative_findings = {}
        for col in count_columns:
            if col in df.columns:
                negative_mask = df[col] < 0
                negative_count = negative_mask.sum()
                if negative_count > 0:
                    negative_findings[col] = {
                        'count': negative_count,
                        'examples': df[negative_mask][['label', 'location1', 'year', 'week', col]].head(5).to_dict('records')
                    }

        if negative_findings:
            # Build detailed error message
            error_details = []
            total_negative = sum(v['count'] for v in negative_findings.values())
            for col, info in negative_findings.items():
                error_details.append(f"  - {col}: {info['count']} negative values")
                error_details.append(f"    Examples: {info['examples'][:2]}")

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"Found {total_negative} negative count values across {len(negative_findings)} columns:\n" + "\n".join(error_details),
                metadata={
                    "negative_counts_by_column": {col: info['count'] for col, info in negative_findings.items()},
                    "total_rows_checked": len(df)
                }
            )

        return AssetCheckResult(
            passed=True,
            description=f"All count columns validated successfully. Checked {len(df)} rows across {len(count_columns)} columns.",
            metadata={
                "total_rows_checked": len(df),
                "columns_checked": count_columns
            }
        )

    except Exception as e:
        logger.error(f"Error during mpox_weekly negative count check: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed with error: {str(e)}"
        )


@asset_check(asset=AssetKey(["cdc", "measles_weekly"]), description="Validates that no count columns contain negative values", required_resource_keys={"s3"})
def check_measles_weekly_no_negative_counts(context):
    """
    Validates that count columns in measles_weekly data do not contain negative values.
    Checks: CorrectCount, current_week, current_YTD__cummulative, previous_YTD__cummulative
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    try:
        # Read the most recent measles_weekly data from S3 (CSV format)
        bucket_name = s3_resource.bucket
        file_path = f'{s3_output_path}/output/measles_weekly_states.csv'

        # Get object from S3
        obj = s3_resource.get_client().get_object(Bucket=bucket_name, Key=file_path)
        df = pd.read_csv(obj['Body'])

        if df is None or df.empty:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description="Could not read measles_weekly data from S3 or data is empty"
            )

        # Define count columns to check
        count_columns = ['Raw_Difference', 'current_week', 'current_YTD__cummulative', 'previous_YTD__cummulative']

        # Check for negative values in each column
        negative_findings = {}
        for col in count_columns:
            if col in df.columns:
                negative_mask = df[col] < 0
                negative_count = negative_mask.sum()
                if negative_count > 0:
                    negative_findings[col] = {
                        'count': negative_count,
                        'examples': df[negative_mask][['label', 'location1', 'year', 'week', col]].head(5).to_dict('records')
                    }

        if negative_findings:
            # Build detailed error message
            error_details = []
            total_negative = sum(v['count'] for v in negative_findings.values())
            for col, info in negative_findings.items():
                error_details.append(f"  - {col}: {info['count']} negative values")
                error_details.append(f"    Examples: {info['examples'][:2]}")

            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                description=f"Found {total_negative} negative count values across {len(negative_findings)} columns:\n" + "\n".join(error_details),
                metadata={
                    "negative_counts_by_column": {col: info['count'] for col, info in negative_findings.items()},
                    "total_rows_checked": len(df)
                }
            )

        return AssetCheckResult(
            passed=True,
            description=f"All count columns validated successfully. Checked {len(df)} rows across {len(count_columns)} columns.",
            metadata={
                "total_rows_checked": len(df),
                "columns_checked": count_columns
            }
        )

    except Exception as e:
        logger.error(f"Error during measles_weekly negative count check: {e}")
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=f"Check failed with error: {str(e)}"
        )


@asset(group_name="pathogens", key_prefix="cdc",
       name="nndss_weekly_by_year", required_resource_keys={"s3", "airtable"}
       ,partitions_def=yearly_partitions
       )
def nndss_weekly_by_year(context):
    s3_resource = context.resources.s3
    filedate = context.asset_partition_key_for_output()
    #url=f"https://data.cdc.gov/resource/x9gk-5huc.json?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    #url=f"https://data.cdc.gov/resource/x9gk-5huc.geojson?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_eq(%60year%60%2C%20%22{filedate}%22)%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    # GET COUNT
    # url="https://data.cdc.gov/resource/x9gk-5huc.geojson?$select=count(year)&year=2022"
    # LOOP url="https://data.cdc.gov/resource/x9gk-5huc.geojson?year=2022"
    #get_dagster_logger().info(url)
    #response = requests.get(mpox_url)
    property="year"
    count_query=f"$select=count({property})&{property}={filedate}"
    query = f"$select=*&{property}={filedate}"
    base_url="https://data.cdc.gov/resource/x9gk-5huc.geojson?"
    #response = requests.get(mpox_url)
    # get count
    count_url = f"{base_url}{count_query}"
    get_dagster_logger().info(f"url :{count_url} ")
    response = requests.get(f"{count_url}")
    if response.status_code == 200:
        get_dagster_logger().info(f"url :{count_url} ")
        count_json = response.json()
        count = int(count_json["features"][0]["properties"][f"count_{property}"])
        get_dagster_logger().info(f"count {count} for url :{count_url} ")
    else:
        raise Exception(f"access failed: {response.status_code} {response.text}")
    limit =1000
    offset = 0
    r_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}"
        get_dagster_logger().info(f"url :{mpox_url} ")
        try:
            this_df = gpd.read_file(mpox_url)
            if  r_df is None:
                r_df = this_df
            else:
                r_df = pd.concat( [ r_df, this_df],ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{ mpox_url} {e} ")
    r_df["key"] = r_df["label"] + '_' + r_df["year"] + '_' + r_df["week"] + '_' + r_df["location1"]
    r_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    r_df["lat"] =  r_df.geometry.y
    r_df["lon"] =  r_df.geometry.x
    r_df['date'] =  r_df.apply(lambda row: date.fromisocalendar(int(row['year']), int(row['week']), 1), axis=1)
    r_df['date'] = pd.to_datetime( r_df['date'])
    r_df['current_week'] =  r_df['m1']
    r_df['previous_52_weeks_max'] =  r_df['m2']
    r_df['current_YTD_cummulative'] =  r_df['m3']
    r_df['previous_YTD_cummulative'] =  r_df['m4']

    # Add CorrectCount column using the centralized function
    r_df = calculate_correct_count(r_df, cumulative_col='current_YTD_cummulative')

    filename = f'{s3_output_path}/raw/nndss_weekly_year/nndss_weekly_{filedate}'
    store_assets.geodataframe_to_s3(r_df, filename, s3_resource )

    r_df.dropna(inplace=True, subset=["lat", "lon"])

    filename = f'{s3_output_path}/raw/nndss_weekly_year/nndss_weekly_states_{filedate}'
    store_assets.geodataframe_to_s3(r_df, filename, s3_resource )

@asset(group_name="pathogens", key_prefix="cdc",
       name="nndss_weekly", required_resource_keys={"s3", "airtable"}
       ,partitions_def=weekly_partitions
       )
def nndss_weekly(context):
    s3_resource = context.resources.s3
    filedate = context.asset_partition_key_for_output()
    #url=f"https://data.cdc.gov/resource/x9gk-5huc.json?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    #url=f"https://data.cdc.gov/resource/x9gk-5huc.geojson?$query=SELECT%0A%20%20%60states%60%2C%0A%20%20%60year%60%2C%0A%20%20%60week%60%2C%0A%20%20%60label%60%2C%0A%20%20%60m1%60%2C%0A%20%20%60m1_flag%60%2C%0A%20%20%60m2%60%2C%0A%20%20%60m2_flag%60%2C%0A%20%20%60m3%60%2C%0A%20%20%60m3_flag%60%2C%0A%20%20%60m4%60%2C%0A%20%20%60m4_flag%60%2C%0A%20%20%60location1%60%2C%0A%20%20%60location2%60%2C%0A%20%20%60sort_order%60%2C%0A%20%20%60geocode%60%0AWHERE%20caseless_eq(%60year%60%2C%20%22{filedate}%22)%0AORDER%20BY%20%60sort_order%60%20ASC%20NULL%20LAST"
    # GET COUNT
    # url="https://data.cdc.gov/resource/x9gk-5huc.geojson?$select=count(year)&year=2022"
    # LOOP url="https://data.cdc.gov/resource/x9gk-5huc.geojson?year=2022"
    #get_dagster_logger().info(url)
    #response = requests.get(mpox_url)
    if isinstance(filedate,str):
        filedate = date.fromisoformat(filedate)
        year, week, day = filedate.isocalendar()
    elif isinstance(filedate, date):
        year, week, day = filedate.isocalendar()
    elif isinstance(filedate, datetime):
        year, week, day = filedate.isocalendar()
    else:
        year = filedate.year
        week = filedate.week
    property="year"
    property2="week"
    count_query= f'$select=count({property})&{property}={year}&{property2}={week}'
    query = f"$select=*&{property}={year}&{property2}={week}"
    base_url="https://data.cdc.gov/resource/x9gk-5huc.geojson?"
    #response = requests.get(mpox_url)
    # get count
    count_url = f"{base_url}{count_query}"
    get_dagster_logger().info(f"url :{count_url} ")
    response = requests.get(f"{count_url}")
    if response.status_code == 200:
        get_dagster_logger().info(f"url :{count_url} ")
        count_json = response.json()
        count = int(count_json["features"][0]["properties"][f"count_{property}"])
        get_dagster_logger().info(f"count {count} for url :{count_url} ")
        if count == 0:
            raise Exception(f"No Data count {count} for url :{count_url} ")
    else:
        raise Exception(f"access failed: {response.status_code} {response.text}")
    limit =1000
    offset = 0
    r_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}"
        get_dagster_logger().info(f"url :{mpox_url} ")
        try:
            this_df = gpd.read_file(mpox_url)
            if  r_df is None:
                r_df = this_df
            else:
                r_df = pd.concat( [ r_df, this_df],ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{ mpox_url} {e} ")
    r_df["key"] = r_df["label"] + '_' + r_df["year"] + '_' + r_df["week"] + '_' + r_df["location1"]
    r_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    r_df["lat"] =  r_df.geometry.y
    r_df["lon"] =  r_df.geometry.x
    r_df['date'] =  r_df.apply(lambda row: date.fromisocalendar(int(row['year']), int(row['week']), 1), axis=1)
    r_df['date'] = pd.to_datetime( r_df['date'])

    r_df['current_week'] =  r_df['m1'].fillna(0)
    r_df['previous_52_weeks_max'] =  r_df['m2'].fillna(0)
    r_df['current_YTD_cummulative'] =  r_df['m3'].fillna(0)
    r_df['previous_YTD_cummulative'] =  r_df['m4'].fillna(0)

    # Add CorrectCount column using the centralized function
    r_df = calculate_correct_count(r_df, cumulative_col='current_YTD_cummulative')

    filename = f'{s3_output_path}/raw/nndss_weekly/nndss_weekly_{year}_{week}'
    store_assets.geodataframe_to_s3(r_df, filename, s3_resource )

    r_df.dropna(inplace=True, subset=["lat", "lon"])

    filename = f'{s3_output_path}/raw/nndss_weekly/nndss_weekly_states_{year}_{week}'
    store_assets.geodataframe_to_s3(r_df, filename, s3_resource )

# schedules and jobs
cdc_nndss_weekly_job = define_asset_job(
    "cdc_weekly", selection=[ AssetKey(["cdc", "measles_weekly"]), AssetKey(["cdc", "mpox_weekly"])]
)

@schedule(job=cdc_nndss_weekly_job, cron_schedule="@weekly", name="cdc_nndss_weekly_job")
def cdc_nndss_weekly_schedule(context):
    return RunRequest(
    )

cdc_nndss_raw_job = define_asset_job(
    "cdc_raw_weekly", selection=[ AssetKey(["cdc", "nndss_weekly"])]
, partitions_def=weekly_partitions
)

@schedule(job=cdc_nndss_raw_job, cron_schedule="@weekly", name="cdc_nndss_raw_job")
def cdc_nndss_raw_schedule(context):
    # this causes an error. no partion
    #partition_key = weekly_partitions.get_partition_key_for_timestamp(context.scheduled_execution_time.timestamp())

    thisweek=context.scheduled_execution_time.timestamp()
    last_week=thisweek-timedelta(days=7).total_seconds()
    partition_key = weekly_partitions.get_partition_key_for_timestamp(last_week)


    return RunRequest(
        partition_key=partition_key,

    )
