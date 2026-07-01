import requests
import os
import io
import json
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
from resilient_core.utils.constants import ICONS
from resilient_core.utils import store_assets
from resilient_core.utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    StatisticalExtensionSchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    transform_to_basic_epidemiology,
    create_statistical_extension_record
)
from epiweeks import Week, Year


def _smooth_negatives(df, group_cols):
    """
    Smooth negative Raw_Difference values by redistributing across neighboring weeks.

    Small negatives (abs <= 3): average over 3-week window (1 prev + self + 1 next).
    Large negatives (abs > 3): average over 5-week window (2 prev + self + 2 next).
    Residual negatives after smoothing are clamped to zero.

    Args:
        df: DataFrame with Raw_Difference column
        group_cols: Columns defining groups (smoothing stays within groups)

    Returns:
        DataFrame with Smoothed_Difference and _smoothing_applied columns added
    """
    logger = get_dagster_logger()
    df['Smoothed_Difference'] = df['Raw_Difference'].copy()
    df['_smoothing_applied'] = 'None'

    for _, group_df in df.groupby(group_cols):
        group_idx = group_df.index.tolist()
        neg_indices = group_df[group_df['Smoothed_Difference'] < 0].index
        if len(neg_indices) == 0:
            continue

        for idx in neg_indices:
            val = df.loc[idx, 'Smoothed_Difference']
            if val >= 0:
                # May have been fixed by a prior neighbor smoothing
                continue
            pos = group_idx.index(idx)

            # Window radius based on magnitude of negative
            radius = 1 if abs(val) <= 3 else 2
            start = max(0, pos - radius)
            end = min(len(group_idx) - 1, pos + radius)
            window_indices = group_idx[start:end + 1]

            # Redistribute total across window as whole numbers
            window_total = int(df.loc[window_indices, 'Smoothed_Difference'].sum())
            n = len(window_indices)
            avg = window_total // n
            remainder = window_total - avg * n

            new_vals = [avg] * n
            for i in range(abs(remainder)):
                new_vals[i] += 1 if remainder > 0 else -1

            df.loc[window_indices, 'Smoothed_Difference'] = new_vals

            # Label the negative week
            label = 'Smoothed_Small' if abs(val) <= 3 else 'Smoothed_Large'
            df.loc[idx, '_smoothing_applied'] = label

            # Label neighbors that were adjusted
            for nidx in window_indices:
                if nidx != idx and df.loc[nidx, '_smoothing_applied'] == 'None':
                    df.loc[nidx, '_smoothing_applied'] = 'Smoothing_Neighbor'

    # Final pass: clamp any residual negatives to zero
    still_neg = df['Smoothed_Difference'] < 0
    if still_neg.any():
        logger.warning(f"Clamping {still_neg.sum()} residual negative values to zero after smoothing")
        df.loc[still_neg, 'Smoothed_Difference'] = 0
        df.loc[still_neg, '_smoothing_applied'] = 'Smoothed_Residual_Zeroed'

    return df


def calculate_correct_count(df, cumulative_col='current_YTD__cummulative', group_cols=None,
                            previous_cumulative_col=None):
    """
    Calculate weekly case counts by computing differences from cumulative values,
    with smoothing for negative adjustments.

    For previous years, uses previous_cumulative_col (corrected data) when available.
    For the current year, uses cumulative_col (preliminary data).

    Negative weekly diffs are smoothed:
    - abs <= 3: redistributed over 3-week window
    - abs > 3: redistributed over 5-week window
    - Residual negatives clamped to zero

    Args:
        df: pandas DataFrame or GeoDataFrame with disease surveillance data
        cumulative_col: Name of the cumulative count column (default: 'current_YTD__cummulative')
        group_cols: List of columns to group by (default: ['label', 'location1', 'year'])
        previous_cumulative_col: Name of the previous year cumulative column for corrected data

    Returns:
        DataFrame with Raw_Difference, Cases_Added, Cases_Removed, adjusted_week,
        adjusted_YTD__cummulative, and Week_Type columns added
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

    # Build effective cumulative: use previous_cumulative_col for prior years
    current_year = str(datetime.now().year)
    if previous_cumulative_col and previous_cumulative_col in df.columns:
        df[previous_cumulative_col] = pd.to_numeric(df[previous_cumulative_col], errors='coerce').fillna(0)
        is_previous_year = df['year'].astype(str) != current_year
        df['_effective_cumulative'] = df[cumulative_col].copy()
        df.loc[is_previous_year, '_effective_cumulative'] = df.loc[is_previous_year, previous_cumulative_col]
        logger.info(f"Using {previous_cumulative_col} for {is_previous_year.sum()} previous-year rows, "
                     f"{cumulative_col} for {(~is_previous_year).sum()} current-year rows")
    else:
        df['_effective_cumulative'] = df[cumulative_col].copy()

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
    df['Raw_Difference'] = df.groupby(group_cols)['_effective_cumulative'].diff()

    # For the first row of each group (NaN after diff), use the cumulative value
    df['Raw_Difference'] = df['Raw_Difference'].fillna(df['_effective_cumulative'])

    # Explicitly set week 1 values to use cumulative (handles edge cases)
    week_1_mask = df['week'].astype(int) == 1
    df.loc[week_1_mask, 'Raw_Difference'] = df.loc[week_1_mask, '_effective_cumulative']

    # Drop temporary effective cumulative column
    df = df.drop(columns=['_effective_cumulative'])

    # Log negative values before smoothing
    negative_counts = df[df['Raw_Difference'] < 0]
    if len(negative_counts) > 0:
        logger.warning(f"Found {len(negative_counts)} negative Raw_Difference values before smoothing.")
        logger.warning(f"Sample records:\n{negative_counts[['label', 'location1', 'year', 'week', cumulative_col, 'Raw_Difference']].head()}")

    # Smooth negatives
    df = _smooth_negatives(df, group_cols)

    # Audit columns
    df['Cases_Added'] = df['Smoothed_Difference'].clip(lower=0)
    df['Cases_Removed'] = df['Raw_Difference'].clip(upper=0)

    # New adjusted columns (original CDC columns stay untouched)
    df['adjusted_week'] = df['Smoothed_Difference'].clip(lower=0).astype(int)
    df['adjusted_YTD__cummulative'] = df.groupby(group_cols)['adjusted_week'].cumsum().astype(int)

    # Week_Type assignment
    df['Week_Type'] = 'Normal'
    df.loc[df['Cases_Added'] != df['current_week'], 'Week_Type'] = 'Adjustment'
    df.loc[df['Cases_Removed'] < 0, 'Week_Type'] = 'Adjustment_Cases_Removed'

    # Smoothing types override base types
    smoothed = df['_smoothing_applied'] != 'None'
    df.loc[smoothed, 'Week_Type'] = df.loc[smoothed, '_smoothing_applied']
    df = df.drop(columns=['_smoothing_applied'])

    # Prefix current year rows as preliminary (data not yet corrected)
    is_current = df['year'].astype(str) == current_year
    df.loc[is_current, 'Week_Type'] = 'Preliminary_' + df.loc[is_current, 'Week_Type']

    logger.info(f"Calculated CorrectCount for {len(df)} records. "
                f"Week_Type distribution:\n{df['Week_Type'].value_counts().to_string()}")

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
s3_latest_mpox = 'pathogens/mpox/usa'
s3_latest_measles = 'pathogens/measles/usa'


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
                            "m1_flag": "current_week__flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)
    # Convert to numeric BEFORE calculate_correct_count (needed for Week_Type comparison)
    mpox_df['current_week'] = pd.to_numeric(mpox_df['current_week'], errors='coerce').fillna(0)
    mpox_df['previous_52_weeks__max'] = pd.to_numeric(mpox_df['previous_52_weeks__max'], errors='coerce').fillna(0)
    mpox_df['current_YTD__cummulative'] = pd.to_numeric(mpox_df['current_YTD__cummulative'], errors='coerce').fillna(0)
    mpox_df['previous_YTD__cummulative'] = pd.to_numeric(mpox_df['previous_YTD__cummulative'], errors='coerce').fillna(0)

    mpox_df["key"] = mpox_df["label"] + '_' + mpox_df["year"] + '_' + mpox_df["week"] + '_' + mpox_df["location1"]
    mpox_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    mpox_df.drop(columns=["sort_order"], inplace=True)

    # Add CorrectCount column using the centralized function
    mpox_df = calculate_correct_count(mpox_df, cumulative_col='current_YTD__cummulative',
                                      previous_cumulative_col='previous_YTD__cummulative')

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🦠 Processing {len(mpox_df)} Mpox records with resilient epi schemas")

    # Store original format
    filename = f'{s3_output_path}/output/mpox_weekly'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )
    logger.info(f"📊 Stored original Mpox data: {len(mpox_df)} rows")

    # Ensure proper data types before processing
    mpox_df['date'] = pd.to_datetime(mpox_df['date'], errors='coerce')
    # Note: Numeric conversions already done before calculate_correct_count() call

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
            # Create basic epidemiology record using adjusted weekly count
            if pd.notna(row['adjusted_week']):
                basic_data = pd.DataFrame({
                    'Date': [row['date'].strftime('%Y-%m-%d')],
                    'Count': [int(row['adjusted_week'])],
                    'Week_Type': [row['Week_Type']]
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
                ('cases', 'adjusted_week', row['adjusted_week']),
                ('cases', 'adjusted_YTD__cummulative', row['adjusted_YTD__cummulative']),
                ('cases', 'previous_52_weeks__max', row['previous_52_weeks__max']),
                ('cases', 'current_YTD__cummulative', row['current_YTD__cummulative']),
                ('cases', 'previous_YTD__cummulative', row['previous_YTD__cummulative'])
            ]

            is_current_year = str(row['year']) == str(datetime.now().year)

            for metric_type, observation_prefix, value in metrics_data:
                if pd.notna(value) and value >= 0:
                    if observation_prefix in ('adjusted_week', 'adjusted_YTD__cummulative'):
                        observation_type = 'preliminary estimate' if is_current_year else 'corrected'
                    elif observation_prefix == 'cases_added':
                        observation_type = 'preliminary estimate' if is_current_year else 'corrected'
                    else:
                        observation_type = 'reported'

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

        store_assets.store_dataframe_to_s3(combined_basic, filename_basic, "mpox_usa_weekly_basic", s3_resource,
                                               metadata=metadata_basic, formats=['csv', 'json', 'parquet'],
                                               enable_latest_path=True, latestdatasetpath=s3_latest_mpox)


    # Combine and store statistical extension data
    if statistical_extension_records:
        combined_statistical = pd.concat(statistical_extension_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_statistical)} statistical extension records")
        logger.info(f"🔍 Statistical extension schema validation passed for {len(statistical_extension_records)} record batches")

        filename_statistical = f'{s3_output_path}/output/validated_epi_schema/'
        metadata_statistical = store_assets.objectMetadata(
            name="mpox_weekly_statistical_extension",
            description="CDC Mpox weekly data in statistical extension schema format",
            source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson"
        )
        store_assets.store_dataframe_to_s3(combined_statistical, filename_statistical, "mpox_usa_weekly_statistical", s3_resource,
                                           metadata=metadata_statistical, formats=['csv', 'json', 'parquet'],
                                           enable_latest_path=False, latestdatasetpath=s3_latest_mpox)


    mpox_df=mpox_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/mpox_weekly_states'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    mpox_df.drop('geometry', axis=1, inplace=True)
    # try:
    #     at_resource.upsert2Table(AIRTABLE_TABLE_ID, mpox_df, keyfields=['key'])
    # except Exception as e:
    #     get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


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
    disease_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}&{where}"
        get_dagster_logger().info(f"url :{mpox_url} ")
        try:
            this_df = gpd.read_file(mpox_url)
            if disease_df is None:
                disease_df = this_df
            else:
                disease_df = pd.concat( [disease_df, this_df],ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{ mpox_url} {e} ")
    # store raw
    filename = f'{s3_output_path}/raw/measles/measles_usa_raw'
    store_assets.dataframe_to_s3(disease_df, filename, s3_resource )
    disease_df["lat"] = disease_df.geometry.y
    disease_df["lon"] = disease_df.geometry.x
    disease_df['date'] = disease_df.apply(lambda row: Week(int(row['year']), int(row['week'])).startdate(), axis=1)
    disease_df['date'] = pd.to_datetime(disease_df['date'])

    disease_df.rename(columns={"m1": "current_week",
                           "m2": "previous_52_weeks__max",
                           "m3": "current_YTD__cummulative",
   "m4": "previous_YTD__cummulative",
                            "m1_flag": "current_week__flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)

    # Convert to numeric BEFORE calculate_correct_count (needed for Week_Type comparison)
    disease_df['current_week'] = pd.to_numeric(disease_df['current_week'], errors='coerce').fillna(0)
    disease_df['previous_52_weeks__max'] = pd.to_numeric(disease_df['previous_52_weeks__max'], errors='coerce').fillna(0)
    disease_df['current_YTD__cummulative'] = pd.to_numeric(disease_df['current_YTD__cummulative'], errors='coerce').fillna(0)
    disease_df['previous_YTD__cummulative'] = pd.to_numeric(disease_df['previous_YTD__cummulative'], errors='coerce').fillna(0)

    disease_df["key"] = disease_df["label"] + '_' + disease_df["year"] + '_' + disease_df["week"] + '_' + disease_df["location1"]
    disease_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    disease_df.drop(columns=["sort_order"], inplace=True)

    # Add CorrectCount column using the centralized function
    disease_df = calculate_correct_count(disease_df, cumulative_col='current_YTD__cummulative',
                                         previous_cumulative_col='previous_YTD__cummulative')

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🦠 Processing {len(disease_df)} Measles records with resilient epi schemas")

    # Store original format
    filename = f'{s3_output_path}/output/measles_usa_weekly'
    store_assets.geodataframe_to_s3(disease_df, filename, s3_resource )
    logger.info(f"📊 Stored original Measles data: {len(disease_df)} rows")

    # Ensure proper data types before processing
    disease_df['date'] = pd.to_datetime(disease_df['date'], errors='coerce')
    # Note: Numeric conversions already done before calculate_correct_count() call

    # Remove rows with invalid dates
    disease_df = disease_df.dropna(subset=['date'])
    logger.info(f"🔧 After type conversion and cleaning: {len(disease_df)} records")

    # Process through resilient epi schemas by state/location
    validated_basic_records = []
    statistical_extension_records = []

    for _, row in disease_df.iterrows():
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
            # Create basic epidemiology record using adjusted weekly count
            if pd.notna(row['adjusted_week']):
                basic_data = pd.DataFrame({
                    'Date': [row['date'].strftime('%Y-%m-%d')],
                    'Count': [int(row['adjusted_week'])],
                    'Week_Type': [str(row['Week_Type'])]
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
                ('cases', 'adjusted_week', row['adjusted_week']),
                ('cases', 'adjusted_YTD__cummulative', row['adjusted_YTD__cummulative']),
                ('cases', 'previous_52_weeks__max', row['previous_52_weeks__max']),
                ('cases', 'current_YTD__cummulative', row['current_YTD__cummulative']),
                ('cases', 'previous_YTD__cummulative', row['previous_YTD__cummulative'])
            ]

            is_current_year = str(row['year']) == str(datetime.now().year)

            for metric_type, observation_prefix, value in metrics_data:
                if pd.notna(value) and value >= 0:
                    if observation_prefix in ('adjusted_week', 'adjusted_YTD__cummulative'):
                        observation_type = 'preliminary estimate' if is_current_year else 'corrected'
                    elif observation_prefix == 'cases_added':
                        observation_type = 'preliminary estimate' if is_current_year else 'corrected'
                    else:
                        observation_type = 'reported'

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
        store_assets.store_dataframe_to_s3(combined_basic, filename_basic, "measles_usa_weekly_basic", s3_resource,
                                               metadata=metadata_basic, formats=['csv', 'json'],
                                               enable_latest_path=True, latestdatasetpath=s3_latest_measles)

    # Combine and store statistical extension data
    if statistical_extension_records:
        combined_statistical = pd.concat(statistical_extension_records, ignore_index=True)
        logger.info(f"✅ Created {len(combined_statistical)} statistical extension records")
        logger.info(f"🔍 Statistical extension schema validation passed for {len(statistical_extension_records)} record batches")

        filename_statistical = f'{s3_output_path}/output/validated_epi_schema/measles_usa_weekly_statistical'
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

    disease_df=disease_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/measles_weekly_states'
    store_assets.geodataframe_to_s3(disease_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    disease_df.drop('geometry', axis=1, inplace=True)
    # try:
    #     at_resource.upsert2Table(AIRTABLE_TABLE_ID, disease_df, keyfields=['key'])
    # except Exception as e:
    #     get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


@asset_check(asset=AssetKey(["cdc", "mpox_weekly"]), description="Validates that adjusted count columns contain no negative values", required_resource_keys={"s3"})
def check_mpox_weekly_no_negative_counts(context):
    """
    Validates that adjusted count columns in mpox_weekly data do not contain negative values.
    Checks: adjusted_week, adjusted_YTD__cummulative
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

        # Define adjusted count columns to check (should never be negative after smoothing)
        count_columns = ['adjusted_week', 'adjusted_YTD__cummulative']

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


@asset_check(asset=AssetKey(["cdc", "measles_weekly"]), description="Validates that adjusted count columns contain no negative values", required_resource_keys={"s3"})
def check_measles_weekly_no_negative_counts(context):
    """
    Validates that adjusted count columns in measles_weekly data do not contain negative values.
    Checks: adjusted_week, adjusted_YTD__cummulative
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

        # Define adjusted count columns to check (should never be negative after smoothing)
        count_columns = ['adjusted_week', 'adjusted_YTD__cummulative']

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
    r_df = calculate_correct_count(r_df, cumulative_col='current_YTD_cummulative',
                                    previous_cumulative_col='previous_YTD_cummulative')

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
    r_df = calculate_correct_count(r_df, cumulative_col='current_YTD_cummulative',
                                    previous_cumulative_col='previous_YTD_cummulative')

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


# =============================================================================
# NNDSS aggregated basis + data-driven disease subsets
# =============================================================================
#
# Three assets generalize the mpox/measles pattern to every NNDSS label:
#   1. nndss_all_basis      – one cleaned file across ALL labels (mpox-style
#                             correction: prior years use m4/corrected, current
#                             year uses m3/preliminary, with negative smoothing).
#   2. nndss_label_mapping  – discovers distinct labels and clusters them into
#                             "core" diseases (a disease may span several labels,
#                             e.g. dengue=3, measles=2); emits a validated mapping.
#   3. nndss_disease_subsets– per-disease resilient-epi outputs, BOTH a combined
#                             per-disease total (summed member labels) AND the
#                             per-label breakdown.

CDC_BASE_URL = "https://data.cdc.gov/resource/x9gk-5huc.geojson?"
CDC_SOURCE_URL = "https://data.cdc.gov/resource/x9gk-5huc.geojson"

s3_basis_path = f'{s3_output_path}/basis'          # pathogens/cdc/nndss/basis
s3_latest_basis = 'pathogens/nndss/basis'
s3_config_path = f'{s3_output_path}/config'        # pathogens/cdc/nndss/config
s3_diseases_path = f'{s3_output_path}/diseases'    # pathogens/cdc/nndss/diseases
s3_latest_diseases = 'pathogens/nndss/diseases'
LABEL_OVERRIDES_KEY = f'{s3_config_path}/label_overrides.json'

# Statistical-extension metrics as (metric_category, source_column, rule). The
# 'adjusted' rule maps to a preliminary estimate for the current year and a
# corrected value for prior years; everything else is 'reported'.
_STAT_METRIC_MAP = [
    ('current_week', 'current_week', 'reported'),
    ('net_cases', 'Raw_Difference', 'reported'),
    ('cases_added', 'Cases_Added', 'adjusted'),
    ('cases_removed', 'Cases_Removed', 'reported'),
    ('adjusted_week', 'adjusted_week', 'adjusted'),
    ('adjusted_YTD__cummulative', 'adjusted_YTD__cummulative', 'adjusted'),
    ('previous_52_weeks__max', 'previous_52_weeks__max', 'reported'),
    ('current_YTD__cummulative', 'current_YTD__cummulative', 'reported'),
    ('previous_YTD__cummulative', 'previous_YTD__cummulative', 'reported'),
]
# per-week numeric columns summed when combining member labels into a disease total
_COMBINE_SUM_COLS = ['current_week', 'Raw_Difference', 'Cases_Added',
                     'Cases_Removed', 'adjusted_week', 'previous_52_weeks__max']


def _cdc_count(count_query, count_field):
    """Fetch a row count from the CDC endpoint for a given $select=count(...) query."""
    resp = requests.get(f"{CDC_BASE_URL}{count_query}")
    if resp.status_code != 200:
        raise Exception(f"access failed: {resp.status_code} {resp.text}")
    return int(resp.json()["features"][0]["properties"][count_field])


def _cdc_paged_pull(query, count, limit=1000, extra=""):
    """Page through the CDC geojson endpoint, concatenating into one GeoDataFrame."""
    df = None
    for i in range(0, count, limit):
        url = f"{CDC_BASE_URL}{query}&$offset={i}&$limit={limit}{extra}"
        get_dagster_logger().info(f"url :{url} ")
        try:
            this_df = gpd.read_file(url)
            df = this_df if df is None else pd.concat([df, this_df], ignore_index=True)
        except Exception as e:
            get_dagster_logger().error(f"{i}: access failed:{url} {e} ")
    return df


def _core_disease(label):
    """Derive the core disease name from an NNDSS label.

    Splits on the first TOP-LEVEL comma (commas inside parentheses are not
    hierarchy delimiters and are ignored). A label with no top-level comma is
    its own core disease.
    """
    label = str(label)
    # Mask commas inside (...) so they don't act as split points
    masked = re.sub(r'\(([^)]*)\)', lambda m: '(' + m.group(1).replace(',', '\0') + ')', label)
    core = masked.split(',', 1)[0]
    core = core.replace('\0', ',').strip()
    return re.sub(r'\s+', ' ', core)


def cluster_labels_to_diseases(labels, overrides=None):
    """Cluster NNDSS labels into core diseases.

    Returns a dict: disease -> [ {'label': str, 'is_rollup': bool}, ... ].
    A member is flagged is_rollup when its label equals the bare core name while
    comma-qualified children also exist for that disease — such a published
    parent/total would double-count if summed with its children.
    Optional `overrides` maps a specific label to an explicit disease name and is
    applied on top of the heuristic.
    """
    overrides = overrides or {}
    groups = {}
    for label in labels:
        if label is None or (isinstance(label, float) and pd.isna(label)):
            continue
        label = str(label)
        disease = overrides.get(label) or _core_disease(label)
        groups.setdefault(disease, set()).add(label)

    mapping = {}
    for disease, lbls in groups.items():
        lbls = sorted(lbls)
        has_children = len(lbls) > 1
        members = []
        for lbl in lbls:
            is_rollup = has_children and _core_disease(lbl) == lbl.strip()
            members.append({'label': lbl, 'is_rollup': is_rollup})
        mapping[disease] = members
    return mapping


def combine_disease_total(leaf_df, disease_name):
    """Sum the cleaned per-week metrics of a disease's leaf labels into one total.

    MUST run on already-cleaned rows (post calculate_correct_count). Groups by
    (states, location1, year, week, date), sums the additive metric columns, and
    recomputes the YTD cumulatives per (states, location1, year).
    """
    group_keys = ['states', 'location1', 'year', 'week', 'date']
    present_sum = [c for c in _COMBINE_SUM_COLS if c in leaf_df.columns]
    agg = leaf_df.groupby(group_keys, as_index=False)[present_sum].sum()

    agg['_wk'] = pd.to_numeric(agg['week'], errors='coerce')
    agg = agg.sort_values(['states', 'location1', 'year', '_wk']).reset_index(drop=True)
    if 'adjusted_week' in agg.columns:
        agg['adjusted_YTD__cummulative'] = (
            agg.groupby(['states', 'location1', 'year'])['adjusted_week'].cumsum().astype(int))
    if 'current_week' in agg.columns:
        agg['current_YTD__cummulative'] = (
            agg.groupby(['states', 'location1', 'year'])['current_week'].cumsum())
    agg = agg.drop(columns=['_wk'])

    current_year = str(datetime.now().year)
    agg['Week_Type'] = agg['year'].astype(str).apply(
        lambda y: 'Preliminary_Combined' if y == current_year else 'Combined')
    return agg


def _camel_jurisdiction(states):
    """Vectorized CamelCase of a state-name Series (no spaces, schema-compliant)."""
    j = states.fillna('Unknown').astype(str).str.strip()
    j = j.replace('', 'Unknown')
    camel = j.str.title().str.replace(r'\s+', '', regex=True)
    return camel.mask(j.str.upper() == 'UNKNOWN', 'Unknown')


def records_to_resilient_epi(df, disease_name, label_col=None):
    """Transform cleaned per-week rows into (basic_df, statistical_df).

    Fully vectorized (no per-row iteration / per-record validation) so it scales
    to the full NNDSS basis. Produces the same basic-epidemiology and statistical
    -extension shapes as the mpox/measles assets. `label_col`, when given, tags
    each output row with the source member label (disease_label).
    """
    logger = get_dagster_logger()
    if df is None or len(df) == 0:
        return pd.DataFrame(), pd.DataFrame()

    d = df.copy()
    d['date'] = pd.to_datetime(d['date'], errors='coerce')
    d = d.dropna(subset=['date'])
    if d.empty:
        return pd.DataFrame(), pd.DataFrame()

    current_year = str(datetime.now().year)
    states = d['states'] if 'states' in d.columns else pd.Series('Unknown', index=d.index)
    juris = _camel_jurisdiction(states)
    year_i = pd.to_numeric(d['year'], errors='coerce')
    week_i = pd.to_numeric(d['week'], errors='coerce')
    date_start = d['date'].dt.strftime('%Y-%m-%d')

    # ---- basic epidemiology ----
    basic = pd.DataFrame({
        'Jurisdiction': juris.to_numpy(),
        'date_week_start': date_start.to_numpy(),
        'date_week_end': (d['date'] + pd.Timedelta(days=6)).dt.strftime('%Y-%m-%d').to_numpy(),
        'Week_Number': week_i.to_numpy(),
        'Year': year_i.to_numpy(),
        'Cases': pd.to_numeric(d.get('adjusted_week'), errors='coerce').fillna(0).clip(lower=0).to_numpy(),
        'Week_Type': d['Week_Type'].astype(str).to_numpy(),
        'disease': disease_name,
    })
    if label_col and label_col in d.columns:
        basic['disease_label'] = d[label_col].astype(str).to_numpy()
    basic = basic.dropna(subset=['Week_Number', 'Year'])
    basic = basic[(basic['Week_Number'].between(1, 53)) & (basic['Year'].between(2000, 2100))]
    basic = basic[basic['Week_Type'].str.len() > 0]
    if not basic.empty:
        basic['Week_Number'] = basic['Week_Number'].astype(int)
        basic['Year'] = basic['Year'].astype(int)
        basic['Cases'] = basic['Cases'].astype(float)
        basic['Week_Year'] = basic['Week_Number'].astype(str) + '-' + basic['Year'].astype(str)
        core = ['Jurisdiction', 'date_week_start', 'date_week_end', 'Week_Number',
                'Year', 'Week_Year', 'Cases', 'Week_Type']
        try:
            BasicEpidemiologySchema.validate(basic[core])
        except EpidemiologyValidationError as e:
            logger.warning(f"Basic schema validation failed for {disease_name}: {e}")
        basic = basic[core + [c for c in ('disease', 'disease_label') if c in basic.columns]]

    # ---- statistical extension (long form) ----
    base = pd.DataFrame({
        'Jurisdiction': juris.to_numpy(),
        'date': date_start.to_numpy(),
        'cdc_week': d['week'].to_numpy(),
        'cdc_year': d['year'].to_numpy(),
        '_year_i': year_i.to_numpy(),
        'original_state': states.to_numpy(),
        'original_location': (d['location1'] if 'location1' in d.columns
                              else pd.Series('Unknown', index=d.index)).to_numpy(),
    })
    if label_col and label_col in d.columns:
        base['disease_label'] = d[label_col].astype(str).to_numpy()

    frames = []
    for metric_cat, src, rule in _STAT_METRIC_MAP:
        if src not in d.columns:
            continue
        part = base.copy()
        part['count'] = pd.to_numeric(d[src], errors='coerce').to_numpy()
        part['metric_category'] = metric_cat
        part['_rule'] = rule
        frames.append(part)

    if frames:
        stat = pd.concat(frames, ignore_index=True)
        stat = stat[stat['count'].notna() & (stat['count'] >= 0)].copy()
        stat['metric'] = 'cases'
        stat['disease'] = disease_name
        stat['count'] = stat['count'].astype(float)
        is_cur = stat['_year_i'].astype('Int64').astype(str) == current_year
        adj = stat['_rule'] == 'adjusted'
        stat['observation_type'] = 'reported'
        stat.loc[adj & is_cur, 'observation_type'] = 'preliminary estimate'
        stat.loc[adj & ~is_cur, 'observation_type'] = 'corrected'
        stat = stat.drop(columns=['_rule', '_year_i'])
        try:
            StatisticalExtensionSchema.validate(stat)
        except EpidemiologyValidationError as e:
            logger.warning(f"Statistical schema validation failed for {disease_name}: {e}")
    else:
        stat = pd.DataFrame()

    return basic, stat


def _disease_slug(disease):
    slug = re.sub(r'[^a-z0-9]+', '_', str(disease).lower()).strip('_')
    return slug or 'unknown'


def _read_basis_df(s3_resource):
    """Read the latest nndss_all_basis from S3 into a DataFrame.

    Reads the parquet copy (columnar, ~25x smaller than the CSV) to keep the
    1.5M-row basis memory footprint low downstream.
    """
    key = f"{store_assets.get_latest_basepath()}/{s3_latest_basis}/nndss_all_basis.parquet"
    raw = s3_resource.getFile(key)
    return pd.read_parquet(io.BytesIO(raw))


def _read_label_mapping(s3_resource):
    """Read the label->diseases mapping JSON from S3."""
    raw = s3_resource.getFile(f"{s3_config_path}/label_mapping.json")
    return json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)


def _read_all_weekly_partitions(s3_resource):
    """Read and concatenate the raw nndss_weekly partition CSVs already stored in S3.

    Uses the base (non-_states_) weekly files, which retain the raw m1-m4 columns
    so the basis can be re-cleaned across all weeks at once.
    """
    logger = get_dagster_logger()
    prefix = f'{s3_output_path}/raw/nndss_weekly/'
    frames = []
    files = []
    for obj in s3_resource.listPath(path=prefix):
        name = obj.object_name
        if name.endswith('.csv') and '/nndss_weekly_' in name and '_states_' not in name:
            files.append(name)
    for name in sorted(files):
        raw = s3_resource.getFile(name)
        frames.append(pd.read_csv(io.BytesIO(raw) if isinstance(raw, bytes)
                                  else io.StringIO(raw.decode('utf-8'))))
    logger.info(f"Read {len(frames)} nndss_weekly partition files from {prefix}")
    return pd.concat(frames, ignore_index=True) if frames else None


@asset(group_name="pathogens", key_prefix="cdc",
       name="nndss_all_basis", required_resource_keys={"s3"},
       deps=[AssetKey(["cdc", "nndss_weekly"])])
def nndss_all_basis(context):
    """Aggregated, cleaned CDC NNDSS weekly data across ALL labels.

    Rather than re-pulling per disease, this reuses the raw data already fetched
    by the weekly-partitioned ``cdc/nndss_weekly`` asset: it reads every stored
    weekly partition, concatenates them so each (label, location1) group spans
    all weeks/years, then applies the same mpox-style correction (prior years use
    corrected m4, current year uses preliminary m3, with negative-diff smoothing).
    This file is the validated basis for subsetting. Completeness follows whatever
    ``cdc/nndss_weekly`` partitions have been materialized.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    raw_df = _read_all_weekly_partitions(s3_resource)
    if raw_df is None or raw_df.empty:
        raise Exception("No nndss_weekly partition files found — materialize "
                        "cdc/nndss_weekly first")

    # Keep the raw source columns and rebuild the cleaned metrics from scratch
    keep = ['states', 'year', 'week', 'label', 'm1', 'm2', 'm3', 'm4',
            'location1', 'location2', 'lat', 'lon']
    raw_df = raw_df[[c for c in keep if c in raw_df.columns]].copy()

    raw_df.rename(columns={"m1": "current_week",
                           "m2": "previous_52_weeks__max",
                           "m3": "current_YTD__cummulative",
                           "m4": "previous_YTD__cummulative"}, inplace=True)
    for col in ['current_week', 'previous_52_weeks__max',
                'current_YTD__cummulative', 'previous_YTD__cummulative']:
        raw_df[col] = pd.to_numeric(raw_df[col], errors='coerce').fillna(0)

    raw_df['date'] = raw_df.apply(
        lambda row: Week(int(row['year']), int(row['week'])).startdate(), axis=1)
    raw_df['date'] = pd.to_datetime(raw_df['date'])

    raw_df["key"] = (raw_df["label"].astype(str) + '_' + raw_df["year"].astype(str)
                     + '_' + raw_df["week"].astype(str) + '_' + raw_df["location1"].astype(str))
    raw_df.dropna(inplace=True, subset=['key'])
    raw_df.drop_duplicates(subset=['key'], inplace=True)

    basis_df = calculate_correct_count(raw_df, cumulative_col='current_YTD__cummulative',
                                       previous_cumulative_col='previous_YTD__cummulative')
    basis_df = pd.DataFrame(basis_df)
    logger.info(f"Storing NNDSS basis: {len(basis_df)} rows, "
                f"{basis_df['label'].nunique()} distinct labels")

    metadata = store_assets.objectMetadata(
        name="nndss_all_basis",
        description=("Aggregated cleaned CDC NNDSS weekly data across all labels, built "
                     "from the stored nndss_weekly partitions. Prior years use corrected "
                     "cumulative (m4); current year uses preliminary (m3), with "
                     "negative-diff smoothing."),
        source_url=CDC_SOURCE_URL,
    )
    store_assets.store_dataframe_to_s3(
        basis_df, s3_basis_path, "nndss_all_basis", s3_resource,
        metadata=metadata, formats=['csv', 'parquet'],
        enable_latest_path=True, latestdatasetpath=s3_latest_basis)


@asset(group_name="pathogens", key_prefix="cdc",
       name="nndss_label_mapping", required_resource_keys={"s3"},
       deps=[AssetKey(["cdc", "nndss_all_basis"])])
def nndss_label_mapping(context):
    """Discover distinct NNDSS labels and cluster them into core diseases.

    Emits a validated disease->labels mapping (JSON + CSV catalog) that drives
    per-disease subsetting. An optional overrides file at
    pathogens/cdc/nndss/config/label_overrides.json is applied on top.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    basis = _read_basis_df(s3_resource)
    labels = sorted(basis['label'].dropna().unique())

    overrides = {}
    try:
        raw = s3_resource.getFile(LABEL_OVERRIDES_KEY)
        overrides = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
        logger.info(f"Applied {len(overrides)} label overrides")
    except Exception:
        logger.info("No label overrides file found; using pure heuristic")

    mapping = cluster_labels_to_diseases(labels, overrides=overrides)
    logger.info(f"Clustered {len(labels)} labels into {len(mapping)} core diseases")

    # JSON mapping (canonical, read by subsets + coverage check)
    store_assets.text_to_s3(json.dumps(mapping, indent=2),
                            f"{s3_config_path}/label_mapping.json",
                            s3_resource, contenttype='application/json')

    # Human-readable CSV catalog with per-cluster counts
    catalog_rows = []
    for disease, members in mapping.items():
        for m in members:
            catalog_rows.append({
                'disease': disease,
                'label': m['label'],
                'is_rollup': m['is_rollup'],
                'label_count': len(members),
            })
    catalog = pd.DataFrame(catalog_rows).sort_values(['disease', 'label'])
    metadata = store_assets.objectMetadata(
        name="nndss_label_mapping",
        description="Mapping of NNDSS labels to core diseases used for subsetting",
        source_url=CDC_SOURCE_URL,
    )
    # Distinct basename so the catalog .json does not clobber label_mapping.json
    store_assets.dataframe_to_s3(catalog, f"{s3_config_path}/label_mapping_catalog",
                                 s3_resource, metadata=metadata, formats=['csv', 'json'])


@asset(group_name="pathogens", key_prefix="cdc",
       name="nndss_disease_subsets", required_resource_keys={"s3"},
       deps=[AssetKey(["cdc", "nndss_all_basis"]),
             AssetKey(["cdc", "nndss_label_mapping"])])
def nndss_disease_subsets(context):
    """Per-disease resilient-epi outputs driven by the label mapping.

    For each disease writes BOTH a combined per-disease total (summed leaf
    labels) and the per-label breakdown, each in basic + statistical schemas.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    basis = _read_basis_df(s3_resource)
    mapping = _read_label_mapping(s3_resource)

    for disease, members in mapping.items():
        member_labels = [m['label'] for m in members]
        leaf_labels = [m['label'] for m in members if not m['is_rollup']]
        slug = _disease_slug(disease)

        disease_rows = basis[basis['label'].isin(member_labels)]
        if disease_rows.empty:
            logger.warning(f"No basis rows for disease '{disease}'; skipping")
            continue
        logger.info(f"Disease '{disease}': {len(member_labels)} labels, "
                    f"{len(disease_rows)} rows")

        base_out = f"{s3_diseases_path}/{slug}"
        latest_out = f"{s3_latest_diseases}/{slug}"

        # --- per-label breakdown ---
        bd_basic, bd_stat = records_to_resilient_epi(disease_rows, disease, label_col='label')
        if not bd_basic.empty:
            md = store_assets.objectMetadata(
                name=f"{slug}_bylabel_basic",
                description=f"{disease} weekly cases by NNDSS label (basic epidemiology)",
                source_url=CDC_SOURCE_URL)
            store_assets.store_dataframe_to_s3(
                bd_basic, base_out, f"{slug}_bylabel_basic", s3_resource,
                metadata=md, formats=['csv', 'parquet'],
                enable_latest_path=True, latestdatasetpath=latest_out)
        if not bd_stat.empty:
            md = store_assets.objectMetadata(
                name=f"{slug}_bylabel_statistical",
                description=f"{disease} weekly metrics by NNDSS label (statistical extension)",
                source_url=CDC_SOURCE_URL)
            store_assets.store_dataframe_to_s3(
                bd_stat, base_out, f"{slug}_bylabel_statistical", s3_resource,
                metadata=md, formats=['parquet'],
                enable_latest_path=False, latestdatasetpath=latest_out)

        # --- combined disease total (leaf labels only, avoids double-counting) ---
        leaf_rows = basis[basis['label'].isin(leaf_labels)]
        if leaf_rows.empty:
            continue
        combined = combine_disease_total(leaf_rows, disease)
        cb_basic, cb_stat = records_to_resilient_epi(combined, disease, label_col=None)
        if not cb_basic.empty:
            md = store_assets.objectMetadata(
                name=f"{slug}_combined_basic",
                description=f"{disease} combined weekly cases across labels (basic epidemiology)",
                source_url=CDC_SOURCE_URL)
            store_assets.store_dataframe_to_s3(
                cb_basic, base_out, f"{slug}_combined_basic", s3_resource,
                metadata=md, formats=['csv', 'parquet'],
                enable_latest_path=True, latestdatasetpath=latest_out)
        if not cb_stat.empty:
            md = store_assets.objectMetadata(
                name=f"{slug}_combined_statistical",
                description=f"{disease} combined weekly metrics across labels (statistical extension)",
                source_url=CDC_SOURCE_URL)
            store_assets.store_dataframe_to_s3(
                cb_stat, base_out, f"{slug}_combined_statistical", s3_resource,
                metadata=md, formats=['parquet'],
                enable_latest_path=False, latestdatasetpath=latest_out)


@asset_check(asset=AssetKey(["cdc", "nndss_label_mapping"]),
             description="Every basis label maps to exactly one disease (no orphans, no duplicates)",
             required_resource_keys={"s3"})
def check_nndss_label_mapping_coverage(context):
    """Validate the label mapping fully and uniquely covers the basis labels."""
    s3_resource = context.resources.s3
    try:
        basis = _read_basis_df(s3_resource)
        mapping = _read_label_mapping(s3_resource)

        label_to_diseases = {}
        for disease, members in mapping.items():
            for m in members:
                label_to_diseases.setdefault(m['label'], []).append(disease)

        basis_labels = set(basis['label'].dropna().unique())
        orphans = sorted(basis_labels - set(label_to_diseases))
        duplicates = {lbl: ds for lbl, ds in label_to_diseases.items() if len(ds) > 1}

        passed = not orphans and not duplicates
        return AssetCheckResult(
            passed=passed,
            severity=AssetCheckSeverity.ERROR if not passed else AssetCheckSeverity.WARN,
            description=(f"{len(orphans)} orphan label(s), {len(duplicates)} duplicated label(s)"
                        if not passed else
                        f"All {len(basis_labels)} labels mapped to exactly one disease"),
            metadata={
                "distinct_labels": len(basis_labels),
                "diseases": len(mapping),
                "orphan_examples": orphans[:10],
                "duplicate_examples": {k: v for k, v in list(duplicates.items())[:10]},
            },
        )
    except Exception as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR,
            description=f"Check failed with error: {e}")


@asset_check(asset=AssetKey(["cdc", "nndss_all_basis"]),
             description="Adjusted count columns contain no negative values",
             required_resource_keys={"s3"})
def check_nndss_all_basis_no_negative_counts(context):
    """Validate adjusted_week / adjusted_YTD__cummulative are non-negative after smoothing."""
    s3_resource = context.resources.s3
    try:
        df = _read_basis_df(s3_resource)
        count_columns = ['adjusted_week', 'adjusted_YTD__cummulative']
        findings = {}
        for col in count_columns:
            if col in df.columns:
                neg = int((df[col] < 0).sum())
                if neg > 0:
                    findings[col] = neg
        if findings:
            return AssetCheckResult(
                passed=False, severity=AssetCheckSeverity.ERROR,
                description=f"Negative values found: {findings}",
                metadata={"negative_counts_by_column": findings, "rows_checked": len(df)})
        return AssetCheckResult(
            passed=True,
            description=f"No negative counts across {len(df)} rows",
            metadata={"rows_checked": len(df), "columns_checked": count_columns})
    except Exception as e:
        return AssetCheckResult(
            passed=False, severity=AssetCheckSeverity.ERROR,
            description=f"Check failed with error: {e}")


# schedules and jobs
nndss_all_job = define_asset_job(
    "nndss_all",
    selection=[AssetKey(["cdc", "nndss_all_basis"]),
               AssetKey(["cdc", "nndss_label_mapping"]),
               AssetKey(["cdc", "nndss_disease_subsets"])],
)


@schedule(job=nndss_all_job, cron_schedule="@weekly", name="nndss_all_job")
def nndss_all_schedule(context):
    return RunRequest()
