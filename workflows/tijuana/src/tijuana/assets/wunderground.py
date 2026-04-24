import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from dagster import (
    asset,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    RunRequest,
    schedule,
    AutomationCondition,
    TimeWindowPartitionsDefinition,
)
from resilient_core.utils import store_assets
from .openmeteo import site_name_to_slug

WU_API_KEY = os.environ.get('WUNDERGROUND_API_KEY')
WU_BASE_URL = 'https://api.weather.com/v2/pws/history/hourly'

s3_output_path = 'tijuana/weather_wu'

# Station-to-sensor mapping
STATION_SENSOR_MAP = {
    'KCASANYS3': ['SAN YSIDRO'],
    'KCAIMPER28': ['NESTOR - BES'],
    'KCAIMPER32': ['IB CIVIC CTR'],
}

STATIONS = list(STATION_SENSOR_MAP.keys())

# WU metric field -> Open-Meteo column name
WU_COLUMN_MAP = {
    'tempAvg': 'temperature_2m',
    'windspeedAvg': 'wind_speed_10m',
    'windgustAvg': 'wind_gusts_10m',
    'winddirAvg': 'wind_direction_10m',
    'humidityAvg': 'relative_humidity_2m',
    'pressureAvg': 'surface_pressure',
    'precipTotal': 'precipitation',
    'dewptAvg': 'dewpoint_2m',
}

# WU wind fields that need km/h -> m/s conversion
WIND_KMH_FIELDS = {'windspeedAvg', 'windgustAvg'}

start_date_wu = datetime(2023, 1, 1)
yearly_partitions_wu = TimeWindowPartitionsDefinition(
    start=start_date_wu, fmt='%Y', cron_schedule="@yearly"
)


@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def fetch_wu_day(station_id: str, date_str: str, api_key: str) -> pd.DataFrame:
    """Fetch one day of hourly observations from a WU PWS.

    Parameters
    ----------
    station_id : str
        Weather Underground station ID (e.g. 'KCASANYS3').
    date_str : str
        Date in YYYYMMDD format.
    api_key : str
        Weather Underground / Weather Company API key.

    Returns
    -------
    pd.DataFrame with columns matching Open-Meteo schema, or empty DataFrame on no data.
    """
    params = {
        'stationId': station_id,
        'format': 'json',
        'units': 'm',
        'date': date_str,
        'apiKey': api_key,
    }
    resp = requests.get(WU_BASE_URL, params=params, timeout=30)
    resp.raise_for_status()

    data = resp.json()
    observations = data.get('observations', [])
    if not observations:
        return pd.DataFrame()

    rows = []
    for obs in observations:
        metric = obs.get('metric', {})
        row = {'date': pd.to_datetime(obs['obsTimeUtc'], utc=True)}
        for wu_field, om_col in WU_COLUMN_MAP.items():
            value = metric.get(wu_field)
            if value is not None and wu_field in WIND_KMH_FIELDS:
                value = value / 3.6  # km/h -> m/s
            row[om_col] = value
        row['wu_station_id'] = station_id
        rows.append(row)

    df = pd.DataFrame(rows)
    # WU doesn't provide these; will be filled by Open-Meteo fallback
    df['cloud_cover'] = pd.NA
    df['visibility'] = pd.NA
    return df


def expand_station_to_sites(station_df: pd.DataFrame, station_id: str) -> pd.DataFrame:
    """Duplicate rows for each sensor site mapped to this station."""
    frames = []
    for site_name in STATION_SENSOR_MAP[station_id]:
        site_df = station_df.copy()
        site_df['site_name'] = site_name
        frames.append(site_df)
    return pd.concat(frames, ignore_index=True)


def fetch_date_range(start: str, end: str, logger) -> pd.DataFrame:
    """Fetch WU data for all stations across a date range.

    Parameters
    ----------
    start, end : str
        Dates in YYYY-MM-DD format.
    logger : dagster logger
    """
    if not WU_API_KEY:
        logger.warning("WUNDERGROUND_API_KEY not set, skipping WU weather fetch")
        return pd.DataFrame()

    start_dt = datetime.strptime(start, '%Y-%m-%d')
    end_dt = datetime.strptime(end, '%Y-%m-%d')

    all_frames = []
    current = start_dt
    while current <= end_dt:
        date_str = current.strftime('%Y%m%d')
        for station_id in STATIONS:
            try:
                day_df = fetch_wu_day(station_id, date_str, WU_API_KEY)
                if not day_df.empty:
                    expanded = expand_station_to_sites(day_df, station_id)
                    all_frames.append(expanded)
            except Exception as e:
                logger.warning(f"Failed to fetch {station_id} for {date_str}: {e}")
            time.sleep(1)  # rate limit
        current += timedelta(days=1)

    if not all_frames:
        logger.warning(f"No WU data fetched for {start} to {end}")
        return pd.DataFrame()

    return pd.concat(all_frames, ignore_index=True)


@asset(
    group_name="tijuana",
    key_prefix="weather_wu",
    name="wu_historical",
    required_resource_keys={"s3"},
    partitions_def=yearly_partitions_wu,
    metadata={
        "source": "https://api.weather.com/v2/pws/history/hourly",
        "description": "Historical Weather Underground PWS data for H2S sites",
    },
)
def wu_historical(context):
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    year = context.asset_partition_key_for_output()
    start = f'{year}-01-01'
    end = f'{year}-12-31'
    today = datetime.today().strftime('%Y-%m-%d')
    if end > today:
        end = today

    logger.info(f"Fetching WU historical data for {start} to {end}")

    combined_df = fetch_date_range(start, end, logger)
    if combined_df.empty:
        return combined_df

    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=f"wu_historical_{year}",
        description=meta["description"],
        source_url=meta.get("source"),
    )

    # Store per-site
    for site_name in combined_df['site_name'].unique():
        site_df = combined_df[combined_df['site_name'] == site_name]
        site_slug = site_name_to_slug(site_name)
        store_assets.store_dataframe_to_s3(
            site_df,
            f'{s3_output_path}/raw/yearly/{site_slug}/',
            str(year),
            s3_resource,
            metadata=metadata,
            enable_latest_path=True,
            latestdatasetpath=f'{s3_output_path}/{site_slug}',
            formats=['csv', 'parquet'],
        )

    # Store combined
    store_assets.store_dataframe_to_s3(
        combined_df,
        f'{s3_output_path}/raw/yearly/all/',
        str(year),
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=s3_output_path,
        formats=['csv', 'parquet'],
    )

    logger.info(f"Stored {len(combined_df)} WU records for {year}")
    return combined_df


@asset(
    group_name="tijuana",
    key_prefix="weather_wu",
    name="wu_current_year",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": "https://api.weather.com/v2/pws/history/hourly",
        "description": "Current-year Weather Underground PWS data, refreshed daily for all H2S sites",
    },
)
def wu_current_year(context):
    """Fetches WU PWS data from Jan 1 of current year through yesterday."""
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    year = datetime.today().strftime('%Y')
    start = f'{year}-01-01'
    # WU history is available up to yesterday
    end = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    logger.info(f"Fetching WU current-year data {start} to {end}")

    combined_df = fetch_date_range(start, end, logger)
    if combined_df.empty:
        return combined_df

    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=f"wu_current_year_{year}",
        description=meta["description"],
        source_url=meta.get("source"),
    )

    # Store per-site
    for site_name in combined_df['site_name'].unique():
        site_df = combined_df[combined_df['site_name'] == site_name]
        site_slug = site_name_to_slug(site_name)
        store_assets.store_dataframe_to_s3(
            site_df,
            f'{s3_output_path}/raw/yearly/{site_slug}/',
            f'{year}_current',
            s3_resource,
            metadata=metadata,
            enable_latest_path=True,
            latestdatasetpath=f'{s3_output_path}/{site_slug}',
            formats=['csv', 'parquet'],
        )

    # Store combined
    store_assets.store_dataframe_to_s3(
        combined_df,
        f'{s3_output_path}/raw/yearly/all/',
        f'{year}_current',
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=s3_output_path,
        formats=['csv', 'parquet'],
    )

    logger.info(f"Stored {len(combined_df)} WU current-year records")
    return combined_df


wu_weather_job = define_asset_job(
    "wu_weather_all",
    selection=[AssetKey(["weather_wu", "wu_current_year"])],
)


@schedule(job=wu_weather_job, cron_schedule="0 6 * * *", name="wu_weather_daily")
def wu_weather_daily_schedule(context):
    return RunRequest()
