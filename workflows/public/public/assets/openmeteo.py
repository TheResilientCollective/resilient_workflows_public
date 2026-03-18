from datetime import datetime
from io import StringIO
import re

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
from tenacity import retry as tenacity_retry, wait_fixed, stop_after_attempt, retry_if_exception_message

from dagster import (asset,
                     get_dagster_logger,
                     define_asset_job, AssetKey,
                     RunRequest,
                     schedule,
                     TimeWindowPartitionsDefinition, AssetIn, AutomationCondition
                     )

from ..resources import minio
from ..utils import store_assets
from .sd_apcd import SITES_CSV

s3_output_path = 'tijuana/weather'


def site_name_to_slug(site_name: str) -> str:
    slug = site_name.strip().lower()
    slug = re.sub(r'[^a-z0-9]+', '_', slug)
    return slug.strip('_')


def load_sites() -> pd.DataFrame:
    df = pd.read_csv(StringIO(SITES_CSV), sep=',', on_bad_lines='warn')
    df = df.rename(columns={'SiteName': 'site_name', 'Latitude': 'lat', 'Longitude': 'lon'})
    df['site_name'] = df['site_name'].str.strip()
    df['lat'] = df['lat'].astype(float)
    df['lon'] = df['lon'].astype(float)
    return df


@asset(group_name="tijuana", key_prefix="weather",
       name="openmeteo_forecast", required_resource_keys={"s3", "airtable"},
       metadata={
           "source": "https://api.open-meteo.com/v1/forecast",
           "description": "Recent Weather Forecast",
       })
def forecast(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description,
                                           source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3

    locations = load_sites()
    locations = locations[locations['site_name'] != 'EL CAJON LES']

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"

    # List to store all forecast dataframes
    all_forecasts = []

    # Loop over each location in the locations dataframe
    for index, location in locations.iterrows():
        site_name = location['site_name']
        lat = location['lat']
        lon = location['lon']

        get_dagster_logger().info(f"Processing forecast for site: {site_name} at {lat}, {lon}")

        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": ["temperature_2m", "wind_speed_10m",
                       "wind_direction_10m", "precipitation",
                       "relative_humidity_2m", 'surface_pressure',
                       'cloud_cover',
                       'visibility',
                       'dewpoint_2m'
                       ],
            "past_days": 30
        }

        responses = openmeteo.weather_api(url, params=params)

        response = responses[0]
        get_dagster_logger().info(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        get_dagster_logger().info(f"Elevation {response.Elevation()} m asl")
        get_dagster_logger().info(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
        get_dagster_logger().info(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(2).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(4).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(5).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(6).ValuesAsNumpy()
        hourly_visibility = hourly.Variables(7).ValuesAsNumpy()
        hourly_dewpoint_2m = hourly.Variables(8).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["visibility"] = hourly_visibility
        hourly_data["dewpoint_2m"] = hourly_dewpoint_2m
        hourly_data["site_name"] = site_name

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        all_forecasts.append(hourly_dataframe)

        # Per-site storage
        site_slug = site_name_to_slug(site_name)
        store_assets.store_dataframe_to_s3(
            hourly_dataframe, f'{s3_output_path}/raw/forecast/{site_slug}/', 'forecast', s3_resource,
            metadata=metadata, enable_latest_path=True,
            latestdatasetpath=f"{s3_output_path}_forecast/{site_slug}",
            formats=['csv', 'parquet'])

    # Combine all forecasts and store combined (all sites) to canonical path
    combined_dataframe = pd.concat(all_forecasts, ignore_index=True)
    store_assets.store_dataframe_to_s3(combined_dataframe, f'{s3_output_path}/raw/forecast/all/', 'forecast', s3_resource,
                                       metadata=metadata, enable_latest_path=True,
                                       latestdatasetpath=f"{s3_output_path}_forecast",
                                       formats=['csv', 'parquet'])
    return combined_dataframe


# Define a yearly partition
start_date_closures = datetime(2015, 1, 1)
yearly_partitions = TimeWindowPartitionsDefinition(start=start_date_closures, fmt='%Y',
                                                   cron_schedule="@yearly"
                                                   )


@asset(group_name="tijuana", key_prefix="weather",
       name="openmeteo_historical", required_resource_keys={"s3", "airtable"},
       partitions_def=yearly_partitions,
       metadata={
           "source": "https://api.open-meteo.com/v1/archive",
           "description": "Historical Weather Data",
       })
def weather_historical(context):
    s3_resource = context.resources.s3
    year = context.asset_partition_key_for_output()
    get_dagster_logger().info(f'year: {year}')
    start = f'{year}-01-01'
    end = f'{year}-12-31'
    today = datetime.today().strftime('%Y-%m-%d')
    if end > today:
        end = today

    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"

    locations = load_sites()
    locations = locations[locations['site_name'] != 'EL CAJON LES']

    all_sites = []

    for index, location in locations.iterrows():
        site_name = location['site_name']
        lat = location['lat']
        lon = location['lon']
        site_slug = site_name_to_slug(site_name)

        get_dagster_logger().info(f"Processing historical for site: {site_name} at {lat}, {lon}")

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "hourly": ["temperature_2m", "wind_speed_10m",
                       "wind_direction_10m", "wind_gusts_10m",
                       "precipitation", "relative_humidity_2m",
                       'surface_pressure', 'cloud_cover',
                       'visibility', 'dewpoint_2m'
                       ]
        }

        @tenacity_retry(
            retry=retry_if_exception_message(match=".*request limit exceeded.*"),
            wait=wait_fixed(65),
            stop=stop_after_attempt(3),
            reraise=True
        )
        def fetch():
            return openmeteo.weather_api(url, params=params)

        responses = fetch()

        response = responses[0]
        get_dagster_logger().info(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        get_dagster_logger().info(f"Elevation {response.Elevation()} m asl")
        get_dagster_logger().info(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
        get_dagster_logger().info(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(2).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(3).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(5).ValuesAsNumpy()
        hourly_surface_pressure = hourly.Variables(6).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(7).ValuesAsNumpy()
        hourly_visibility = hourly.Variables(8).ValuesAsNumpy()
        hourly_dewpoint_2m = hourly.Variables(9).ValuesAsNumpy()

        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        hourly_data["temperature_2m"] = hourly_temperature_2m
        hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
        hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
        hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m
        hourly_data["precipitation"] = hourly_precipitation
        hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
        hourly_data["surface_pressure"] = hourly_surface_pressure
        hourly_data["cloud_cover"] = hourly_cloud_cover
        hourly_data["visibility"] = hourly_visibility
        hourly_data["dewpoint_2m"] = hourly_dewpoint_2m
        hourly_data["site_name"] = site_name

        hourly_dataframe = pd.DataFrame(data=hourly_data)
        all_sites.append(hourly_dataframe)

        name = str(context.asset_key.path[-1]) + str(year) + '_' + site_slug  # metadata name keeps slug for clarity
        site_metadata = store_assets.objectMetadata(name=name, description=description,
                                                    source_url=source_url, variableMeasured=variableMeasured)
        filepath = f'{s3_output_path}/raw/yearly/{site_slug}/'
        store_assets.store_dataframe_to_s3(
            hourly_dataframe, filepath, str(year), s3_resource,
            metadata=site_metadata, enable_latest_path=True,
            latestdatasetpath=f'{s3_output_path}/{site_slug}',
            formats=['csv', 'parquet'])

    # Concatenate all sites and store combined to canonical path for hysplit
    combined_dataframe = pd.concat(all_sites, ignore_index=True)
    name = str(context.asset_key.path[-1]) + str(year)
    combined_metadata = store_assets.objectMetadata(name=name, description=description,
                                                    source_url=source_url, variableMeasured=variableMeasured)
    filepath = f'{s3_output_path}/raw/yearly/all/'
    store_assets.store_dataframe_to_s3(combined_dataframe, filepath, str(year), s3_resource,
                                       metadata=combined_metadata, enable_latest_path=True,
                                       latestdatasetpath=s3_output_path,
                                       formats=['csv', 'parquet'])
    return combined_dataframe


@asset(group_name="tijuana", key_prefix="weather",
       name="openmeteo_current_year", required_resource_keys={"s3", "airtable"},
       automation_condition=AutomationCondition.eager(),
       metadata={
           "source": "https://archive-api.open-meteo.com/v1/archive",
           "description": "Current-year historical weather data, refreshed daily for all H2S sites",
       })
def weather_current_year(context):
    """Fetches weather from Jan 1 of the current year through today for all H2S sites.
    Runs daily so hysplit always has up-to-date data without waiting for the yearly partition."""
    s3_resource = context.resources.s3
    year = datetime.today().strftime('%Y')
    start = f'{year}-01-01'
    end = datetime.today().strftime('%Y-%m-%d')
    get_dagster_logger().info(f'Fetching current-year weather {start} → {end}')

    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")

    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    url = "https://archive-api.open-meteo.com/v1/archive"

    locations = load_sites()
    locations = locations[locations['site_name'] != 'EL CAJON LES']

    all_sites = []

    for _, location in locations.iterrows():
        site_name = location['site_name']
        lat = location['lat']
        lon = location['lon']
        site_slug = site_name_to_slug(site_name)

        get_dagster_logger().info(f"Fetching current-year weather for {site_name} at {lat}, {lon}")

        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start,
            "end_date": end,
            "hourly": ["temperature_2m", "wind_speed_10m",
                       "wind_direction_10m", "wind_gusts_10m",
                       "precipitation", "relative_humidity_2m",
                       'surface_pressure', 'cloud_cover',
                       'visibility', 'dewpoint_2m'
                       ]
        }

        @tenacity_retry(
            retry=retry_if_exception_message(match=".*request limit exceeded.*"),
            wait=wait_fixed(65),
            stop=stop_after_attempt(3),
            reraise=True
        )
        def fetch():
            return openmeteo.weather_api(url, params=params)

        responses = fetch()
        response = responses[0]

        hourly = response.Hourly()
        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}
        hourly_data["temperature_2m"] = hourly.Variables(0).ValuesAsNumpy()
        hourly_data["wind_speed_10m"] = hourly.Variables(1).ValuesAsNumpy()
        hourly_data["wind_direction_10m"] = hourly.Variables(2).ValuesAsNumpy()
        hourly_data["wind_gusts_10m"] = hourly.Variables(3).ValuesAsNumpy()
        hourly_data["precipitation"] = hourly.Variables(4).ValuesAsNumpy()
        hourly_data["relative_humidity_2m"] = hourly.Variables(5).ValuesAsNumpy()
        hourly_data["surface_pressure"] = hourly.Variables(6).ValuesAsNumpy()
        hourly_data["cloud_cover"] = hourly.Variables(7).ValuesAsNumpy()
        hourly_data["visibility"] = hourly.Variables(8).ValuesAsNumpy()
        hourly_data["dewpoint_2m"] = hourly.Variables(9).ValuesAsNumpy()
        hourly_data["site_name"] = site_name

        site_df = pd.DataFrame(data=hourly_data)
        all_sites.append(site_df)

        name = str(context.asset_key.path[-1]) + year + '_' + site_slug
        site_metadata = store_assets.objectMetadata(name=name, description=description,
                                                    source_url=source_url, variableMeasured=variableMeasured)
        store_assets.store_dataframe_to_s3(
            site_df, f'{s3_output_path}/raw/yearly/{site_slug}/', f'{year}_current', s3_resource,
            metadata=site_metadata, enable_latest_path=True,
            latestdatasetpath=f'{s3_output_path}/{site_slug}',
            formats=['csv', 'parquet'])

    combined_df = pd.concat(all_sites, ignore_index=True)
    name = str(context.asset_key.path[-1]) + year
    combined_metadata = store_assets.objectMetadata(name=name, description=description,
                                                    source_url=source_url, variableMeasured=variableMeasured)
    store_assets.store_dataframe_to_s3(
        combined_df, f'{s3_output_path}/raw/yearly/all/', f'{year}_current', s3_resource,
        metadata=combined_metadata, enable_latest_path=True,
        latestdatasetpath=s3_output_path,
        formats=['csv', 'parquet'])
    return combined_df


weather_all_job = define_asset_job(
    "weather_all", selection=[AssetKey(["weather", "openmeteo_forecast"]), ]
)


# daily but at 3 am
@schedule(job=weather_all_job, cron_schedule="@hourly", name="weather_all")
def weather_all_schedule(context):

    return RunRequest(
    )
