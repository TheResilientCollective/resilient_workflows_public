from datetime import datetime

import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry

from dagster import ( asset,
                     get_dagster_logger,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      TimeWindowPartitionsDefinition
                      )

from ..resources import minio

s3_output_path = 'tijuana/weather/'
@asset(group_name="tijuana",key_prefix="weather",
       name="openmeteo_forecast", required_resource_keys={"s3", "airtable"}
  )
def forecast(context):
    s3_resource = context.resources.s3
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 32.552794,
        "longitude": -117.047286,
        "hourly": ["temperature_2m", "wind_speed_10m",
                   "wind_direction_10m", "precipitation",
                   "relative_humidity_2m",
                   'surface_pressure',
                                   'cloud_cover',
                   'visibility',
                   'dewpoint_2m'
                   ],
        "past_days": 30
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    get_dagster_logger().info(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    get_dagster_logger().info(f"Elevation {response.Elevation()} m asl")
    get_dagster_logger().info(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    get_dagster_logger().info(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(2).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(4).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(6).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(7).ValuesAsNumpy()
    hourly_visibility = hourly.Variables(8).ValuesAsNumpy()
    hourly_dewpoint_2m = hourly.Variables(9).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
       inclusive="left"  # pandas > 2
       #  closed="left", # pandas > 2
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

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    hourly_csv = hourly_dataframe.to_csv(index=False)
    filename = f'{s3_output_path}raw/forecast.csv'
    s3_resource.putFile_text(data=hourly_csv, path=filename)
    return hourly_dataframe

# Define a yearly partition
start_date_closures=datetime(2015,1,1)
yearly_partitions= TimeWindowPartitionsDefinition(start=start_date_closures,fmt='%Y',
cron_schedule = "@monthly"
)

@asset(group_name="tijuana", key_prefix="weather",
       name="openmeteo_historical", required_resource_keys={"s3", "airtable"},
       partitions_def=yearly_partitions # Add partitions_def here
       )
def weather_historical(context):
     s3_resource = context.resources.s3
     year = context.asset_partition_key_for_output()
     get_dagster_logger().info(f'year: {year}')
     start = f'{year}-01-01'
     end = f'{year}-12-31'
     today = datetime.today().strftime('%Y-%m-%d')
     if end > today:
         end = today
     # Setup the Open-Meteo API client with cache and retry on error
     cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
     retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
     openmeteo = openmeteo_requests.Client(session=retry_session)
     url = "https://archive-api.open-meteo.com/v1/archive"
     params = {
         "latitude": 52.52,
         "longitude": 13.41,
         "start_date": start,
         "end_date": end,
         "hourly": ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "precipitation",
                    "relative_humidity_2m",
                    'surface_pressure',
            'cloud_cover',
            'visibility',
            'dewpoint_2m'
                    ]
     }
     responses = openmeteo.weather_api(url, params=params)

     # Process first location. Add a for-loop for multiple locations or weather models
     response = responses[0]
     get_dagster_logger().info(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
     get_dagster_logger().info(f"Elevation {response.Elevation()} m asl")
     get_dagster_logger().info(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
     get_dagster_logger().info(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

     # Process hourly data. The order of variables needs to be the same as requested.
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
         inclusive="left"  # pandas > 2
         #  closed="left", # pandas > 2
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


     hourly_dataframe = pd.DataFrame(data=hourly_data)

     hourly_csv = hourly_dataframe.to_csv(index=False)
     filename = f'{s3_output_path}raw/{year}.csv'
     s3_resource.putFile_text(data=hourly_csv, path=filename)
     return hourly_dataframe


weather_all_job = define_asset_job(
    "weather_all", selection=[AssetKey(["weather", "openmeteo_forecast"]), ]
)

# daily but at 3 am
@schedule(job=weather_all_job, cron_schedule="@hourly", name="weather_all")
def weather_all_schedule(context):

    return RunRequest(
    )
