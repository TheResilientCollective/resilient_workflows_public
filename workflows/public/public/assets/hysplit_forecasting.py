from datetime import datetime
from io import StringIO
import pandas as pd


from dagster import (asset,
                     get_dagster_logger,
                     define_asset_job, AssetKey,
                     RunRequest,
                     schedule,
                     TimeWindowPartitionsDefinition, AssetIn, AutomationCondition
                     )
import duckdb
from ..resources import minio
from ..utils import store_assets
#from .sd_apcd import s3_output_path as apcd_s3_output_path
from astral import LocationInfo
from astral.sun import sun

OUTPUT_PATH='tijuana/forecast/output/'
LATEST='tijuana/forecast_data'

PARQUET_PATTERN='*.parquet'
CSV_PATTERN='*.csv'

H2S_PATH='latest/tijuana/sd_apcd_air/h2s'
WEATHER_BASE='latest/tijuana/weather'
STREAMFLOW_BASE='latest/tijuana/streamflow'
STREAMFLOW_SITE_YEARLY='boundary_cms'
STREAMFLOW_SITE_RECENT=STREAMFLOW_SITE_YEARLY
TIDAL_BASE='latest/tijuana/tides'


sites_csv = """LongName,site_name,lat,lon,AgencyName
Berry Elementary School,NESTOR - BES, 32.567097, -117.090656,San Diego APCD
Imperial Beach Civic Center,IB CIVIC CTR, 32.576139,  -117.115361,San Diego APCD
El Cajon - Lexington Elementary School,EL CAJON LES, 32.789561,  -116.944222,San Diego APCD
San Ysidro,SAN YSIDRO,	32.552794,	-117.047286,San Diego APCD
        """

def degrees_to_direction(degrees):
    """
    Convert wind direction in degrees to categorical text directions.

    Args:
        degrees: Wind direction in degrees (0-360)

    Returns:
        str: Cardinal/intercardinal direction (N, NE, E, SE, S, SW, W, NW)
    """
    if pd.isna(degrees):
        return None

    # Normalize degrees to 0-360 range
    degrees = degrees % 360

    # Define direction ranges (each direction covers 45 degrees, centered on the cardinal direction)
    directions = [
        (0, 22.5, "N"),      # 337.5-22.5 degrees
        (22.5, 67.5, "NE"),  # 22.5-67.5 degrees
        (67.5, 112.5, "E"),  # 67.5-112.5 degrees
        (112.5, 157.5, "SE"), # 112.5-157.5 degrees
        (157.5, 202.5, "S"),  # 157.5-202.5 degrees
        (202.5, 247.5, "SW"), # 202.5-247.5 degrees
        (247.5, 292.5, "W"),  # 247.5-292.5 degrees
        (292.5, 337.5, "NW"), # 292.5-337.5 degrees
        (337.5, 360, "N")     # 337.5-360 degrees (wrapping to North)
    ]

    for min_deg, max_deg, direction in directions:
        if min_deg <= degrees < max_deg:
            return direction

    return "N"  # Default to North for edge cases


def duckdb_connection(s3_resource: minio.S3Resource):

    server=s3_resource.S3_ADDRESS
    bucket= s3_resource.S3_BUCKET
    s3_setup=f"""CREATE OR REPLACE SECRET s3_credentials(
        TYPE s3,    
        ENDPOINT '{server}',
        URL_STYLE 'path'
    );
    """
    con = duckdb.connect()
    con.execute(s3_setup)
    return con

@asset(group_name="tijuana",
       key_prefix="h2sforecast",
       name="hs2_locations",
       required_resource_keys={"s3"},
       metadata={
           "source": "San Diego APCD H2S location data"
           , "description": "Location San Diego Air Pollution Control District Air Quality Monitoring Sites"
       },
       automation_condition=AutomationCondition.eager()
       )
def h2s_locations(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    sites_df = pd.read_csv(StringIO(sites_csv), sep=',', on_bad_lines='warn')
    store_assets.store_dataframe_to_s3( sites_df, OUTPUT_PATH,'h2s_locations', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata)
    return sites_df



@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s",
    required_resource_keys={"s3"},
    deps=[AssetKey(["apcd", 'yearly_aggregated_h2s']),
          AssetKey(['streamflow', 'boundary_cms_yearly']),
          AssetKey(['streamflow', 'boundary_cms']),
          AssetKey(['weather', 'openmeteo_historical'])
          ],
       metadata={
           "source": "San Diego APCD, IBWC Streamflow and OpenMeteo historical data"
           , "description": "Data for Forecast Modeling of H2S includes Wind Direction, Wind Speed, and complete Tijuana River streamflow (yearly historical + recent 30 days). THIS IS UP TO DATE DAILY. Not hourly. "
           , "variableMeasured": ["H2S", 'Wind Direction', 'Wind Speed', "Streamflow"]
       },
       automation_condition=AutomationCondition.eager()
)
def data_for_models(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()
    duckdb_con=duckdb_connection(s3_resource)
    # filename = f'{apcd_s3_output_path}/h2s.csv'
    # apcd_s3_output_path='tijuana/sd_apcd_air/output'
    # h2surl = s3_resource.publicUrl(path=f'{apcd_s3_output_path}/h2s.csv', bucket=s3_resource.S3_BUCKET)
    # dagster_logger.info(f"Downloading {h2surl}")
    # h2s_sensor_data_all = pd.read_csv(h2surl)
    hs2_files = f"s3://{s3_resource.S3_BUCKET}/{H2S_PATH}/{PARQUET_PATTERN}"
    try:
         h2s_sensor_data_all = duckdb_con.read_parquet(hs2_files).df()
    except Exception as e:
        dagster_logger.error(f"Error reading apcd parquet files {hs2_files} {e}")
        raise e
    # using names causes a parsing error, do just drop after loading
    try:
        h2s_sensor_data_all = h2s_sensor_data_all.drop(
            ['Original Value', 'Icons', 'level', 'Parameter', 'LongName', 'Site Name', 'Latitude', 'Longitude',
             'AgencyName'], axis=1)
        h2s_sensor_data_all['time'] = pd.to_datetime(h2s_sensor_data_all['Date with time'], utc=True)
        h2s_sensor_data_all = h2s_sensor_data_all.rename(
            columns={'SiteName': 'site_name', 'Result': 'H2S', 'Qualifier': 'H2S_qualifier'})
        h2s_sensor_data_all["time"] = h2s_sensor_data_all["time"].dt.tz_convert("America/Los_Angeles")
        h2s_sensor_data_all = h2s_sensor_data_all.rename(columns={'Result': 'H2S', 'Qualifier': 'H2S_qualifier'})

        # 2s_sensor_data_all.index = pd.to_datetime(h2s_sensor_data_all['Date with time']).dt.tz_localize('America/Los_Angeles', ambiguous=True)
        h2s_sensor_data_all = h2s_sensor_data_all.drop('Date with time', axis=1)
        h2s_sensor_data_all = h2s_sensor_data_all.set_index(pd.DatetimeIndex(h2s_sensor_data_all['time']))
        h2s_sensor_data_all = h2s_sensor_data_all.drop('time', axis=1)
        h2s_sensor_data_all.index = h2s_sensor_data_all.index.astype("datetime64[ns, America/Los_Angeles]")
        h2s_sensor_data_all = h2s_sensor_data_all.sort_index()
        pre_dedup = h2s_sensor_data_all.shape[0]
        h2s_sensor_data_all = (h2s_sensor_data_all
                               .reset_index()
                               .drop_duplicates(subset=['time', 'site_name'], keep='last')
                               .set_index('time'))
        h2s_sensor_data_all.index = h2s_sensor_data_all.index.astype("datetime64[ns, America/Los_Angeles]")
        dagster_logger.info(f"Deduplicated h2s: {pre_dedup} -> {h2s_sensor_data_all.shape[0]} rows")
    except Exception as e:
        dagster_logger.error(f"Error processing h2s data {e}")
        raise e
    dagster_logger.info(f"Matched {h2s_sensor_data_all.shape[0]} rows")

    # weather_df = pd.DataFrame()
    # for wurl in weather_urls:
    #     wyear_df = pd.read_csv(wurl)
    #     wyear_df['time'] = pd.to_datetime(wyear_df['date'], utc=True)
    #     # forecast_df["time"] = forecast_df["time"].dt.tz_localize("America/Los_Angeles", ambiguous=True)
    #     wyear_df = wyear_df.set_index(pd.DatetimeIndex(wyear_df['time']))
    #     wyear_df = wyear_df.drop(['time', 'date'], axis=1)
    #     weather_df = pd.concat([weather_df, wyear_df], )
    weather_files = f"s3://{s3_resource.S3_BUCKET}/{WEATHER_BASE}/{CSV_PATTERN}"
    try:
        weather_df = duckdb_con.read_csv(weather_files).df()
    except Exception as e:
        dagster_logger.error(f"Error reading weather csv files {weather_files} {e}")
        raise e
    try:
        weather_df = weather_df.rename(columns={'date': 'time'})
        weather_df["time"] = weather_df["time"].dt.tz_convert("America/Los_Angeles")
        weather_df = weather_df.set_index(pd.DatetimeIndex(weather_df['time']))
        weather_df = weather_df.drop(['time'], axis=1)
        weather_df.index = weather_df.index.astype("datetime64[ns, America/Los_Angeles]")
        weather_df = weather_df.sort_index()

        # Convert wind direction from degrees to categorical text
        if 'wind_direction_10m' in weather_df.columns:
            weather_df['wind_direction_categorical'] = weather_df['wind_direction_10m'].apply(degrees_to_direction)

            # Add sine and cosine components for circular wind direction (better for ML models)
            import numpy as np
            # Convert degrees to radians for trigonometric functions
            wind_direction_rad = np.deg2rad(weather_df['wind_direction_10m'])
            weather_df['wind_direction_sin'] = np.sin(wind_direction_rad)
            weather_df['wind_direction_cos'] = np.cos(wind_direction_rad)

            dagster_logger.info("Added sine and cosine components for wind direction")

        # Add rolling window calculations for wind speed and gusts
        dagster_logger.info("Calculating rolling wind metrics for 2, 3, and 4 hour windows")

        # Rolling average wind speed for 2, 3, 4 hours
        if 'wind_speed_10m' in weather_df.columns:
            weather_df['wind_speed_10m_avg_2h'] = weather_df['wind_speed_10m'].rolling(window=2, min_periods=1).mean()
            weather_df['wind_speed_10m_avg_3h'] = weather_df['wind_speed_10m'].rolling(window=3, min_periods=1).mean()
            weather_df['wind_speed_10m_avg_4h'] = weather_df['wind_speed_10m'].rolling(window=4, min_periods=1).mean()

        # Rolling maximum wind gusts for 2, 3, 4 hours
        if 'wind_gusts_10m' in weather_df.columns:
            weather_df['wind_gusts_10m_max_2h'] = weather_df['wind_gusts_10m'].rolling(window=2, min_periods=1).max()
            weather_df['wind_gusts_10m_max_3h'] = weather_df['wind_gusts_10m'].rolling(window=3, min_periods=1).max()
            weather_df['wind_gusts_10m_max_4h'] = weather_df['wind_gusts_10m'].rolling(window=4, min_periods=1).max()
            dagster_logger.info("Added wind gust rolling maximums for 2, 3, 4 hour windows")
        else:
            dagster_logger.warning("wind_gusts_10m column not found - skipping gust calculations")
        # Interaction features
        if 'wind_speed_10m' in weather_df.columns and 'temperature_2m' in weather_df.columns:
            weather_df['wind_temp_interaction'] = weather_df['wind_speed_10m'] * weather_df['temperature_2m']

        if 'relative_humidity_2m' in weather_df.columns and 'temperature_2m' in weather_df.columns:
            weather_df['humidity_temp_interaction'] = weather_df['relative_humidity_2m'] * weather_df['temperature_2m']

        # Encode categorical variables using dict lookups (instead of LabelEncoder)
        if 'wind_direction_categorical' in weather_df.columns:
            wind_direction_mapping = {
                'N': 0,
                'NE': 1,
                'E': 2,
                'SE': 3,
                'S': 4,
                'SW': 5,
                'W': 6,
                'NW': 7
            }
            weather_df['wind_direction_categorical_encoded'] = weather_df['wind_direction_categorical'].map(wind_direction_mapping).fillna(-1).astype(int)
            dagster_logger.info("Encoded wind_direction_categorical to integers (N=0, NE=1, ..., NW=7)")

        dagster_logger.info("Completed rolling wind calculations")
    except Exception as e:
        dagster_logger.error(f"Error processing weather data {e}")
        raise e

    dagster_logger.info(f"Matched {weather_df.shape[0]} rows")
    # merged_df = pd.merge(h2s_sensor_data_all, weather_df, on="time", how="inner")
    try:
        matched_df = pd.merge_asof(h2s_sensor_data_all, weather_df, left_on="time", right_on="time", direction="nearest")
    except Exception as e:
        dagster_logger.error(f"Error merging weather and h2s data {e}")
        raise e
    dagster_logger.info(f"Matched {matched_df.shape[0]} rows")
    # border streamflow - load yearly historical data
    streamflow_border_files = f"s3://{s3_resource.S3_BUCKET}/{STREAMFLOW_BASE}/{STREAMFLOW_SITE_YEARLY}/{PARQUET_PATTERN}"
    try:
        streamflow_border_df = duckdb_con.read_parquet(streamflow_border_files).df()
        dagster_logger.info(f"Loaded {streamflow_border_df.shape[0]} yearly streamflow records")
    except Exception as e:
        dagster_logger.error(f"Error reading streamflow parquet files {streamflow_border_files} {e}")
        raise e
    # also load recent boundary_cms (last 30 days) and combine for a complete flow record
    # try:
    #     streamflow_recent_files = f"s3://{s3_resource.S3_BUCKET}/{STREAMFLOW_BASE}/{STREAMFLOW_SITE_RECENT}/{CSV_PATTERN}"
    #     streamflow_recent_df = duckdb_con.read_csv(streamflow_recent_files).df()
    #     dagster_logger.info(f"Loaded {streamflow_recent_df.shape[0]} recent boundary_cms records")
    #     streamflow_border_df = pd.concat([streamflow_border_df, streamflow_recent_df], ignore_index=True)
    # except Exception as e:
    #     dagster_logger.warning(f"Could not load recent boundary_cms data, continuing with yearly only: {e}")
    try:
        streamflow_border_df['time'] = pd.to_datetime(streamflow_border_df['End of Interval (UTC-08:00)'])
        # Data is UTC-8 fixed offset (no DST) — localize to fixed offset, then convert to LA
        streamflow_border_df["time"] = streamflow_border_df["time"].dt.tz_localize('Etc/GMT+8').dt.tz_convert("America/Los_Angeles")
        streamflow_border_df = streamflow_border_df.rename(columns={'Average (m^3/s)': 'Flow (m^3/s)--Border'})
        streamflow_border_df = streamflow_border_df.set_index(pd.DatetimeIndex(streamflow_border_df['time']))
        streamflow_border_df = streamflow_border_df.drop(
            ['time', 'End of Interval (UTC-08:00)', 'Start of Interval (UTC-08:00)'], axis=1, errors='ignore')
        # deduplicate - recent data takes precedence over yearly
        streamflow_border_df = streamflow_border_df[~streamflow_border_df.index.duplicated(keep='last')]
        streamflow_border_df = streamflow_border_df.sort_index()
        dagster_logger.info(f"Combined streamflow has {streamflow_border_df.shape[0]} records after dedup")
    except Exception as e:
        dagster_logger.error(f"Error processing streamflow files {streamflow_border_files} {e}")
        raise e
    try:
        matched_df = pd.merge_asof(matched_df, streamflow_border_df, left_on="time", right_on="time", direction="nearest")
    except Exception as e:
        dagster_logger.error(f"Error merging weather and h2s  AND STREAMFLOW data {e}")
        raise e
    # add tides
    try:
        tidal_files = f"s3://{s3_resource.S3_BUCKET}/{TIDAL_BASE}/{PARQUET_PATTERN}"
        tidal_df = duckdb_con.read_parquet(tidal_files).df()
    except Exception as e:
        dagster_logger.error(f"Error reading tidal_files  files {tidal_files} {e}")
        raise e
    try:
        tidal_df['time'] = pd.to_datetime(tidal_df['time'], utc=False)
        tidal_df['time'] = tidal_df['time'].dt.tz_localize(None)
        tidal_df['time'] = tidal_df['time'].astype('datetime64[ns]')
        tidal_df['time'] = tidal_df['time'].dt.tz_localize("UTC")
        tidal_df["time"] = tidal_df["time"].dt.tz_convert("America/Los_Angeles")

        tidal_df = tidal_df.set_index(pd.DatetimeIndex(tidal_df['time']))
        tidal_df = tidal_df.drop(
            ['time'], axis=1)

        # Encode tidal states to integers
        tidal_mapping = {
            'low': 0,
            'rising': 1,
            'high': 2,
            'falling': 3,
            'ebb': 3,      # falling/ebb are the same
            'flood': 1     # rising/flood are the same
        }

        if 'tidal_state' in tidal_df.columns:
            tidal_df['tidal_state_encoded'] = tidal_df['tidal_state'].map(tidal_mapping).fillna(-1).astype(int)
        else:
            # Default to -1 if column missing (unknown category)
            tidal_df['tidal_state_encoded'] = -1
        tidal_df = tidal_df.sort_index()
    except Exception as e:
        dagster_logger.error(f"Error reading tidals   files {tidal_df} {e}")
        raise e
    try:
        matched_df = pd.merge_asof(matched_df, tidal_df, left_on="time", right_on="time", direction="nearest")
    except Exception as e:
        dagster_logger.error(f"Error merging with tidal files data {e}")
        raise e

    # add night day
    # 2. Create a LocationInfo object for San Diego
    san_diego_location = LocationInfo(
        name='San Diego',
        region='USA',
        timezone='America/Los_Angeles',
        latitude=32.7157,
        longitude=-117.1611
    )
    unique_dates = matched_df['time'].dt.date.unique()

    # 2. Initialize an empty dictionary to store the sunrise and sunset times
    daily_sun_times = {}

    # 3. For each unique date, calculate sunrise and sunset times
    for date in unique_dates:
        # Get sun times for the day using the San Diego location
        s = sun(san_diego_location.observer, date=date, tzinfo=san_diego_location.timezone)

        # 4. Store the sunrise and sunset times in the daily_sun_times dictionary
        daily_sun_times[date] = {
            'sunrise': s['sunrise'],
            'sunset': s['sunset']
        }

    def get_day_night(timestamp, sun_times_dict):
        # Extract the date part from the timestamp
        date_only = timestamp.date()

        # Retrieve sunrise and sunset times for the specific date
        if date_only in sun_times_dict:
            sun_info = sun_times_dict[date_only]
            sunrise = sun_info['sunrise']
            sunset = sun_info['sunset']

            # Compare the hourly timestamp with sunrise and sunset to determine 'day' or 'night'
            if sunrise <= timestamp < sunset:
                return 'day'
            else:
                return 'night'
        else:
            # Handle cases where sun times might not be available for a date
            return 'unknown'

    matched_df['day_night'] = matched_df['time'].apply(lambda x: get_day_night(x, daily_sun_times))

    # Fill missing H2S values for each site_name based on min/max time ranges
    # and flag filled values
    dagster_logger.info("Filling missing H2S values for each site_name based on min/max time ranges")

    # Add flag column to track measured vs filled values
    matched_df['h2s_measured'] = True

    # Process each site separately
    filled_dfs = []
    for site_name in matched_df['site_name'].unique():
        site_df = matched_df[matched_df['site_name'] == site_name].copy()

        # Find min/max time where H2S data exists for this site
        h2s_valid_mask = site_df['H2S'].notna()
        if h2s_valid_mask.any():
            min_time = site_df[h2s_valid_mask].index.min()
            max_time = site_df[h2s_valid_mask].index.max()

            # Create time range mask for this site
            time_range_mask = (site_df.index >= min_time) & (site_df.index <= max_time)

            # Find missing H2S values within the time range
            missing_mask = time_range_mask & site_df['H2S'].isna()

            if missing_mask.any():
                # Fill missing values using forward fill then backward fill
                h2s_series = site_df.loc[time_range_mask, 'H2S'].ffill().bfill()
                site_df.loc[time_range_mask, 'H2S'] = h2s_series

                # Flag the filled values as not measured
                site_df.loc[site_df[missing_mask].index, 'h2s_measured'] = False

                dagster_logger.info(f"Filled {missing_mask.sum()} missing H2S values for site {site_name} between {min_time} and {max_time}")
            else:
                dagster_logger.info(f"No missing H2S values to fill for site {site_name}")
        else:
            dagster_logger.warning(f"No valid H2S data found for site {site_name}")

        filled_dfs.append(site_df)

    # Combine all sites back together
    matched_df = pd.concat(filled_dfs, ignore_index=False).sort_index()

    # Log summary of filling operation
    total_filled = (~matched_df['h2s_measured']).sum()
    dagster_logger.info(f"Total H2S values filled across all sites: {total_filled}")

    # H2S risk score using log-logistic (Hill) function: x^b / (x^b + c^b), c=5, b=1.23
    matched_df['h2s_risk'] = matched_df['H2S'].pow(1.23) / (matched_df['H2S'].pow(1.23) + 5**1.23)

    if 'date_processed' in matched_df.columns:
            matched_df = matched_df.drop(columns=['date_processed'])
    if 'aggregation_year' in matched_df.columns:
        matched_df = matched_df.drop(columns=['aggregation_year'])
    if 'H2S_qualifier' in matched_df.columns:
        matched_df = matched_df.drop(columns=['H2S_qualifier'])
    if 'visibility' in matched_df.columns:
        matched_df = matched_df.drop(columns=['visibility'])
    store_assets.store_dataframe_to_s3( matched_df, OUTPUT_PATH,'modeldata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv', 'parquet'], metadata=metadata )
    return matched_df

@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s_nofill",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", "modeldata_h2s"])],
    ins={
        "modeldata_h2s": AssetIn(
            key=AssetKey(["h2sforecast", "modeldata_h2s"])
        )
    },
    metadata={
        "source": "San Diego APCD, IBWC Streamflow and OpenMeteo historical data"
        , "description": "Model data identical to modeldata_h2s but with filled H2S values reverted to N/A. Only measured H2S values are retained."
        , "variableMeasured": ["H2S", "Wind Direction", "Wind Speed", "Streamflow"]
    },
    automation_condition=AutomationCondition.eager()
)
def modeldata_h2s_nofill(context, modeldata_h2s):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3

    result_df = modeldata_h2s.copy()
    if "h2s_measured" in result_df.columns:
        result_df.loc[~result_df["h2s_measured"], "H2S"] = None

    store_assets.store_dataframe_to_s3(result_df, OUTPUT_PATH, "modeldata_h2s_nofill", s3_resource,
                                       latestdatasetpath=LATEST, enable_latest_path=True,
                                       formats=["csv", "parquet"], metadata=metadata)
    return result_df


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="hysplit_h2s",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast",'modeldata_h2s']),
          ] ,
    ins={
        "data_for_models": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s'])
        )
    },
metadata={
           "source": "San Diego APCD and OpenMeteo historical data"
       ,"description":"Data for Hysplit Model of H2S includes Wind Direction and Wind Speed. This is one day behind."
,"variableMeasured":["H2S",'Wind Direction','Wind Speed']
},
       automation_condition=AutomationCondition.eager()
)
def data_for_hysplit(context, data_for_models):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    # Include both numeric and categorical wind direction columns
    columns_to_select = ['time', 'site_name', 'H2S', 'wind_speed_10m', 'wind_direction_10m']
    if 'wind_direction_categorical' in data_for_models.columns:
        columns_to_select.append('wind_direction_categorical')
    if 'temperature_2m' in data_for_models.columns:
        columns_to_select.append('temperature_2m')
    if 'h2s_measured' in data_for_models.columns:
        columns_to_select.append('h2s_measured')
    if 'h2s_risk' in data_for_models.columns:
        columns_to_select.append('h2s_risk')

    h2s_df = data_for_models[columns_to_select]
    h2s_df = h2s_df.rename(columns={'wind_speed_10m':'wind_speed', 'wind_direction_10m':'wind_direction'})
    store_assets.store_dataframe_to_s3( h2s_df, OUTPUT_PATH,'hysplitdata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata )


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_peaks",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", 'modeldata_h2s'])],
    ins={
        "modeldata_h2s": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s'])
        )
    },
    metadata={
        "source": "San Diego APCD H2S data analysis"
        , "description": "Hourly counts of H2S threshold exceedances by day/night periods"
        , "variableMeasured": ["H2S", "Exceedance Counts"]
    },
    automation_condition=AutomationCondition.eager()
)
def h2s_peaks_analysis(context, modeldata_h2s):
    """
    Create hourly counts of H2S exceedances for day and night periods

    Counts hourly occurrences when H2S exceeds 5 ppb and 30 ppb thresholds,
    separated by day (6 AM - 6 PM) and night (6 PM - 6 AM) periods.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()

    try:
        # Work with the H2S model data
        h2s_data = modeldata_h2s.copy()

        if h2s_data.empty:
            dagster_logger.warning("No H2S data available")
            return pd.DataFrame()

        # Ensure we have the time index and site_name
        if 'site_name' not in h2s_data.columns:
            dagster_logger.error("site_name column not found in data")
            return pd.DataFrame()

        # Reset index to work with datetime as a column
        h2s_data = h2s_data.reset_index()

        # Ensure datetime column exists
        if 'time' not in h2s_data.columns:
            dagster_logger.error("time column not found in data")
            return pd.DataFrame()

        # Convert time to datetime if it's not already
        h2s_data['datetime'] = pd.to_datetime(h2s_data['time'])

        # Extract hour for day/night classification
        h2s_data['hour'] = h2s_data['datetime'].dt.hour
        h2s_data['date'] = h2s_data['datetime'].dt.date

        # Classify periods: Day = 6 AM to 6 PM, Night = 6 PM to 6 AM
        h2s_data['period'] = h2s_data['hour'].apply(lambda h: 'day' if 6 <= h < 18 else 'night')

        # Filter for valid H2S measurements only
        h2s_valid = h2s_data[h2s_data['H2S'].notna()].copy()

        if h2s_valid.empty:
            dagster_logger.warning("No valid H2S measurements found")
            return pd.DataFrame()

        # Create threshold exceedance flags
        h2s_valid['exceeds_5'] = h2s_valid['H2S'] > 5
        h2s_valid['exceeds_30'] = h2s_valid['H2S'] > 30

        dagster_logger.info(f"Processing {len(h2s_valid)} valid H2S measurements")
        dagster_logger.info(f"Found {h2s_valid['exceeds_5'].sum()} exceedances > 5 ppb")
        dagster_logger.info(f"Found {h2s_valid['exceeds_30'].sum()} exceedances > 30 ppb")

        # Calculate daily totals by site and period (no hourly summaries)
        daily_totals = h2s_valid.groupby(['site_name', 'date', 'period']).agg({
            'exceeds_5': 'sum',
            'exceeds_30': 'sum',
            'H2S': ['count', 'max', 'mean'],
            'h2s_measured': lambda x: (~x).sum()
        }).reset_index()

        # Flatten column names for daily totals
        daily_totals.columns = [
            'site_name', 'date', 'period',
            'count_exceeds_5', 'count_exceeds_30',
            'total_measurements', 'max_h2s', 'mean_h2s', 'count_filled'
        ]

        daily_totals['summary_type'] = 'daily_by_period'
        daily_totals['date_processed'] = datetime.now().isoformat()

        dagster_logger.info(f"Generated {len(daily_totals)} daily H2S peak records by period")
        dagster_logger.info(f"Day period exceedances > 5: {daily_totals[daily_totals['period']=='day']['count_exceeds_5'].sum()}")
        dagster_logger.info(f"Night period exceedances > 5: {daily_totals[daily_totals['period']=='night']['count_exceeds_5'].sum()}")
        dagster_logger.info(f"Day period exceedances > 30: {daily_totals[daily_totals['period']=='day']['count_exceeds_30'].sum()}")
        dagster_logger.info(f"Night period exceedances > 30: {daily_totals[daily_totals['period']=='night']['count_exceeds_30'].sum()}")

        # Store the results
        store_assets.store_dataframe_to_s3(
            daily_totals,
            OUTPUT_PATH,
            'h2s_peaks',
            s3_resource,
            latestdatasetpath=LATEST,
            enable_latest_path=True,
            formats=['csv', 'parquet'],
            metadata=metadata
        )

        return daily_totals

    except Exception as e:
        dagster_logger.error(f"Error processing H2S peaks: {e}")
        raise e


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_exceedance_periods",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", 'h2s_peaks']), AssetKey(["h2sforecast", 'modeldata_h2s'])],
    ins={
        "h2s_peaks": AssetIn(
            key=AssetKey(['h2sforecast', 'h2s_peaks'])
        ),
        "modeldata_h2s": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s'])
        )
    },
    metadata={
        "source": "San Diego APCD H2S exceedance analysis"
        , "description": "Filtered datasets for day/night periods with H2S exceedances above thresholds"
        , "variableMeasured": ["H2S Exceedances", "Day/Night Periods"]
    },
    automation_condition=AutomationCondition.eager()
)
def h2s_exceedance_periods_filter(context, h2s_peaks, modeldata_h2s):
    """
    Return hourly model data for periods where H2S exceedances occurred.

    Creates two datasets with full hourly environmental data (H2S, weather, streamflow, tidal):
    1. Hours within day/night periods where H2S exceeded 5 ppb
    2. Hours within day/night periods where H2S exceeded 30 ppb

    Perfect for forecast modeling as it provides all environmental variables.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()

    try:
        if h2s_peaks.empty or modeldata_h2s.empty:
            dagster_logger.warning("No H2S peaks data or model data available")
            return {"h2s_exceeds_5": pd.DataFrame(), "h2s_exceeds_30": pd.DataFrame()}

        # Prepare model data with time information
        model_data = modeldata_h2s.copy().reset_index()
        model_data['datetime'] = pd.to_datetime(model_data['time'])
        model_data['hour'] = model_data['datetime'].dt.hour
        model_data['date'] = model_data['datetime'].dt.date
        model_data['period'] = model_data['hour'].apply(lambda h: 'day' if 6 <= h < 18 else 'night')

        def filter_model_data_by_exceedances(peaks_df, threshold_name):
            """Filter model data to only include hours from exceedance periods"""
            if peaks_df.empty:
                return pd.DataFrame()

            # Create list of (site_name, date, period) tuples for exceedance periods
            exceedance_periods = []
            for _, row in peaks_df.iterrows():
                exceedance_periods.append((row['site_name'], row['date'], row['period']))

            # Filter model data to only include matching periods
            filtered_data = []
            for site_name, date, period in exceedance_periods:
                mask = (
                    (model_data['site_name'] == site_name) &
                    (model_data['date'] == date) &
                    (model_data['period'] == period)
                )
                period_data = model_data[mask].copy()
                if not period_data.empty:
                    period_data['exceedance_threshold'] = threshold_name
                    filtered_data.append(period_data)

            if filtered_data:
                return pd.concat(filtered_data, ignore_index=True)
            else:
                return pd.DataFrame()

        # Filter for periods where H2S exceeded 5 ppb - get hourly model data
        h2s_5_periods = h2s_peaks[h2s_peaks['count_exceeds_5'] > 0]
        h2s_exceeds_5_hourly = filter_model_data_by_exceedances(h2s_5_periods, '5_ppb')

        dagster_logger.info(f"Found {len(h2s_5_periods)} day/night periods with H2S > 5 ppb")
        dagster_logger.info(f"Retrieved {len(h2s_exceeds_5_hourly)} hourly records for H2S > 5 ppb periods")

        # Filter for periods where H2S exceeded 30 ppb - get hourly model data
        h2s_30_periods = h2s_peaks[h2s_peaks['count_exceeds_30'] > 0]
        h2s_exceeds_30_hourly = filter_model_data_by_exceedances(h2s_30_periods, '30_ppb')

        dagster_logger.info(f"Found {len(h2s_30_periods)} day/night periods with H2S > 30 ppb")
        dagster_logger.info(f"Retrieved {len(h2s_exceeds_30_hourly)} hourly records for H2S > 30 ppb periods")

        # Add summary statistics
        if not h2s_exceeds_5_hourly.empty:
            unique_hours_5 = h2s_exceeds_5_hourly.groupby(['site_name', 'date', 'period']).size().sum()
            actual_exceedances_5 = (h2s_exceeds_5_hourly['H2S'] > 5).sum()
            dagster_logger.info(f"H2S > 5 ppb: {unique_hours_5} total hours, {actual_exceedances_5} hours with actual H2S > 5")

        if not h2s_exceeds_30_hourly.empty:
            unique_hours_30 = h2s_exceeds_30_hourly.groupby(['site_name', 'date', 'period']).size().sum()
            actual_exceedances_30 = (h2s_exceeds_30_hourly['H2S'] > 30).sum()
            dagster_logger.info(f"H2S > 30 ppb: {unique_hours_30} total hours, {actual_exceedances_30} hours with actual H2S > 30")

        # Store H2S > 5 ppb hourly model data
        if not h2s_exceeds_5_hourly.empty:
            store_assets.store_dataframe_to_s3(
                h2s_exceeds_5_hourly,
                OUTPUT_PATH,
                'h2s_exceedance_model_data_5ppb',
                s3_resource,
                latestdatasetpath=LATEST,
                enable_latest_path=True,
                formats=['csv', 'parquet'],
                metadata=metadata
            )
            dagster_logger.info(f"✓ Stored H2S > 5 ppb hourly model data for forecast modeling")
        else:
            dagster_logger.info("No H2S > 5 ppb exceedance model data found")

        # Store H2S > 30 ppb hourly model data
        if not h2s_exceeds_30_hourly.empty:
            store_assets.store_dataframe_to_s3(
                h2s_exceeds_30_hourly,
                OUTPUT_PATH,
                'h2s_exceedance_model_data_30ppb',
                s3_resource,
                latestdatasetpath=LATEST,
                enable_latest_path=True,
                formats=['csv', 'parquet'],
                metadata=metadata
            )
            dagster_logger.info(f"✓ Stored H2S > 30 ppb hourly model data for forecast modeling")
        else:
            dagster_logger.info("No H2S > 30 ppb exceedance model data found")

        # Return both hourly model datasets
        return {
            "h2s_exceeds_5": h2s_exceeds_5_hourly,
            "h2s_exceeds_30": h2s_exceeds_30_hourly
        }

    except Exception as e:
        dagster_logger.error(f"Error filtering H2S exceedance model data: {e}")
        raise e



