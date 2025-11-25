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

OUTPUT_PATH='tijuana/forecast/output/'
LATEST='tijuana/forecast_data'

PARQUET_PATTERN='*.parquet'
CSV_PATTERN='*.csv'

H2S_PATH='latest/tijuana/sd_apcd_air/h2s'
WEATHER_BASE='latest/tijuana/weather'


sites_csv = """LongName,site_name,lat,lon,AgencyName
Berry Elementary School,NESTOR - BES, 32.567097, -117.090656,San Diego APCD
Imperial Beach Civic Center,IB CIVIC CTR, 32.576139,  -117.115361,San Diego APCD
El Cajon - Lexington Elementary School,EL CAJON LES, 32.789561,  -116.944222,San Diego APCD
        """

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
          AssetKey(['weather', 'openmeteo_historical'])
          ],
       metadata={
           "source": "San Diego APCD, IBWC Streamflow and OpenMeteo historical data"
           , "description": "Data for Forecast Modeling of H2S includes Wind Direction and Wind Speed"
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
        h2s_sensor_data_all["time"] = h2s_sensor_data_all["time"].dt.tz_convert("America/Los_Angeles")
        h2s_sensor_data_all = h2s_sensor_data_all.rename(columns={'Result': 'H2S', 'Qualifier': 'H2S_qualifier'})

        # 2s_sensor_data_all.index = pd.to_datetime(h2s_sensor_data_all['Date with time']).dt.tz_localize('America/Los_Angeles', ambiguous=True)
        h2s_sensor_data_all = h2s_sensor_data_all.drop('Date with time', axis=1)
        h2s_sensor_data_all = h2s_sensor_data_all.set_index(pd.DatetimeIndex(h2s_sensor_data_all['time']))
        h2s_sensor_data_all = h2s_sensor_data_all.drop('time', axis=1)
        h2s_sensor_data_all.index = h2s_sensor_data_all.index.astype("datetime64[ns, America/Los_Angeles]")
        h2s_sensor_data_all = h2s_sensor_data_all.sort_index()
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
        dagster_logger.error(f"Error reading weather csv files {hs2_files} {e}")
        raise e
    try:
        weather_df = weather_df.rename(columns={'date': 'time'})
        weather_df["time"] = weather_df["time"].dt.tz_convert("America/Los_Angeles")
        weather_df = weather_df.set_index(pd.DatetimeIndex(weather_df['time']))
        weather_df = weather_df.drop(['time'], axis=1)
        weather_df.index = weather_df.index.astype("datetime64[ns, America/Los_Angeles]")
        weather_df = weather_df.sort_index()
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

    store_assets.store_dataframe_to_s3( matched_df, OUTPUT_PATH,'modeldata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv', 'parquet'], metadata=metadata )
    return matched_df

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
       ,"description":"Data for Hysplit Model of H2S includes Wind Direction and Wind Speed"
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
    h2s_df = data_for_models[['time','site_name','H2S', 'wind_speed_10m', 'wind_direction_10m']]
    h2s_df.rename(columns={'wind_speed_10m':'wind_speed', 'wind_direction_10m':'wind_direction'} )
    store_assets.store_dataframe_to_s3( h2s_df, OUTPUT_PATH,'hysplitdata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata )

