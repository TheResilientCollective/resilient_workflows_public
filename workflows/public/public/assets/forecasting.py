from datetime import datetime
from io import StringIO
import pandas as pd


from dagster import ( asset,
                     get_dagster_logger,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      TimeWindowPartitionsDefinition
                      )

from ..resources import minio
from ..utils import store_assets
#from .sd_apcd import s3_output_path as apcd_s3_output_path



@asset(group_name="tijuana",
    key_prefix="forecast",
    name="models_h2s",
    required_resource_keys={"s3"},
    deps=[AssetKey(["apcd", 'subset_h2s_s02']),
          AssetKey(['streamflow', 'boundary_cms']),
          AssetKey(['weather', 'openmeteo_historical'])
          ]
)
def data_for_models(context):
    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()
    # filename = f'{apcd_s3_output_path}/h2s.csv'
    apcd_s3_output_path='tijuana/sd_apcd_air/output'
    h2surl = s3_resource.publicUrl(path=f'{apcd_s3_output_path}/h2s.csv', bucket=s3_resource.S3_BUCKET)
    dagster_logger.info(f"Downloading {h2surl}")
    h2s_sensor_data_all = pd.read_csv(h2surl)
    h2s_sensor_data_all["time"] = pd.to_datetime(
        h2s_sensor_data_all["Date with time"])  # Ensure time format consistency
    h2s_sensor_data_all["time"] = h2s_sensor_data_all["time"].dt.tz_localize("America/Los_Angeles", ambiguous=True)
    h2s_sensor_data_all = h2s_sensor_data_all.set_index(pd.DatetimeIndex(h2s_sensor_data_all['time']))
    h2s_sensor_data_all = h2s_sensor_data_all.drop('time', axis=1)
    dagster_logger.info(f"Matched {h2s_sensor_data_all.shape[0]} rows")

    weather_df = context.repository_def.load_asset_value(AssetKey([f"weather", "openmeteo_historical"]))
    weather_df["time"] = weather_df["time"].dt.tz_localize("America/Los_Angeles", ambiguous=True)
    weather_df = weather_df.set_index(pd.DatetimeIndex(weather_df['time']))
    weather_df = weather_df.drop('time', axis=1)
    dagster_logger.info(f"Matched {weather_df.shape[0]} rows")
    # merged_df = pd.merge(h2s_sensor_data_all, weather_df, on="time", how="inner")
    matched_df = pd.merge_asof(h2s_sensor_data_all, weather_df, left_on="time", right_on="date", direction="nearest")
    dagster_logger.info(f"Matched {matched_df.shape[0]} rows")

    return matched_df

# @asset(
#     group_name="tijuana",
#     key_prefix="forecast",
#     name="hysplit_h2s",
#     required_resource_keys={"s3"},
#     deps=[AssetKey(["forecast",'models_h2s']),
#           ] ,
# metadata={
#            "source": "San Diego APCD and OpenMeteo historical data"
#        ,"description":"Data for Hysplit Model of H2S includes Wind Direction and Wind Speed"
# ,"variableMeasured":["H2S",'Wind Direction','Wind Speed']
# }
# )
# def data_for_hysplit(context):
#     s3_resource = context.resources.s3
#
#
#     pass
