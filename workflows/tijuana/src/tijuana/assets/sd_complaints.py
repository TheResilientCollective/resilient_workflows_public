import os
from unittest.mock import inplace

import pytz
import requests
import json
import pandas as pd
import geopandas as gpd
from dagster import ( asset, op,
                     get_dagster_logger,
                      AssetKey,sensor,
DailyPartitionsDefinition, TimeWindowPartitionsDefinition, define_asset_job,job, RunRequest, schedule,
AutomationCondition,
SensorEvaluationContext,AssetCheckSpec,
                      AssetCheckResult,
                      asset_check,
                      AssetCheckExecutionContext, AssetIn
                      )
from resilient_core.utils.constants import ICONS
from .gis import subregions
from resilient_core.utils import store_assets
import datetime
import pytz

import xml.etree.ElementTree as ET

# docker env has RESILIENT_ prefix
#             - SLACK_CHANNEL=${RESILIENT_SLACK_CHANNEL:-"#test"}
#             - SLACK_TOKEN=${RESILIENT_SLACK_TOKEN}
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_UPDATES", "#test")

output_path="tijuana/sd_complaints"

# complaints_daily_job = define_asset_job(
#     "complaints_daily", selection=[AssetKey(["complaints", "sd_complaints"])]
# )

sd_complaints_arcgis_base = "https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/"

sd_complaints_yearly_partitions = TimeWindowPartitionsDefinition(
    start=datetime.datetime(2023, 1, 1),
    fmt="%Y",
    cron_schedule="@yearly",
    end_offset=1
)

def dropUnnecessaryColumns(df):
    return df.drop( columns=['response_duration__hours_',
                            "investigation_outcome",
                            "cross_street___intersection",
                            'Icons',
                            ]
                    )

@asset(group_name="tijuana", key_prefix="complaints",
       name="sd_complaints_raw",
       partitions_def=sd_complaints_yearly_partitions,
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def get_sd_complaints(context) -> None:
    year = int(context.asset_partition_key_for_output())
    where = (
        f"date_received >= timestamp '{year}-01-01 00:00:00' "
        f"AND date_received < timestamp '{year + 1}-01-01 00:00:00'"
    )
    params = {
        "where": where,
        "outFields": (
            "nature_of_complaint,date_received,record_number,record_status,"
            "investigation_outcome,response_duration__hours_,x_coordinate,"
            "y_coordinate,cross_street___intersection,zip,city"
        ),
        "returnGeometry": "true",
        "f": "geojson",
    }
    url = f"{sd_complaints_arcgis_base}/query"
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    if data.get("exceededTransferLimit"):
        get_dagster_logger().warning(f"exceededTransferLimit for year {year} — consider finer partitioning")
    path = f"{output_path}/raw/complaints_{year}.json"
    context.resources.s3.putFile_text(response.text, path=path)

@asset(group_name="tijuana", key_prefix="complaints",
       name="sd_complaints",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager(),
       deps=[AssetKey(["complaints", "sd_complaints_raw"])])
def sd_complaints(context):
    name='sd_complaints'
    description='''
    Data from San Diego Air Pollution Control District Complaints ArcGIS service
    '''
    source_url='https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name,description=description, source_url=source_url)
    s3_resource = context.resources.s3
    all_features = []
    for obj in s3_resource.listPath(f"{output_path}/raw/"):
        obj_name = obj.object_name
        if "complaints_" in obj_name and obj_name.endswith(".json"):
            raw = s3_resource.getFile(obj_name)
            data = json.loads(raw)
            all_features.extend(data.get("features", []))
    featueres = {"type": "FeatureCollection", "features": all_features}
    complaints_gdf = gpd.GeoDataFrame.from_features(featueres)
    complaints_gdf['datetime'] = pd.to_datetime(complaints_gdf['date_received'], unit='ms')
    complaints_gdf['datetime']=complaints_gdf['datetime'].dt.tz_localize('US/Pacific')
    complaints_gdf['date'] = complaints_gdf['datetime'].dt.strftime('%Y-%m-%d')
    complaints_gdf.dropna(how='any', subset=['x_coordinate', 'y_coordinate','geometry'], inplace=True)
    complaints_gdf= complaints_gdf[(complaints_gdf['x_coordinate']!=0) | (complaints_gdf['y_coordinate']!=0)]
    complaints_gdf['Icons'] = ICONS['beach']
    # complaints_csv=complaints_gdf.to_csv(index=False)
    # path = f"{output_path}/output/complaints.csv"
    # s3_resource.putFile_text(data=complaints_csv, path=path)
    #
    # complaints_gdf['datetime']= complaints_gdf['datetime'].apply(lambda x: x.isoformat())
    # complaints_json = complaints_gdf.to_json()
    # path = f"{output_path}/output/complaints.geojson"
    # s3_resource.putFile_text(data=complaints_json, path=path)
    # filename = f"{output_path}/output/complaints"
    # store_assets.geodataframe_to_s3(complaints_gdf, filename, s3_resource, metadata=metadata)
    filepath = f"{output_path}/output/"
    latestdatasetpath=f'{output_path}'
    filename="complaints"
    store_assets.store_dataframe_to_s3(complaints_gdf,
            filepath, filename, s3_resource,
            metadata=metadata,
           enable_latest_path=True,
          latestdatasetpath=latestdatasetpath,
                                       formats=['json','geojson','csv','parquet'])


    return complaints_gdf

# Assuming sd_complaints asset returns a GeoDataFrame
# and has a 'datetime' column with timezone-aware datetimes.

@asset_check(asset=AssetKey(["complaints", "sd_complaints"]),
           #  specs=[AssetCheckSpec(name="sd_complaints_freshness_check")]
             )
def sd_complaints_freshness_check(context: AssetCheckExecutionContext, sd_complaints):
    """
    Checks if the most recent data point in the sd_complaints asset is
    no older than three days ago.
    """
    if sd_complaints.empty:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": "Asset is empty, cannot determine freshness."
            }
        )

    # Find the most recent datetime in the asset
    sd_complaints['datetime'] = pd.to_datetime(sd_complaints['datetime'], errors='coerce')
    sd_complaints = sd_complaints.dropna(subset=['datetime'])

    most_recent_datetime = sd_complaints['datetime'].max()

    # Get the current datetime in the same timezone as the data
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Los_Angeles"))  # Assuming US/Pacific

    # Calculate the difference
    time_difference = current_datetime - most_recent_datetime

    # Check if the difference is greater than three days
    passed = time_difference <= datetime.timedelta(days=4)

    metadata = {
        "most_recent_datetime": str(most_recent_datetime),
        "current_datetime": str(current_datetime),
        "time_difference": str(time_difference),
    }

    if not passed:
        metadata["reason"] = "Data is older than three days."
    return AssetCheckResult(passed=passed, metadata=metadata)

@asset(group_name="tijuana", key_prefix="complaints",
       name="sd_complaints_90_days",
       required_resource_keys={"s3"},
       ins={
           "sd_complaints": AssetIn(key=AssetKey(["complaints", "sd_complaints"])),
       },
       automation_condition=AutomationCondition.eager())
def sd_complaints_90_days(context, sd_complaints: gpd.GeoDataFrame):
    datestart = datetime.datetime.now(pytz.timezone("America/Los_Angeles")) - datetime.timedelta(days=90)
    name = 'sd_complaints_90_days'
    description = '''
    Data from San Diego Air Pollution Control District Complaints ArcGIS service
    '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    complaints_gdf = sd_complaints
    complaints_90_gdf = complaints_gdf[complaints_gdf['datetime']> datestart.strftime('%Y-%m-%d')]
    complaints_90_gdf= dropUnnecessaryColumns(complaints_90_gdf)
    filename = f"latest/{output_path}/complaints_90_days"
    store_assets.geodataframe_to_s3(complaints_90_gdf, filename, s3_resource, metadata=metadata)

    return complaints_90_gdf
@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_summary",
       required_resource_keys={"s3"},
       ins={
           "sd_complaints": AssetIn(key=AssetKey(["complaints","sd_complaints"])),
       },
 automation_condition = AutomationCondition.eager()
)
def sd_complaints_summary(context, sd_complaints: gpd.GeoDataFrame):
    name = 'complaints_by_date'
    description = '''A daily count of the odor complaints from
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    complaints_gdf = sd_complaints
    complaint_groupby_date = complaints_gdf[complaints_gdf['nature_of_complaint'] == 'Odor'].groupby(
        by=['date', 'nature_of_complaint'],  as_index=False).agg(count=('date_received', 'count'))
    complaint_groupby_date['Icon'] = 'pin'

    filename = f"{output_path}/output/complaints_by_date"
    store_assets.dataframe_to_s3(complaint_groupby_date, filename, s3_resource, metadata=metadata)
    return complaint_groupby_date

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_latest_bydate",
       required_resource_keys={"s3"},
       ins={
           "sd_complaints_summary": AssetIn(key=AssetKey(["complaints","sd_complaints_summary"])),
       },
 automation_condition = AutomationCondition.eager()
)
def sd_complaints_latest_bydate(context, sd_complaints_summary: pd.DataFrame):
    name = 'complaints_latest_bydate'
    description = '''The last 90 days of daily count of the odor complaints from
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = sd_complaints_summary
    complaints_gdf['datetime'] = pd.to_datetime(complaints_gdf['date'], )
    start = datetime.datetime.now() - datetime.timedelta(days=90)
    complaint_last_90 = complaints_gdf[complaints_gdf['datetime'] >= start]
    complaint_last_90['Icon'] = 'pin'
    complaint_last_90.drop(columns=['datetime'], inplace=True)
    filename = f"{output_path}/output/complaints_latest_bydate"
    store_assets.dataframe_to_s3(complaint_last_90, filename, s3_resource, metadata=metadata)
    return complaint_last_90

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_by_subregional",
       required_resource_keys={"s3"},
       ins={
           "sd_complaints": AssetIn(key=AssetKey(["complaints","sd_complaints"])),
           "subregional_areas": AssetIn(key=AssetKey(["gis","subregional_areas"])),
       },
       automation_condition=AutomationCondition.eager() )

def sd_complaints_spatial_subregional(context, sd_complaints: gpd.GeoDataFrame, subregional_areas: gpd.GeoDataFrame):
    name = 'complaints_by_subregional'
    description = '''Complaints with joined with census subregional areas  
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = sd_complaints
    geo_gdf = subregional_areas

    complaint_with_tract_gdf = gpd.sjoin(
        geo_gdf,
        complaints_gdf,
        how='left',
        predicate='contains'
    )
    complaint_with_tract_gdf.dropna(how='any', subset=['x_coordinate', 'y_coordinate'], inplace=True)
    complaint_with_tract_gdf.reset_index(inplace=True)
    filename = f"{output_path}/output/complaints_with_subregional"
    store_assets.geodataframe_to_s3(complaint_with_tract_gdf.drop('geometry', axis='columns'), filename, s3_resource, metadata=metadata)

    complaint_by_tract = complaint_with_tract_gdf[complaint_with_tract_gdf['nature_of_complaint'] == 'Odor'].groupby(
        by=['sra', 'date', 'nature_of_complaint', 'geometry','name','globalid'], as_index=False).agg(count=('date', 'count'))
    complaint_by_tract['Icon'] = 'pin'
    complaint_with_tract_gdf=gpd.GeoDataFrame(complaint_by_tract)
    filename = f"{output_path}/output/complaints_by_tract"
    store_assets.geodataframe_to_s3(complaint_with_tract_gdf, filename, s3_resource)
    return complaint_with_tract_gdf

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_by_tract",
       required_resource_keys={"s3"},
       ins={
           "sd_complaints": AssetIn(key=AssetKey(["complaints","sd_complaints"])),
           "tracts": AssetIn(key=AssetKey(["gis","tracts"])),
       },
       automation_condition=AutomationCondition.eager() )

def sd_complaints_spatial_tract(context, sd_complaints: gpd.GeoDataFrame, tracts: gpd.GeoDataFrame):
    name = 'complaints_with_tract'
    description = '''Complaints with joined with census tracts areas
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = sd_complaints
    geo_gdf = tracts
    complaint_with_tract_gdf = gpd.sjoin(
        geo_gdf,
        complaints_gdf,
        how='left',
        predicate='contains'
    )
    complaint_with_tract_gdf=complaint_with_tract_gdf.dropna(how='any', subset=['x_coordinate', 'y_coordinate'])
    complaint_with_tract_gdf.reset_index(inplace=True)
    filename = f"{output_path}/output/complaints_with_tract"
    store_assets.geodataframe_to_s3(complaint_with_tract_gdf.drop('geometry', axis='columns'), filename, s3_resource, metadata=metadata)

    complaint_by_tract = complaint_with_tract_gdf[complaint_with_tract_gdf['nature_of_complaint'] == 'Odor'].groupby(
        by=['tract','date', 'nature_of_complaint','geometry'],  as_index=False).agg(count=('date', 'count'))
    complaint_by_tract['Icon'] = 'pin'
    complaint_with_tract_gdf = gpd.GeoDataFrame(complaint_by_tract)
    filename = f"{output_path}/output/complaints_by_tract"
    store_assets.geodataframe_to_s3(complaint_with_tract_gdf, filename, s3_resource)
    return complaint_with_tract_gdf
# @job()
# def complaints_daily_job():
#     sd_complaints(get_sd_complaints())

complaints_sensor_job = define_asset_job(
    "complaints_sensor", selection=[
        AssetKey(["complaints", "sd_complaints_raw"]),
        #AssetKey(["complaints", "sd_complaints"]),  # maybe we do not need this

    ]
)
# weekly updates seem to happen at 8 am, so lets check at 8:30.
# need to do an arcgis sensor to check use the layer /metadata, that will return xml
# @schedule(job=complaints_daily_job, cron_schedule="35 8 * * *", name="complaints_daily")
# def complaints_daily_schedule(context):
#     return RunRequest(
#     )

# simple sensor. Watches the metadata file, if the value for metadata.Esri.CreDate changes, it runs the job
@sensor(
    #job_name="complaints_daily_job",
    job=complaints_sensor_job,
          minimum_interval_seconds=3600,
          required_resource_keys={"slack"})
def complaints_data_sensor(context: SensorEvaluationContext):
    slack = context.resources.slack
    last_value = context.cursor if context.cursor else None
    meta_url= f"{sd_complaints_arcgis_base}metadata"
    response = requests.get(meta_url)
    if response.status_code == 200:
        xml = response.text
        root = ET.fromstring(xml)
        print(root.tag)
        create_date = root.find('./Esri/CreaDate').text
        if last_value != create_date:
            get_dagster_logger().info(f'complaints updated cursor to {create_date}')
            context.update_cursor(create_date)
            try:
                slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f'complaints updated to {create_date}')
            except Exception as e:
                get_dagster_logger().error('Slack post error for complaints updated')
            yield RunRequest(
                run_key=f"property_{create_date}",
                partition_key=str(datetime.datetime.now().year)
            )
            #slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f'complaints updated to {create_date}')
        else:
            return


