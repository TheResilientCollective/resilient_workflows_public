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
DailyPartitionsDefinition, define_asset_job,job, RunRequest, schedule,
AutomationCondition,
SensorEvaluationContext,AssetCheckSpec,
                      AssetCheckResult,
                      asset_check,
                      AssetCheckExecutionContext
                      )
from ..utils.constants import ICONS
from .gis import subregions
from ..utils import store_assets
import datetime
import pytz

import xml.etree.ElementTree as ET

# docker env has RESILIENT_ prefix
#             - SLACK_CHANNEL=${RESILIENT_SLACK_CHANNEL:-"#test"}
#             - SLACK_TOKEN=${RESILIENT_SLACK_TOKEN}
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#test")

output_path="tijuana/sd_complaints"

# complaints_daily_job = define_asset_job(
#     "complaints_daily", selection=[AssetKey(["complaints", "sd_complaints"])]
# )

sd_complaints_arcgis_base = "https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/"

def dropUnnecessaryColumns(df):
    return df.drop( columns=['response_duration__hours_',
                            "investigation_outcome",
                            "cross_street___intersection",
                            'Icons',
                            ]
                    )

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_raw",
       required_resource_keys={"s3"} ,
       automation_condition=AutomationCondition.eager() )
def get_sd_complaints(context  ) -> str:
    #sd_complaints_arcgis_base="https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/"
    sd_complaints_arcgis_action="query"
    sd_complaints_parmam1="where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam="
    sd_complaints_fields= "outFields=nature_of_complaint%2C+date_received%2C+record_number%2C+record_status%2Cinvestigation_outcome%2C+response_duration__hours_%2C+x_coordinate%2C+y_coordinate%2C+cross_street___intersection%2C+zip%2C+city"
    sd_complaints_parmam2="&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnCentroid=false&timeReferenceUnknownClient=false&sqlFormat=none&resultType=&datumTransformation=&lodType=geohash&lod=&lodSR=&f=geojson"
    sd_complaints_url=f"{sd_complaints_arcgis_base}/{sd_complaints_arcgis_action}?{sd_complaints_parmam1}&{sd_complaints_fields}&{sd_complaints_parmam2}"
    sd_complaints_url = "https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=nature_of_complaint%2C+date_received%2C+record_number%2C+record_status%2Cinvestigation_outcome%2C+response_duration__hours_%2C+x_coordinate%2C+y_coordinate%2C+cross_street___intersection%2C+zip%2C+city&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnCentroid=false&timeReferenceUnknownClient=false&sqlFormat=none&resultType=&datumTransformation=&lodType=geohash&lod=&lodSR=&f=geojson&resultOffset=3000"
    path=f"{output_path}/raw/complaints.json"
    s3_resource = context.resources.s3
    sd_complaints_response = requests.get(sd_complaints_url)
    sd_complaints = sd_complaints_response.text

    s3_resource.putFile_text( sd_complaints, path=path)
    return sd_complaints

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints",
       required_resource_keys={"s3"},
       deps=[AssetKey(["complaints","sd_complaints_raw"])]
       ,
       automation_condition=AutomationCondition.eager())
def sd_complaints(context):
    name='sd_complaints'
    description='''
    Data from San Diego Air Pollution Control District Complaints ArcGIS service
    '''
    source_url='https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name,description=description, source_url=source_url)
    s3_resource = context.resources.s3
    json_txt = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints_raw"]))
    featueres = json.loads(json_txt)
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
    filename = f"{output_path}/output/complaints"
    store_assets.geodataframe_to_s3(complaints_gdf, filename, s3_resource, metadata=metadata)


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
    passed = time_difference <= datetime.timedelta(days=3)

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
       deps=[AssetKey(["complaints", "sd_complaints"])]
    ,
       automation_condition=AutomationCondition.eager())
def sd_complaints_90_days(context):
    datestart = datetime.datetime.now(pytz.timezone("America/Los_Angeles")) - datetime.timedelta(days=90)
    name = 'sd_complaints_90_days'
    description = '''
    Data from San Diego Air Pollution Control District Complaints ArcGIS service
    '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    complaints_gdf = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints"]))
    complaints_90_gdf = complaints_gdf[complaints_gdf['datetime']> datestart.strftime('%Y-%m-%d')]
    complaints_90_gdf= dropUnnecessaryColumns(complaints_90_gdf)
    filename = f"{output_path}/output/latest/complaints"
    store_assets.geodataframe_to_s3(complaints_90_gdf, filename, s3_resource, metadata=metadata)

    return complaints_90_gdf
@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_summary",
       required_resource_keys={"s3"},
       deps=[AssetKey(["complaints","sd_complaints"])],

 automation_condition = AutomationCondition.eager()
)
def sd_complaints_summary(context):
    name = 'complaints_by_date'
    description = '''A daily count of the odor complaints from 
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    complaints_gdf = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints"]))
    complaint_groupby_date = complaints_gdf[complaints_gdf['nature_of_complaint'] == 'Odor'].groupby(
        by=['date', 'nature_of_complaint'],  as_index=False).agg(count=('date_received', 'count'))
    complaint_groupby_date['Icon'] = 'pin'

    filename = f"{output_path}/output/complaints_by_date"
    store_assets.dataframe_to_s3(complaint_groupby_date, filename, s3_resource, metadata=metadata)
    return complaint_groupby_date

@asset(group_name="tijuana",key_prefix="complaints",
       name="sd_complaints_latest_bydate",
       required_resource_keys={"s3"},
       deps=[AssetKey(["complaints","sd_complaints_summary"])],

 automation_condition = AutomationCondition.eager()
)
def sd_complaints_latest_bydate(context):
    name = 'complaints_latest_bydate'
    description = '''The last 90 days of daily count of the odor complaints from 
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints_summary"]))
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
       deps=[AssetKey(["complaints","sd_complaints"]), AssetKey(["gis","subregional_areas"])]
       , automation_condition=AutomationCondition.eager() )

def sd_complaints_spatial_subregional(context):
    name = 'complaints_by_subregional'
    description = '''Complaints with joined with census subregional areas  
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints"]))
    geo_gdf = context.repository_def.load_asset_value(AssetKey([f"gis", "subregional_areas"]))

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
       deps=[AssetKey(["complaints","sd_complaints"]), AssetKey(["gis","tracts"])]
       , automation_condition=AutomationCondition.eager() )

def sd_complaints_spatial_tract(context):
    name = 'complaints_with_tract'
    description = '''Complaints with joined with census tracts areas  
     the from San Diego Air Pollution Control District Complaints ArcGIS service
        '''
    source_url = 'https://gis-public.sandiegocounty.gov/arcgis/rest/services/Hosted/SDAPCD_Complaints/FeatureServer/0/'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    complaints_gdf = context.repository_def.load_asset_value(AssetKey([f"complaints", "sd_complaints"]))
    geo_gdf = context.repository_def.load_asset_value(AssetKey([f"gis", "tracts"]))
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
            yield RunRequest(run_key=f"property_{create_date}")
            #slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f'complaints updated to {create_date}')
        else:
            return


