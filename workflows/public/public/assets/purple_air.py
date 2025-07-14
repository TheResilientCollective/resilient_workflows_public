import json
import os
import pandas as pd
import geopandas as gpd
import aqicalc as aqi
import requests
from dagster import ( asset, op,
                     get_dagster_logger,
                      AssetKey,sensor,AssetIn,
DailyPartitionsDefinition, define_asset_job,job, RunRequest, schedule,
AutomationCondition,
SensorEvaluationContext
                      )
from shapely import Point
from ..utils import store_assets

output_path = 'tijuana/airquality/purpleair'
# Here we store our API read key in a string variable that we can reference later.
baseurl = 'https://api.purpleair.com/'
my_api_read_key = os.environ.get('PURPLE_AIR_API_KEY_READ')
if my_api_read_key is None:
    raise ValueError("PURPLE_AIR_API_KEY_READ environment variable not set")
read_headers = {'X-API-Key':my_api_read_key ,
           'Content-Type': 'application/json'}
my_api_write_key = os.environ.get('PURPLE_AIR_API_KEY_WRITE')
if my_api_write_key is None:
    raise ValueError("PURPLE_AIR_API_KEY_WRITE environment variable not set")
write_headers = {'X-API-Key':my_api_write_key ,
           'Content-Type': 'application/json'}

GROUPID = os.environ.get('PURPLE_AIR_GROUPID', 2644)

# badly assumed that if name was the same, group_id would be the same. Nope.
# so now a fixed group_id will be used.
# @op()
# def createMemberGroup(name ) -> int:
#     create_group_url = f'{baseurl}v1/groups'
#     group_params = {"name": name}
#     r = requests.post(create_group_url, data=json.dumps(group_params), headers=write_headers, )
#     if r.status_code == 200 or r.status_code == 201:
#         groups = r.text
#         j = json.loads(groups)
#         group_id = j["group_id"]
#         get_dagster_logger().error(f'group : {memberGroup} {groups} ')
#         return group_id
#     else:
#         get_dagster_logger().error(f'{r.status_code} {r.text}')
#         raise Exception(r.text)
@op()
def getSensorsForLocations(nwlat=32.7, nwlng= -117.2, selat=32.5, selng= -117.0) -> gpd.GeoDataFrame:
    get_sensors_url = f'{baseurl}v1/sensors'
    fields = ["sensor_index",
              "last_modified",
              "name",
              "latitude",
              "longitude",
              "location_type"
              ]
    fields = ",".join(fields)

    sensors_params = {"nwlat": nwlat, "nwlng": nwlng, "selat": selat, "selng": selng, 'fields': fields}

    r = requests.get(get_sensors_url, headers=read_headers, params=sensors_params)
    if r.status_code == 200 or r.status_code == 201:
        sensors = r.json()
        get_dagster_logger().info(f'group : {sensors}  ')
        df = pd.DataFrame(sensors["data"], columns=sensors["fields"])
        df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        gdf_outside = gdf[gdf['location_type'] == 0]
        return gdf_outside
    else:
        get_dagster_logger().error(f'{r.status_code} {r.text}')
        raise Exception(r.text)
@op()
def addMembersToGroup(group_id, members:gpd.GeoDataFrame):
    add_members_url = f'{baseurl}v1/groups/{group_id}/members'
    get_dagster_logger().info(f' addMembersToGroup  group {group_id}')
    for member in members["sensor_index"]:
        add_members_params = {"group_id": group_id, 'sensor_index': member}
        r = requests.post(add_members_url, headers=write_headers, data=json.dumps(add_members_params))
        if r.status_code == 200 or r.status_code == 201:
            get_dagster_logger().info(f' added member {member} to group{group_id} {r.status_code} {r.text}')
        else:
            get_dagster_logger().error(f'failed to add member {member} to group{group_id} {r.status_code} {r.text}')

@asset(group_name="tijuana",key_prefix="airquality",
       name="memberGroup",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def memberGroup(context):
    # group_id=createMemberGroup("Resilient_SouthRegion")
    group_id =GROUPID
    nwlat = 32.7
    nwlng = -117.2
    selat = 32.5
    selng = -117.0
    gdf_outside=getSensorsForLocations(nwlat, nwlng, selat, selng)
    addMembersToGroup(group_id, gdf_outside)
    return group_id

@asset(group_name="tijuana",key_prefix="airquality",
       name="purple_air_data",
       required_resource_keys={"s3"},
       #deps=[AssetKey(['airquality','purple_group' ])],
       ins={"memberGroup": AssetIn(key_prefix=['airquality' ])},
       automation_condition=AutomationCondition.eager())
def getGroupData(context, memberGroup ):
    s3_resource = context.resources.s3
    name = 'purple air current data'
    description = '''Collects data from the Purple Air API u
        '''
    source_url = baseurl
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    get_members_data_url = f'{baseurl}v1/groups/{memberGroup}/members'
    add_members_url = f'{baseurl}v1/groups/{memberGroup}/members'
    get_dagster_logger().info(f'get data from group: {memberGroup} {get_members_data_url}')
    fields = ["sensor_index", "pm2.5", "pm2.5_60minute","pm10.0","voc", "humidity", "temperature",
              "last_modified",
              "name",
              "latitude",
              "longitude",
              "location_type"
              ]
    fields = ",".join(fields)
    members_params = {"group_id": memberGroup, "fields": fields, 'location_type': 0}
    r = requests.get(get_members_data_url, headers=read_headers, params=members_params)
    if r.status_code == 200 or r.status_code == 201:
        sensors = r.json()
        get_dagster_logger().info(f'get members data from group: {memberGroup} {sensors}')
        df = pd.DataFrame(sensors["data"], columns=sensors["fields"])
        df['geometry'] = df.apply(lambda row: Point(row['longitude'], row['latitude']), axis=1)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
        gdf['AQI'] = gdf.apply(lambda row: float(aqi.to_aqi([(aqi.POLLUTANT_PM25, row.get('pm2.5')),
                                                     (aqi.POLLUTANT_PM10, row.get('pm10.0'))]
                                                    )), axis=1)
        filename = f"{output_path}/output/current_data"
        store_assets.geodataframe_to_s3(gdf, filename, s3_resource, metadata=metadata)
        return gdf
    else:
        get_dagster_logger().error(f'{r.status_code} {r.text}')
        raise Exception(f'{r.status_code} {r.text}')

purple_air_job = define_asset_job(
    "purple_air_current", selection=[AssetKey(["airquality", "purple_air_data"])]
)
@schedule(job=purple_air_job,
          #cron_schedule="@hourly",
          cron_schedule="20 * * * *", # run on the 20th minute. File is produced on the :10
          name="purple_air_current",
          execution_timezone="America/Los_Angeles",)
def purple_air_schedule(context):
    return RunRequest(
    )
