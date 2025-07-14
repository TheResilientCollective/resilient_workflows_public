import requests
import os
from dagster import ( asset,
                     get_dagster_logger ,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      TimeWindowPartitionsDefinition,
WeeklyPartitionsDefinition
                      )


import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from datetime import datetime, timedelta, date
import re
from ..utils.constants import ICONS
from ..utils import store_assets

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

    mpox_df["lat"] = mpox_df.geometry.y
    mpox_df["lon"] = mpox_df.geometry.x
    mpox_df['date'] = mpox_df.apply(lambda row: date.fromisocalendar(int(row['year']), int(row['week']), 1), axis=1)
    mpox_df['date'] = pd.to_datetime(mpox_df['date'])

    mpox_df.rename(columns={"m1": "current_week",
                            "m2": "previous_52_weeks__max",
                            "m3": "current_YTD__cummulative",
             "m4": "previous_YTD__cummulative",
                            "m1_flag": "current_week1_flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)
    mpox_df['current_week']= mpox_df['current_week'].fillna(0)
    mpox_df['previous_52_weeks__max']=mpox_df['previous_52_weeks__max'].fillna(0)
    mpox_df['current_YTD__cummulative']=mpox_df['current_YTD__cummulative'].fillna(0)
    mpox_df['previous_YTD__cummulative']=mpox_df['previous_YTD__cummulative'].fillna(0)
    mpox_df["key"] = mpox_df["label"] + '_' + mpox_df["year"] + '_' + mpox_df["week"] + '_' + mpox_df["location1"]
    mpox_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    mpox_df.drop(columns=["sort_order"], inplace=True)
    filename = f'{s3_output_path}/output/mpox_weekly'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    mpox_df=mpox_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/mpox_weekly_states'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    mpox_df.drop('geometry', axis=1, inplace=True)
    try:
        at_resource.upsert2Table(AIRTABLE_TABLE_ID, mpox_df, keyfields=['key'])
    except Exception as e:
        get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


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
    mpox_df=None
    for i in range(0,count,limit):
        mpox_url = f"{base_url}{query}&$offset={i}&$limit={limit}&{where}"
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

    mpox_df["lat"] = mpox_df.geometry.y
    mpox_df["lon"] = mpox_df.geometry.x
    mpox_df['date'] = mpox_df.apply(lambda row: date.fromisocalendar(int(row['year']), int(row['week']), 1), axis=1)
    mpox_df['date'] = pd.to_datetime(mpox_df['date'])

    mpox_df.rename(columns={"m1": "current_week",
                           "m2": "previous_52_weeks__max",
                           "m3": "current_YTD__cummulative",
   "m4": "previous_YTD__cummulative",
                            "m1_flag": "current_week1_flag",
                             "m2_flag": "previous_52_weeks__max__flag",
                             "m3_flag": "current_YTD__cummulative__flag",
                             "m4_flag": "previous_YTD__cummulative__flag"
    }, inplace=True)

    mpox_df['current_week']= mpox_df['current_week'].fillna(0)
    mpox_df['previous_52_weeks__max']=mpox_df['previous_52_weeks__max'].fillna(0)
    mpox_df['current_YTD__cummulative']=mpox_df['current_YTD__cummulative'].fillna(0)
    mpox_df['previous_YTD__cummulative']=mpox_df['previous_YTD__cummulative'].fillna(0)
    mpox_df["key"] = mpox_df["label"] + '_' + mpox_df["year"] + '_' + mpox_df["week"] + '_' + mpox_df["location1"]
    mpox_df.dropna(inplace=True, subset=['key']) # if a key is not generate
    mpox_df.drop(columns=["sort_order"], inplace=True)
    filename = f'{s3_output_path}/output/measles_weekly'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    mpox_df=mpox_df.dropna( subset=["lat", "lon"])

    filename = f'{s3_output_path}/output/measles_weekly_states'
    store_assets.geodataframe_to_s3(mpox_df, filename, s3_resource )

    # airtable
    #mpox_df["key"] = mpox_df["label"] + mpox_df["year"] +mpox_df["week"] +mpox_df["location1"]
    #keyfields = ['label', 'year', 'week', 'location1']
    mpox_df.drop('geometry', axis=1, inplace=True)
    try:
        at_resource.upsert2Table(AIRTABLE_TABLE_ID, mpox_df, keyfields=['key'])
    except Exception as e:
        get_dagster_logger().error(f" airtable failed measles_weekly {e} ")


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
    partition_key = weekly_partitions.get_partition_key_for_timestamp(context.scheduled_execution_time.timestamp())

    return RunRequest(
        partition_key=partition_key,

    )
