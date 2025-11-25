import logging

import pandas as pd
import csv
import os
from datetime import datetime, timedelta

import pytz
import requests

from dagster import (asset, op,
                     get_dagster_logger,
                     AssetKey, asset_sensor,
                     DailyPartitionsDefinition,
                     schedule, RunRequest, define_asset_job, AssetKey, AssetIn,
                     AutomationCondition,
                     AssetCheckSpec, AssetCheckResult, asset_check, AssetCheckExecutionContext,
                     TimeWindowPartitionsDefinition
                     )
from ..utils.constants import ICONS
from ..resources import minio

import pandas as pd
import geopandas as gpd
import csv
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pytz
import glob
from string import Template
import requests
from urllib.request import urlopen
from io import StringIO
from ..utils import store_assets

# docker env has RESILIENT_ prefix
#             - SLACK_CHANNEL=${RESILIENT_SLACK_CHANNEL:-"#test"}
#             - SLACK_TOKEN=${RESILIENT_SLACK_TOKEN}
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#test")

daily_apcd_partitions = DailyPartitionsDefinition(start_date="2025-08-01")
start_date_apcd=datetime(2023,1,1)
yearly_apcd_partitions = TimeWindowPartitionsDefinition(start=start_date_apcd,fmt='%Y',
cron_schedule = "@yearly"
)

airnow_station_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/Monitoring_Site_Locations_V2.dat"

base_url = 'http://jtimmer.digitalspacemail17.net/data/'
current_file = 'current.CSV'
pattern_file = 'yesterday_$filedate.CSV'

# yesterday_20241002.CSV

#s3_bucket = os.getenv('PUBLIC_BUCKET', 'resilient-public')# defined in s3
s3_data_path = 'tijuana/sd_apcd_air/source'
s3_raw_path = 'tijuana/sd_apcd_air/source'
s3_output_path = 'tijuana/sd_apcd_air/output'
s3_lastest_key="tijuana/sd_apcd_air"

so2_parameter = '28 SO2 Tr PPB'
h2s_parameter = '07 H2S PPB'

outputs = [
    {'parameter': "01 OZONE PPM", 'name': "01 OZONE PPM", 'file': "o2"},
    {'parameter': "28 SO2 Tr PPB", 'name': "S02 PPB", 'file': "s02"},
    {'parameter': "07 H2S PPB", 'name': "H2S PPM", 'file': "h2s2"},
    {'parameter': "11 PM2.5 �g/M3", 'name': "PM2.5 microg/M3", 'file': "pm25"},
    {'parameter': "PM10 STD", 'name': "PM10 STD", 'file': "pm10"},
    ]


@asset(group_name="tijuana",key_prefix="apcd",
       name="current_apcd", required_resource_keys={"s3", "airtable"},
       deps=[AssetKey([f"apcd", "locations"])]
  )
def current(context) -> gpd.GeoDataFrame:
    name = 'current_apcd'
    description = '''Air Quality data for today and yesterday
        Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
        '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    # using two, since at 1 am, the previous day is not yet available during daylight savings
    # this causes a missed midnight H2S event
    yesterday = files_root(num_days=2)
    # get_dagster_logger().info(f'yesteday {yesterday} ')
    # filelist = yesterday.append(f'{base_url}{current_file}')
    get_dagster_logger().info(f'current file paths {yesterday} ')
    output_df = process_csv_files(yesterday)
    if output_df.empty:
        raise Exception("No data for today")
    #output_csv = output_df.to_csv(index=False)
    # filename= f'{s3_output_path}/current.csv'
    # s3_resource.putFile_text(data=output_csv, path=filename)
    locations_gdf = context.repository_def.load_asset_value(AssetKey([f"apcd", "locations"]))
    output_gdf = locations_gdf.merge(output_df, how='inner', left_on='SiteName', right_on='Site Name',
                                     suffixes=('', '_y'))
    filename = f'{s3_output_path}/current'
    store_assets.geodataframe_to_s3(output_gdf, filename, s3_resource , metadata=metadata)

    name = 'lastvalue_h2s'
    description = '''Air Quality data for H2S for the most recent sites. This is used mainly for locating active stations on the map
            Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
            '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    latest_h2s_df = output_gdf[output_gdf['Parameter'] == h2s_parameter]
    latest_h2s_df['level'] = latest_h2s_df['Result'].apply(lambda r: h2s_guidance(r))
    latest_h2s_df= latest_h2s_df.groupby(['Parameter', 'Site Name', ], as_index=False).tail(1)
# use this to get last levels
    #  latest_h2s_df= latest_h2s_df.groupby(['Parameter', 'Site Name','levels' ], as_index=False).tail(1)
    # filename = f'{s3_output_path}/latest_h2s.csv'
    # s3_resource.putFile_text(data=h2s.to_csv( index=False), path=filename)
    filename = f'{s3_output_path}/lastvalue_h2s'
    store_assets.geodataframe_to_s3(latest_h2s_df, filename, s3_resource , metadata=metadata)

    return output_gdf


# Assuming current asset returns a GeoDataFrame
# and has a 'Date with time' column with timezone-aware datetimes.

@asset_check(asset=AssetKey(['apcd','current_apcd']),
           #  specs=[AssetCheckSpec(name="current_freshness_check")]
             )
def current_freshness_check(context: AssetCheckExecutionContext, current):
    """
    Checks if the most recent data point in the current asset is
    no older than three hours ago.
    """
    if current.empty:
        return AssetCheckResult(
            passed=False,
            metadata={
                "reason": "Asset is empty, cannot determine freshness."
            }
        )

    # Find the most recent datetime in the asset
    # Assuming 'Date with time' is already in ISO format (string)
    most_recent_datetime_str = current['Date with time'].max()

    # Convert the string to a timezone-aware datetime object
    most_recent_datetime = datetime.fromisoformat(most_recent_datetime_str)

    # Get the current datetime in the same timezone as the data
    current_datetime = datetime.now(tz=ZoneInfo("America/Los_Angeles"))

    # Calculate the difference
    time_difference = current_datetime - most_recent_datetime

    # Check if the difference is greater than three hours
    passed = time_difference <= timedelta(hours=3)

    metadata = {
        "most_recent_datetime": str(most_recent_datetime),
        "current_datetime": str(current_datetime),
        "time_difference": str(time_difference),
    }

    if not passed:
        metadata["reason"] = "Data is older than three hours."
    return AssetCheckResult(passed=passed, metadata=metadata)

@asset(group_name="tijuana", key_prefix="apcd",
       name="h2s_warnings", required_resource_keys={"s3", "airtable", "slack"},
       deps=[AssetKey(['apcd','current_apcd']), AssetKey(['apcd', 'locations'])],
        automation_condition=AutomationCondition.eager()
       )
def highh2s(context):
    name = 'h2s_warnings'
    description = '''Records where H2S exceeds the standard for Yesterday and Today

               Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
               '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    slack = context.resources.slack
    try:
        last_h2s = context.repository_def.load_asset_value(AssetKey([f"apcd", "h2s_warnings"]))
        get_dagster_logger().info(f'h2s events {last_h2s} ')
        if (len(last_h2s) != 0):
            last_h2s_date = last_h2s['Date with time'].max()
            get_dagster_logger().info(f'last h2s date from asset {datetime.min} ')
        else:
            last_h2s_date = datetime.min
            get_dagster_logger().info(f'last h2s date set to  {datetime.min} ')
    except:
        last_h2s = gpd.GeoDataFrame()
        last_h2s_date = datetime.min
        get_dagster_logger().info(f'issue starting the last date datetime min ')
    current = context.repository_def.load_asset_value(AssetKey([f"apcd", "current_apcd"]))
    h2s = current[current['Parameter'] == h2s_parameter]
    h2s['level'] = h2s['Result'].apply(lambda r: h2s_guidance(r))
    h2s.dropna(subset=['Result'], inplace=True)
    h2s=h2s[h2s['Result']>=30 ]
    if len(h2s) >0:
        get_dagster_logger().info(f'h2s events {len(h2s)} ')
        #filename = f'{s3_output_path}/warnings/h2s.json'
        #s3_resource.putFile_text(data=h2s.to_json( index=False, orient='records'), path=filename)
        filename = f'{s3_output_path}/warnings/h2s'
        store_assets.dataframe_to_s3(h2s, filename, s3_resource, formats=['csv'], metadata=metadata)
        try:
            if last_h2s is None:
                get_dagster_logger().info('Last h2s is None')
            else:
                get_dagster_logger().info(f'last h2s {last_h2s.to_csv()} ')
                get_dagster_logger().info(f' h2s {h2s.to_csv()} ')
                #get_dagster_logger().info(f' previous h2s {last_h2s.dtypes} ')
                #get_dagster_logger().info(f' h2s {h2s.dtypes} ')
                #diff_df= last_h2s.compare(h2s)
                rows = h2s[h2s['Date with time']>str(last_h2s_date)]
                get_dagster_logger().info(f' new data {len(rows)} rows ')
                for i,h in rows.iterrows():
                    msg = f":wave: {h['Site Name']} high h2s {h['Result']} at {h['Date with time']} "
                    get_dagster_logger().info(f'slack {msg} ')
                    try:
                        slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=msg)
                    except Exception as e:
                        get_dagster_logger().error(f'slack error {e}')
        except Exception as e:
            get_dagster_logger().error(f'issue with h2s comparisons {e} ')
    else:
        get_dagster_logger().info(f'no h2s events')
    return h2s

@asset(group_name="tijuana", key_prefix="apcd",
       name="hs2_latest", required_resource_keys={"s3", "airtable", "slack"},
       deps=[AssetKey(['apcd','current_apcd']), AssetKey(['apcd', 'locations'])],
        automation_condition=AutomationCondition.eager()
       )
def hs2_latest(context):


    s3_resource = context.resources.s3
    slack = context.resources.slack

    current = context.repository_def.load_asset_value(AssetKey([f"apcd", "current_apcd"]))

    name = 'hs2_current'
    description = '''Records for H2S for Yesterday and Today

                      Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
                      '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    h2s = current[current['Parameter'] == h2s_parameter]
    h2s['level'] = h2s['Result'].apply(lambda r: h2s_guidance(r))
    h2s.dropna(subset=['Result'], inplace=True)
    current_df = h2s.drop_duplicates(keep='last', subset=['Site Name'])
    filename = f'{s3_output_path}/hs2_current'
    store_assets.geodataframe_to_s3(current_df, filename, s3_resource, formats=['csv','json','geojson'],metadata=metadata)

    name = 'hs2_lastday'
    description = '''Records for H2S for Yesterday and Today

                      Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
                      '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    last_gdf = h2s[h2s['Parameter'] == h2s_parameter]
    last_gdf.sort_values(by=['Site Name','Date with time' ], inplace=True)
    #last_gdf = last_gdf.groupby(['Parameter', 'Site Name', ], group_keys=False, as_index=False).tail(3)
    filename = f'{s3_output_path}/hs2_lastday'
    store_assets.geodataframe_to_s3(last_gdf, filename, s3_resource, formats=['csv','json','geojson'])
    # filename = f'{s3_output_path}/warnings/hs2_lastdayv2'
    # store_assets.dataframe_to_s3(last_gdf, filename, s3_resource, formats=['csv','json'])

    return h2s

@asset(group_name="tijuana",key_prefix="apcd",
       name="all_sd_airquality", required_resource_keys={"s3", "airtable"},
       deps=[AssetKey(['apcd', 'locations'])],
  )
def apcd_all(context, ) -> pd.DataFrame:
    name = 'all_sd_airquality'
    description = '''Air Quality Monitoring Site for all APCD locations

                          Data from San Diego Air Pollution Control District Air Quality Monitoring Sites
                          '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    #earliest = context.asset_partition_key_for_output()
    earliest=os.environ.get('APCD_EARLIEST','2024-10-02' )
    earliest_date=datetime.fromisoformat(earliest).replace(tzinfo=timezone(timedelta(hours=-7)))
    num_days = datetime.now(tz=ZoneInfo("America/Los_Angeles")) - earliest_date
    get_dagster_logger().info(f'days to get from {earliest_date} {num_days.days}' )
    file_paths = files_root(num_days=num_days.days)
    get_dagster_logger().info(f'file paths {file_paths} ' )
    # Process the files
    output_df = process_csv_files(file_paths)

    #output_df.to_csv( index=False)
    # filename = f'{s3_output_path}/all.csv'
    # s3_resource.putFile_text(data=output_df.to_csv( index=False), path=filename)
    filename = f'{s3_output_path}/all'
    store_assets.geodataframe_to_s3(output_df, filename, s3_resource, metadata=metadata )
    return output_df


@asset(group_name="tijuana", key_prefix="apcd",
       name="day", required_resource_keys={"s3", "airtable"},
       partitions_def=daily_apcd_partitions
       )
def get_oneday(context, ) -> pd.DataFrame:
    s3_resource = context.resources.s3
    filedate = context.asset_partition_key_for_output()

    #earliest=os.environ.get('APCD_EARLIEST','2024-10-02' )
    #earliest_date=datetime.fromisoformat(earliest)
    #num_days = datetime.now() - earliest_date
    #get_dagster_logger().info(f'days to get from {earliest_date} {num_days.days}' )
    #file_paths = files_root(num_days=num_days.days)
    #print(file_paths)
    # Process the files
    output_df = process_csv_files([filedate])
    #output_df.to_csv( index=False)
    filename = f'{s3_output_path}/raw/apcd_{filedate}.csv'
    s3_resource.putFile_text(data=output_df.to_csv( index=False), path=filename)

    return output_df

def generateLongName(row):
    get_dagster_logger().debug(f'LongName row: {row}')
    if ( pd.isna(row.get('LongName')) or row.get('LongName')==''):
        longName = row.get('SiteName')
        get_dagster_logger().debug(f'LongName longName: {longName}')
        if ( pd.notna(longName) ):
            return longName.title()
        else:
            return None
    else:
        return row.get('LongName')

@asset(group_name="tijuana", key_prefix="apcd",
       name="locations", required_resource_keys={"s3", "airtable"},
automation_condition=AutomationCondition.eager()
       )
def get_airnow_locations(context, ) -> pd.DataFrame:
    s3_resource = context.resources.s3
    locations_df = pd.read_csv(airnow_station_url, sep='|', on_bad_lines='warn')
    locations_df = locations_df[locations_df['Status'] == 'Active']
    #SiteName,Latitude,Longitude,AgencyName,geometry
    locations_df = locations_df[['SiteName', 'Latitude', 'Longitude', 'AgencyName']].drop_duplicates(['SiteName'])
    gs = gpd.GeoSeries.from_xy(locations_df['Longitude'], locations_df['Latitude'])
    locations_gdf = gpd.GeoDataFrame(locations_df,
                                     geometry=gs,
                                     crs='EPSG:4326')
    locations_gdf['SiteName'] = locations_gdf['SiteName'].str.upper()
    filename = f'{s3_data_path}/airnow_locations'
    store_assets.geodataframe_to_s3(locations_gdf, filename, s3_resource )
    sites_csv = """LongName,SiteName,Latitude,Longitude,AgencyName
Berry Elementary School,NESTOR - BES, 32.567097, -117.090656,San Diego APCD
Imperial Beach Civic Center,IB CIVIC CTR, 32.576139,  -117.115361,San Diego APCD
El Cajon - Lexington Elementary School,EL CAJON LES, 32.789561,  -116.944222,San Diego APCD
        """
    sites_df = pd.read_csv(StringIO(sites_csv), sep=',', on_bad_lines='warn')
    geom = gpd.points_from_xy(sites_df.Longitude, sites_df.Latitude, )
    sites_gdf = gpd.GeoDataFrame(sites_df, geometry=geom, crs='EPSG:4326')
    locations2_gdf = pd.concat([locations_gdf, sites_gdf])
    get_dagster_logger().debug(f'locations2_gdf: {locations2_gdf}')
    locations2_gdf['LongName'] = locations2_gdf.apply(generateLongName, axis=1)
    filename = f'{s3_data_path}/all_locations'
    store_assets.geodataframe_to_s3(locations2_gdf, filename, s3_resource )
    return locations2_gdf

@asset(group_name="tijuana",key_prefix="apcd",
       name="subset_h2s_s02", required_resource_keys={"s3", "airtable"},
       deps=[AssetKey(["apcd","all_sd_airquality"]), AssetKey(["apcd","locations"])],
       automation_condition=AutomationCondition.eager()
  )
def generate_apcd(context):
    name = 'subset_h2s_s02'
    description = '''H2S and SO2 subsets of the Air Quality Monitoring Data from San Diego Air Pollution Control District
                          '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    interface_days = 30
    s3_resource = context.resources.s3
    output_df =context.repository_def.load_asset_value(AssetKey([f"apcd","all_sd_airquality"]))
    locations_gdf = context.repository_def.load_asset_value(AssetKey([f"apcd", "locations"]))
    output_gdf = locations_gdf.merge(output_df, how='inner', left_on='SiteName', right_on='Site Name',
                                      suffixes=('', '_y'))
    h2s = output_gdf[output_gdf['Parameter'] == h2s_parameter]
    h2s['level'] = h2s['Result'].apply(lambda r: h2s_guidance(r))

    # filename = f'{s3_output_path}/h2s.csv'
    # s3_resource.putFile_text(data=h2s.to_csv( index=False), path=filename)
    filename = f'{s3_output_path}/h2s'
    store_assets.geodataframe_to_s3(h2s, filename, s3_resource, metadata=metadata )

    so2 = output_gdf[output_gdf['Parameter'] == so2_parameter]
    so2.to_csv(index=False)
    # filename = f'{s3_output_path}/s02.csv'
    # s3_resource.putFile_text(data=so2.to_csv(index=False), path=filename)
    filename = f'{s3_output_path}/s02'
    store_assets.geodataframe_to_s3(so2, filename, s3_resource, metadata=metadata )

    date_30 = (datetime.now() - timedelta(days=interface_days)).isoformat()
    last_30_df=output_gdf[output_gdf['Date with time']>date_30]

    h2s = last_30_df[last_30_df['Parameter'] == h2s_parameter]
    h2s['level'] = h2s['Result'].apply(lambda r: h2s_guidance(r))
    # filename = f'{s3_output_path}/latest_h2s.csv'
    # s3_resource.putFile_text(data=h2s.to_csv( index=False), path=filename)
    filename = f'{s3_output_path}/h2s_30days'
    store_assets.geodataframe_to_s3(h2s, filename, s3_resource )

    return last_30_df



def files_root(base_url='http://jtimmer.digitalspacemail17.net/data/', filepattern='yesterday_$filedate.CSV', num_days=90):
    today = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    dates = [(today - timedelta(days=i)).strftime('%Y%m%d') for i in range(num_days)]
    template_string = Template(filepattern)
    filenames = [template_string.safe_substitute(filedate=s) for s in dates]
    filenames.append(current_file)
    urls = [f'{base_url}{f}' for f in filenames]
    return urls

def h2s_guidance(result):
    levels=[{ 'min':0, 'max':5, 'level':"green"},
            { 'min':5, 'max':30, 'level':"yellow"},
            { 'min':30, 'max':27000, 'level':"orange"},
            { 'min':27000, 'max': None, 'level':"purple"}]
    if pd.isna(result) or result == '':
        return 'white'
    else:
        result = float(result)
    for level in levels:
        if level['max'] is None:
            if result >= level['min']:
                return level['level']
        elif result >= level['min'] and result < level['max']:
            return level['level']

def process_csv_files(file_paths):
    transformed_data = []

    with requests.Session() as s:
        # Set user agent to identify the Python client and project
        s.headers.update({
        #    'User-Agent': 'python-requests/2.28.2 (UCSD-Resilient-Environmental-Monitoring)'
         #  'User-Agent':  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
           'User-Agent':  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 python-requests/2.28.2 (UCSD-Resilient-Environmental-Monitoring)'
        })
        for file_path in file_paths:
            # Read the date from the third row
            get_dagster_logger().info(f'get file {file_path}')

            response = s.get(file_path)
            if response.status_code == 200:
                get_dagster_logger().info(f'response {response.status_code}')
                data = response.text
                lines = data.splitlines()

                date_str = lines[2]  # third line 0 base
                if ',' in date_str:
                    date_str = date_str.strip().split('),')[1]  # Get date from third row
                    # Parse the date
                date = datetime.strptime(date_str.strip(), '%m/%d/%Y')
                date =pytz.timezone("America/Los_Angeles").localize(date)
                get_dagster_logger().info(f'date dst {date.dst()}')
                hours_header = lines[3]  # First row with parameter names
                parameter_header = lines[4]  # Skip second row parmeters
                # next(csv_reader)  # Skip third row (date)
                # parameter_header = next(csv_reader)  # Fourth row with hour headers

                # Find the index where hour columns start
                hour_start_index = hours_header.index('0')
                # parameter_index = parameter_header.index('Parameter')
                # site_index = parameter_header.index('SiteName')
                parameter_index = 0
                site_index = 1
                # Process each row
                for row in lines[5:]:
                    row = row.strip().split(',')
                    if not row or row[0] == 'Parameter':  # Skip empty rows or new parameter headers
                        continue

                    site_name = row[site_index]

                    # Find the corresponding parameter
                    # parameter = None
                    # for i in range(len(hours_header)):
                    #     if hours_header[i] and row[i]:
                    #         parameter = hours_header[i]
                    #         break
                    if row[parameter_index] and len(row[parameter_index]) > 0:
                        parameter = row[parameter_index]

                    if not parameter:
                        continue

                    # Process each hour's result
                    for hour in range(24):
                        result = row[hour_start_index + hour].strip()
                        if result and len(result) > 0:
                            value = result
                            try:
                                qualifier = ''
                                if '<=' in value:
                                    value = value.replace('<=', '')
                                    qualifier = "<="
                                if '<' in value:
                                    value = value.replace('<', '')
                                    qualifier = "<"
                                if '>' in value:
                                    value = value.replace('>', '')
                                    qualifier = ">"
                                date_time = date + timedelta(hours=hour) + date.dst()
                                if len(value) > 0:
                                    value = float(value)

                                    transformed_data.append({
                                        'Parameter': parameter,
                                        'Site Name': site_name,
                                        'Date with time': date_time.isoformat(),  # ('%Y-%m-%d %H:%M'),
                                        'Result': float(value),
                                        'Qualifier': qualifier,
                                        'Original Value': result
                                    })
                                else:
                                    transformed_data.append({
                                        'Parameter': parameter,
                                        'Site Name': site_name,
                                        'Date with time': date_time.isoformat(),  # ('%Y-%m-%d %H:%M'),
                                        'Result': None,
                                        'Qualifier': qualifier,
                                        'Original Value': result
                                    })
                            except ValueError:
                                get_dagster_logger().debug(f' "{result}" is not a float')
                                transformed_data.append({
                                    'Parameter': parameter,
                                    'Site Name': site_name,
                                    'Date with time': date_time.isoformat(),  # ('%Y-%m-%d %H:%M'),
                                    'Result': None,
                                    'Qualifier': '',
                                    'Original Value': result
                                })
            else:
                get_dagster_logger().error(f'get file {file_path} {response.status_code} {response.text}' )

    # Create DataFrame from transformed data
    output_df = pd.DataFrame(transformed_data)
    output_df["Parameter"] = output_df["Parameter"].astype("category")
    output_df["Site Name"] = output_df["Site Name"].astype("category")
    output_df['Icons'] = ICONS['beach']
    return output_df

apcd_current_job = define_asset_job(
    "apcd_current", selection=[AssetKey(["apcd", "current_apcd"])]
)
@schedule(job=apcd_current_job,
          #cron_schedule="@hourly",
          cron_schedule="20 * * * *", # run on the 20th minute. File is produced on the :10
          name="apcd_current",
          execution_timezone="America/Los_Angeles",)
def apcd_current_schedule(context):
    return RunRequest(
    )


apcd_all_job = define_asset_job(
    "apcd_all", selection=[AssetKey(["apcd", "all_sd_airquality"])]
)

# daily but at 3 am
@schedule(job=apcd_all_job, cron_schedule="0 3 * * *", name="apcd_all",
          execution_timezone="America/Los_Angeles", )
def apcd_all_schedule(context):

    return RunRequest(
    )


# Yearly aggregation jobs and schedules
apcd_yearly_job = define_asset_job(
    "apcd_yearly_aggregation",
    selection=[
        AssetKey(["apcd", "yearly_aggregated_all"]),
        AssetKey(["apcd", "yearly_aggregated_h2s"])
    ]
)

# Weekly schedule for yearly aggregation (runs on Sundays at 4 AM)
@schedule(job=apcd_yearly_job, cron_schedule="0 4 * * 0", name="apcd_yearly_aggregation",
          execution_timezone="America/Los_Angeles", )
def apcd_yearly_schedule(context):
    return RunRequest(
    )


@asset(group_name="tijuana", key_prefix="apcd",
       name="yearly_aggregated_all", required_resource_keys={"s3"},
       partitions_def=yearly_apcd_partitions,
       automation_condition=AutomationCondition.eager()
       )
def yearly_aggregated_all(context) -> pd.DataFrame:
    """
    Aggregate all APCD files by year from S3 raw data

    Reads all daily files for each year from test/tijuana/sd_apcd_air/raw/YEAR/
    and creates yearly aggregated files uploaded to tijuana/sd_apcd_air/output/yearly/all/YEAR/
    """
    name = 'yearly_aggregated_all'
    description = '''Yearly aggregated APCD air quality data for all parameters

    Data aggregated from daily San Diego Air Pollution Control District files.
    Contains all parameters and measurements for the entire year.
    '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    # Get the year from partition key
    partition_key = context.asset_partition_key_for_output()
    year = int(partition_key)

    logger.info(f"Processing year {year} for aggregation")

    # List all files for the year
    prefix = f"tijuana/sd_apcd_air/raw/{year}/"

    try:
        objects = s3_resource.getClient().list_objects(
            s3_resource.S3_BUCKET,
            prefix=prefix,
            recursive=True
        )

        file_paths = []
        for obj in objects:
            if obj.object_name.endswith('.CSV') or obj.object_name.endswith('.csv'):
                file_paths.append(obj.object_name)

        logger.info(f"Found {len(file_paths)} files for year {year}")

        if not file_paths:
            logger.warning(f"No files found for year {year}")
            return pd.DataFrame()

        # Create file URLs that process_csv_files can access
        # We need to construct accessible URLs for the raw APCD files
        file_urls = []

        for file_path in sorted(file_paths):
            # Get the filename from the S3 path
            filename = file_path.split('/')[-1]

            # Create a signed URL or public URL for the file if possible
            # For now, we'll try to create a direct URL to the S3 file
            try:
                # If your S3 bucket is configured for public access, construct the URL
                # Otherwise, you might need to create signed URLs
                if hasattr(s3_resource, 'getFileUrl'):
                    file_url = s3_resource.getFileUrl(file_path)
                else:
                    # Fallback: construct URL based on S3 configuration
                    # This assumes public access or proper CORS configuration
                    bucket_name = s3_resource.S3_BUCKET
                    s3_address = s3_resource.S3_ADDRESS

                    # Handle different S3 endpoint formats
                    if s3_address.startswith('http'):
                        file_url = f"{s3_address}/{bucket_name}/{file_path}"
                    else:
                        # Use HTTPS by default
                        protocol = "https" if s3_resource.S3_USE_SSL else "http"
                        file_url = f"{protocol}://{s3_address}/{bucket_name}/{file_path}"

                file_urls.append(file_url)
                logger.debug(f"Created URL for {filename}: {file_url}")

            except Exception as e:
                logger.warning(f"Could not create URL for {file_path}: {e}")
                continue

        if file_urls:
            logger.info(f"Processing {len(file_urls)} files for year {year} using process_csv_files")

            # Use the existing process_csv_files function to process the files
            year_data = process_csv_files(file_urls)

            # Add metadata columns to the processed data
            year_data['aggregation_year'] = year
            year_data['date_processed'] = datetime.now().isoformat()

            logger.info(f"Aggregated {len(year_data)} rows for year {year} using process_csv_files")

            # Upload yearly aggregated data
            output_path = f"tijuana/sd_apcd_air/output/yearly/all/{year}"
            output_name= f'apcd_all_{year}'
            # Use store_assets to save in multiple formats
            store_assets.store_dataframe_to_s3(
                year_data,
                output_path,
                output_name,
                s3_resource,
                formats=['csv', 'parquet'],  # Note: parquet not implemented in store_assets yet
                metadata=metadata
            )

            logger.info(f"✓ Successfully processed and uploaded data for year {year}")
            return year_data
        else:
            logger.error(f"Failed to read any files for year {year}")
            return pd.DataFrame()

    except Exception as e:
        logger.error(f"Failed to process year {year}: {e}")
        return pd.DataFrame()


@asset(group_name="tijuana", key_prefix="apcd",
       name="yearly_aggregated_h2s", required_resource_keys={"s3"},
       partitions_def=yearly_apcd_partitions,
       deps=[AssetKey(["apcd", "yearly_aggregated_all"]), AssetKey(['apcd', 'locations'])],
       automation_condition=AutomationCondition.eager()
       )
def yearly_aggregated_h2s(context) -> pd.DataFrame:
    """
    Create yearly H2S-only aggregated files from yearly all data

    Filters the yearly aggregated data to only include H2S measurements
    and uploads to tijuana/sd_apcd_air/output/yearly/h2s/YEAR/
    """
    name = 'yearly_aggregated_h2s'
    description = '''Yearly aggregated H2S air quality data only

    Data filtered from yearly aggregated APCD data to include only H2S measurements.
    Contains H2S parameter data and measurements for the entire year.
    '''
    source_url = base_url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    locations_gdf = context.repository_def.load_asset_value(AssetKey([f"apcd", "locations"]))

    # Get the year from partition key
    partition_key = context.asset_partition_key_for_output()
    year = int(partition_key)

    # Get the yearly aggregated data for the specific partition
    try:
        # Load the yearly aggregated data from the same partition
        all_data = context.repository_def.load_asset_value(
            AssetKey(["apcd", "yearly_aggregated_all"]),
            partition_key=partition_key
        )

        if all_data.empty:
            logger.warning(f"No yearly aggregated data available for year {year}")
            return pd.DataFrame()

        # Filter for H2S data only
        h2s_data = all_data[all_data['Parameter'] == h2s_parameter].copy()

        if h2s_data.empty:
            logger.warning(f"No H2S data found in yearly aggregated data for year {year}")
            return pd.DataFrame()

        # Add H2S-specific processing
        h2s_data['level'] = h2s_data['Result'].apply(lambda r: h2s_guidance(r))

        logger.info(f"Filtered to {len(h2s_data)} H2S measurements for year {year}")


        h2s_data = locations_gdf.merge(h2s_data, how='inner', left_on='SiteName', right_on='Site Name',
                                         suffixes=('', '_y'))
        # Upload yearly H2S data for this specific year
        output_path = f"tijuana/sd_apcd_air/output/yearly/h2s/{year}/"
        output_name = f'apcd_h2s_{year}'
        # Use store_assets to save in multiple formats
        store_assets.store_dataframe_to_s3(
            h2s_data,
            output_path,
            output_name,
            s3_resource,
            formats=['csv', 'parquet'],
            metadata=metadata,
            latestdatasetpath=f'{s3_lastest_key}/h2s',
            enable_latest_path=True,
        )

        logger.info(f"✓ Successfully uploaded H2S data for year {year}")

        return h2s_data

    except Exception as e:
        logger.error(f"Failed to process H2S yearly aggregation for year {year}: {e}")
        return pd.DataFrame()


def test_current():
    df = process_csv_files(f'{base_url}{current_file}')
    df.to_csv('../../data/apcd_sd/test_current_data.csv', index=False)

def test_localfile():
    df = process_csv_files('../../data/apcd_sd/current.csv')
    df.to_csv('../../data/apcd_sd/test_output_data.csv', index=False)
