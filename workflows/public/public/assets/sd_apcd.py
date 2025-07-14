import logging

import pandas as pd
import csv
import os
from datetime import datetime, timedelta

import pytz
import requests

from dagster import ( asset, op,
                     get_dagster_logger,
                      AssetKey,asset_sensor,
DailyPartitionsDefinition,
schedule, RunRequest, define_asset_job, AssetKey, AssetIn,
AutomationCondition,
AssetCheckSpec, AssetCheckResult, asset_check, AssetCheckExecutionContext
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

daily_apcd_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

airnow_station_url = "https://s3-us-west-1.amazonaws.com//files.airnowtech.org/airnow/today/Monitoring_Site_Locations_V2.dat"

base_url = 'http://jtimmer.digitalspacemail17.net/data/'
current_file = 'current.CSV'
pattern_file = 'yesterday_$filedate.CSV'

# yesterday_20241002.CSV

#s3_bucket = os.getenv('PUBLIC_BUCKET', 'resilient-public')# defined in s3
s3_data_path = 'tijuana/sd_apcd_air/source'
s3_output_path = 'tijuana/sd_apcd_air/output'

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
                get_dagster_logger().error(f'get file {file_path} {response.status_code}' )

    # Create DataFrame from transformed data
    output_df = pd.DataFrame(transformed_data)
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


def test_current():
    df = process_csv_files(f'{base_url}{current_file}')
    df.to_csv('../../data/apcd_sd/test_current_data.csv', index=False)

def test_localfile():
    df = process_csv_files('../../data/apcd_sd/current.csv')
    df.to_csv('../../data/apcd_sd/test_output_data.csv', index=False)
