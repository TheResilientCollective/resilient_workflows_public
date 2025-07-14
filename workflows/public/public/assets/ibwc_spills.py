import os

import requests

from dagster import ( asset,
                     get_dagster_logger ,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      sensor,
                      SensorEvaluationContext,
                      AutomationCondition)

from ..resources import minio

import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from datetime import datetime, timedelta
import re
from ..utils.constants import ICONS
from ..utils import store_assets
from bs4 import BeautifulSoup

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#test")

baseurl = "https://beachwatch.waterboards.ca.gov/public/"
reports_page=f"{baseurl}result.php"
exports_page=f"{baseurl}export.php"

s3_output_path = 'tijuana/ibwc/'

stations_csv="""Latitude,Longitude,Discharge Location
32.5416,-117.038333,Tijuana River
32.5416,-117.038333,Tijuana River Main Channel
32.5377,-117.08623,Smuggler's Gulch
32.5369,-117.09916,Goat Canyon
32.5476,-117.088374,Hollister Street Pump Station
32.54064,-117.05801,Stewart's Drain
32.5393,-117.06885,Canyon del Sol
32.5393,-117.06885,Del Sol Canyon Collector
32.543476,-117.108026,Goat Canyon Pump Station
32.5416,-117.12315,Yogurt Canyon/ Border Field State Park
32.539743,-117.064269,Silva Drain
"""

def cleaned_date_range(start_date, end_date):
    if pd.isnull(start_date) or pd.isnull(end_date):
        return pd.NaT
    min_date = datetime.today() - timedelta(days=90)
    if start_date > min_date:
        return start_date.strftime('%Y-%m-%dT%H:%M:%S') + '/' + end_date.strftime('%Y-%m-%dT%H:%M:%S')
    else:
        return min_date.strftime('%Y-%m-%dT%H:%M:%S') + '/' + end_date.strftime('%Y-%m-%dT%H:%M:%S')





def gallons(num_str):
    if 'TBD' in num_str or 'Ongoing' in num_str:
        return -1

    powers = {'billion': 10 ** 9, 'million': 10 ** 6}
    num_str = num_str.strip().replace('gallons', '')
    num_str = num_str.strip().replace(',', '')
    match = re.search(r"([0-9\.]+)\s?(million|billion)", num_str)
    if match is not None:
        quantity = match.group(1)
        magnitude = match.group(2)
        return float(quantity) * powers[magnitude]
    else:
        return float(num_str.strip())

def spillStatus(row):
    enddate_str=row.get('End Date')
    if 'Ongoing' in enddate_str:
        return 'Ongoing'
    else:
        return 'Completed'
@asset(group_name="tijuana", key_prefix="ibwc",
       name="spills", required_resource_keys={"s3", "airtable","slack"},
 automation_condition=AutomationCondition.eager()
       )
def spills(context):
    url = 'https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/sewage_issue.html'
    name = 'spills'
    description = '''Spill information from the International Water Boundary Commission from the most recent spills page
     '''
    source_url = url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    slack = context.resources.slack
    df = pd.read_html(url, header=0)[0]  # [0] is the first table in the page
    df = df.drop(index=0) # drop the accumulative row
    df['Start Time'] = pd.to_datetime(df['Start Date'], )

    df['End Time'] = pd.to_datetime(df['End Date'], errors='coerce')
    df['End Time'].fillna(datetime.now(), inplace=True)
    df['Status'] = df.apply(spillStatus, axis=1)
    # date range attempt
    # df= df.dropna(subset=['Start Date', 'End date'])
    # df["daterange"] = df.apply(lambda x: pd.date_range(x['Start Date'], x['End date']), axis=1)
    df['Date Range'] = df['Start Time'].dt.strftime('%Y-%m-%dT%H:%M:%S') + '/' + df['End Time'].dt.strftime(
        '%Y-%m-%dT%H:%M:%S')
    df['Date Range(90 Days)'] = df.apply(lambda x: cleaned_date_range(x['Start Time'], x['End Time']), axis=1)

    df['Start Time'] = df['Start Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['End Time'] = df['End Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    df['Approximate Discharge Volume Value'] = df['Approximate Discharge Volume'].apply(lambda x: gallons(x))
    locations_df = pd.read_csv(StringIO(stations_csv), on_bad_lines='warn')
    gs = gpd.GeoSeries.from_xy(locations_df['Longitude'], locations_df['Latitude'])
    locations_df =  gpd.GeoDataFrame(locations_df,
                                 geometry=gs,
                                 crs='EPSG:4326')

    spill_gdf=locations_df.merge(df, on='Discharge Location', how='right')
    new_location =spill_gdf[spill_gdf['geometry']==None]
    if not new_location.empty:
        get_dagster_logger().warning(f'New spill location found. add or update : {new_location}')
        slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f'New spill location found add or update: {new_location}')
    spill_gdf['Icons'] = ICONS['spills']
    # spill_json=spill_gdf.to_json()
    # spill_csv=spill_gdf.to_csv( date_format='%Y-%m-%dT%H:%M:%SZ')
    #
    # filename = f'{s3_output_path}/spills.csv'
    # s3_resource.putFile_text(data=spill_csv, path=filename)
    #
    # filename = f'{s3_output_path}/spills.geojson'
    # s3_resource.putFile_text(data=spill_json, path=filename)
    filename = f'{s3_output_path}output/spills'
    store_assets.geodataframe_to_s3(spill_gdf, filename, s3_resource, metadata=metadata )
    return spill_gdf

@asset(group_name="tijuana", key_prefix="ibwc",
       name="spills_reports", required_resource_keys={"s3", "airtable"},
 automation_condition=AutomationCondition.eager()
       )
def spills_reports(context):
    url = 'https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/spill_report.html'
    # on the page there are a series of tables, from 2016 to 2022 (as of 2025-04-27)
    # this is a hash to pull data, that will be used as a date for file output, and future check if data changes.
    tables2Pull={"2015":7, "2016":6,
                         "2017":5,
              "2018":4,
                 "2019":3,
                 "2020":2,
                 "2021":1,
                 "2022":0}
    name = 'spills_reports'
    description = '''Spill information from the International Water Boundary Commission from spill reports  page
    This is the historic spills report data
     '''
    source_url = url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    combined_gdf = None
    for table in tables2Pull.keys():
        df = pd.read_html(url, header=0)[tables2Pull[table]]  # [0] is the first table in the page
        df = df.drop(index=0) # drop the accumulative row
        get_dagster_logger().info(f'dataframe {df.columns}')
        df['Start Time'] = pd.to_datetime(df['Start Date'], )

        df['End Time'] = pd.to_datetime(df['End Date'], errors='coerce')
        df['End Time'].fillna(datetime.now(), inplace=True)
        df['Status'] = df.apply(spillStatus, axis=1)
        # date range attempt
        # df= df.dropna(subset=['Start Date', 'End date'])
        # df["daterange"] = df.apply(lambda x: pd.date_range(x['Start Date'], x['End date']), axis=1)
        df['Date Range'] = df['Start Time'].dt.strftime('%Y-%m-%dT%H:%M:%S') + '/' + df['End Time'].dt.strftime(
            '%Y-%m-%dT%H:%M:%S')
        df['Start Time'] = df['Start Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        df['End Time'] = df['End Time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
        # clean into a column name matching this location dataframe
        df['Discharge Location'] = df['Location'].apply( lambda t : t.encode('ascii', 'ignore').decode('utf-8') )
        # if  'Type (A or B)' in df.columns:
        #     df.drop(columns=['Type (A or B)'], inplace=True)
        #df['Volume (Gallons)'] = df['Volume (Gallons)'].apply(lambda x: gallons(x))
        locations_df = pd.read_csv(StringIO(stations_csv), on_bad_lines='warn')
        gs = gpd.GeoSeries.from_xy(locations_df['Longitude'], locations_df['Latitude'])
        locations_df = gpd.GeoDataFrame(locations_df,
                                        geometry=gs,
                                        crs='EPSG:4326')

        get_dagster_logger().info(f'location dataframe {locations_df.columns}')
        get_dagster_logger().info(f'spill dataframe {df.columns}')
        spill_gdf = locations_df.merge(df, on='Discharge Location')
        spill_gdf['Icons'] = ICONS['spills']
        filename = f'{s3_output_path}raw/spills_{table}'
        store_assets.geodataframe_to_s3(spill_gdf, filename, s3_resource, metadata=metadata)
        if combined_gdf is None:
            combined_gdf = spill_gdf
        else:
            combined_gdf= pd.concat([combined_gdf, spill_gdf],ignore_index=True)
        get_dagster_logger().info(f'combined gdf {combined_gdf.columns}')

    filename = f'{s3_output_path}output/spills_historic'
    # add some temporal timeframe to the metadata
    store_assets.geodataframe_to_s3(combined_gdf, filename, s3_resource, metadata=metadata )
    return combined_gdf

@asset(group_name="tijuana", key_prefix="ibwc",
       name="spills_last_by_site", required_resource_keys={"s3", "airtable"},
       deps=[AssetKey(["ibwc","spills"])],
       automation_condition=AutomationCondition.eager()
       )
def spills_last(context):
    url = 'https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/sewage_issue.html'
    name = 'spills_last_by_site'
    description = '''Last Spill at each location from  the International Water Boundary Commission from the most recent spills page
     '''
    source_url = url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    spills_gdf = context.repository_def.load_asset_value(AssetKey(["ibwc","spills"]))
    spills_gdf.drop_duplicates(subset=['Discharge Location'],keep='last', inplace=True)

    filename = f'{s3_output_path}output/spills_last_by_site'
    store_assets.geodataframe_to_s3(spills_gdf, filename, s3_resource, metadata=metadata )

@asset(group_name="tijuana", key_prefix="ibwc",
       name="spills_all", required_resource_keys={"s3", "airtable"},
       deps=[AssetKey(["ibwc","spills"]), AssetKey(["ibwc","spills_reports"])],
       automation_condition=AutomationCondition.eager()
       )
def spills_all(context):
    url = 'https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/sewage_issue.html'
    name = 'spills_all'
    description = '''Spill information from from  the International Water Boundary Commission including the
     most [recent spills page] (https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/sewage_issue.html) 
     and [historic spills report page](https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/spill_report.html
     '''
    source_url = url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    s3_resource = context.resources.s3
    spills_gdf = context.repository_def.load_asset_value(AssetKey(["ibwc","spills"]))
    spills_reports_gdf = context.repository_def.load_asset_value(AssetKey(["ibwc","spills_reports"]))
    if spills_gdf is not None and spills_reports_gdf is not None:
        combined_gdf = pd.concat([spills_reports_gdf, spills_gdf], ignore_index=True)
    elif spills_gdf is None:
        combined_gdf = spills_reports_gdf
    elif spills_reports_gdf is None:
        combined_gdf = spills_gdf
    else:
        raise ValueError("No spills data found")
    filename = f'{s3_output_path}output/spills_all'
    store_assets.geodataframe_to_s3(combined_gdf, filename, s3_resource, metadata=metadata )

spills_latest_job = define_asset_job(
    "ibwc_spills_latest_job",  selection=[AssetKey(["ibwc", "spills"]) ]
)
def fetch_last_update(url):
    """Fetch the 'Last Updated' timestamp from the website."""

    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Search for all elements that might contain the phrase
    matches = []
    pattern = re.compile(r'Page last updated', re.IGNORECASE)

    # Loop through all text-containing tags
    for elem in soup.find_all(text=pattern):
        # You can adjust this to get the parent element, or just the string
        parent = elem.parent
        if parent:
            return parent.get_text(strip=True)
    return None


@sensor(job=spills_latest_job, name="ibwc_spills_latest_sensor",
       minimum_interval_seconds=3600,
        required_resource_keys={"slack"})
def spills_latest_sensor(context: SensorEvaluationContext):
    slack = context.resources.slack
    url = "https://www.waterboards.ca.gov/sandiego/water_issues/programs/tijuana_river_valley_strategy/sewage_issue.html"

    try:
        current_timestamp = fetch_last_update(url)
        if not current_timestamp:
            context.log.info("No 'Page last updated' timestamp found on the website.")
            return
        if current_timestamp:
            current_date = current_timestamp
            last_known_date = context.cursor or None

            if current_date != last_known_date:
                context.update_cursor(current_date)
                context.log.info(f"IBWC Spills Page update detected. New date: {current_date}, Previous: {last_known_date}")
                slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                    text=f'IBWC Spills Page update detected New date: {current_date}, Previous: {last_known_date}')
                return RunRequest(run_key=f"spills_update_{current_date}")
            else:
                context.log.info(f"No page update detected. Current date: {current_date}")
                #slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                 #                                   text=f'IBWC Spills Page No page update detected: {current_date}')

                return None
        else:
            context.log.warning("Could not find 'Page last updated' text on the webpage")
            slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                text=f'IBWC Spills Page Could not find "Page last updated" text on the webpage')
            return None

    except Exception as e:
        context.log.error(f"Error checking IBWC webpage for updates: {str(e)}")
        slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                            text=f'Error checking IBWC webpage for updates')
        return None

spills_historic_job = define_asset_job(
    "ibwc_spills_historic_job", selection=[AssetKey(["ibwc", "spills_reports"]), ]
)

# daily but at 3 am
@schedule(job=spills_historic_job, cron_schedule="@monthly", name="ibwc_spills_historic")
def spills_historic_schedule(context):

    return RunRequest(
    )
