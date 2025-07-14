import requests
import os
import re
from dagster import ( asset,
                     get_dagster_logger,
                      TimeWindowPartitionsDefinition,
DailyPartitionsDefinition,
schedule, RunRequest, define_asset_job, AssetKey,
AutomationCondition,
sensor,SensorEvaluationContext
                      )

from .. import utils
from ..resources import minio
from ..utils import store_assets

import requests
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_string_dtype
from io import StringIO
import geopandas as gpd
import datetime
from ..utils.constants import ICONS
from bs4 import BeautifulSoup

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#test")

baseurl = "https://beachwatch.waterboards.ca.gov/public/"
reports_page=f"{baseurl}result.php"
exports_page=f"{baseurl}export.php"

s3_output_path = 'tijuana/beachwatch'

formdata={
   "County":10,
"stationID":"",
"parameter":"",
"qualifier":"",
"method":"",
"created":"",
"year":2025,
"sort":"`SampleDate`",
"sortOrder":"DESC",
"submit":"Search ",
}

limits=[
    {'method':'ddPCR', 'limits':[{'level':'close', 'min':1413,}],},
     {'method':'MF', 'limits':[{'level':'close', 'min':104,}],},
     {'method':'MTF', 'limits':[{'level':'close', 'min':104,}],},
{'method':'Enterolert', 'limits':[{'level':'close', 'min':104,}],}]
start_date=datetime.datetime(2000,1,1)
end_date=datetime.datetime.today()- datetime.timedelta(weeks=52)
year_partitions = TimeWindowPartitionsDefinition(start=start_date,fmt='%Y', end=end_date,
                                                 cron_schedule="@monthly" )
start_date_closures=datetime.datetime(2011,1,1)
year_closure_partitions = TimeWindowPartitionsDefinition(start=start_date_closures,fmt='%Y', end=end_date,
                                                 cron_schedule="@monthly" )

#daily_partitions = DailyPartitionsDefinition(start_date="2025-01-01")
###########################
### Data Fetch functions
###########################
'''
California Government BeachWatch data sources use cookies for exports
'''
def get_beachwatch_data(reports_page, exports_page, formdata) -> pd.DataFrame:
    response = requests.post(reports_page, data=formdata)
    if response.status_code == 200:
        cookies = response.cookies

        response = requests.get(exports_page, cookies=cookies)
        if response.status_code == 200:
            data = response.text
            try:
            #beach_df = pd.read_csv(StringIO(data), sep="\t", parse_dates=['Start Date', 'End Date'], date_format="%Y-%m-%d")
                beach_df = pd.read_csv(StringIO(data), sep="\t")
                return beach_df
            except Exception as  ex:
                get_dagster_logger().info('Failed to parse beach data', ex)
                raise ex

        else:
            get_dagster_logger().info(f'Failed to download beach data from {exports_page}', response.status_code)
    else:
        get_dagster_logger().info(f'Failed to access beach data start page {reports_page}', response.status_code)
    raise Exception("Failed to download beach data")


###########################
### Data Cleaning functions
###########################
def beachwatch_clean_data(beach_df) -> gpd.GeoDataFrame:

        # beach_df['date']=  pd.to_datetime(f'{beach_df["SampleDate"]}T{beach_df["SampleTime"]}')#, format='%Y-%m-%d %H:%M:%S')
        beach_df['date'] = pd.to_datetime(beach_df["SampleDate"], format='%Y-%m-%d')
        beach_df['time'] = pd.to_timedelta(beach_df['SampleTime'])
        beach_df['datetime'] = beach_df['date'] + beach_df['time']
        gs = gpd.GeoSeries.from_xy(beach_df['Longitude'], beach_df['Latitude'])
        beach_gdf = gpd.GeoDataFrame(beach_df,
                                     geometry=gs,
                                     crs='EPSG:4326')
        beach_gdf.drop(['date', 'time', 'id', 'Station_ID'], axis=1, inplace=True)
        beach_gdf['Icons'] = ICONS['beach']
        return beach_gdf


def beachwatch_closure_clean_data(beach_df) -> gpd.GeoDataFrame:
    # beach_df['date']=  pd.to_datetime(f'{beach_df["SampleDate"]}T{beach_df["SampleTime"]}')#, format='%Y-%m-%d %H:%M:%S')
    beach_df['start'] = pd.to_datetime(beach_df["Start Date"], format='%Y-%m-%d', errors='coerce')
    beach_df['end'] = pd.to_datetime(beach_df["End Date"], format='%Y-%m-%d', errors='coerce')
    beach_df['end'].fillna(value=datetime.datetime.today(), inplace=True)
    beach_df['end'] = beach_df['end'].fillna(value=datetime.datetime.now())
    beach_df.dropna(subset=['start'], inplace=True)
    gs = gpd.GeoSeries.from_xy(beach_df['Longitude'], beach_df['Latitude'])
    beach_gdf = gpd.GeoDataFrame(beach_df,
                                 geometry=gs,
                                 crs='EPSG:4326')
    #beach_gdf.drop(['date', 'time', 'id', 'Station_ID'], axis=1, inplace=True)
    beach_gdf['Icons'] = ICONS['beach']
    return beach_gdf
''' This is used to parse the Date of the Advisory/Closure notices from the SD Beach Info website'''
def parseSince(message):
    date_str = ""
    pattern=re.compile(
        r"<strong>Status Since:\s*</strong>\s*(?:&nbsp;)*\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})(?:&nbsp;)*",
        re.IGNORECASE
    )
    match = pattern.search(message)
    if match:
        date_str = match.group(1)
        get_dagster_logger().debug(f"Extracted date: {date_str}", )
        dt = datetime.datetime.strptime(date_str, "%B %d, %Y")
        date_str = dt.strftime("%b {day}, %Y").format(day=dt.day)
        get_dagster_logger().debug(f"Modified date: {date_str}")
    else:
        print("No date found.")
    get_dagster_logger().debug(f" Since: {date_str}")
    return date_str

''' This is used to parse the Basic Content of the Advisory/Closure notices from the SD Beach Info website'''
def parseNote(message):
    pattern = re.compile(r"<strong>(.*?)</strong>", re.DOTALL)
    matches = pattern.findall(message)
    get_dagster_logger().debug(f" statusNote match: {matches}")
    if matches is not None  and len(matches) > 0:
        statusNote = matches[len(matches)- 1]
    else:
        statusNote="";
    get_dagster_logger().debug(f" statusNote: {statusNote}")
    return statusNote
''' This is used to parse the Advisory/Closure notices from the SD Beach Info website'''
def parseSdBeachinfoRow( row):
    status = ""
    statusNote = ""
    get_dagster_logger().debug(f" IndicatorID: {row['IndicatorID']}")
    if row['IndicatorID']== 1 and (row['Closure'] is not None and row['Closure'] !=''):
        get_dagster_logger().debug(f" row['Closure']: {row['Closure']}")
        status = parseSince(row['Closure'])
        statusNote = parseNote(row['Closure'])
    elif row['IndicatorID'] == 3 and (row['Advisory'] is not None and row['Advisory'] != ''):
        get_dagster_logger().debug(f" row['Advisory']: {row['Advisory']}")
        status = parseSince(row['Advisory'])
        statusNote = parseNote(row['Advisory'])
    return [status, statusNote]
def removeMapLegendLink(html_string):
    ''' Removes the map links which often point to the page.
    tried many regex's but they never all succeeded.  Even when chatgpt was asked to write one for all 17,
    never fully worked. '''
    if html_string is None:
        return ""
    link = '<a href="/">Full County Map</a>&nbsp; <strong>|</strong> &nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_map_key_v.1.pdf">Map Legend</a>'
    linkv2 = '<span style="font-size:medium"><span style="color:#000000"><span style="font-family:Calibri">.</span></span><strong><span style="font-family:&quot;Calibri&quot;,sans-serif"><span style="color:#000000">&nbsp;</span><a href="/"><span style="color:#0000ff">Full County Map</span></a><span style="color:#000000">&nbsp; | &nbsp;</span><a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0000ff">Map Legend</span></a></span></strong></span>'
    linkv3 = '<strong><a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf">Map Legend</a></strong>'
    linkv4 = '<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&Bay/bb_maplegend.pdf"><strong><span style="color:#0563c1">Map Legend</span></strong></a>'
    linkv5 = '<strong><a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf">Map Legend</a>&nbsp;</strong>'
    linkv6 = '<strong>&nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf">Map Legend</a></strong>'
    linkv7 = '<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><strong><span style="color:#0563c1">Map Legend</span></strong></a>'
    linkv8 = '<strong>&nbsp;<a href="/"><span style="color:#0066cc">Full County Map</span></a>&nbsp; | &nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a></strong>'
    linkv9 = '<strong><a href="/">Full County Map</a>&nbsp; | &nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf">Map Legend</a></strong>'
    linkv10 = '<strong><a href="/"><span style="color:#0066cc">Full County Map</span></a>&nbsp; | &nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a></strong>'
    link11 = '<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><strong>Map Legend</strong></a>'
    link12 = '<strong><a href="/"><span style="color:#0066cc">Full County Map</span></a>&nbsp; | &nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a>&nbsp;</strong>'
    link13 = '<strong>&nbsp;<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a></strong>'
    link14 = '<strong><a href="https://admin.sdbeachinfo.com/"><span style="color:#0066cc">Full County Map</span></a>&nbsp; | <a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a></strong>'
    link15 = '<strong><a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><span style="color:#0066cc">Map Legend</span></a></strong>'
    link16 = '<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf">Map Legend</a>'
    link17 = '<a href="http://www.sandiegocounty.gov/content/dam/sdc/deh/lwqd/Beach&amp;Bay/bb_maplegend.pdf"><strong>&nbsp;Map Legend</strong></a>'
    html_string=html_string.replace(link,'').replace(linkv2,'').replace(linkv3,'').replace(linkv4,'').replace(linkv5,'').replace(linkv6,'').replace(linkv7,'').replace(linkv8,'').replace(linkv9,'').replace(linkv10,'').replace(link11,'').replace(link12,'').replace(link13,'').replace(link14,'').replace(link15,'').replace(link16,' ').replace(link17,'')
    return html_string

def sdbeachinfo_clean_data(beachinfo_df) -> gpd.GeoDataFrame:
    indicator2status = {
        1: 'Closure',
        2: 'Open',
        3: 'Advisory',
        4: 'Outfall'
        #'Warning' # never seen a number
    }
    typeId2type ={
        1: 'Monitoring Point',
        2: 'Outfall'
    }
     # beach_df['date']=  pd.to_datetime(f'{beach_df["SampleDate"]}T{beach_df["SampleTime"]}')#, format='%Y-%m-%d %H:%M:%S')
    gs = gpd.GeoSeries.from_xy(beachinfo_df['Longitude'], beachinfo_df['Latitude'])
    beachinfo_gdf = gpd.GeoDataFrame(beachinfo_df, geometry=gs, crs='EPSG:4326')
    beachinfo_gdf['Icon'] = ICONS['beach']

    beachinfo_gdf['RBGColor'] = beachinfo_gdf['RBGColor'].map(lambda x: x.strip().replace('.png', '').replace('Outfall', 'Black'))
    beachinfo_gdf['Icon'] = beachinfo_gdf['RBGColor'].map(lambda x: ICONS[x])
    beachinfo_gdf['beachStatus'] =  beachinfo_gdf['IndicatorID'].apply(lambda i: indicator2status[i] )
    beachinfo_gdf['LocationType'] = beachinfo_gdf['TypeID'].apply(lambda i: typeId2type[i])
    beachinfo_gdf[['StatusSince','StatusNote']] = beachinfo_gdf.apply(parseSdBeachinfoRow,axis=1, result_type='expand')
    beachinfo_gdf['Descirption_original']=beachinfo_gdf['Description']
    beachinfo_gdf['Description']=beachinfo_gdf['Description'].apply(removeMapLegendLink)
    return beachinfo_gdf
@asset(group_name="tijuana", key_prefix="waterquality",
       name="sdbeachinfo_status", required_resource_keys={"s3", "airtable"},

       automation_condition=AutomationCondition.eager()       )
def get_sdbeachinfo_status(context) -> gpd.GeoDataFrame:
    '''
    Collects the San Diego Beachinfo Notices on daily schedule
    www.sdbeachinfo.com
    '''
    name = 'sdbeachinfo_status'
    description = '''Collects the San Diego Beachinfo Notices on daily schedule
         www.sdbeachinfo.com
         The source is a json response from ASP.Net component that provides information about all San Diego County Beaches
         adds: 
         * text information about beachStatus based on IndicatorID
         * text information on the Station Type based on TypeID
         * statusNote that is a cleaned version of the (Closure/Advisory) messages
        '''
    source_url = 'https://www.sdbeachinfo.com/Home/GetTargetByID'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    # need to pass in as optional env variable
    # https://www.sdbeachinfo.com/Home/GetTargetByID
    closures_page='https://www.sdbeachinfo.com/Home/GetTargetByID'
    response = requests.post(closures_page)
    if response.status_code != 200:
        get_dagster_logger().error(f" get beach clousure error: {response.status_code} {response.text}")
        raise  Exception (response.text)
    else:
        try:
            data = response.json()
            closures_df = pd.DataFrame(data)
            # gs = gpd.GeoSeries.from_xy(closures_df['Longitude'], closures_df['Latitude'])
            # closures_gdf = gpd.GeoDataFrame(closures_df, geometry=gs, crs='EPSG:4326')
            # closures_gdf['Icon'] = 'place'
            # closures_gdf['RBGColor'] = closures_gdf['RBGColor'].map(lambda x: x.strip().replace('.png', ''))


        except Exception as e:
            get_dagster_logger().error(f" get beach clousure parsing response: {e}")
            return gpd.GeoDataFrame()
    closures_gdf= sdbeachinfo_clean_data(closures_df)
    closures_gdf= closures_gdf[['SiteID','DehID','Name','Latitude','Longitude', 'IndicatorID', 'Active','RBGColor', 'Icon',
                                    'Description', 'Advisory',	'Closure',
                                    'beachStatus','LocationType', 'StatusSince', 'StatusNote', 'geometry'
                                    ]]
    filename = f'{s3_output_path}/output/current/sdbeachinfo_status'
    store_assets.geodataframe_to_s3(closures_gdf, filename, s3_resource, formats=['json','csv','geojson'], metadata=metadata)
    # just name, location
   # closures_name_df= closures_gdf[['SiteID','DehID','Name','Latitude','Longitude', 'IndicatorID', 'Active','RBGColor', 'Icon',
     #                               'Description', 'Advisory',	'Closure',
      #                               'beachStatus','LocationType', 'StatusSince', 'StatusNote'
      #                              ]]

    #store_assets.dataframe_to_s3(closures_name_df, filename, s3_resource, formats=['json'])
    return closures_gdf


def translate(message,  openai_client, language_code='es', model='llama3',):
    if message is None or message == '':
        return ''
    get_dagster_logger().debug(f" get beach closure translation: {message}")
    instructions = f'''
Translate the following text into Spanish using the regional expressions common in the Tijuana area. Please:
   * Use “aguas negras” in place of “sewage.”
   * Use “aguas pluviales” for “runoff water.”
   * Adapt terms like “dashboard” to more colloquial equivalents, for example “tablero.”
   * Ensure the translation resonates naturally with the local public in Tijuana.
Maintain the html elements" 
    '''

    prompt = f'{message } (Note: AI-generated translation.)'
    resp= openai_client.chat.completions.create(
        model=model,
        messages=[ {
            'role': 'user',
             "content": f"{instructions}{prompt}"
                    }]
    )
    # resp= openai_client.responses.create(
    #     model=model,
    #     instructions=instructions,
    #     input= prompt
    # )
    #get_dagster_logger().info(f" get beach closure response: {resp}")
    translation = resp.choices[0].message.content
    get_dagster_logger().debug(f" get beach closure translation: {translation}")
    return f'{translation}'
@asset(group_name="tijuana", key_prefix="waterquality",
       name="sdbeachinfo_status_translation", required_resource_keys={"s3", "airtable","openai"},
       deps=[AssetKey(['waterquality','sdbeachinfo_status'])],
        compute_kind="OpenAI",
       automation_condition=AutomationCondition.eager()
       )
def beachwatch_status_translation(context) -> gpd.GeoDataFrame:
    ''' Translates www.sdbeachinfo.com status to spanish
     adds:
       * Description_es
       * Closure_es
       * Advisory_es
     '''
    name = 'sdbeachinfo_status_translation'
    description = '''Translated notices from San Diego Beachinfo Notices
         Using the processed data sdbeachinfo_status  Translates www.sdbeachinfo.com status to spanish
     adds:
       * Description_es
       * Closure_es
       * Advisory_es
       * statusNote_es 
        '''
    source_url = 'https://www.sdbeachinfo.com/Home/GetTargetByID'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    openai_resource = context.resources.openai
    closure_gdf = context.repository_def.load_asset_value(AssetKey([f"waterquality", "sdbeachinfo_status"]))
    with openai_resource.get_client(context) as client:
        closure_translated = closure_gdf
        #closure_translated = closure_gdf.head(10)
        closure_translated['Description_es']= closure_translated['Description'].apply(lambda x : translate(x, client, language_code='es', model='llama3'))
        #closure_translated['Closure_es']= closure_translated['Closure'].apply(lambda x : translate(x, client, language_code='es', model='llama3'))
        #closure_translated['Advisory_es']= closure_translated['Advisory'].apply(lambda x : translate(x, client, language_code='es', model='llama3'))
        closure_translated['StatusNote_es'] = closure_translated['StatusNote'].apply(
            lambda x: translate(x, client, language_code='es', model='llama3'))

    filename = f'{s3_output_path}/output/current/sdbeachinfo_status_translated'
    store_assets.geodataframe_to_s3(closure_translated, filename, s3_resource, formats=['json', 'geojson'], metadata=metadata )
    return closure_gdf




beachwatch_closure_job = define_asset_job(name="beachwatch_closure",
    selection=
    [AssetKey(["waterquality","sdbeachinfo_status"])]
)
@schedule(job=beachwatch_closure_job, cron_schedule="@daily", name="beachwatch_closure")
def beachwatch_closure_schedule(context):
    return RunRequest(
    )


@asset(group_name="tijuana",key_prefix="waterquality",
       name="beachwatch_analyses_daily", required_resource_keys={"s3", "airtable"},
  )
def beachwatch_analyses_daily(context):
    '''Collects and cleans the recent years analyses on a daily schedule'''
    s3_resource = context.resources.s3

    year = datetime.date.today().year
    get_data = formdata.copy()
    get_data['year'] = year
    beach_df = get_beachwatch_data(reports_page, exports_page, get_data)
    beach_csv = beach_df.to_csv(
        index=False)
    filename = f'{s3_output_path}/raw/current/analyses/current/beachwatch_raw.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)

    beach_gdf = beachwatch_clean_data(beach_df)
    # beach_csv= beach_gdf.to_csv(
    #      index=False)
    # filename = f'{s3_output_path}/output/beachwatch.csv'
    # s3_resource.putFile_text(data=beach_csv, path_w_basename=filename)
    filename = f'{s3_output_path}/output/analyses/current/beachwatch'
    store_assets.geodataframe_to_s3(beach_gdf, filename, s3_resource )

@asset(group_name="tijuana",key_prefix="waterquality",
       name="beachwatch_closures_recent", required_resource_keys={"s3", "airtable"},
  )
def beachwatch_closures_recent(context) -> gpd.GeoDataFrame:
    '''Collects and cleans the recent years closure data on a daily schedule'''
    reports_page = f"{baseurl}advisory.php"
    exports_page = f"{baseurl}export.php"

    name = 'beachwatch_closures_recent'
    description = '''Collects the Closure notices  for San Diego County from the California Data Site 
                 https://beachwatch.waterboards.ca.gov/public/
                 for the present year
                '''
    source_url = reports_page
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    formdata = {
        "type": "3",  # closure
        "County": 10,
        "stationID": "",
        "cause": "",
        "source": "",
        "substance": "",
        "created": "",
        "year": "",
        "sort": "`Start Date`",
        "sortOrder": "DESC",
        "submit": "Search ",
    }

    s3_resource = context.resources.s3

    year = datetime.date.today().year
    get_data = formdata.copy()
    get_data['year'] = year
    beach_df = get_beachwatch_data(reports_page, exports_page, get_data)
    if (len(beach_df)==0 ):
        get_dagster_logger().info(f'beachwatch_closures_recent {len(beach_df)}', )
        return gpd.GeoDataFrame()
    beach_csv = beach_df.to_csv(
        index=False)
    get_dagster_logger().info(beach_df, )
    filename = f'{s3_output_path}/raw/current/closures/beachwatch_closures_raw_{year}.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)

    #store_assets.metadata_to_s3(metadata, path=f'{filename}.meta.json')
    beach_gdf = beachwatch_closure_clean_data(beach_df)
    # beach_csv= beach_gdf.to_csv(
    #      index=False)
    # filename = f'{s3_output_path}/output/beachwatch.csv'
    # s3_resource.putFile_text(data=beach_csv, path_w_basename=filename)
    filename = f'{s3_output_path}/output/current/closures/beachwatch_closures_recent'
    store_assets.geodataframe_to_s3(beach_gdf, filename, s3_resource, metadata=metadata)
    get_dagster_logger().info(f'beachwatch_closures_recent {len(beach_gdf)}', )
    return beach_gdf

def weeks_spanned(row):
    ''' This is utilized to create weekly counts of the closed beaches'''
    # Calculate the Monday of the week for both start and end dates.
    # This ensures that even a one-day event (start == end) lands in a week.
    start_week = row['start'] - pd.to_timedelta(row['start'].weekday(), unit='D')
    end_week = row['end'] - pd.to_timedelta(row['end'].weekday(), unit='D')
    # Create a weekly date_range with a step of 7 days.
    return pd.date_range(start=start_week, end=end_week, freq='7D')
@asset(group_name="tijuana", key_prefix="waterquality",
       name="beachwatch_closures_recent_weekly", required_resource_keys={"s3", "airtable"},
       automation_condition=AutomationCondition.eager(),
       deps=[AssetKey([f"waterquality", "beachwatch_closures_recent"])]
       )
def beachwatch_closure_recent_weekly(context)-> pd.DataFrame:
    name = 'beachwatch_closures_recent_weekly'
    description = '''Aggregates by conount  Closure notices for San Diego County from the California  Data Site 
                 https://beachwatch.waterboards.ca.gov/public/
                 for the present year
                '''
    metadata = store_assets.objectMetadata(name=name, description=description)

    s3_resource = context.resources.s3
    closure_gdf = context.repository_def.load_asset_value(AssetKey([f"waterquality", "beachwatch_closures_recent"]))
    get_dagster_logger().info(f'beachwatch_closures_recent unpickle {len(closure_gdf)}', )
    get_dagster_logger().info(f'beachwatch_closures_recent  unpickle types {closure_gdf.dtypes}', )
    # for some reason this does not get a clean unpickle... just clean it again
    closure_gdf=beachwatch_closure_clean_data(closure_gdf)
    closure_gdf['week'] = closure_gdf.apply(weeks_spanned, axis=1)

# Explode the list of weeks into individual rows
    df_exploded = closure_gdf.explode('week')

    weekly_counts = df_exploded['week'].value_counts().reset_index().sort_values(by='week')
    weekly_counts.columns = ['week', 'count']  # Rename columns for clarity

    filename = f'{s3_output_path}/output/current/closures/beachwatch_closures_recent_weekly'
    store_assets.dataframe_to_s3(weekly_counts, filename, s3_resource , metadata=metadata)
    return weekly_counts

beach_waterquality_daily_job = define_asset_job(
    "beaches_daily", selection=[AssetKey(["waterquality", "sdbeachinfo_status"]),
                                    AssetKey(["waterquality", "beachwatch_closures_recent"]),
                                    AssetKey(["waterquality", "beachwatch_analyses_daily"]),
                                ]
)
@schedule(job=beach_waterquality_daily_job, cron_schedule="@daily", name="beaches_daily")
def beach_waterquality_schedule(context):
    return RunRequest(
    )
@asset(group_name="tijuana",key_prefix="waterquality",
       name="beachwatch_year", required_resource_keys={"s3", "airtable"},
       partitions_def=year_partitions,

  )
def beachwatch_year(context):
    '''Collects and cleans the analyses by year '''
    name = 'beachwatch_year'
    description = '''Collects Analysis data by year  for San Diego County from the California  Data Site 
                 https://beachwatch.waterboards.ca.gov/public/
                '''

    metadata = store_assets.objectMetadata(name=name, description=description)
    s3_resource = context.resources.s3
    year = context.asset_partition_key_for_output()

    get_data = formdata.copy()
    get_data['year'] = year
    beach_df = get_beachwatch_data(reports_page, exports_page, get_data)
    beach_csv= beach_df.to_csv(
         index=False)
    filename = f'{s3_output_path}/raw/analyses/year/beachwatch_raw_{year}.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)
    beach_gdf = beachwatch_clean_data(beach_df)
    beach_csv= beach_gdf.to_csv(
         index=False)
    filename = f'{s3_output_path}/output/analyses/year/beachwatch_{year}.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)

@asset(group_name="tijuana",key_prefix="waterquality",
       name="beachwatch_closure_year", required_resource_keys={"s3", "airtable"},
       partitions_def=year_closure_partitions,

  )
def beachwatch__closures_year(context):
    '''Collects and cleans the closure information by year. Hostorical back to 2011. Data change in 2010'''
    # the time is from 2011. There was a format change in 2010.
    reports_page = f"{baseurl}advisory.php"
    exports_page = f"{baseurl}export.php"
    name = 'beachwatch_closures_recent_weekly'
    description = '''Collects and cleans the closure information by year. Historical back to 2011. Data change in 2010
                     https://beachwatch.waterboards.ca.gov/public/
                    
                    '''
    source_url = reports_page
    metadata = store_assets.objectMetadata(name=name, description=description, source=source_url)

    formdata = {
        "type": "3",  # closure
        "County": 10,
        "stationID": "",
        "cause": "",
        "source": "",
        "substance": "",
        "created": "",
        "year": "",
        "sort": "`Start Date`",
        "sortOrder": "DESC",
        "submit": "Search ",
    }
    s3_resource = context.resources.s3
    year = context.asset_partition_key_for_output()

    get_data = formdata.copy()
    get_data['year'] = year
    beach_df = get_beachwatch_data(reports_page, exports_page, get_data)
    beach_csv= beach_df.to_csv(
         index=False)
    filename = f'{s3_output_path}/raw/closures/year/beachwatch_closure_raw_{year}.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)
    beach_gdf = beachwatch_closure_clean_data(beach_df)
    beach_csv= beach_gdf.to_csv(
         index=False)
    filename = f'{s3_output_path}/output/closures/year/beachwatch_closure_{year}.csv'
    s3_resource.putFile_text(data=beach_csv, path=filename)

#### AI generate sensor


def fetch_last_updated():
    """Fetch the 'Last Updated' timestamp from the website."""
    url = "https://www.sdbeachinfo.com/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    last_updated_div = soup.find("div", string=lambda text: text and "Last Updated:" in text)
    if last_updated_div:
        return last_updated_div.text.strip()
    return None




@sensor(job_name="beaches_daily",
        minimum_interval_seconds=3600,
          required_resource_keys={"slack"})
def beachinfo_updated_sensor(context: SensorEvaluationContext):
    """Sensor to monitor the website for changes in the 'Last Updated' timestamp."""
    slack = context.resources.slack
    try:
        current_timestamp = fetch_last_updated()
        if not current_timestamp:
            context.log.info("No 'Last Updated' timestamp found on the website.")
            return

        # Retrieve the last seen timestamp from the cursor
        last_seen_timestamp = context.cursor if context.cursor else None

        if current_timestamp != last_seen_timestamp:
            # Update the cursor with the new timestamp
            context.update_cursor(current_timestamp)

            context.log.info(f"'sbbeachinfo Last Updated' timestamp changed: {current_timestamp}")
            try:
                slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f'sbbeachinfo updated to {current_timestamp}')
            except Exception as e:
                get_dagster_logger().error('Slack post error for sbbeachinfo updated')

            # Trigger the asset
            yield RunRequest(
                run_key=current_timestamp,
                asset_selection=[AssetKey(["waterquality", "sdbeachinfo_status"]),
                                    AssetKey(["waterquality", "beachwatch_closures_recent"]),
                                    AssetKey(["waterquality", "beachwatch_analyses_daily"]),
                                ]
            )
        else:
            context.log.info("No change in 'Last Updated' timestamp.")

    except Exception as e:
        context.log.error(f"Error monitoring website: {e}")
