import requests
import os
import re
import urllib.parse
from dagster import ( asset,
                     get_dagster_logger,
                      TimeWindowPartitionsDefinition,
DailyPartitionsDefinition,
schedule, RunRequest, define_asset_job, AssetKey,
AutomationCondition,
sensor,SensorEvaluationContext, AssetIn
                      )

from resilient_core import utils
from resilient_core.resources import minio
from resilient_core.utils import store_assets

import requests
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_string_dtype
from io import StringIO
import geopandas as gpd
import datetime
from resilient_core.utils.constants import ICONS
from bs4 import BeautifulSoup
import hashlib
import json

LLM_MODEL=os.environ.get("OPENAI_BASE_MODEL", "gemma3")
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_UPDATES", "#test")

baseurl = "https://beachwatch.waterboards.ca.gov/public/"
reports_page=f"{baseurl}result.php"
exports_page=f"{baseurl}export.php"

s3_output_path = 'tijuana/beachwatch'

# New CoSD beach info site (replaced www.sdbeachinfo.com)
COSD_BEACH_URL = "https://cosdapps.sandiegocounty.gov/sdbeachinfo/"
COSD_HOME_BLOCK = "screenservices/CoSD_Beach_Water_CW/MainFlow/HomeBlockNew"
COSD_BLOCK_MARKUP = "screenservices/CoSD_Beach_Water_CW/MainFlow/BlockMarkup"

# EventTypeId values from the CoSD API
COSD_EVENT_TYPE_ADVISORY = 1
COSD_EVENT_TYPE_CLOSURE = 2
COSD_EVENT_TYPE_WARNING = 3

# Map CoSD EventTypeId to old IndicatorID (for backward compatibility)
COSD_EVENT_TYPE_TO_INDICATOR = {
    COSD_EVENT_TYPE_CLOSURE: 1,   # Closure → IndicatorID 1
    0: 2,                          # Open (no event) → IndicatorID 2
    COSD_EVENT_TYPE_ADVISORY: 3,  # Advisory → IndicatorID 3
    COSD_EVENT_TYPE_WARNING: 4,   # Warning → IndicatorID 4
}

STATUS_COLOR = {
    'Closure': 'Red',
    'Advisory': 'Orange',
    'Open': 'Green',
    'Warning': 'Yellow',
}

STATUS_LABEL = {
    COSD_EVENT_TYPE_ADVISORY: 'Advisory',
    COSD_EVENT_TYPE_CLOSURE: 'Closure',
    COSD_EVENT_TYPE_WARNING: 'Warning',
}

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
            if data is None or len(data) == 0:
                get_dagster_logger().info("Empty response when downloading beachwatch data. No data yet for New Year?")
                return pd.DataFrame()
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
        beach_gdf=beach_gdf.drop(['date', 'time', 'id', 'Station_ID'], axis=1)
        beach_gdf['Icons'] = ICONS['beach']
        return beach_gdf


def beachwatch_closure_clean_data(beach_df) -> gpd.GeoDataFrame:
    # beach_df['date']=  pd.to_datetime(f'{beach_df["SampleDate"]}T{beach_df["SampleTime"]}')#, format='%Y-%m-%d %H:%M:%S')
    beach_df['start'] = pd.to_datetime(beach_df["Start Date"], format='%Y-%m-%d', errors='coerce')
    beach_df['end'] = pd.to_datetime(beach_df["End Date"], format='%Y-%m-%d', errors='coerce')
    beach_df['end']=beach_df['end'].fillna(value=datetime.datetime.today())
    beach_df['end'] = beach_df['end'].fillna(value=datetime.datetime.now())
    beach_df=beach_df.dropna(subset=['start'])
    gs = gpd.GeoSeries.from_xy(beach_df['Longitude'], beach_df['Latitude'])
    beach_gdf = gpd.GeoDataFrame(beach_df,
                                 geometry=gs,
                                 crs='EPSG:4326')
    #each_gdf=beach_gdf.drop(['date', 'time', 'id', 'Station_ID'], axis=1)
    beach_gdf['Icons'] = ICONS['beach']
    return beach_gdf
###########################
### CoSD Beach Info Scraper (new site: cosdapps.sandiegocounty.gov)
### Replaces the old www.sdbeachinfo.com/Home/GetTargetByID endpoint
###########################

def _cosd_make_fetch_js(url, module_version, api_version, screen_data_vars, input_params, csrf_token):
    """Returns JS code string to make a POST request from within the Playwright browser context."""
    return f"""
    async () => {{
        const resp = await fetch('{url}', {{
            method: 'POST',
            headers: {{
                'Content-Type': 'application/json; charset=UTF-8',
                'Accept': 'application/json',
                'X-CSRFToken': {json.dumps(csrf_token)},
            }},
            credentials: 'include',
            body: JSON.stringify({{
                "versionInfo": {{"moduleVersion": {json.dumps(module_version)}, "apiVersion": {json.dumps(api_version)}}},
                "viewName": "MainFlow.Home",
                "screenData": {{"variables": {json.dumps(screen_data_vars)}}},
                "inputParameters": {json.dumps(input_params)}
            }})
        }});
        return {{status: resp.status, data: await resp.json()}};
    }}
    """


def cosd_get_beach_status() -> list[dict]:
    """
    Fetch all beach monitoring sites and their current advisory/closure status
    from the CoSD OutSystems beach info app.

    Returns a list of site dicts with status, coordinates, and event details.
    """
    from playwright.sync_api import sync_playwright

    sites_by_id = {}
    advisory_ids = set()
    closure_ids = set()
    warning_ids = set()
    event_details = {}
    captured_api_versions = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=['--ignore-certificate-errors', '--no-sandbox', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            ignore_https_errors=True,
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()

        def on_request(request):
            if 'screenservices' in request.url:
                try:
                    payload = json.loads(request.post_data or '{}')
                    vi = payload.get('versionInfo', {})
                    name = request.url.split('/')[-1].split('?')[0]
                    if vi.get('apiVersion'):
                        captured_api_versions[name] = {
                            'apiVersion': vi['apiVersion'],
                            'moduleVersion': vi['moduleVersion'],
                            'url': request.url,
                        }
                except Exception:
                    pass

        page.on('request', on_request)
        page.goto(COSD_BEACH_URL, wait_until='networkidle', timeout=45000)
        page.wait_for_timeout(3000)

        # Get current module version
        mv_resp = page.request.get(f"{COSD_BEACH_URL}moduleservices/moduleversioninfo")
        module_version = mv_resp.json().get('versionToken', '')
        get_dagster_logger().info(f"CoSD module version: {module_version}")

        # Extract CSRF token from nr2Users cookie
        cookies = {c['name']: c['value'] for c in context.cookies()}
        nr2 = urllib.parse.unquote(cookies.get('nr2Users', ''))
        csrf_token = next((part[4:] for part in nr2.split(';') if part.startswith('crf=')), '')
        get_dagster_logger().info(f"CoSD CSRF token obtained: {bool(csrf_token)}")

        # Get apiVersion for GetSiteById from captured requests (loaded on page init)
        get_site_api_version = captured_api_versions.get('ScreenDataSetGetSiteById', {}).get('apiVersion', 'dPYSR+2HPtITJ2nn6p+WaQ')

        def call_get_site_by_id(event_type_id: int) -> list[dict]:
            url = f"{COSD_BEACH_URL}{COSD_HOME_BLOCK}/ScreenDataSetGetSiteById"
            screen_vars = {
                "ShowDetails": True, "MapCenter": "33.001319,-117.278388",
                "SiteIdAux": "0", "EventTypeId": event_type_id,
                "ZoomCurrent": 8, "id": "1", "_idInDataFetchStatus": 1
            }
            js = _cosd_make_fetch_js(url, module_version, get_site_api_version, screen_vars,
                                     {"StartIndex": 0, "MaxRecords": 900}, csrf_token)
            result = page.evaluate(f"({js})()")
            if result.get('status') != 200:
                get_dagster_logger().warning(f"GetSiteById EventTypeId={event_type_id} returned {result.get('status')}")
                return []
            if result.get('data', {}).get('versionInfo', {}).get('hasApiVersionChanged'):
                get_dagster_logger().warning("GetSiteById: apiVersion has changed — results may be stale")
            return result.get('data', {}).get('data', {}).get('List', {}).get('List', [])

        def call_get_events_list(site_id: str) -> list[dict]:
            # apiVersion from user-captured curl; detect changes via hasApiVersionChanged
            api_version = 'JqlpdF6Psvnxe_pXo1Jjtw'
            url = f"{COSD_BEACH_URL}{COSD_BLOCK_MARKUP}/ScreenDataSetGetEventsList"
            screen_vars = {"SiteId": site_id, "_siteIdInDataFetchStatus": 1}
            js = _cosd_make_fetch_js(url, module_version, api_version, screen_vars,
                                     {"StartIndex": 0, "MaxRecords": 50}, csrf_token)
            result = page.evaluate(f"({js})()")
            if result.get('status') != 200:
                get_dagster_logger().warning(f"GetEventsList site={site_id} returned {result.get('status')}")
                return []
            if result.get('data', {}).get('versionInfo', {}).get('hasApiVersionChanged'):
                get_dagster_logger().warning(f"GetEventsList: apiVersion changed for site {site_id}")
            return result.get('data', {}).get('data', {}).get('List', {}).get('List', [])

        # Step 1: Get all 89+ monitoring sites with coordinates
        all_site_items = call_get_site_by_id(0)
        get_dagster_logger().info(f"CoSD: fetched {len(all_site_items)} total sites")
        for item in all_site_items:
            site_id = item.get('Id', '')
            sites_by_id[site_id] = {
                'SiteID': site_id,
                'DehID': site_id,
                'Name': item.get('LocationName', ''),
                'BeachName': item.get('BeachName', ''),
                'City': item.get('City', ''),
                'Region': item.get('Region', ''),
                'Latitude': float(item.get('Latitude', 0) or 0),
                'Longitude': float(item.get('Longitude', 0) or 0),
                'Description': item.get('Description', ''),
                'beachStatus': 'Open',
                'IndicatorID': 2,
                'Active': True,
                'RBGColor': 'Green',
                'Advisory': '',
                'Closure': '',
                'StatusSince': '',
                'StatusNote': '',
                'LocationType': 'Monitoring Point',
            }

        # Step 2: Mark advisory sites (EventTypeId=1)
        advisory_items = call_get_site_by_id(COSD_EVENT_TYPE_ADVISORY)
        get_dagster_logger().info(f"CoSD: {len(advisory_items)} advisory sites")
        for item in advisory_items:
            site_id = item.get('Id', '')
            advisory_ids.add(site_id)
            if site_id in sites_by_id:
                sites_by_id[site_id]['beachStatus'] = 'Advisory'
                sites_by_id[site_id]['IndicatorID'] = 3
                sites_by_id[site_id]['RBGColor'] = 'Orange'
            else:
                # Site appeared in advisory list but not in all-sites (use advisory item data)
                sites_by_id[site_id] = {
                    'SiteID': site_id, 'DehID': site_id,
                    'Name': item.get('LocationName', ''), 'BeachName': item.get('BeachName', ''),
                    'City': item.get('City', ''), 'Region': item.get('Region', ''),
                    'Latitude': float(item.get('Latitude', 0) or 0),
                    'Longitude': float(item.get('Longitude', 0) or 0),
                    'Description': item.get('Description', ''),
                    'beachStatus': 'Advisory', 'IndicatorID': 3, 'Active': True,
                    'RBGColor': 'Orange', 'Advisory': '', 'Closure': '',
                    'StatusSince': '', 'StatusNote': '', 'LocationType': 'Monitoring Point',
                }

        # Step 3: Mark closure sites (EventTypeId=2)
        closure_items = call_get_site_by_id(COSD_EVENT_TYPE_CLOSURE)
        get_dagster_logger().info(f"CoSD: {len(closure_items)} closure sites")
        for item in closure_items:
            site_id = item.get('Id', '')
            closure_ids.add(site_id)
            if site_id in sites_by_id:
                sites_by_id[site_id]['beachStatus'] = 'Closure'
                sites_by_id[site_id]['IndicatorID'] = 1
                sites_by_id[site_id]['RBGColor'] = 'Red'
            else:
                sites_by_id[site_id] = {
                    'SiteID': site_id, 'DehID': site_id,
                    'Name': item.get('LocationName', ''), 'BeachName': item.get('BeachName', ''),
                    'City': item.get('City', ''), 'Region': item.get('Region', ''),
                    'Latitude': float(item.get('Latitude', 0) or 0),
                    'Longitude': float(item.get('Longitude', 0) or 0),
                    'Description': item.get('Description', ''),
                    'beachStatus': 'Closure', 'IndicatorID': 1, 'Active': True,
                    'RBGColor': 'Red', 'Advisory': '', 'Closure': '',
                    'StatusSince': '', 'StatusNote': '', 'LocationType': 'Monitoring Point',
                }

        # Step 4: Mark warning sites (EventTypeId=3)
        warning_items = call_get_site_by_id(COSD_EVENT_TYPE_WARNING)
        get_dagster_logger().info(f"CoSD: {len(warning_items)} warning sites")
        for item in warning_items:
            site_id = item.get('Id', '')
            warning_ids.add(site_id)
            if site_id in sites_by_id:
                # Only downgrade to Warning if not already Closure or Advisory
                if sites_by_id[site_id]['beachStatus'] == 'Open':
                    sites_by_id[site_id]['beachStatus'] = 'Warning'
                    sites_by_id[site_id]['IndicatorID'] = 4
                    sites_by_id[site_id]['RBGColor'] = 'Yellow'
            else:
                sites_by_id[site_id] = {
                    'SiteID': site_id, 'DehID': site_id,
                    'Name': item.get('LocationName', ''), 'BeachName': item.get('BeachName', ''),
                    'City': item.get('City', ''), 'Region': item.get('Region', ''),
                    'Latitude': float(item.get('Latitude', 0) or 0),
                    'Longitude': float(item.get('Longitude', 0) or 0),
                    'Description': item.get('Description', ''),
                    'beachStatus': 'Warning', 'IndicatorID': 4, 'Active': True,
                    'RBGColor': 'Yellow', 'Advisory': '', 'Closure': '',
                    'StatusSince': '', 'StatusNote': '', 'LocationType': 'Monitoring Point',
                }

        # Step 5: Get event details (issue date, description) for affected sites
        affected_ids = advisory_ids | closure_ids | warning_ids
        get_dagster_logger().info(f"CoSD: fetching event details for {len(affected_ids)} affected sites")
        for site_id in affected_ids:
            events = call_get_events_list(site_id)
            if events:
                event_details[site_id] = events[0]  # most recent event

        browser.close()

    # Step 6: Merge event details into site records
    for site_id, event_item in event_details.items():
        if site_id not in sites_by_id:
            continue
        site = sites_by_id[site_id]
        event = event_item.get('Event', {})
        event_type = event_item.get('EventType', {})
        city_label = event_item.get('City', {}).get('Label', '')

        # Parse issue datetime
        issue_dt_str = event.get('IssueDateTime', '')
        status_since = ''
        if issue_dt_str and not issue_dt_str.startswith('1900'):
            try:
                dt = datetime.datetime.fromisoformat(issue_dt_str.replace('Z', '+00:00'))
                status_since = dt.strftime(f"%b {dt.day}, %Y")
            except Exception:
                status_since = issue_dt_str[:10]

        description_issue = event.get('DescriptionIssue', '')
        event_type_id = event_type.get('Id', 0)
        status_label = STATUS_LABEL.get(event_type_id, site['beachStatus'])

        site['StatusSince'] = status_since
        site['StatusNote'] = description_issue or (f"{status_label} since {status_since}" if status_since else status_label)

        if site['beachStatus'] == 'Advisory':
            site['Advisory'] = description_issue or f"Advisory issued {status_since}"
        elif site['beachStatus'] == 'Closure':
            site['Closure'] = description_issue or f"Closure issued {status_since}"
        elif site['beachStatus'] == 'Warning':
            site['Advisory'] = description_issue or f"Warning issued {status_since}"

        if city_label and not site['City']:
            site['City'] = city_label

    return list(sites_by_id.values())


def cosd_build_geodataframe(sites: list[dict]) -> gpd.GeoDataFrame:
    """Convert the site list from cosd_get_beach_status() to a GeoDataFrame."""
    df = pd.DataFrame(sites)
    if df.empty:
        return gpd.GeoDataFrame()

    # Filter out sites with missing coordinates
    valid = df[(df['Latitude'] != 0) | (df['Longitude'] != 0)].copy()
    if valid.empty:
        get_dagster_logger().warning("All sites have zero coordinates")
        valid = df.copy()

    gs = gpd.GeoSeries.from_xy(valid['Longitude'], valid['Latitude'])
    gdf = gpd.GeoDataFrame(valid, geometry=gs, crs='EPSG:4326')

    # Map RBGColor to Icon
    def _rgb_to_icon(color):
        return ICONS.get(color, ICONS.get('beach', 'place'))

    gdf['Icon'] = gdf['RBGColor'].apply(_rgb_to_icon)
    return gdf


@asset(group_name="tijuana", key_prefix="waterquality",
       name="sdbeachinfo_status", required_resource_keys={"s3", "airtable"},
       automation_condition=AutomationCondition.eager())
def get_sdbeachinfo_status(context) -> gpd.GeoDataFrame:
    """
    Collects San Diego County beach advisory/closure status from the CoSD beach info site.
    Source: https://cosdapps.sandiegocounty.gov/sdbeachinfo/
    (Replaced the old www.sdbeachinfo.com/Home/GetTargetByID endpoint)
    """
    name = 'sdbeachinfo_status'
    description = '''Collects San Diego County beach advisory and closure status.
         Source: https://cosdapps.sandiegocounty.gov/sdbeachinfo/
         Uses the CoSD OutSystems API to retrieve all monitoring sites with:
         * Current status (Open / Advisory / Closure)
         * Site location (latitude/longitude)
         * Event details (issue date, description)
        '''
    source_url = COSD_BEACH_URL
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3

    try:
        sites = cosd_get_beach_status()
    except Exception as e:
        get_dagster_logger().error(f"Failed to fetch CoSD beach status: {e}")
        raise

    if not sites:
        get_dagster_logger().warning("CoSD beach status returned no sites")
        return gpd.GeoDataFrame()

    closures_gdf = cosd_build_geodataframe(sites)
    get_dagster_logger().info(f"CoSD beach status: {len(closures_gdf)} sites total")
    get_dagster_logger().info(f"  Advisory: {(closures_gdf['beachStatus']=='Advisory').sum()}")
    get_dagster_logger().info(f"  Closure: {(closures_gdf['beachStatus']=='Closure').sum()}")
    get_dagster_logger().info(f"  Open: {(closures_gdf['beachStatus']=='Open').sum()}")

    output_cols = ['SiteID', 'DehID', 'Name', 'BeachName', 'City', 'Region',
                   'Latitude', 'Longitude', 'IndicatorID', 'Active', 'RBGColor', 'Icon',
                   'Description', 'Advisory', 'Closure',
                   'beachStatus', 'LocationType', 'StatusSince', 'StatusNote', 'geometry']
    available_cols = [c for c in output_cols if c in closures_gdf.columns]
    closures_gdf = closures_gdf[available_cols]

    filename = f'{s3_output_path}/output/current/sdbeachinfo_status'
    store_assets.geodataframe_to_s3(closures_gdf, filename, s3_resource,
                                    formats=['json', 'csv', 'geojson'], metadata=metadata)
    return closures_gdf


@asset(
    group_name="tijuana",
    key_prefix="waterquality",
    name="beach_site_id_mapping",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    ins={
        "sdbeachinfo_status": AssetIn(key=AssetKey(["waterquality", "sdbeachinfo_status"])),
    },
    description=(
        "Cross-reference between CoSD beach monitoring numeric site IDs and "
        "California state beachwatch Station_IDs (letter-number format). "
        "Matched by nearest-neighbor spatial join."
    ),
)
def beach_site_id_mapping(context, sdbeachinfo_status: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Produces a mapping table linking CoSD numeric site IDs to CA state
    beachwatch Station_IDs.  Both systems cover the same San Diego beaches
    but use incompatible identifiers, making this table the join key for
    cross-referencing advisory/closure histories from the state dataset
    against CoSD site descriptions and locations.

    Matching strategy: nearest-neighbor spatial join (projected to UTM 11N
    for accurate metre distances). Rows with match_distance_m > 500 should
    be reviewed — they likely indicate sites with no direct counterpart.
    """
    s3_resource = context.resources.s3

    # --- CoSD sites (already fetched upstream) ---
    cosd_gdf = sdbeachinfo_status.copy()
    if cosd_gdf.empty:
        get_dagster_logger().warning("beach_site_id_mapping: sdbeachinfo_status is empty, skipping")
        return pd.DataFrame()

    cosd_keep = [c for c in ['SiteID', 'Name', 'BeachName', 'City', 'Description', 'geometry'] if c in cosd_gdf.columns]
    cosd_gdf = cosd_gdf[cosd_keep]

    # --- CA state beachwatch stations ---
    # Use a year with dense data; blank year returns all years (slow) so use current.
    current_year = datetime.date.today().year
    get_data = {**formdata, 'year': current_year}
    try:
        bw_raw = get_beachwatch_data(reports_page, exports_page, get_data)
    except Exception as e:
        get_dagster_logger().warning(f"beach_site_id_mapping: beachwatch fetch failed ({e}), skipping")
        return pd.DataFrame()

    if bw_raw.empty:
        get_dagster_logger().warning("beach_site_id_mapping: no beachwatch data returned")
        return pd.DataFrame()

    get_dagster_logger().info(f"beach_site_id_mapping: beachwatch columns: {list(bw_raw.columns)}")

    # Station_ID is the state letter-number ID (e.g. 'B-061'); keep it and the name/location cols.
    name_col = next((c for c in ['StationName', 'Station_Name', 'BeachName', 'Beach_Name', 'Name'] if c in bw_raw.columns), None)
    required = ['Station_ID', 'Latitude', 'Longitude']
    if not all(c in bw_raw.columns for c in required):
        get_dagster_logger().warning(f"beach_site_id_mapping: missing required columns {required} in beachwatch data")
        return pd.DataFrame()

    keep = required + ([name_col] if name_col else [])
    stations = (
        bw_raw[keep]
        .drop_duplicates(subset=['Station_ID'])
        .assign(
            Latitude=lambda d: pd.to_numeric(d['Latitude'], errors='coerce'),
            Longitude=lambda d: pd.to_numeric(d['Longitude'], errors='coerce'),
        )
        .dropna(subset=['Latitude', 'Longitude'])
    )

    bw_gdf = gpd.GeoDataFrame(
        stations,
        geometry=gpd.points_from_xy(stations['Longitude'], stations['Latitude']),
        crs='EPSG:4326',
    )

    # --- Spatial nearest-neighbor join (UTM zone 11N for metre distances) ---
    cosd_proj = cosd_gdf.to_crs('EPSG:32611')
    bw_proj = bw_gdf.to_crs('EPSG:32611')

    joined = gpd.sjoin_nearest(cosd_proj, bw_proj, how='left', distance_col='match_distance_m')

    rename = {'SiteID': 'cosd_id', 'Name': 'cosd_name', 'BeachName': 'cosd_beach_name',
              'City': 'cosd_city', 'Description': 'cosd_description',
              'Station_ID': 'bw_station_id'}
    if name_col:
        rename[name_col] = 'bw_station_name'

    out_cols = [c for c in list(rename.keys()) + ['match_distance_m'] if c in joined.columns]
    mapping = joined[out_cols].copy().rename(columns=rename)
    mapping['match_distance_m'] = mapping['match_distance_m'].round(1)
    mapping['match_quality'] = mapping['match_distance_m'].apply(
        lambda d: 'good' if d < 200 else ('ok' if d < 500 else 'poor')
    )

    good = (mapping['match_quality'] == 'good').sum()
    ok = (mapping['match_quality'] == 'ok').sum()
    poor = (mapping['match_quality'] == 'poor').sum()
    get_dagster_logger().info(
        f"beach_site_id_mapping: {len(mapping)} CoSD sites — "
        f"{good} good (<200 m), {ok} ok (<500 m), {poor} poor (>500 m)"
    )

    name = 'beach_site_id_mapping'
    description = (
        "Cross-reference between CoSD beach monitoring site IDs (numeric) and "
        "CA state beachwatch Station_IDs (letter-number). Matched by nearest-neighbor "
        "spatial join. match_distance_m is the distance in metres between the two points; "
        "rows with match_quality='poor' (>500 m) should be reviewed manually."
    )
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=COSD_BEACH_URL)
    filename = f'{s3_output_path}/output/reference/beach_site_id_mapping'
    store_assets.dataframe_to_s3(mapping, filename, s3_resource, metadata=metadata)

    return mapping


def get_cache_s3_path(cache_key):
    """Get the S3 path for a translation cache entry"""
    return f'{s3_output_path}/cache/translations/{cache_key}.json'

def init_translation_cache():
    """Initialize the translation cache - no setup needed for S3 JSON storage"""
    get_dagster_logger().info("Translation cache initialized for S3 JSON storage")
    pass

def get_translation_cache_key(message, language_code, model):
    """Generate a cache key for the translation"""
    content = f"{message}|{language_code}|{model}"
    return hashlib.md5(content.encode('utf-8')).hexdigest()

def get_cached_translation(message, language_code, model, s3_resource):
    """Retrieve cached translation if it exists from S3"""
    if not message or message.strip() == '':
        return None

    cache_key = get_translation_cache_key(message, language_code, model)
    cache_path = get_cache_s3_path(cache_key)

    try:
        # Use Minio client directly with correct syntax
        minio_client = s3_resource.getClient()
        cache_data = minio_client.get_object(s3_resource.S3_BUCKET, cache_path)
        cache_content = cache_data.read().decode('utf-8')
        cache_json = json.loads(cache_content)

        get_dagster_logger().debug(f"Cache hit for translation: {message[:50]}...")
        return cache_json['translated_text']

    except Exception as e:
        get_dagster_logger().debug(f"Cache miss for translation: {message[:50]}... - {e}")
        return None

def cache_translation(message, translation, language_code, model, s3_resource):
    """Store translation in S3 cache"""
    if not message or message.strip() == '':
        return

    cache_key = get_translation_cache_key(message, language_code, model)
    cache_path = get_cache_s3_path(cache_key)

    cache_data = {
        'input_hash': cache_key,
        'original_text': message,
        'translated_text': translation,
        'language_code': language_code,
        'model': model,
        'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat()
    }

    try:
        cache_json = json.dumps(cache_data, indent=2)
        s3_resource.putFile_text(data=cache_json, path=cache_path)
        get_dagster_logger().debug(f"Cached translation for: {message[:50]}...")

    except Exception as e:
        get_dagster_logger().warning(f"Error caching translation: {e}")

def translate(message, openai_client, s3_resource, language_code='es', model=LLM_MODEL):
    if message is None or message == '':
        return ''



    # Check cache first
    cached_result = get_cached_translation(message, language_code, model, s3_resource)
    if cached_result is not None:
        return cached_result

    get_dagster_logger().debug(f" get beach closure translation: {message}")
    instructions = f'''
Translate the following text into Spanish using the regional expressions common in the Tijuana area. Please:
   * Use "aguas negras" in place of "sewage."
   * Use "aguas pluviales" for "runoff water."
   * Adapt terms like "dashboard" to more colloquial equivalents, for example "tablero."
   * Ensure the translation resonates naturally with the local public in Tijuana.
Maintain the html elements" 
    '''

    prompt = f'{message } (Note: AI-generated translation.)'
    try:
        resp= openai_client.chat.completions.create(
            model=model,
            messages=[ {
                'role': 'user',
                 "content": f"{instructions}{prompt}"
                        }]
        )
    except Exception as e:
        get_dagster_logger().error(f"translation issue : {e}")
    # resp= openai_client.responses.create(
    #     model=model,
    #     instructions=instructions,
    #     input= prompt
    # )
    #get_dagster_logger().info(f" get beach closure response: {resp}")
    # Handle different response formats from dagster_openai wrapper
    try:
        if isinstance(resp, str):
            # If the response is already a string (processed by dagster wrapper)
            translation = resp
        elif hasattr(resp, 'choices') and resp.choices:
            # Standard OpenAI response format
            translation = resp.choices[0].message.content
        elif hasattr(resp, 'content'):
            # Alternative response format
            translation = resp.content
        else:
            # Fallback - convert to string
            translation = str(resp)
            get_dagster_logger().warning(f"Unexpected response format: {type(resp)}")
    except AttributeError as e:
        get_dagster_logger().error(f"Error accessing response content: {e}")
        translation = str(resp)
    get_dagster_logger().debug(f" get beach closure translation: {translation}")

    # Cache the result
    cache_translation(message, translation, language_code, model, s3_resource)

    return f'{translation}'

@asset(group_name="tijuana", key_prefix="waterquality",
       name="sdbeachinfo_status_translation", required_resource_keys={"s3", "airtable","openai"},
       ins={
           "sdbeachinfo_status": AssetIn(key=AssetKey(['waterquality','sdbeachinfo_status'])),
       },
        compute_kind="OpenAI",
       automation_condition=AutomationCondition.eager()
       )
def beachwatch_status_translation(context, sdbeachinfo_status: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
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
    source_url = COSD_BEACH_URL
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    openai_resource = context.resources.openai
    closure_gdf = sdbeachinfo_status
    # Initialize cache on first use
    init_translation_cache()

    with openai_resource.get_client(context) as client:
        closure_translated = closure_gdf
        #closure_translated = closure_gdf.head(10)
        closure_translated['Description_es']= closure_translated['Description'].apply(lambda x : translate(x, client, s3_resource, language_code='es', model=LLM_MODEL))
        #closure_translated['Closure_es']= closure_translated['Closure'].apply(lambda x : translate(x, client, language_code='es', model=LLM_MODEL))
        #closure_translated['Advisory_es']= closure_translated['Advisory'].apply(lambda x : translate(x, client, language_code='es', model=LLM_MODEL))
        closure_translated['StatusNote_es'] = closure_translated['StatusNote'].apply(
            lambda x: translate(x, client, s3_resource, language_code='es', model=LLM_MODEL))

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
    if (len(beach_df)==0 ):
        get_dagster_logger().info(f'beachwatch_analyses_daily {len(beach_df)} is zero. No file written', )
        return
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
       ins={
           "beachwatch_closures_recent": AssetIn(key=AssetKey([f"waterquality", "beachwatch_closures_recent"])),
       }
       )
def beachwatch_closure_recent_weekly(context, beachwatch_closures_recent: pd.DataFrame)-> pd.DataFrame:
    name = 'beachwatch_closures_recent_weekly'
    description = '''Aggregates by conount  Closure notices for San Diego County from the California  Data Site 
                 https://beachwatch.waterboards.ca.gov/public/
                 for the present year
                '''
    metadata = store_assets.objectMetadata(name=name, description=description)

    s3_resource = context.resources.s3
    closure_gdf = beachwatch_closures_recent
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
    """Return the CoSD module version token, used as a change-detection cursor."""
    import warnings
    import urllib3
    warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)
    # moduleservices/moduleversioninfo is a public GET endpoint; the screenservices
    # data-action endpoints require a POST with session auth, so we don't touch them here.
    mv_resp = requests.get(f"{COSD_BEACH_URL}moduleservices/moduleversioninfo", verify=False, timeout=15)
    if mv_resp.status_code == 200:
        return mv_resp.json().get('versionToken', '')
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
