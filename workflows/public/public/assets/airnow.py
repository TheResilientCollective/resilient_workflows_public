import json

import requests
import urllib
import os
import datetime
import pytz
import pandas as pd
import geopandas as gpd
gpd.options.io_engine = "pyogrio"
# fiona takes a long time to build in the docker container.
#import fiona
#from fiona.drvsupport import supported_drivers
#supported_drivers['LIBKML'] = 'rw'
#supported_drivers['KML'] = 'rw'
#supported_drivers['CSV'] = 'rw'
import kml2geojson
from io import StringIO, BytesIO
from shapely.geometry import Point
from dagster import ( asset,
                     get_dagster_logger,
                      AssetKey,AutomationCondition
                      )
from ..utils import store_assets

#https://www.airnow.gov/aqi/aqi-calculator/
#https://docs.airnowapi.org/webservices

output_path='tijuana/airnow'
base_url= 'https://www.airnowapi.org/aq'
# url https://www.airnowapi.org/aq/kml/Combined/?DATE=2025-04-18T19&BBOX=-117.611081,32.5,-116.08094,33.505025&SRS=EPSG:4326&API_KEY=

def AIRNOW_API_KEY():
    return os.environ.get('AIRNOW_API_KEY')
# bounding boxes

sd_bbox='-117.611081,32.528832,-116.08094,33.505025'
tj_lat=32.552044
tj_lon=-117.081305
def date_hour_param():
    return datetime.datetime.now(pytz.UTC).strftime('%Y-%m-%dT%H')
def date_hour_future_param(num_hours=1):
    date= datetime.datetime.now(pytz.UTC) + datetime.timedelta(hours=num_hours)
    return date.strftime('%Y-%m-%dT%H')
def date_param():
    return datetime.datetime.now(pytz.UTC).strftime('%Y-%m-%d')

@asset(group_name="tijuana",key_prefix="airquality",
       name="airnow_forecast_contours_kml",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def get_aq_combined_kml(context):
    bbox=sd_bbox
    api_key=AIRNOW_API_KEY()
    s3_resource=context.resources.s3
    name = 'airnow_aq_contours'
    description = '''
       AirNow Air Quality Contours 
       '''
    source_url = 'https://docs.airnowapi.org/KML/Combined/docs'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    url = f'{base_url}/kml/Combined/?DATE={date_hour_future_param(num_hours=-1)}&BBOX={bbox}&SRS=EPSG:4326&API_KEY={api_key}'
    get_dagster_logger().info(url)
    try:
        response = requests.get(url)
        if response.status_code== 429:
            get_dagster_logger().error(f"Rate error. Lower quest rate: {response.text}")
            raise f"Rate Error. Waiting a period: {response.text}"
        elif response.status_code > 400 and response.status_code< 500:
            get_dagster_logger().error(f"bad url: {response.text}")
            raise f"Bad URL: {response.text}"
        elif response.status_code > 500 and response.status_code < 600:
            get_dagster_logger().error(f"service error: {response.text}")
            raise f"Service Issues: {response.text}"
        xml = response.text
        filename = f"{output_path}/raw/aq_forecast_contours.kml"
        s3_resource.putFile_text(data=xml, path=f"{filename}")
        return xml
    except Exception as e:
        get_dagster_logger().error(f"Error reading File: {e}")
        raise f"Error reading File: {e}"
@asset(group_name="tijuana",key_prefix="airquality",
       name="airnow_forecast_contours",
       required_resource_keys={"s3"},
       deps=[AssetKey(['airquality', 'airnow_forecast_contours_kml'])],
       automation_condition=AutomationCondition.eager())
def aq_combined_geojson(context):
    bbox=sd_bbox
    api_key=AIRNOW_API_KEY()
    s3_resource=context.resources.s3
    name = 'airnow_aq_contours'
    description = '''
       AirNow Air Quality Contours 
       '''
    source_url = 'https://docs.airnowapi.org/KML/Combined/docs'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    aq_combined_kml = context.repository_def.load_asset_value(AssetKey([f"airquality", "airnow_forecast_contours_kml"]))

    # 1. (Optional) List available layers in the KML
    # kml_buffer = aq_combined_kml.encode('utf-8')
    # layers = fiona.listlayers(kml_buffer)
    # get_dagster_logger.info("Available layers:", layers)

    # 2. Enable KML support in GeoPandas/Fiona
   # gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'


    try:
        kml_buffer = StringIO(aq_combined_kml)
        geojson_data = kml2geojson.main.convert(kml_buffer)
        get_dagster_logger().info(geojson_data)
        filename = f"{output_path}/raw/aq_forecast_contours.geojson.json"
        s3_resource.putFile_text(data=json.dumps(geojson_data[0], indent=2), path=f"{filename}")
        kml_buffer = StringIO(json.dumps(geojson_data[0], indent=2))
        gdf = gpd.GeoDataFrame.from_file(kml_buffer)
        get_dagster_logger().info(gdf)
        filename = f"{output_path}/output/aq_forecast_contours"
        store_assets.geodataframe_to_s3(gdf, filename, s3_resource, metadata=metadata, formats=['geojson'])
        return gdf
    except Exception as e:
        get_dagster_logger().error(f"Error reading File: {e}")
        raise f"Error reading File: {e}"



#  url='https://www.airnowapi.org/aq/forecast/latLong/?format=text/csv&latitude=32.528832&longitude=-117.611081&date=2025-04-18&distance=25&API_KEY='
@asset(group_name="tijuana",key_prefix="airquality",
       name="airnow_forecasts",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def get_aq_forecast(context):
    bbox=sd_bbox
    api_key=AIRNOW_API_KEY()
    s3_resource = context.resources.s3
    name = 'airnow_aq_forecast'
    description = '''
       AirNow Air Quality Forecast 
       '''
    source_url = 'https://docs.airnowapi.org/forecastsbylatlon/docs'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    url = f'{base_url}/forecast/latLong/?format=text/csv&latitude={tj_lat}2&longitude={tj_lon}&distance=25&API_KEY={api_key}'
# date not required. uncomment to use date
    #url = f'{base_url}/forecast/latLong/?format=text/csv&latitude={tj_lat}2&longitude={tj_lon}&date={date_param()}&distance=25&API_KEY={api_key}'
    get_dagster_logger().info(url)
    try:
        response = requests.get(url)
        if response.status_code == 200:
            csv = response.text
            filename = f"{output_path}/raw/aq_forecast.csv"
            s3_resource.putFile_text(data=csv, path=f"{filename}")
            csv_buffer = StringIO(csv)
            df = pd.read_csv(csv_buffer)
            gs = gpd.GeoSeries.from_xy(df['Longitude'], df['Latitude'])
            gdf = gpd.GeoDataFrame(df,geometry=gs,
                                     crs='EPSG:4326')
            filename = f"{output_path}/output/aq_forecast"
            store_assets.geodataframe_to_s3(gdf, filename, s3_resource, metadata=metadata, formats=['geojson', 'csv'])
            return gdf
        else:
            get_dagster_logger().error(f"Error reading service:{response.status_code} {response.text}")
            raise f"Error reading service:{response.status_code}  {response.text}"
    except Exception as e:
        get_dagster_logger().error(f"Error reading File: {e}")
        raise f"Error reading File: {e}"

# url='https://www.airnowapi.org/aq/data/?startDate=2025-04-18T19&endDate=2025-04-18T20&parameters=OZONE,PM25&BBOX=-117.611081,32.528832,-116.08094,33.505025&dataType=A&format=application/json&verbose=0&monitorType=0&includerawconcentrations=0&API_KEY='

#  url='https://www.airnowapi.org/aq/data/?startDate=2025-04-18T21&endDate=2025-04-18T22&parameters=OZONE,PM25,PM10&BBOX=-124.205070,28.716781,-75.337882,45.419415&dataType=A&format=application/json&verbose=0&monitorType=0&includerawconcentrations=0&API_KEY=701918E6-528C-4501-B750-DDC4A87DD2EA'
@asset(group_name="tijuana",key_prefix="airquality",
       name="airnow_current",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def get_aq_site(context,):
    bbox=sd_bbox
    api_key=AIRNOW_API_KEY()
    s3_resource = context.resources.s3
    name = 'airnow_aq_current'
    description = '''
       AirNow Air Quality Current Data 
       '''
    source_url = 'https://docs.airnowapi.org/CurrentObservationsByLatLon/docs'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    url = f'{base_url}/data/?startDate={date_hour_param()}&endDate={date_hour_future_param()}&parameters=OZONE,PM25,PM10&BBOX={bbox}&dataType=A&format=application/json&verbose=0&monitorType=0&includerawconcentrations=0&API_KEY={api_key}'
    get_dagster_logger().info(url)
    try:
        df=pd.read_json(url)
        df['geometry'] = df.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")

        filename = f"{output_path}/output/aq_current"
        store_assets.geodataframe_to_s3(gdf, filename, s3_resource, metadata=metadata, formats=['geojson', 'csv'])
        return gdf
    except Exception as e:
        get_dagster_logger().error(f"Error reading File: {e}")
        raise f"Error reading File: {e}"

@asset()
def get_aq_history(context, date=None, api_key=AIRNOW_API_KEY):
    bbox=sd_bbox
    api_key=AIRNOW_API_KEY()
    pass
