import requests
import os
from dagster import ( asset,
                     get_dagster_logger ,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule,
                      AutomationCondition
                      )

from ..resources import minio

import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from datetime import datetime, timedelta, date
import re
from ..utils.constants import ICONS
from ..utils import store_assets

s3_output_path = 'tijuana/gis/boundaries'
tracts_geojson="https://oss.resilientservice.mooo.com/resilentpublic/tijuana/geographic/tracts_cleanwater.geojson"
subregion_url="https://geo.sandag.org/server/rest/services/Hosted/Subregional_Areas_2020/FeatureServer/0/query?where=1%3D1&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&defaultSR=&spatialRel=esriSpatialRelIntersects&distance=&units=esriSRUnit_Foot&relationParam=&outFields=sra%2Cname%2Cglobalid&returnGeometry=true&maxAllowableOffset=&geometryPrecision=&outSR=&havingClause=&gdbVersion=&historicMoment=&returnDistinctValues=false&returnIdsOnly=false&returnCountOnly=false&returnExtentOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&multipatchOption=xyFootprint&resultOffset=&resultRecordCount=&returnTrueCurves=false&returnCentroid=false&returnEnvelope=false&timeReferenceUnknownClient=false&maxRecordCountFactor=&sqlFormat=none&resultType=&datumTransformation=&lodType=geohash&lod=&lodSR=&cacheHint=false&f=geojson"
@asset(group_name="tijuana",key_prefix="gis",
       name="subregional_areas",
       required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager()
       )
def subregions(context) -> gpd.GeoDataFrame:
    s3_resource = context.resources.s3
    sr_gdf = gpd.read_file(subregion_url)
    sr_gdf.to_crs('EPSG:4326')
    sr_gdf = sr_gdf.drop_duplicates(['sra','globalid'])
    filename = f'{s3_output_path}/subregional_areas'
    store_assets.geodataframe_to_s3(sr_gdf, filename, s3_resource)
    return sr_gdf

@asset(group_name="tijuana",key_prefix="gis",
       name="tracts",
       required_resource_keys={"s3"}, )
def tracts(context):

    s3_resource = context.resources.s3
    sr_gdf = gpd.read_file(tracts_geojson)
    sr_gdf.to_crs('EPSG:4326')
    sr_gdf=sr_gdf[['tract', 'geometry']].drop_duplicates(['tract'])
    filename = f'{s3_output_path}/tracts'
    store_assets.geodataframe_to_s3(sr_gdf, filename, s3_resource)
    return sr_gdf

