import logging
import json
from typing import List

import pandas as pd
import geopandas as gpd
import csv
import os
from datetime import datetime, timedelta
import pytz

import awswrangler as wr
from dagster import get_dagster_logger
from foursquare.data_sdk.models import DatasetMetadata

from pydantic_schemaorg.Dataset import Dataset
from pydantic_schemaorg.DataDownload import DataDownload
from pydantic_schemaorg.URL import URL
from pydantic_schemaorg.Organization import Organization
from pydantic_schemaorg.PropertyValue import PropertyValue
from ..resources.minio import S3Resource
def getTodayAsIso():
    return datetime.now(pytz.timezone('America/Los_Angeles')).isoformat()
def fix_col_types(df, date_format=None):
    columns = [col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])]
    for col in columns:
        df[col] = df[col].apply(lambda x: x.strftime(date_format) if pd.notnull(x) and date_format else x.isoformat() if pd.notnull(x) else None)
    return df
def addLastUpdatedGeojson(json_str, date_str) -> str:
    json_obj = json.loads(json_str)
    json_obj['lastUpdated'] = date_str
    return json.dumps(json_obj, indent=2)
def addLastUpdatedRecords(json_str, date_str) -> str:
    json_obj = json.loads(json_str)
    new_json = {'lastUpdated': date_str, 'data': json_obj}
    return json.dumps(new_json, indent=2)
def geodataframe_to_s3(geodataframe, path_w_basename, s3_resource:S3Resource,
                       formats=[
                             'geojson',
                             'csv',
                           #'json' # write a non-geometry json
                            # 'parquet',
                             # 'arrow',
                         ], metadata=None

                       ):
    date = getTodayAsIso()
    distributions:List[distribution]=[]
    for format in formats:
       if format == 'geojson':
            df = fix_col_types(geodataframe)
            gdf_json = df.to_json()
            new_json = addLastUpdatedGeojson(gdf_json, date)
            path = f"{path_w_basename}.geojson"
            object = s3_resource.putFile_text(data=new_json, path=path)
            distributions.append(distribution('geojson',object))
       elif format == 'json':
           df = fix_col_types(geodataframe)
           new_df= df.drop(columns='geometry')
           #gdf_json = df.to_json()
           records= new_df.to_dict(orient='records')
           cleaned_data = [
               {k: v for k, v in record.items() if pd.notna(v)}
               for record in records
           ]
           json_records = json.dumps(cleaned_data)
           new_json =addLastUpdatedRecords(json_records, date)

           path = f"{path_w_basename}.records.json"
           object = s3_resource.putFile_text(data=new_json, path=path)
           distributions.append(distribution('json', object))
       elif format == 'csv':
           complaints_csv = geodataframe.to_csv(index=False)
           path = f"{path_w_basename}.csv"
           object= s3_resource.putFile_text(data=complaints_csv, path=path)
           distributions.append(distribution('csv', object))
       elif format == 'parquet':
           # https://github.com/aws/aws-sdk-pandas
           # get the url to the minio, somewhere from the client.
           pass
           object= wr.s3.to_parquet(
               df=geodataframe,
               path="s3://bucket/dataset/",
               dataset=True,
               database="my_db",
               table="my_table"
           )
           distributions.append(distribution('parquet', object.name))
    if metadata is not None:
        metadata.distribution = distributions
        metadata_to_s3(metadata, path_w_basename, s3_resource)

def dataframe_to_s3(dataframe, path_w_basename, s3_resource:S3Resource,
                         formats=[
                             'csv',
                             'json',

                            # 'parquet',
                             # 'arrow',
                         ], metadata=None

                          ):
    date = getTodayAsIso()
    distributions: List[distribution] = []
    for format in formats:
       if format == 'json':
            if isinstance(dataframe, gpd.GeoDataFrame):
                get_dagster_logger().info("geodataframe_to_s3, pass format['csv','json','geojson] to get a flat json ")
            df = fix_col_types(dataframe)
            gdf_json = df.to_json(orient='records')
            new_json =addLastUpdatedRecords(gdf_json, date)

            path = f"{path_w_basename}.json"
            object= s3_resource.putFile_text(data=new_json, path=path)
            distributions.append(distribution('json', object))
       elif format == 'csv':
           complaints_csv = dataframe.to_csv(index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
           path = f"{path_w_basename}.csv"
           object= s3_resource.putFile_text(data=complaints_csv, path=path)
           distributions.append(distribution('csv', object))
       elif format == 'parquet':
           # https://github.com/aws/aws-sdk-pandas
           # get the url to the minio, somewhere from the client.
           pass
           wr.s3.to_parquet(
               df=dataframe,
               path="s3://bucket/dataset/",
               dataset=True,
               database="my_db",
               table="my_table"
           )
           distributions.append(distribution('parquet', object))
    if metadata is not None:
        metadata.distribution = distributions
        metadata_to_s3(metadata, path_w_basename, s3_resource)
def series_to_s3(pdseries, path_w_basename, s3_resource:S3Resource,
                         formats=[
                             'csv',
                             'json',

                            # 'parquet',
                             # 'arrow',
                         ], metadata=None

                          ):
    date = getTodayAsIso()
    distributions: List[distribution] = []
    for format in formats:
       if format == 'json':
            if isinstance(pdseries, gpd.GeoDataFrame) or isinstance(pdseries, pd.DataFrame):
                get_dagster_logger().info("series_to_s3, is for pandas series")
            series  = pdseries #fix_col_types(dataframe)
            gdf_json = series.to_json(orient='records')
            new_json =addLastUpdatedRecords(gdf_json, date)

            path = f"{path_w_basename}.json"
            object= s3_resource.putFile_text(data=new_json, path=path)
            distributions.append(distribution('json', object))
       elif format == 'csv':
           complaints_csv = pdseries.to_csv(index=False, date_format='%Y-%m-%dT%H:%M:%SZ')
           path = f"{path_w_basename}.csv"
           object= s3_resource.putFile_text(data=complaints_csv, path=path)
           distributions.append(distribution('csv', object))
       # elif format == 'parquet':
       #     # https://github.com/aws/aws-sdk-pandas
       #     # get the url to the minio, somewhere from the client.
       #     pass
       #     wr.s3.to_parquet(
       #         df=pdseries,
       #         path="s3://bucket/dataset/",
       #         dataset=True,
       #         database="my_db",
       #         table="my_table"
       #     )
    if metadata is not None:
        metadata.distribution = distributions
        metadata_to_s3(metadata, path_w_basename, s3_resource)

#### METADATA
def variable(name=None,description=None,measurementTechnique=None,propertyID=None):
    property = PropertyValue(name=None,description=None,measurementTechnique=None,propertyID=None)
    return property

def distribution(format, url):
    download = DataDownload(encodingFormat=format, contentUrl=url)
    return download
def objectMetadata(name=None,altName=None, description=None,distribution:List[DataDownload]=[],
                   source:Organization=None,
                   variableMeasured=None, source_url=None) -> Dataset:
    if source is not None:
        url = URL(url=source_url)
    else:
        url= None
    return Dataset(name=name,alternateName=altName, description=description,distribution=distribution,source=source,
                   variableMeasured=variableMeasured,isBasedOn=url)

def metadata_to_s3(metadata:objectMetadata, path_w_basename, s3_resource,):
    '''This will write out objectMetadata to as a file'''

    s3_resource.putFile_text(data=metadata.json(indent=2), path=f"{path_w_basename}.metadata.json")
    pass


def metadata_to_airtable(metadata: objectMetadata, airtable_basename, formats=['csv'],  ):
    '''This will write out objectMetadata to airtable'''
    name = metadata.name

    pass
