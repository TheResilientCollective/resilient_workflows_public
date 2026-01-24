"""
Storage utilities for the Epidemiology Forecast Component.

Provides functions for storing DataFrames and GeoDataFrames to S3 in multiple formats.
"""

import json
import os
from datetime import datetime
from typing import List, Optional

import pandas as pd
import geopandas as gpd
import pytz
from dagster import get_dagster_logger
from pydantic_schemaorg.Dataset import Dataset
from pydantic_schemaorg.DataDownload import DataDownload
from pydantic_schemaorg.URL import URL
from pydantic_schemaorg.Organization import Organization
from pydantic_schemaorg.PropertyValue import PropertyValue

from ..resources.s3 import S3Resource


def getTodayAsIso() -> str:
    """Get today's date as ISO format string in Los Angeles timezone."""
    return datetime.now(pytz.timezone("America/Los_Angeles")).isoformat()


def fix_col_types(df: pd.DataFrame, date_format: Optional[str] = None) -> pd.DataFrame:
    """
    Fix column types for JSON serialization.

    Converts datetime columns to ISO format strings.
    """
    columns = [
        col for col in df.columns if pd.api.types.is_datetime64_any_dtype(df[col])
    ]
    for col in columns:
        get_dagster_logger().debug(f"fixing col {col} to {date_format}")
        df[col] = df[col].apply(
            lambda x: x.strftime(date_format)
            if pd.notnull(x) and date_format
            else x.isoformat()
            if pd.notnull(x)
            else None
        )
    return df


def addLastUpdatedGeojson(json_str: str, date_str: str) -> str:
    """Add lastUpdated field to GeoJSON."""
    json_obj = json.loads(json_str)
    json_obj["lastUpdated"] = date_str
    return json.dumps(json_obj, indent=2)


def addLastUpdatedRecords(json_str: str, date_str: str) -> str:
    """Add lastUpdated field and wrap records in data object."""
    json_obj = json.loads(json_str)
    new_json = {"lastUpdated": date_str, "data": json_obj}
    return json.dumps(new_json, indent=2)


def get_latest_basepath() -> str:
    """Get the LATEST_BASEPATH environment variable with fallback."""
    latest = os.environ.get("LATEST_BASEPATH", "latest")
    if latest.endswith("/"):
        latest = latest[:-1]
    return latest


def text_to_s3(
    textdata: str,
    path_w_name: str,
    s3_resource: S3Resource,
    contenttype: str = "text/plain",
    metadata=None,
) -> None:
    """
    Write text data to S3.

    Args:
        textdata: Text content to store
        path_w_name: S3 path including filename
        s3_resource: S3Resource instance
        contenttype: MIME type of the content
        metadata: Optional Dataset metadata
    """
    obj = s3_resource.putFile_text(
        data=textdata, path=f"{path_w_name}", content_type=contenttype
    )
    if metadata is not None:
        metadata.distribution = distribution(contenttype, obj)
        metadata_to_s3(metadata, path_w_name, s3_resource)


def raw_to_s3(
    rawdata: bytes,
    path_w_name: str,
    s3_resource: S3Resource,
    contenttype: str = "application/octet-stream",
    metadata=None,
) -> None:
    """
    Write binary data to S3.

    Args:
        rawdata: Binary content to store
        path_w_name: S3 path including filename
        s3_resource: S3Resource instance
        contenttype: MIME type of the content
        metadata: Optional Dataset metadata
    """
    obj = s3_resource.putFile(
        data=rawdata, path=f"{path_w_name}", content_type=contenttype
    )
    if metadata is not None:
        metadata.distribution = distribution(contenttype, obj)
        metadata_to_s3(metadata, path_w_name, s3_resource)


def store_dataframe_to_s3(
    df: pd.DataFrame,
    path: str,
    dataset_identifier: str,
    s3_resource: S3Resource,
    metadata=None,
    latestdatasetpath: Optional[str] = None,
    enable_latest_path: bool = False,
    formats: List[str] = None,
) -> None:
    """
    Store a DataFrame or GeoDataFrame to S3 in multiple formats.

    Args:
        df: DataFrame or GeoDataFrame to store
        path: Base S3 path
        dataset_identifier: Identifier for the dataset
        s3_resource: S3Resource instance
        metadata: Optional Dataset metadata
        latestdatasetpath: Path for "latest" copy
        enable_latest_path: Whether to also store in latest path
        formats: List of output formats (default: ['csv', 'json'])
    """
    if formats is None:
        formats = ["csv", "json"]
    logger = get_dagster_logger()

    if latestdatasetpath and latestdatasetpath.endswith("/"):
        latestdatasetpath = latestdatasetpath[:-1]
    if path and path.endswith("/"):
        path = path[:-1]
    path_w_basename = f"{path}/{dataset_identifier}"

    if enable_latest_path:
        latestdatasetpath_basename = (
            f"{get_latest_basepath()}/{latestdatasetpath}/{dataset_identifier}"
        )
        lastest_metadtata = metadata.copy()
        lastest_metadtata.name = f"latest {metadata.name}"
        if lastest_metadtata.alternateName:
            lastest_metadtata.alternateName = f"latest {metadata.alternateName}"
        lastest_metadtata.description = f"latest {metadata.description}"

    if isinstance(df, gpd.GeoDataFrame):
        gdf = df
        logger.info(f"is a GeoDataFrame for {dataset_identifier}")
        geodataframe_to_s3(
            gdf, path_w_basename, s3_resource, metadata=metadata, formats=formats
        )
        logger.info(
            f"Stored GeoDataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{path_w_basename}"
        )

        if enable_latest_path:
            geodataframe_to_s3(
                gdf,
                latestdatasetpath_basename,
                s3_resource,
                metadata=lastest_metadtata,
                formats=formats,
            )
            logger.info(
                f"Stored GeoDataFrame for {dataset_identifier} to latest path: s3://{s3_resource.S3_BUCKET}/{latestdatasetpath_basename}"
            )
    else:
        logger.info(f"Normal DataFrame for {dataset_identifier}")
        dataframe_to_s3(
            df, path_w_basename, s3_resource, metadata=metadata, formats=formats
        )
        logger.info(
            f"Stored DataFrame for {dataset_identifier} to S3: s3://{s3_resource.S3_BUCKET}/{path_w_basename}"
        )

        if enable_latest_path:
            dataframe_to_s3(
                df,
                latestdatasetpath_basename,
                s3_resource,
                metadata=lastest_metadtata,
                formats=formats,
            )
            logger.info(
                f"Stored DataFrame for {dataset_identifier} to latest path: s3://{s3_resource.S3_BUCKET}/{latestdatasetpath_basename}"
            )


def geodataframe_to_s3(
    geodataframe: gpd.GeoDataFrame,
    path_w_basename: str,
    s3_resource: S3Resource,
    formats: List[str] = None,
    metadata=None,
) -> None:
    """
    Store a GeoDataFrame to S3 in multiple formats.

    Args:
        geodataframe: GeoDataFrame to store
        path_w_basename: S3 path including base filename
        s3_resource: S3Resource instance
        formats: List of output formats (default: ['geojson', 'csv'])
        metadata: Optional Dataset metadata
    """
    if formats is None:
        formats = ["geojson", "csv"]
    date = getTodayAsIso()
    distributions: List[DataDownload] = []

    for fmt in formats:
        if fmt == "geojson":
            df = fix_col_types(geodataframe)
            gdf_json = df.to_json()
            new_json = addLastUpdatedGeojson(gdf_json, date)
            path = f"{path_w_basename}.geojson"
            obj = s3_resource.putFile_text(data=new_json, path=path)
            distributions.append(distribution("geojson", obj))
        elif fmt == "json":
            df = fix_col_types(geodataframe)
            new_df = df.drop(columns="geometry")
            records = new_df.to_dict(orient="records")
            cleaned_data = [
                {k: v for k, v in record.items() if pd.notna(v)} for record in records
            ]
            json_records = json.dumps(cleaned_data)
            new_json = addLastUpdatedRecords(json_records, date)
            path = f"{path_w_basename}.records.json"
            obj = s3_resource.putFile_text(data=new_json, path=path)
            distributions.append(distribution("json", obj))
        elif fmt == "csv":
            csv_data = geodataframe.to_csv(index=False)
            path = f"{path_w_basename}.csv"
            obj = s3_resource.putFile_text(data=csv_data, path=path)
            distributions.append(distribution("csv", obj))
        elif fmt == "parquet":
            get_dagster_logger().info(
                "geodataframe_to_parquet removes geometry column"
            )
            dataframe = geodataframe.drop(columns="geometry")
            dataframe = pd.DataFrame(dataframe)
            path = f"{path_w_basename}.parquet"
            parquet_object = dataframe.to_parquet()
            obj = s3_resource.putFile(
                data=parquet_object,
                path=path,
                content_type="application/vnd.apache.parquet",
            )
            distributions.append(distribution("parquet", obj))

    if metadata is not None:
        metadata.distribution = distributions
        metadata_to_s3(metadata, path_w_basename, s3_resource)


def dataframe_to_s3(
    dataframe: pd.DataFrame,
    path_w_basename: str,
    s3_resource: S3Resource,
    formats: List[str] = None,
    metadata=None,
) -> None:
    """
    Store a DataFrame to S3 in multiple formats.

    Args:
        dataframe: DataFrame to store
        path_w_basename: S3 path including base filename
        s3_resource: S3Resource instance
        formats: List of output formats (default: ['csv', 'json'])
        metadata: Optional Dataset metadata
    """
    if formats is None:
        formats = ["csv", "json"]
    date = getTodayAsIso()
    distributions: List[DataDownload] = []

    for fmt in formats:
        if fmt == "json":
            if isinstance(dataframe, gpd.GeoDataFrame):
                get_dagster_logger().info(
                    "dataframe_to_s3, pass format['csv','json','geojson] to get a flat json"
                )
            df = fix_col_types(dataframe)
            try:
                gdf_json = df.to_json(orient="records")
                new_json = addLastUpdatedRecords(gdf_json, date)
                path = f"{path_w_basename}.json"
                obj = s3_resource.putFile_text(data=new_json, path=path)
                distributions.append(distribution("json", obj))
            except Exception as e:
                get_dagster_logger().info(f"dataframe_to_s3, failed to write json {e}")
        elif fmt == "csv":
            csv_data = dataframe.to_csv(index=False, date_format="%Y-%m-%dT%H:%M:%SZ")
            path = f"{path_w_basename}.csv"
            obj = s3_resource.putFile_text(data=csv_data, path=path)
            distributions.append(distribution("csv", obj))
        elif fmt == "parquet":
            path = f"{path_w_basename}.parquet"
            parquet_object = dataframe.to_parquet()
            obj = s3_resource.putFile(
                data=parquet_object,
                path=path,
                content_type="application/vnd.apache.parquet",
            )
            distributions.append(distribution("parquet", obj))

    if metadata is not None:
        metadata.distribution = distributions
        metadata_to_s3(metadata, path_w_basename, s3_resource)


# ---- METADATA ----


def variable(
    name: str = None,
    description: str = None,
    measurementTechnique: str = None,
    propertyID: str = None,
) -> PropertyValue:
    """Create a PropertyValue for variable metadata."""
    return PropertyValue(
        name=name,
        description=description,
        measurementTechnique=measurementTechnique,
        propertyID=propertyID,
    )


def distribution(format_type: str, url: str) -> DataDownload:
    """Create a DataDownload for distribution metadata."""
    return DataDownload(encodingFormat=format_type, contentUrl=url)


def objectMetadata(
    name: str = None,
    altName: str = None,
    description: str = None,
    distribution: List[DataDownload] = None,
    source: Organization = None,
    variableMeasured=None,
    source_url: str = None,
) -> Dataset:
    """
    Create Schema.org Dataset metadata.

    Args:
        name: Dataset name
        altName: Alternate name
        description: Dataset description
        distribution: List of DataDownload objects
        source: Source organization
        variableMeasured: Variables measured
        source_url: Source URL

    Returns:
        Dataset metadata object
    """
    if distribution is None:
        distribution = []
    if source is not None:
        url = URL(url=source_url)
    else:
        url = None
    return Dataset(
        name=name,
        alternateName=altName,
        description=description,
        distribution=distribution,
        source=source,
        variableMeasured=variableMeasured,
        isBasedOn=url,
    )


def metadata_to_s3(
    metadata: Dataset, path_w_basename: str, s3_resource: S3Resource
) -> None:
    """Write Dataset metadata to S3 as JSON."""
    s3_resource.putFile_text(
        data=metadata.json(indent=2), path=f"{path_w_basename}.metadata.json"
    )
