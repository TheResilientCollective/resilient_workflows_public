"""
San Diego County Epidemiology Tableau Data Assets

This module extracts data from San Diego County epidemiology Tableau workbooks
and processes them for environmental health surveillance.
"""

import tempfile
from pathlib import Path
import pandas as pd
from dagster import asset, get_dagster_logger
from typing import Dict, Any
import json

from ..resources import minio
from ..utils import store_assets

from workflows.public.public.utils.tableau_workbook import TableauWorkbookConfig, TableauWorkbookProcessor
s3_output_path = 'pathogens/sandiego/sandiego_epidemiology/'
config= TableauWorkbookConfig()
@asset( group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_workbook_download",
    required_resource_keys={"s3"},
    description="Download San Diego epidemiology Tableau workbook and store in S3"
)
def sandiego_epidemiology_workbook_download(
    context,
    config: TableauWorkbookConfig,

) -> Dict[str, Any]:
    """Download Tableau workbook from URL and store in S3"""
    name = 'sandiego_epidemiology_workbook_download'
    description = '''
       San Diego Epidemiology Data from Tableau website 
       '''
    source_url = config.url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)

    # Download workbook
    workbook_content = processor.download_workbook(config.url)
    workbook_length = len(workbook_content)
    # Store in S3
    s3_key = f"{s3_output_path}raw/workbook.twb"
    store_assets.raw_to_s3(workbook_content, s3_key, s3_resource
                          ,contenttype='application/octet-stream',
                          metadata=metadata
                          )
    #s3_resource.getClient().put_object(
    #    Bucket=s3_resource.bucket,
    #    Key=s3_key,
    #    Body=workbook_content,
    #    ContentType='application/octet-stream'
   # )

    logger.info(f"Stored workbook in S3: s3://{s3_resource.S3_BUCKET}/{s3_key} ({workbook_length} bytes)")

    return {
        "s3_key": s3_key,
        "file_size": workbook_length,
        "url": config.url,
        "workbook_name": config.workbook_name
    }


@asset( group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_hyper_extraction",
    deps=[sandiego_epidemiology_workbook_download],
    required_resource_keys={"s3"},
    description="Extract Hyper files from Tableau workbook and store in S3"
)
def sandiego_epidemiology_hyper_extraction(
    context,
    sandiego_epidemiology_workbook_download: Dict[str, Any],

) -> Dict[str, Any]:
    """Extract Hyper files from workbook and store in S3"""
    name = 'sandiego_epidemiology_workbook_data'
    description = '''
          San Diego Epidemiology Data files from Tableau website 
          '''
    source_url = config.url
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)
    s3_resource= context.resources.s3
    workbook_name = sandiego_epidemiology_workbook_download["workbook_name"]
    s3_key = sandiego_epidemiology_workbook_download["s3_key"]

    with tempfile.TemporaryDirectory() as temp_dir:
        # Download workbook from S3
        workbook_path = Path(temp_dir) / "workbook.twb"

        # try:
        #     with open(workbook_path, 'wb') as f:
        #         s3_resource.getClient().download_fileobj(s3_resource.S3_BUCKET, s3_key, f)
        # except Exception as e:
        #     logger.error(f"Error downloading workbook: {e}")
        #     raise e

        # Extract workbook
        extract_dir = Path(temp_dir) / "extracted"
        # with open(workbook_path, 'rb') as f:
        #     workbook_content = f.read()
        workbook_content = s3_resource.getFile(s3_key)
        extraction_info = processor.extract_workbook(workbook_content, extract_dir)

        # Store each Hyper file in S3
        hyper_files_stored = []

        for hyper_file_rel_path in extraction_info["hyper_files"]:
            hyper_file_path = extract_dir / hyper_file_rel_path

            if hyper_file_path.exists():
                # Store Hyper file in S3
                hyper_s3_key = f"health/sandiego_epidemiology/{workbook_name}/hyper/{hyper_file_path.name}"

                with open(hyper_file_path, 'rb') as f:
                   store_assets.raw_to_s3(f.read(), hyper_s3_key, s3_resource, contenttype='application/octet-stream', metadata=metadata)
                #     s3_resource.getClient().put_object(
                #         s3_resource.bucket,
                #         hyper_s3_key,
                #         f.read(),
                #         'application/octet-stream'
                #     )



                hyper_files_stored.append({
                    "filename": hyper_file_path.name,
                    "s3_key": hyper_s3_key,
                    "size": hyper_file_path.stat().st_size
                })

                logger.info(f"Stored Hyper file: {hyper_file_path.name}")

        result = {
            "workbook_name": workbook_name,
            "hyper_files": hyper_files_stored,
            "extraction_info": extraction_info
        }

        # Store extraction metadata
        # metadata_key = f"health/sandiego_epidemiology/{workbook_name}/metadata/hyper_extraction.json"
        # store_assets.raw_to_s3(json.dumps(result, indent=2), metadata_key, s3_resource)
        # s3_resource.getClient().put_object(
        #     Bucket=s3_resource.bucket,
        #     Key=metadata_key,
        #     Body=json.dumps(result, indent=2),
        #     ContentType='application/json'
        # )

        logger.info(f"Extracted and stored {len(hyper_files_stored)} Hyper files")

        return result


@asset( group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_processed_data",
    deps=[sandiego_epidemiology_hyper_extraction],
    required_resource_keys={"s3"},
    description="Convert Hyper files to DataFrames and process for storage"
)
def sandiego_epidemiology_processed_data(
    context,
    sandiego_epidemiology_hyper_extraction: Dict[str, Any],

) -> Dict[str, Any]:
    """Convert Hyper files to processed DataFrames and store using utility functions"""
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)
    s3_resource = context.resources.s3
    workbook_name = sandiego_epidemiology_hyper_extraction["workbook_name"]
    hyper_files = sandiego_epidemiology_hyper_extraction["hyper_files"]

    all_dataframes = {}
    processed_count = 0

    with tempfile.TemporaryDirectory() as temp_dir:
        for hyper_info in hyper_files:
            hyper_s3_key = hyper_info["s3_key"]
            filename = hyper_info["filename"]

            # Download Hyper file from S3
            hyper_path = Path(temp_dir) / filename

            # with open(hyper_path, 'wb') as f:
            #     #s3_resource.getClient().download_fileobj(s3_resource.S3_BUCKET, hyper_s3_key, f)
            f=s3_resource.getFile(path=hyper_s3_key)
            # Extract data from Hyper file
            extracted_data = processor.extract_hyper_data(hyper_path)

            for table_name, df in extracted_data.items():
                if not df.empty:
                    # Add metadata columns
                    df['extraction_date'] = pd.Timestamp.now().isoformat()
                    df['source'] = 'sandiego_epidemiology_tableau'
                    df['workbook_name'] = workbook_name
                    df['hyper_file'] = filename

                    # Store using utility functions
                    s3_path = f"health/sandiego_epidemiology/{workbook_name}/processed/{table_name}"

                    # Create metadata
                    metadata = store_assets.objectMetadata(
                        name=f"sandiego_epidemiology_{table_name}",
                        description=f"San Diego epidemiology data from {table_name}",
                        source_url="https://public.tableau.com/workbooks/DraftRespDash.twb"
                    )

                    # Store as geodataframe (will handle non-geo data gracefully)
                    try:
                        import geopandas as gpd
                        # Try to convert to GeoDataFrame if geometry columns exist
                        geo_columns = [col for col in df.columns if 'geo' in col.lower() or 'lat' in col.lower() or 'lon' in col.lower()]

                        if geo_columns:
                            # Convert to GeoDataFrame if possible
                            gdf = gpd.GeoDataFrame(df)
                        else:
                            gdf = gpd.GeoDataFrame(df)

                        store_assets.geodataframe_to_s3(gdf, s3_path, s3_resource, metadata=metadata)

                    except Exception as geo_error:
                        logger.warning(f"Could not create GeoDataFrame for {table_name}: {geo_error}")
                        store_assets.dataframe_to_s3(gdf, s3_path, s3_resource, metadata=metadata)
                        # Fallback to regular DataFrame storage
                        # csv_content = df.to_csv(index=False)
                        # s3_resource.getClient().put_object(
                        #     Bucket=s3_resource.S3_BUCKET,
                        #     Key=f"{s3_path}.csv",
                        #     Body=csv_content.encode('utf-8'),
                        #     ContentType='text/csv'
                        # )

                    all_dataframes[table_name] = {
                        "rows": len(df),
                        "columns": len(df.columns),
                        "s3_path": s3_path
                    }
                    processed_count += 1

                    logger.info(f"Processed {table_name}: {len(df)} rows, {len(df.columns)} columns")

    # Store processing summary
    summary = {
        "workbook_name": workbook_name,
        "processed_datasets": all_dataframes,
        "total_datasets": processed_count,
        "processing_timestamp": pd.Timestamp.now().isoformat()
    }

    summary_key = f"health/sandiego_epidemiology/{workbook_name}/metadata/processing_summary.json"
    s3_resource.getClient().put_object(
        Bucket=s3_resource.bucket,
        Key=summary_key,
        Body=json.dumps(summary, indent=2),
        ContentType='application/json'
    )

    logger.info(f"Processing complete: {processed_count} datasets processed")

    return summary
