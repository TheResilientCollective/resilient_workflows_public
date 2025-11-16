import os
import tempfile
from datetime import datetime
from pathlib import Path
import io

import pandas as pd
import dask.dataframe as dd
from dagster import SensorEvaluationContext, sensor, get_dagster_logger, RunRequest, RunConfig, asset, Config, \
    define_asset_job, AssetKey, AssetIn
from duckdb import duckdb

from workflows.public.public.utils import store_assets
from ..utils.resilient_epi_schemas import (
    detailed_epidemiology_format,

)

WAHIS_S3_PATH = os.environ.get("WAHIS_PATH", "pathogens/wahis/")
WAHIS_UPLOAD_PATH = os.environ.get("WAHIS_UPLOAD_PATH", f"{WAHIS_S3_PATH}upload/")
WAHIS_RAW_PATH = os.environ.get("WAHIS_RAW_PATH", f"{WAHIS_S3_PATH}raw/")
WAHIS_OUTPUT_PATH = os.environ.get("WAHIS_OUTPUT_PATH", f"{WAHIS_S3_PATH}output/")
WAHIS_BUCKET=os.environ.get("PUBLIC_BUCKET", 'test')
LATEST = os.environ.get("WAHIS_LATEST", 'wahis')




class WahisUploadConfig(Config):
    wahis_upload_path: str

def getDuckDb(s3_resource, input_path=None, bucket=WAHIS_BUCKET):
    if input_path is None:
        raise ValueError("output_path  for wahis parquest must be provided")
    file_url = s3_resource.publicUrl(path=input_path, bucket=bucket)
    db1 = duckdb.sql(f"SELECT * FROM read_parquet('{file_url}') ")
    return db1

@asset(
    group_name="health",
    key_prefix="wahis",
    name="wahis_excpetional_pathogens",
    required_resource_keys={"s3"},
    deps=[AssetKey(['wahis','wahis_excel'])],
    ins={
            "wahis_excel": AssetIn(
                key=AssetKey(['wahis','wahis_excel'])
            )
        },
)
def outbreak_by_pathogen(context, wahis_excel ):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    input_path = f"{wahis_excel['output_path']}.parquet"
    db1 = getDuckDb(s3_resource, input_path=input_path, bucket=WAHIS_BUCKET)
    diseases_df = duckdb.sql("""
                             SELECT DISTINCT disease_id,
                                             reporting_level,
                                             strain_eng,
                                             sero_sub_genotype_eng,
                                             disease_eng
                             FROM db1
                             """).df()
    metadata = store_assets.objectMetadata(name="WAHIS_outbreak_pathogens",
                                           description="WAHIS exceptional  pathogens  ")
    output_path = f"{WAHIS_OUTPUT_PATH}"
    store_assets.store_dataframe_to_s3(diseases_df, output_path,'diseases', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata)

    for disease_id in diseases_df['disease_id']:
        name = diseases_df[diseases_df['disease_id'] == disease_id]['disease_eng'].unique()
        if len(name) == 0:
            print(f'bad id {disease_id}')
        else:
            print(f'disease_id  {disease_id} {len(name)}')
            print(f'name: {name[0]}')
        # Filter outbreaks for this disease
        disease_outbreaks = duckdb.sql(f"""
               SELECT Report_id,Outbreak_id,
                      disease_id,
                      reporting_level,
                      Location_name,
                      strain_eng,
                      sero_sub_genotype_eng,
                      disease_eng,
                      country,
                      region,
                      "reason of notification",
                      Species,
                      quantitative_unit,
                      Outbreak_start_date        as Outbreak_start_date,
                      Outbreak_end_date           as Outbreak_end_date,
                      susceptible    as susceptible,
                      cases           as cases,
                      dead           as dead,
                      killed_disposed as killed_disposed,
                      slaughtered     as slaughtered,
                      vaccinated     as vaccinated,
                      morbidity      as morbidity,
                      mortality      as mortality,
                      Longitude,	Latitude,	Location_aprox
                      
                      FROM db1
                      WHERE disease_id = {disease_id}
                   """).df()

        # Create a safe filename from disease name
        safe_filename = name[0].replace('/', '_').replace(' ', '_').lower()
        metadata=store_assets.objectMetadata(name=f"WAHIS_outbreak_by_pathogen_{safe_filename}", description=f"WAHIS exceptional events records for pathogen {name[0]}   ")
        output_path= f"{WAHIS_RAW_PATH}pathogens/"
        store_assets.dataframe_to_s3(disease_outbreaks, output_path, s3_resource, formats=['parquet', 'csv'], metadata=metadata)
        print(f"Disease: {name} | Records: {len(disease_outbreaks)} | File: {output_path}")

        output_path = f"{WAHIS_OUTPUT_PATH}pathogens/"
        latest_path = f"{LATEST}/pathogens"
        disease_outbreaks_geo=detailed_epidemiology_format(disease_outbreaks)
        metadata = store_assets.objectMetadata(name=f"WAHIS_outbreak_by_pathogen_{safe_filename}_cleaned",
                                               description=f"WAHIS exceptional events records for pathogen {name[0]}  in detailed format  ")
        store_assets.store_dataframe_to_s3(disease_outbreaks_geo, output_path,safe_filename, s3_resource,
                                           formats=['geojson', 'csv', 'parquet'],
                                           latestdatasetpath=latest_path,enable_latest_path=True,
                                 metadata=metadata)

    print(f"\nTotal unique diseases: {len(diseases_df)}")

@asset(
    group_name="health",
    key_prefix="wahis",
    name="wahis_excpetional_summary",
    required_resource_keys={"s3"},
    deps=[AssetKey(['wahis','wahis_excel'])],
    ins={
            "wahis_excel": AssetIn(
                key=AssetKey(['wahis','wahis_excel'])
            )
        },
)
def outbreak_summaries(context, wahis_excel ):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    input_path= f"{wahis_excel['output_path']}.parquet"
    db1= getDuckDb(s3_resource, input_path=input_path, bucket=WAHIS_BUCKET)
    outbreaks_df = duckdb.sql("""
                              SELECT epi_event_id,
                                     Report_id,
                                     disease_id,
                                  Location_name,
                                     reporting_level,
                                     strain_eng,
                                     sero_sub_genotype_eng,
                                     disease_eng,
                                     country,
                                     region,
                                     "reason of notification",
                                     Species,
                                     quantitative_unit,
                                     MIN(Outbreak_start_date)          as Outbreak_start_date,
                                     MAX(Outbreak_end_date)            as Outbreak_end_date,
                                     SUM(COALESCE(susceptible, 0))     as susceptible,
                                     SUM(COALESCE(cases, 0))           as cases,
                                     SUM(COALESCE(dead, 0))            as dead,
                                     SUM(COALESCE(killed_disposed, 0)) as killed_disposed,
                                     SUM(COALESCE(slaughtered, 0))     as slaughtered,
                                     SUM(COALESCE(vaccinated, 0))      as vaccinated,
                                     SUM(COALESCE(morbidity, 0))       as morbidity,
                                     SUM(COALESCE(mortality, 0))       as mortality,
                                 MIN(Longitude) as Longitude,	MIN(Latitude) as Latitude,	Location_aprox
                              FROM db1

                              GROUP BY Report_id, epi_event_id,
                                       disease_id,
                                       reporting_level,
                                  Location_name,
                                  Location_aprox,
                                       strain_eng,
                                       sero_sub_genotype_eng,
                                       disease_eng,
                                       country,
                                       region,
                                       "reason of notification",
                                       Species,
                                       quantitative_unit
                            """).df()
    metadata=store_assets.objectMetadata(name="WAHIS_outbreak_summaries", description="WAHIS exceptional events summary for each event for all pathogens  ")
    output_path = f"{WAHIS_OUTPUT_PATH}"

    store_assets.store_dataframe_to_s3(outbreaks_df, output_path,'summary', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=['parquet', 'csv'], metadata=metadata)
    return outbreaks_df

@asset(
    group_name="health",
    key_prefix="wahis",
    name="wahis_excel",
    required_resource_keys={"s3"},
)
def process_wahis_excel_file(context, config: WahisUploadConfig) -> dict:
    """
    Process Excel files from WAHIS upload path and store as DataFrame in raw path

    Args:
        context: Dagster context with resources
        config: Configuration containing wahis_upload_path

    Returns:
        dict: Metadata about the processed file
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    upload_path = config.wahis_upload_path
    logger.info(f"Processing WAHIS Excel file: {upload_path}")

    try:
        # Read Excel file from S3
        #data = s3_resource.getFile(path=upload_path, bucket=WAHIS_BUCKET)
        #data = s3_resource.get_stream(path=upload_path, bucket=WAHIS_BUCKET)
        #  can use an s3 path, so might use that.
        # Convert to DataFrame using pandas
        #dfs = pd.read_excel(data, [1, 'ADIS_events_match', 'ARAHIS_events'])

        #dfs = pd.read_excel(io.BytesIO(data), [1, 'ADIS_events_match', 'ARAHIS_events'])
        #dfs = pd.read_excel( s3_resource.publicUrl(path=upload_path, bucket=WAHIS_BUCKET), [1, 'ADIS_events_match', 'ARAHIS_events'])
        with  tempfile.NamedTemporaryFile() as filename :
            local_file = s3_resource.downloadFile(path=upload_path, bucket=WAHIS_BUCKET, filename=filename.name)
            dfs = pd.read_excel(local_file,
                                [1, 'ADIS_events_match', 'ARAHIS_events']
                                )

            logger.info(f"read WAHIS Excel file: {upload_path}")
        # read second sheet
        df = dfs[1]
        logger.info(f"Read Excel file with {len(df)} rows and {len(df.columns)} columns")

        # Get the basename of the file (without path and extension)
        file_basename = Path(upload_path).stem

        # Create output path in raw directory
        output_path = f"{WAHIS_RAW_PATH}{file_basename}"


        # Store DataFrame  in S3, we only want parquet files for now
        store_assets.dataframe_to_s3(df, output_path, s3_resource, formats=['parquet'])

       # logger.info(f"Stored processed file at: {output_path}")

        return {
            "original_file": upload_path,
            "output_path": output_path,
            "rows": len(df),
            "columns": len(df.columns),
            "column_names": list(df.columns),
            "processed_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to process WAHIS Excel file {upload_path}: {e}")
        raise e

wahis_uploads_job = define_asset_job(
    name="wahis_uploads_job",
    selection=[AssetKey(["wahis", "wahis_excel"])]
)

@sensor(
   job=wahis_uploads_job,
    name="wahis_upload_sensor",
    minimum_interval_seconds=3600,
    required_resource_keys={"s3", "slack"}
)
def wahis_upload_sensor(context: SensorEvaluationContext):
    """
    Sensor to watch for new forecast runs in S3 path /seasonal_forecast/api_run
    Triggers when new run directories matching pattern YYYY-mm-ddTHH-MM-SS_runXX are found
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    try:
        # List directories in the forecast base path
        run_files = s3_resource.listPath(path=WAHIS_UPLOAD_PATH, bucket=WAHIS_BUCKET)
        run_files = list(run_files)
        xl_files = list([f for f in run_files if f.object_name.endswith('.xlsx')])
        logger.info(f"how many new: {len(xl_files)}")

        if not xl_files:
            logger.info("No Excel files found")
            return

        # Sort by name (which corresponds to timestamp) and get the latest
        latest_xl = sorted(xl_files, key=lambda x: x.object_name)[-1]
        lastest_name = f"{latest_xl.object_name}"

        logger.info(f"Latest xl found: {lastest_name}")

        # Check if this run has been processed before using cursor
        last_processed = context.cursor or ""

        if lastest_name == last_processed:
            logger.info(f"Run {lastest_name} already processed")
            return

        # Update cursor and yield run request
        context.update_cursor(lastest_name)

        yield RunRequest(
            run_key=f"wahis_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={"wahis_uploads_job": lastest_name},
            run_config=RunConfig(
                ops={
                   # str(AssetKey(["wahis", "wahis_excel"])): {
                    "wahis__wahis_excel":{
                        "config": {
                            "wahis_upload_path": lastest_name
                        }
                    }
                }
            )
            )
    except Exception as e:
        logger.warning(f"Failed to list run directories: {e}")
        return

