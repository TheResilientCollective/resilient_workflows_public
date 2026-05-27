import io
import os
import tempfile
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from operator import truediv

import dagster
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import re
from minio.commonconfig import REPLACE, CopySource

import requests
import yaml
import json

from dagster_aws.s3 import s3_resource
from jinja2 import Environment, PackageLoader,FileSystemLoader, select_autoescape

from dagster import (
    asset,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    AutomationCondition, RunConfig, Config, EnvVar,
Output, AssetIn
)
from pydantic import BaseModel, Field
from typing import Optional
from pyairtable.formulas import AND, GTE, Field, match, EQ

from icecream import ic


from resilient_core.resources import minio
from resilient_core.utils import store_assets
from resilient_core.utils.resilient_epi_schemas import (
    StatisticalExtensionSchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    create_statistical_extension_record
)
import boto3

from git import Repo
from github import Github
# S3 paths
FORECAST_API_RUN_PATH = os.environ.get("FORECAST_API_RUN_PATH", "api_run/")

FORECAST_OUTPUT_DIRECTORY =  os.environ.get("FORECAST_OUTPUT_DIRECTORY", "pathogens/sandiego/sandiego_epidemiology/output")
FORECAST_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET", 'resilientseasonal')
FORECAST_NETLIFY_PREVIEW_HOOK=os.environ.get("FORECAST_NETLIFY_PREVIEW_HOOK")
FORECAST_NETLIFY_PRODUCTION_HOOK=os.environ.get("FORECAST_NETLIFY_PRODUCTION_HOOK")
FORECAST_NETLIFY_PREVIEW_URL=os.environ.get("FORECAST_NETLIFY_PREVIEW_URL")
FORECAST_NETLIFY_PRODUCTION_URL=os.environ.get("FORECAST_NETLIFY_PRODUCTION_URL")
FORECAST_NETLIFY_PREVIEW_2_HOOK=os.environ.get("FORECAST_NETLIFY_PREVIEW_2_HOOK")
FORECAST_NETLIFY_PRODUCTION_2_HOOK=os.environ.get("FORECAST_NETLIFY_PRODUCTION_2_HOOK")
FORECAST_NETLIFY_PREVIEW_2_URL=os.environ.get("FORECAST_NETLIFY_PREVIEW_2_URL")
FORECAST_NETLIFY_PRODUCTION_2_URL=os.environ.get("FORECAST_NETLIFY_PRODUCTION_2_URL")
FORECAST_NETLIFY_REJECT_MESSAGE=os.environ.get("FORECAST_NETLIFY_REJECT_MESSAGE","Please edit the prompts in Airtable and trigger a new preview when ready." )
FORECAST_NETLIFY_REJECT_2_MESSAGE=os.environ.get("FORECAST_NETLIFY_REJECT_2_MESSAGE","Editing interface is being worked on." )

TRIGGER_PREVIEW_HOOK=os.environ.get("TRIGGER_PREVIEW_HOOK")

# Email configuration
EMAIL_SMTP_SERVER = os.environ.get("EMAIL_SMTP_SERVER", "smtp.gmail.com")
EMAIL_SMTP_PORT = int(os.environ.get("EMAIL_SMTP_PORT", 587))
EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD")
EMAIL_TO = os.environ.get("EMAIL_TO")
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "false").lower() == "true"
FORECAST_AIRTABLE_ENABLED=os.environ.get("FORECAST_AIRTABLE_ENABLE", "false").lower() == "true"
FORECAST_AIRTABLE_RSV_PORTAL_RECORDID=os.environ.get("FORECAST_AIRTABLE_RSV_PORTAL_RECORDID","rec4NITTQNAONirhd")
FORECAST_AIRTABLE_RSV_PORTAL_RECORDNAME=os.environ.get("FORECAST_AIRTABLE_RSV_PORTAL_RECORDNAME","CoSD-ILI-Report")
FORECAST_AIRTABLE_RSV_UPDATES_TABLE_ID=os.environ.get("FORECAST_AIRTABLE_UPDATES_TABLE_ID","Updates") # Updates
FORECAST_AIRTABLE_WIDGETS_TABLE_ID=os.environ.get("FORECAST_AIRTABLE_WIDGETS_TABLE_ID","Widgets") # Widgets
FORECAST_AIRTABLE_WIDGETS_RECORDID=os.environ.get("FORECAST_AIRTABLE_WIDGETS_RECORDID","recowLGi7NgFdWX7B") # "recowLGi7NgFdWX7B"

FORECAST_GITHUB_RT=os.environ.get("FORECAST_GITHUB_RT", 'https://github.com/TheResilientCollective/ResilientDataProducts.git')
FORECAST_GITHUB_RT_PATH="sandiego_rt"
FORECAST_GITHUB_RT_TOKEN=os.environ.get("FORECAST_GITHUB_RT_TOKEN")

SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")
s3_output_path='pathogens/sandiego/sandiego_epidemiology/'
s3_latest_path='latest/sandiego_epidemiology_ili/llm/'
# File to Airtable table mapping - update these UUIDs with actual Airtable table IDs
#AIRTABLE_EPI_DISEASE_TABLE_ID=tblgC8jeTS4c6LPTO
#AIRTABLE_EPI_REPORTS_TABLE_ID=
#AIRTABLE_EPI_INFECTIONS_TABLE_ID=
#AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID=
#AIRTABLE_EPI_HOSPITAL_TABLE_ID=


COLUMN_RENAME_MAPPING={
    "date":"Date",
    "epiweek_start":"Date", # _reports
    "epiweek_end":"Date", # _reports
    "Observations":"Reported hospital admissions", #hosp report
    "cases":"New cases", # reports
    "type":"Type",
    "variable":"Variable",
    "median":"Median",
  # "median":"Estimated mean hospital admissions", # hosp reports
    "mean":"Mean",
    "sd":"Sd",
    "lower_90":"Lower 90",
    "lower_50":"Lower 50",
    "lower_20":"Lower 20",
    "upper_20":"Upper 20",
    "upper_50":"Upper 50",
    "upper_90":"Upper 90"

}

# COLUMN_RENAME_MAPPING_ACTIVE={
#     "date":"Date",
#     "epiweek_start":"Date", # _reports
#     "cases":"Active Cases", # reports
# }
# COLUMN_SUBSET_ACTIVE={
#     "Disease":"Disease",
#     "date":"Date",
#     "epiweek_start":"Date", # _reports
#     "cases":"Active Cases", # reports
# }

COLUMN_RENAME_MAPPING_NEW={
    "epiweek_start":"Date", # _reports
"epiweek_end":"Date",
    "cases":"New cases", # reports
    "Type":"Type",
    "Lower90":"Lower 90",
    "Lower50":"Lower 50",
    "Lower20":"Lower 20",
    "Upper20":"Upper 20",
    "Upper50":"Upper 50",
    "Upper90":"Upper 90"

}

COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS={
    "epiweek_end":"epiweek_start", #@ airtable usese start
    "date":"Date",
    "Observations":"Reported hospital admissions", #hosp report
    "type":"Type",
   "median":"Estimated mean hospital admissions", # hosp reports
    "lower_90":"Lower 90",
    "lower_50":"Lower 50",
    "lower_20":"Lower 20",
    "upper_20":"Upper 20",
    "upper_50":"Upper 50",
    "upper_90":"Upper 90"

}

COLUMN_RENAME_MAPPING_RT_ESTIMATES={
    "epiweek_end":"epiweek_start", #@ airtable usese start
    "date":"Date",
    "variable": "Variable",
    "type": "Type",
    "median":"Median",
    "mean":"Mean",
    "sd":"Sd",
    "lower_90":"Lower 90",
    "lower_50":"Lower 50",
    "lower_20":"Lower 20",
    "upper_20":"Upper 20",
    "upper_50":"Upper 50",
    "upper_90":"Upper 90"

}
# COVID_reports --- pasted to --->  "New cases" in AirTable
# COVID_Rt     --- pasted to --->  "Rt estimates" in AirTable
# COVIDhosp   --- pasted to ---> "Hospital admissions" in AirTable
# COVIDhost_Rt  --- pasted to ---> This was not being used in the portal or AirTable
# FILE_TO_TABLE_MAPPING = {
#     # Example mappings - replace with actual file names and table UUIDs
#     "COVID_reports.csv": {"table":  os.environ.get("AIRTABLE_EPI_NEW_CASES_TABLE_ID") ,"keyfields":["Disease", "Date", "Type"],"mapping":COLUMN_RENAME_MAPPING_NEW }, # new cases
#     "COVID_Rt.csv": {"table" :os.environ.get("AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID"),  "keyfields":["Disease", "Date", "Type",] , "mapping":COLUMN_RENAME_MAPPING_RT_ESTIMATES},     # Infections Epi , Rt Estimates
#     "COVIDhosp_reports.csv":{"table":  os.environ.get("AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID"), "keyfields":["Disease", "Date", "Type"], "mapping":COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS}, # hosptial Admissions
#     #
#     "Influenza_reports.csv": {"table": os.environ.get("AIRTABLE_EPI_NEW_CASES_TABLE_ID"),"keyfields":["Disease", "Date", "Type"],"mapping":COLUMN_RENAME_MAPPING_NEW }, # new cases
#     "Influenza_Rt.csv": {"table": os.environ.get("AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID"),  "keyfields":["Disease",  "Date", "Type"] , "mapping":COLUMN_RENAME_MAPPING_RT_ESTIMATES},     # Infections Epi , Rt Estimates
#     "Influenza_hosp_reports.csv":{"table":  os.environ.get("AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID"), "keyfields":["Disease",  "Date", "Type"], "mapping":COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS}, # hosptial Admissions
#     #
#     "RSV_reports.csv": {"table":  os.environ.get("AIRTABLE_EPI_NEW_CASES_TABLE_ID"),"keyfields":["Disease", "Date", "Type"],"mapping":COLUMN_RENAME_MAPPING_NEW }, # new cases
#     "RSV_Rt.csv": {"table": os.environ.get("AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID"),  "keyfields":["Disease",  "Date", "Type"] , "mapping":COLUMN_RENAME_MAPPING_RT_ESTIMATES},     # Infections Epi , Rt Estimates
#     "RSVhosp_reports.csv":{"table":  os.environ.get("AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID"), "keyfields":["Disease",  "Date", "Type"], "mapping":COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS}, # hosptial Admissions
#
#     # hosptial Admissions
#    # "COVIDhosp_Rt.csv": "tblMNO345PQR678", # not sure
#     # Add more mappings as needed
# }
GENERIC_FILE_TO_TABLE_MAPPING = {
    # Example mappings - replace with actual file names and table UUIDs
    "_case_reports.csv": {"table":  os.environ.get("AIRTABLE_EPI_NEW_CASES_TABLE_ID") ,"mapping":COLUMN_RENAME_MAPPING_NEW }, # new cases
    "_case_Rt.csv": {"table" :os.environ.get("AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID"),   "mapping":COLUMN_RENAME_MAPPING_RT_ESTIMATES},     # Infections Epi , Rt Estimates
    "_hosp_reports.csv":{"table":  os.environ.get("AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID"),  "mapping":COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS}, # hosptial Admissions

}
TABLES_TO_CLEAR=[os.environ.get("AIRTABLE_EPI_NEW_CASES_TABLE_ID"),
    os.environ.get("AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID"),
    os.environ.get("AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID")
]


class DeployConfig(BaseModel):
    """Pydantic model for deployment configuration data"""
    asset_name: str
    metadata: Optional[str] = "optional-metadata"
    preview_hook: Optional[str] = None
    deploy_hook: Optional[str] = None
    preview_url: Optional[str] = None
    deploy_url: Optional[str] = None
    reject_message: Optional[str] = None


def parse_run_id(run_path: str) -> tuple[datetime, str]:
    """
    Parse run ID from path like '2025-09-07T00-07-25_run15'
    Returns (datetime, run_id)
    """
    run_name = run_path
    match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})_(.+)', run_name)
    if match:
        date_str = match.group(1).replace('-', ':')  # Convert back to standard format for last two parts
        date_str = date_str[:10] + 'T' + date_str[11:].replace('-', ':')  # Only fix the time part
        run_id = match.group(2)
        try:
            run_datetime = datetime.fromisoformat(date_str)
            return run_datetime, run_id
        except ValueError:
            get_dagster_logger().warning(f"Could not parse datetime from {date_str}")
            return None, run_id
    return None, run_name

@asset(group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_forecast",
       required_resource_keys={"resilientsims", "slack", "s3"},
       ins={
           "sandiego_epidemiology_hyper_extraction": AssetIn(key=AssetKey([f"sandiego", "sandiego_epidemiology_hyper_extraction"])),
       },
       automation_condition=AutomationCondition.eager()
       )
def run_epidemic_simulation(context, sandiego_epidemiology_hyper_extraction: dict):
  sims = context.resources.resilientsims
  s3_resource=context.resources.s3
  slack = context.resources.slack
  hyper_metadata = sandiego_epidemiology_hyper_extraction
  date_path = hyper_metadata["date_path"]
  if date_path is None:
      raise Exception("No date+_path found in sandiego_epidemiology_hyper_extraction run. Rerun AssetKey([sandiego, sandiego_epidemiology_hyper_extraction]")
  templateLoader = FileSystemLoader(searchpath=["templates","sim/templates", "src/sim/templates","sim/src/sim/templates", "workflows/sim/src/sim/templates" ])
  jinja = Environment(
      loader=templateLoader,
      autoescape=select_autoescape()
  )
  logger = get_dagster_logger()
  try:
      template_config = jinja.get_template("forecast_config.yaml")
      config_config_str=template_config.render(DATE="variables",
                                  LONG_DATE="here",
                                               RUNID=date_path,
                                  sims=sims,
                                  PUBLIC_BUCKET=os.environ.get("PUBLIC_BUCKET"),
                                  RESILIENTSIMS_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET"))
      config_config_yaml=yaml.safe_load(config_config_str)
  except Exception as e:
      logger.error(f"Error rendering forecast_config.yaml: {e} ")
      logger.error(f"config file: {config_config_yaml} ")
      raise e
  config_info = sims.create_configuration(sims.RESILIENTSIMS_SIMULATOR_ID, config_config_yaml)
  try:
      runs_path = f'sims/configs/epi2/config_{date_path}.yaml'
      store_assets.text_to_s3(config_config_str,runs_path,s3_resource)
  except Exception as e:
      logger.error(f"Error storing forecast_config.yaml to s3: {runs_path} {e} ")
      logger.error(f"config file: {config_config_str} ")

  logger.info(f"Created configuration: {config_info.get('id')}")

  template_run = jinja.get_template("forecast_run.yaml")
  config_run_str = template_run.render(s3=s3_resource, sims=sims,
                               CONFIG_ID=config_info.get('id'),
        )
  config_run_yaml = yaml.safe_load(config_run_str)

  # Complete workflow
  result = sims.run_simulator_workflow(
      simulator_pk=sims.RESILIENTSIMS_SIMULATOR_ID,
      #config_data=config_yaml,
      run_data=config_run_yaml,
      slack_resource=slack
  )
  return Output(result)

class forecastsS3AssetConfig(Config):
    forecast_run_path: str
    github_rt_url: str



@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_airtable",
    required_resource_keys={"s3", "airtable", "slack"},
    automation_condition=AutomationCondition.eager()
)
def process_epidemiology_forecasts(context, config: forecastsS3AssetConfig) -> Dict[str, Any]:
    """
    Process CSV files from ForAirTable directory and upsert to Airtable tables
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    airtable_resource = context.resources.airtable
    slack_resource = context.resources.slack
    AIRTABLE_EPI_DISEASE_TABLE_ID = os.environ.get("AIRTABLE_EPI_DISEASE_TABLE_ID")
    results = {
        "processed_files": [],
        "failed_files": [],
        "run_info": {}
    }

    try:
        # Get the latest run directory from sensor context or find latest
        run_key = config.forecast_run_path
        if not run_key:
            logger.warning("No forecast_run_path found in run tags")
            return results

        for_airtable_path = f"{run_key}ForAirTable/"
        logger.info(f"Processing files from: {for_airtable_path}")

        # Parse run information
        run_datetime, run_id = parse_run_id(run_key)
        results["run_info"] = {
            "run_path": run_key,
            "run_datetime": run_datetime.isoformat() if run_datetime else None,
            "run_id": run_id
        }
        try:
            filtered_records = airtable_resource.getTable(AIRTABLE_EPI_DISEASE_TABLE_ID).all(

                sort=["Name"],
                fields=["Name", "Id"]
                )
        except Exception as e:
            logger.error(f"Error listing files in {for_airtable_path}: {e}")
            raise "Cannot find Diseases Table"
        # List CSV files in ForAirTable directory
        try:
            files = s3_resource.listPath(for_airtable_path, bucket=FORECAST_BUCKET)
            csv_files = [f for f in files if f.object_name.endswith('.csv')]
            logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")

            # Create list of files that match GENERIC_FILE_TO_TABLE_MAPPING
            matching_files = []
            for csv_file in csv_files:
                object_name = Path(csv_file.object_name).name

                # Find matching mapping based on filename ending
                matched_mapping = None
                for file_ending, mapping_info in GENERIC_FILE_TO_TABLE_MAPPING.items():
                    if object_name.endswith(file_ending):
                        matched_mapping = mapping_info
                        break

                if matched_mapping is not None:
                    matching_files.append((csv_file, matched_mapping))

            logger.info(f"Found {len(matching_files)} files matching GENERIC_FILE_TO_TABLE_MAPPING")

            # Only clear tables if there are files that will be uploaded
            if len(matching_files) > 0:
                for t in TABLES_TO_CLEAR:
                    airtable_resource.emptyTable(t)
                    dagster.get_dagster_logger().info(f"Cleared table {t}")
            else:
                logger.info("No matching files found - skipping table clearing and processing")
                return results

        except Exception as e:
            logger.error(f"Error listing files in {for_airtable_path}: {e}")
            return results

        # Process each CSV file that has a mapping
        for csv_file, matched_mapping in matching_files:
            object_name = Path(csv_file.object_name).name

            table_id = matched_mapping["table"]
            mappings = matched_mapping["mapping"]
            linked_field_mappings=[{
                "field_in_record": "Disease",
                "filtered_records" : filtered_records,
                 "name_to_match_in_table":"Name"
            }]
            file_path = f"{for_airtable_path}{object_name}"

            try:
                # Read CSV from S3
                csv_content = s3_resource.getFile(csv_file.object_name, bucket=FORECAST_BUCKET)
                df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

                if df.empty:
                    logger.warning(f"Empty dataframe for {csv_file.object_name}")
                    results["failed_files"].append({
                        "file": csv_file.object_name,
                        "error": "Empty dataframe"
                    })
                    continue
                df= df.rename(columns=mappings)

                logger.info(f"📊 Processing {csv_file.object_name} for {object_name} with {len(df)} rows for table {table_id}")

                # Process through resilient epi schemas - Statistical Extension Model
                epi_processor = ResilientEpiProcessor()

                try:
                    logger.info(f"🔬 Integrating statistical extension schema for forecast data: {object_name}")

                    # Determine the metric type from filename
                    metric_type = 'cases'
                    if 'hosp' in object_name.lower():
                        metric_type = 'hospitalizations'

                    # Determine observation type from the data type
                    observation_type = 'forecast'
                    if 'reports' in object_name.lower():
                        observation_type = 'actual'  # Historical data
                    elif 'Rt' in object_name:
                        observation_type = 'prediction'  # Rt estimates

                    # Process each row to create statistical extension records
                    statistical_extension_records = []

                    for _, row in df.iterrows():
                        if 'Disease' in row and 'Date' in row:
                            disease = row['Disease']
                            date_str = pd.to_datetime(row['Date']).strftime('%Y-%m-%d')
                            jurisdiction = f"SanDiego{disease.replace(' ', '').replace('/', '')}"

                            # Create base record with required fields
                            base_fields = {
                                'jurisdiction': jurisdiction,
                                'date': date_str,
                                'disease': disease,
                                'metric': metric_type,
                                'observation_type': observation_type
                            }

                            # Add statistical fields if present
                            statistical_fields = {}

                            # Map statistical columns to our schema fields
                            field_mappings = {
                                'Mean': 'mean',
                                'Median': 'median',
                                'Lower 90': 'lower_90',
                                'Upper 90': 'upper_90',
                                'Lower 50': 'lower_50',
                                'Upper 50': 'upper_50',
                                'Lower 20': 'lower_20',
                                'Upper 20': 'upper_20',
                                'New cases': 'count',
                                'Reported hospital admissions': 'count',
                                'Estimated mean hospital admissions': 'mean'
                            }

                            for df_col, schema_field in field_mappings.items():
                                if df_col in row and pd.notna(row[df_col]):
                                    try:
                                        value = float(row[df_col])
                                        if value >= 0:  # Ensure non-negative values
                                            statistical_fields[schema_field] = value
                                    except (ValueError, TypeError):
                                        continue

                            # Only create record if we have at least one statistical field
                            if statistical_fields:
                                try:
                                    stat_record = create_statistical_extension_record(
                                        **base_fields,
                                        **statistical_fields
                                    )

                                    if not stat_record.empty:
                                        # Add metadata about the forecast
                                        stat_record['forecast_run_id'] = run_id
                                        stat_record['forecast_file'] = object_name
                                        stat_record['data_source'] = 'ResilientSims'
                                        if 'Type' in row:
                                            stat_record['forecast_type'] = row['Type']
                                        if 'Variable' in row:
                                            stat_record['forecast_variable'] = row['Variable']

                                        statistical_extension_records.append(stat_record)

                                except EpidemiologyValidationError as ve:
                                    logger.warning(f"⚠️  Validation error for {disease} on {date_str}: {ve}")
                                except Exception as e:
                                    logger.warning(f"⚠️  Error creating statistical record for {disease}: {e}")

                    # Combine and store validated statistical extension data
                    if statistical_extension_records:
                        combined_statistical = pd.concat(statistical_extension_records, ignore_index=True)
                        logger.info(f"✅ Created {len(combined_statistical)} validated statistical extension records from {object_name}")

                        # Store validated statistical extension data
                        stat_filename = f"{s3_output_path}output/validated_epi_schema/forecasts/{run_id}/{object_name.replace('.csv', '_statistical')}"
                        stat_metadata = store_assets.objectMetadata(
                            name=f"forecast_statistical_extension_{object_name}",
                            description=f"San Diego epidemiology forecast data in statistical extension schema format from {object_name}",
                            source_url="ResilientSims_Forecast"
                        )

                        # store_assets.dataframe_to_s3(combined_statistical, stat_filename, s3_resource,
                        #                            metadata=stat_metadata, formats=['csv', 'json'])
                        store_assets.store_dataframe_to_s3(combined_statistical, stat_filename,object_name, s3_resource,
                                                     metadata=stat_metadata, formats=['csv', 'json'])
                        logger.info(f"📋 Stored validated statistical extension forecast data: {len(combined_statistical)} rows")
                    else:
                        logger.warning(f"⚠️  No valid statistical extension records created from {object_name}")

                except Exception as e:
                    logger.error(f"❌ Error processing {object_name} with statistical extension schema: {e}")
                    # Continue with original processing even if validation fails

                #try:
                at_result=airtable_resource.batch_create2Table(
                    tableid=table_id,
                    df=df,

                    linked_field_mappings=linked_field_mappings,
                )
                # # Upsert to Airtable
                # at_result = airtable_resource.upsert2Table(
                #     tableid=table_id,
                #     df=df,
                #     keyfields=keyfields, # You may want to specify key fields for proper upserts
                # linked_field_mappings=linked_field_mappings,
                #     empty_first=False
                #
                # )
                # except Exception as e:
                #     logger.error(f"Error processing {csv_file.object_name}: {e}")
                #     at_result="error"

                results["processed_files"].append({
                    "file": csv_file.object_name,
                    "table_id": table_id,
                    "rows_processed": len(df),
                    "airtable_result": str(at_result)
                })

                # Store processed data to S3 for backup/audit


            except Exception as e:
                logger.error(f"Error processing {csv_file.object_name}: {e}")
                slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=f"Error processing {csv_file.object_name}: {e}")
                results["failed_files"].append({
                    "file": csv_file.object_name,
                    "error": str(e)
                })

        # Send Slack notification
        total_processed = len(results["processed_files"])
        total_failed = len(results["failed_files"])

        slack_message = f"""
📊 Epidemiology Forecasts Processed
Run: {run_id}
✅ Processed: {total_processed} files
❌ Failed: {total_failed} files
        """

        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=slack_message)
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

    except Exception as e:
        logger.error(f"Error in process_epidemiology_forecasts: {e}")
        results["error"] = str(e)

    return results

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_github_rt",
    required_resource_keys={"s3",  "slack"},
    automation_condition=AutomationCondition.eager()
)
def copy_rt_to_github(context, config: forecastsS3AssetConfig):
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack
    logger = get_dagster_logger()
    run_runpath: str = config.forecast_run_path
    github_url: str = config.github_rt_url

    # Validate GitHub token
    try:
        g = Github(FORECAST_GITHUB_RT_TOKEN)
        user = g.get_user()
        logger.info(f"Authenticated as GitHub user: {user.login}")
    except Exception as e:
        logger.error(f"Error authenticating to GitHub: {e}")
        raise Exception("Cannot authenticate to GitHub") from e

    bucket_name = FORECAST_BUCKET
    commit_message = f"Add files from S3 for {run_runpath}"

    # Find S3 files
    for_airtable_path = f"{run_runpath}ForAirTable/"
    rt_files = s3_resource.listPath(for_airtable_path, bucket=bucket_name)
    csv_files = [f for f in rt_files if f.object_name.endswith('_case_Rt.csv')]
    logger.info(f"Found {len(csv_files)} _case_Rt.csv files")

    if not csv_files:
        logger.warning("No CSV files found to push to GitHub")
        return {"files": [], "message": "No files to push", "github": github_url}

    updated_files = []

    with tempfile.TemporaryDirectory() as tempdir:
        # Configure Git with token for authentication
        # Use token in clone URL for authentication
        if github_url.startswith('https://github.com/'):
            # Insert token into URL for authentication
            auth_url = github_url.replace('https://github.com/', f'https://{FORECAST_GITHUB_RT_TOKEN}@github.com/')
        else:
            raise Exception(f"Unsupported GitHub URL format: {github_url}")

        # Clone repository with authentication
        try:
            repo = Repo.clone_from(auth_url, tempdir)
            logger.info(f"Successfully cloned repository to {tempdir}")
        except Exception as e:
            logger.error(f"Error cloning repository: {e}")
            raise Exception(f"Failed to clone repository: {github_url}") from e

        # Ensure target directory exists
        target_dir = os.path.join(tempdir, FORECAST_GITHUB_RT_PATH)
        os.makedirs(target_dir, exist_ok=True)

        # Download and add files
        for rt in csv_files:
            object_name = Path(rt.object_name).name
            local_file_path = os.path.join(target_dir, object_name)

            try:
                s3_resource.downloadFile(bucket=FORECAST_BUCKET, path=rt.object_name, filename=local_file_path)
                repo.index.add([local_file_path])
                updated_files.append(rt.object_name)
                logger.info(f"Added file to git: {local_file_path}")
            except Exception as e:
                logger.error(f"Error downloading/adding file {rt.object_name}: {e}")
                continue

        if not updated_files:
            logger.warning("No files were successfully added to repository")
            return {"files": [], "message": "No files were added", "github": github_url}

        # Check if there are changes to commit
        if repo.index.diff("HEAD") or repo.untracked_files:
            try:
                repo.index.commit(commit_message)
                logger.info(f"Committed changes: {commit_message}")

                # Push to GitHub
                origin = repo.remote(name="origin")
                origin.push()
                logger.info("Successfully pushed changes to GitHub")

            except Exception as e:
                logger.error(f"Error committing/pushing to GitHub: {e}")
                raise Exception("Failed to push changes to GitHub") from e
        else:
            logger.info("No changes to commit")

    return {
        "files": updated_files,
        "message": commit_message,
        "github": github_url
    }


@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_forecast_latest",
    required_resource_keys={"s3",  "slack"},
    automation_condition=AutomationCondition.eager()
)
def copy_forecast_latest(context, config: forecastsS3AssetConfig ):
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack
    logger=get_dagster_logger()
    run_runpath: str= config.forecast_run_path

    bucket_name = FORECAST_BUCKET
    s3_latest_path = "latest/sandiego_epidemiology_ili/forecast"

    # --- STEP 1: find S3 ---
    for_airtable_path = f"{run_runpath}ForAirTable/"
    rt_files = list(s3_resource.listPath(for_airtable_path, bucket=bucket_name))
    logger.info(f"Found {len(rt_files)}  files")
    updated_files=[]
    s3_client = s3_resource.getClient()
    for rt in rt_files:
        source_object =CopySource(bucket_name,rt.object_name)
        object_name = Path(rt.object_name).name.replace("Influenza","FLU")
        dest_path = f"{s3_latest_path}/{object_name}"
        s3_client.copy_object(s3_resource.S3_BUCKET,dest_path,source_object )
    return { "files": updated_files
             }
# Define asset job
epidemiology_forecasts_job = define_asset_job(
    name="epidemiology_forecasts_job",
    selection=[
        AssetKey(["sandiego", "sandiego_epidemiology_airtable"]),

        AssetKey(["sandiego", "sandiego_epidemiology_github_rt"]),
        AssetKey(["sandiego", "sandiego_epidemiology_forecast_latest"])
    ]
)


@sensor(
    job=epidemiology_forecasts_job,
    name="epidemiology_forecasts_sensor",
    minimum_interval_seconds=600,  # Check every 10
    required_resource_keys={"s3", "slack"}
)
def epidemiology_forecasts_sensor(context: SensorEvaluationContext):
    """
    Sensor to watch for new forecast runs in S3 path /seasonal_forecast/api_run
    Triggers when new run directories matching pattern YYYY-mm-ddTHH-MM-SS_runXX are found
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack

    try:
        # List directories in the forecast base path
        run_directories = s3_resource.listPath(FORECAST_API_RUN_PATH, bucket=FORECAST_BUCKET)
        #run_directories = list(run_directories)
        #ic(run_directories)
        if not run_directories:
            logger.info("No run directories found")
            return

        # Filter for run directories matching the expected pattern
        # run_pattern = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_run\d+')
        # valid_runs= []
        # for d in run_directories:
        #    if run_pattern.match(d.object_name.strip("/")):
        #        valid_runs.append(d)
        #
        # if not valid_runs:
        #     logger.info("No valid run directories found")
        #     return

        # Sort by name (which corresponds to timestamp) and get the latest
        latest_run = sorted(run_directories, key=lambda x: x.object_name)[-1]
        run_path = f"{latest_run.object_name}"

        logger.info(f"Latest run found: {run_path}")

        # Check if this run has been processed before using cursor
        cursor_key = f"last_processed_run"
        last_processed = context.cursor or ""

        if run_path == last_processed:
            logger.info(f"Run {run_path} already processed")
            return
        if FORECAST_AIRTABLE_ENABLED == "true":
            # Check if ForAirTable directory exists in this run
            for_airtable_path = f"{run_path}ForAirTable/"
            try:
                files_in_for_airtable = s3_resource.listPath(for_airtable_path,bucket=FORECAST_BUCKET)
                csv_files = [f for f in files_in_for_airtable if f.object_name.endswith('.csv')]

                if not csv_files:
                    logger.info(f"No CSV files found in {for_airtable_path}")
                    return

            except Exception as e:
                logger.info(f"ForAirTable directory not found or error accessing it: {e}")
                return

        # Parse run info for notification
        run_datetime, run_id = parse_run_id(latest_run.object_name)

        # Send Slack notification about new run
        try:
            slack_message = f"""
🆕 New Epidemiology Forecast Run Detected
Run: {run_id}
Path: {run_path}
CSV Files: {len(csv_files)}
Starting processing...
            """
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         text=slack_message)
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

        # Update cursor and yield run request
        context.update_cursor(run_path)

        yield RunRequest(
            run_key=f"forecast_run_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={"forecast_run_path": run_path},
            run_config=RunConfig(
                ops={
                    "sandiego__sandiego_epidemiology_airtable": {
                        "config": {
                            "forecast_run_path": run_path,
                        "github_rt_url": FORECAST_GITHUB_RT
                        }
                    },
                    "sandiego__sandiego_epidemiology_github_rt": {
                        "config": {
                            "forecast_run_path": run_path,
                            "github_rt_url":FORECAST_GITHUB_RT
                        }
                    },
                    "sandiego__sandiego_epidemiology_forecast_latest": {
                    "config": {
                        "forecast_run_path": run_path,
                        "github_rt_url": FORECAST_GITHUB_RT
                    }
        }
                }
            )
        )

    except Exception as e:
        logger.error(f"Error in epidemiology_forecasts_sensor: {e}")
        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                text=f"❌ Error in epidemiology forecasts sensor: {e}")
        except:
            pass


@asset(
    group_name="health",
    key_prefix="sandiego",
    name="resilientllm_sd",
    required_resource_keys={"resilientllm", "slack", "airtable", "s3"},
    ins={
        "resilientllm_sd_disease": AssetIn(key=AssetKey([f"sandiego", "resilientllm_sd_disease"])),
    },
automation_condition=AutomationCondition.eager()
)
def resilientllm_asset(context, resilientllm_sd_disease: str):
    """
    Calls the ResilientLLM API Forecast Update.
    """
    try:
        slack_resource = context.resources.slack
        llm = context.resources.resilientllm
        name = 'sandiego_epidemiology_sd_llm_generate_content'
        description = '''
                  San Diego Epidemiology Content Generated from ResilientLLM
                  '''
        metadata = store_assets.objectMetadata(name=name, description=description)
        s3_resource = context.resources.s3

        airtable_resource = context.resources.airtable
        #llm_response =  llm.execute(llm.webhook_uuid)
        llm_response_all = json.loads(resilientllm_sd_disease)
        llm_response = llm_response_all['en']
        shortsummry=  llm_response['summary-short'].replace('```', '')
        summary = llm_response['summary'].replace('```', '')
        update = llm_response['updates'].replace('```', '')
        try:
            # the update is now a long volume. just use the summary
            #update_update_table(airtable_resource, summary, update,shortsummry)
            update_update_table(airtable_resource, summary, summary, shortsummry)
            update_portal_record(airtable_resource, summary)
        except Exception as e:
            dagster.get_dagster_logger().error(f"Error in resilientllm_asset: {e}")
            raise e
        date_path = datetime.today().strftime('%Y%m%d')
        s3_updates_key = f"{s3_output_path}output/llm/{date_path}/updates.md"
        s3_summary_key = f"{s3_output_path}output/llm/{date_path}/summary.md"
        s3_short_key = f"{s3_output_path}output/llm/{date_path}/summary-short.md"
        store_assets.text_to_s3(update, s3_updates_key, s3_resource
                               , contenttype='text/markdown',
                               metadata=metadata
                               )
        store_assets.text_to_s3(summary, s3_summary_key, s3_resource
                               , contenttype='text/markdown',
                               metadata=metadata
                               )
        store_assets.text_to_s3(shortsummry, s3_short_key, s3_resource
                               , contenttype='text/markdown',
                               metadata=metadata
                               )



        try:

            # update is now long. Do not slack it
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"# LLM summary-short for San Diego : \n {shortsummry}")
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"# LLM summary for San Diego : \n {summary}")
        except:
            pass

        # Create deployment configuration and trigger deploy

        deploy_config = DeployConfig(
            asset_name=f"sd_epidemiology_forecast original {date_path}",
            #metadata=metadata,
            preview_hook=FORECAST_NETLIFY_PREVIEW_HOOK,
            deploy_hook=FORECAST_NETLIFY_PRODUCTION_HOOK,
            preview_url=FORECAST_NETLIFY_PREVIEW_URL,
            deploy_url=FORECAST_NETLIFY_PRODUCTION_URL,
            reject_message=FORECAST_NETLIFY_REJECT_MESSAGE
         )

        trigger_deploy(deploy_config)

        return summary, update
    except Exception as e:
        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"Error in update_resilientllm_asset: {e}")
        except:
            pass
        dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
        raise e
@asset(
    group_name="health",
    key_prefix="sandiego",
    name="resilientllm_sd_disease",
    required_resource_keys={"resilientllm", "slack",  "s3"},
    deps=[AssetKey([f"sandiego", "sandiego_epidemiology_forecast_latest"])],
automation_condition=AutomationCondition.eager()
)
def resilientllm_by_disease_asset(context):
    """
    Calls the ResilientLLM API Forecast Update.
    """
    diseases_config={ "COVID": [
    { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/COVID_case_reports.csv", "description": "reported cases" },
     { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/COVID_hosp_reports.csv", "description": "hospitalizations" },
     { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/COVID_hosp_Rt.csv", "description": "Rt for hospitalizations" }
     ], "Influenza": [
      { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/FLU_case_reports.csv", "description": "reported cases" },
       { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/FLU_hosp_reports.csv", "description": "hospitalizations" },
        { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/FLU_hosp_Rt.csv", "description": "Rt for hospitalizations" }
        ], "RSV": [
        { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/RSV_case_reports.csv", "description": "reported cases" },
         { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/RSV_hosp_reports.csv", "description": "hospitalizations" },
          { "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/sandiego_epidemiology_ili/forecast/RSV_hosp_Rt.csv", "description": "Rt for hospitalizations" }
          ] }
    diseases_key='diseases'
    toplevel_keys=['super-short-summary', 'short-summary', 'summary','super-short-summary-no-forecast'
        , 'updates', ]
    disease_level_keys=['summary','trend','generated','timestamp','summary-public-forecast','summary-public-nowcast','summary-public health office-forecast']
    try:
        slack_resource = context.resources.slack
        llm = context.resources.resilientllm
        name = 'sandiego_epidemiology_sd_llm_generate_content_by_disease'
        description = '''
                  San Diego Epidemiology Content Generated from ResilientLLM by_disease
                  '''
        metadata = store_assets.objectMetadata(name=name, description=description)
        s3_resource = context.resources.s3

        # llm_response =  llm.execute(llm.webhook_uuid)
        llm_response = llm.execute_with_data(llm.webhook_uuid, diseases_config)
        llm_response_json=json.dumps(llm_response, indent=2)
        date_path = datetime.today().strftime('%Y%m%d')
        s3_json_key = f"{s3_output_path}output/llm/{date_path}/forecast_summary.json"
        s3_json_latest_key = f"{s3_latest_path}forecast_summary.json"
        store_assets.text_to_s3(llm_response_json, s3_json_key, s3_resource
                                , contenttype='application/json',
                                metadata=metadata
                                )
        store_assets.text_to_s3(llm_response_json, s3_json_latest_key, s3_resource
                                , contenttype='application/json',
                                metadata=metadata
                                )
        languages = llm_response.keys()
        get_dagster_logger().debug('languages in response: {}'.format(languages))
        # Post aggregate Slack notification (keep for backward compatibility)
        # disabled for now. grab a sample at one point...
        # try:
        #     slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
        #                                                  markdown_text=f"# LLM Update for San Diego : \n {update}")
        #     slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
        #                                                  markdown_text=f"# LLM summary for San Diego : \n {summary}")
        # except:
        #     pass

        # Send email notification after LLM call
        try:
            email_subject = f"Resilient LLM forecast generated — {date_path}"
            email_body = f"The ResilientLLM forecast for San Diego epidemiology has been generated on {date_path}.\n\nDiseases: {', '.join(diseases_config.keys())}\n\nThis is an automated notification from the Resilient Workflows system."
            email_html = f"""<!DOCTYPE html>
<html>
<body>
    <p>The ResilientLLM forecast for San Diego epidemiology has been generated on {date_path}.</p>
    <p><strong>Diseases:</strong> {', '.join(diseases_config.keys())}</p>
    <hr>
    <small>This is an automated notification from the Resilient Workflows system.</small>
</body>
</html>"""
            send_email(email_subject, email_body, email_html)
        except Exception as e:
            get_dagster_logger().error(f"Email notification failed: {e}")

        # Create deployment configuration and trigger deploy
        deploy_config = DeployConfig(
            asset_name=f"resilientllm_sd_disease new layout {date_path}",
            #metadata=metadata,
            preview_hook=FORECAST_NETLIFY_PREVIEW_2_HOOK,
            deploy_hook=FORECAST_NETLIFY_PRODUCTION_2_HOOK,
            preview_url=FORECAST_NETLIFY_PREVIEW_2_URL,
            deploy_url=FORECAST_NETLIFY_PRODUCTION_2_URL,
            reject_message=FORECAST_NETLIFY_REJECT_2_MESSAGE
         )
        try:
            trigger_deploy(deploy_config)
        except Exception as e:
            get_dagster_logger().error(f"Slack Workflow trigger Failed: {e}")
            try:
                slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                             markdown_text=f"Processing ok, but Slack Workflow trigger Failed in update_resilientllm_asset: {e}")
            except:
                pass

        asset_metadata={'date':date_path, 'diseases': list(diseases_config.keys())}
        return Output(llm_response_json, metadata=asset_metadata)
    except Exception as e:
        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"Error in update_resilientllm_asset: {e}")
        except:
            pass
        dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
        raise e


def update_portal_record(airtable_resource, summary ):
    try:
        recordId = FORECAST_AIRTABLE_WIDGETS_RECORDID
        tableId = FORECAST_AIRTABLE_WIDGETS_TABLE_ID
        widgets_table = airtable_resource.getTable(tableId)
        data = {
            'Text': summary
        }
        updated_record = widgets_table.update(recordId, data)
        dagster.get_dagster_logger().info(f"llm summary: {summary}")
        dagster.get_dagster_logger().info(f"llm summary record updated: {str(updated_record)}")
    except Exception as e:
        dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
        raise e

def update_update_table(airtable_resource, summary, update,short_summary):
    try:
        RsvPortalRecordId = FORECAST_AIRTABLE_RSV_PORTAL_RECORDID
        RsvPortalRecordName = FORECAST_AIRTABLE_RSV_PORTAL_RECORDNAME
        update_table = airtable_resource.getTable(FORECAST_AIRTABLE_RSV_UPDATES_TABLE_ID)
        date = datetime.today().strftime('%Y-%m-%d')
        #  formula = "AND(FIND('rec4NITTQNAONirhd', ARRAYJOIN({Portal})) > 0, IS_SAME({Date}, '2025-10-08', 'day'))"
        # formula = "AND(SEARCH('"+ RsvPortalRecordId + "', ARRAYJOIN({Portal} )) , IS_SAME({Date}, '"+ date +"', 'day'))"
        # formula = " IS_SAME({Date}, '"+ date +"', 'day')"
        # formula = "FIND('" + "rec4NITTQNAONirhd" + "', ARRAYJOIN({Portal} )) >0"
        # formula = "FIND('" + RsvPortalRecordName+ "', {Portal slug} )"
        formula = "AND(FIND('" + RsvPortalRecordName + "', {Portal slug} ) >0 , IS_SAME({Date}, '" + date + "', 'day'))"
        existing_record = update_table.first(formula=formula)
        if existing_record is not None:
            # existing_record["fields"]['Status']=content
            fields = {"Update": update}
            update_table.update(existing_record['id'], fields)
        else:
            data = {
                "Portal": [RsvPortalRecordId],
                "Update": update,
                "Summary": summary,
                "Status": "Published",
                "Date": date,
                # "Portal slug":"CoSD-ILI-Report"
            }
            record = update_table.create(data)
        # attempt to upsert if this gets run more than once a day
    # records = [{ "fields":data}]
    # key_fields=['Date', 'Portal']
    # key_fields = ['Date', 'Portal slug']
    # record = update_table.batch_upsert(records, key_fields)

    except Exception as e:
        dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
        raise e
    return True

def create_deploy_config(asset_name: str, metadata: Optional[str] = "optional-metadata") -> DeployConfig:
    """
    Create a DeployConfig with default values from environment variables.

    Args:
        asset_name: Name of the asset being deployed
        metadata: Optional metadata for the deployment

    Returns:
        DeployConfig: Configured deployment model
    """
    return DeployConfig(
        asset_name=asset_name,
        metadata=metadata,
        preview_hook=FORECAST_NETLIFY_PREVIEW_HOOK,
        deploy_hook=FORECAST_NETLIFY_PRODUCTION_HOOK,
        preview_url=FORECAST_NETLIFY_PREVIEW_URL,
        deploy_url=FORECAST_NETLIFY_PRODUCTION_URL,
        reject_message=FORECAST_NETLIFY_REJECT_MESSAGE
    )


def send_email(subject: str, body: str, html_body: str | None = None) -> bool:
    """Send an email notification via SMTP."""
    logger = get_dagster_logger()
    if not EMAIL_ENABLED or not EMAIL_FROM or not EMAIL_TO:
        logger.info("Email not configured or disabled, skipping email notification")
        return False
    if not EMAIL_PASSWORD:
        logger.error("EMAIL_PASSWORD not configured")
        return False
    try:
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_FROM
        msg['Subject'] = subject
        recipients = [addr.strip() for addr in EMAIL_TO.split(',')]
        msg['To'] = ', '.join(recipients)
        msg.attach(MIMEText(body, 'plain'))
        if html_body:
            msg.attach(MIMEText(html_body, 'html'))
        with smtplib.SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.send_message(msg, to_addrs=recipients)
        logger.info(f"Email sent to {', '.join(recipients)}")
        return True
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        return False


def trigger_deploy(config: DeployConfig) -> bool:
    """
    Trigger a Netlify deployment workflow using the provided configuration.

    Args:
        config: DeployConfig model containing deployment configuration

    Returns:
        bool: True if deployment was triggered successfully, False otherwise
    """
    logger = get_dagster_logger()
    logger.debug(f"Triggering Netlify Deploy Workflow for {config.asset_name}")

    # for local development issues, do not set a FORECAST_NETLIFY_PREVIEW_HOOK and slack workflow will not run.
    if config.preview_hook is None:
        logger.debug(f"No PREVIEW hook configured - skipping Netlify Deploy Workflow")
        return False

    response = requests.post(f"{config.preview_hook}?trigger_title=triggered+by+Dagster")
    if response.status_code == 200:
        logger.info(f'Netlify Preview Hook called successfully')
        logger.info(f'calling slack workflow: {TRIGGER_PREVIEW_HOOK}')

        if TRIGGER_PREVIEW_HOOK is not None:
            # Convert Pydantic model to dict for JSON payload
            data = config.model_dump()

            response = requests.post(
                f"{TRIGGER_PREVIEW_HOOK}?trigger_title=triggered+by+Dagster",
                json=data
            )
            if response.status_code == 200:
                logger.info(f'Slack workflow triggered successfully {TRIGGER_PREVIEW_HOOK}')
            else:
                logger.error(f'Failed to trigger Slack workflow {TRIGGER_PREVIEW_HOOK}')
        else:
            logger.error(f'trigger not configured no TRIGGER_PREVIEW_HOOK environment variable')

        return True

    logger.error(f'Failed to call Netlify Preview Hook')
    return False


#

"""
{
  "preview_hook": "Example text",
  "deploy_hook": "Example text",
  "preview_url": "Example text",
  "deploy_url": "Example text"
}
"""
