import io
import os

import dagster
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import re

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
    AutomationCondition, RunConfig, Config, EnvVar
)

from icecream import ic


from . import sandiego_epidemiology_hyper_extraction
from ..resources import minio
from ..utils import store_assets

# S3 paths
FORECAST_API_RUN_PATH = os.environ.get("FORECAST_API_RUN_PATH", "api_run/")

FORECAST_OUTPUT_DIRECTORY =  os.environ.get("FORECAST_OUTPUT_DIRECTORY", "pathogens/sandiego/sandiego_epidemiology/output")
FORECAST_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET", 'resilientseasonal')
FORECAST_NETILFY_PREVIEW_HOOK=os.environ.get("FORECAST_NETILFY_DEPLOY_TRIGGER")
FORECAST_NETILFY_PRODUCTION_HOOK=os.environ.get("FORECAST_NETILFY_PRODUCTION_TRIGGER")
SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")
s3_output_path='pathogens/sandiego/sandiego_epidemiology/'

# File to Airtable table mapping - update these UUIDs with actual Airtable table IDs
#AIRTABLE_EPI_DISEASE_TABLE_ID=tblgC8jeTS4c6LPTO
#AIRTABLE_EPI_REPORTS_TABLE_ID=
#AIRTABLE_EPI_INFECTIONS_TABLE_ID=
#AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID=
#AIRTABLE_EPI_HOSPITAL_TABLE_ID=


COLUMN_RENAME_MAPPING={
    "date":"Date",
    "epiweek_start":"Date", # _reports
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
       deps=[AssetKey([f"sandiego", "sandiego_epidemiology_hyper_extraction"])],
       automation_condition=AutomationCondition.eager()
       )
def run_epidemic_simulation(context):
  sims = context.resources.resilientsims
  s3_resource=context.resources.s3
  slack = context.resources.slack
  hyper_metadata = context.repository_def.load_asset_value(AssetKey([f"sandiego", "sandiego_epidemiology_hyper_extraction"]))
  date_path = hyper_metadata["date_path"]
  if date_path is None:
      raise Exception("No date+_path found in sandiego_epidemiology_hyper_extraction run. Rerun AssetKey([sandiego, sandiego_epidemiology_hyper_extraction]")
  templateLoader = FileSystemLoader(searchpath=["templates", "public/templates","public/public/templates", "workflows/public/public/templates" ])
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
                                  PUBLIC_BUCKET=os.environ.get("PUBLIC_BUCKET"),)
      config_config_yaml=yaml.safe_load(config_config_str)
  except Exception as e:
      logger.error(f"Error rendering forecast_config.yaml: {e} ")
      logger.error(f"config file: {config_config_yaml} ")
      raise e
  config_info = sims.create_configuration(sims.RESILIENTSIMS_SIMULATOR_ID, config_config_yaml)
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
  return result

class forecastsS3AssetConfig(Config):
    forecast_run_path: str


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

                logger.info(f"Processing {csv_file.object_name}  for {object_name} with {len(df)} rows for table {table_id}")
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


# Define asset job
epidemiology_forecasts_job = define_asset_job(
    name="epidemiology_forecasts_job",
    selection=[AssetKey(["sandiego", "sandiego_epidemiology_airtable"])]
)


@sensor(
    job=epidemiology_forecasts_job,
    name="epidemiology_forecasts_sensor",
    minimum_interval_seconds=3600,  # Check hourly
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
                            "forecast_run_path": run_path
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
    name="resilientllm_sd_summary",
    required_resource_keys={"resilientllm", "slack", "airtable", "s3"},
    deps=[AssetKey([f"sandiego", "sandiego_epidemiology_airtable"])],
automation_condition=AutomationCondition.eager()
)
def summary_resilientllm_asset(context):
    """
    Calls the ResilientLLM API Summary.
    """
    slack_resource = context.resources.slack
    llm = context.resources.resilientllm
    name = 'sandiego_epidemiology_sd_summary'
    description = '''
          San Diego Epidemiology Summary from ResilientLLM
          '''
    metadata = store_assets.objectMetadata(name=name, description=description)
    s3_resource = context.resources.s3

    try:
        summary=llm.execute(llm.summary_id)
        content = summary['message']['data']['content']
        airtable_resource = context.resources.airtable
        try:
            recordId = "recowLGi7NgFdWX7B"
            tableId = "Widgets"
            widgets_table = airtable_resource.getTable(tableId)
            data = {
                'Text': content
            }
            updated_record = widgets_table.update(recordId, data)
            dagster.get_dagster_logger().info(f"llm summary: {summary}")
            dagster.get_dagster_logger().info(f"llm summary record updated: {str(updated_record)}")
        except Exception as e:
            dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
            raise e
        date_path = datetime.today().strftime('%Y%m%d')
        s3_key = f"{s3_output_path}output/llm/{date_path}/summary.md"
        store_assets.text_to_s3(content, s3_key, s3_resource
                               , contenttype='text/markdown',
                               metadata=metadata
                               )
        try:

            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"# LLM Summary for San Diego : \n {content}")
        except:
            pass

        return content
    except Exception as e:
            try:
                slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                             text=f"Error in resilientllm__sd_summary: {e}")
            except:
                pass
            dagster.get_dagster_logger().error(f"Error in resilientllm__sd_summary: {e}")
            raise

@asset(
    group_name="health",
    key_prefix="sandiego",
    name="resilientllm_sd_update",
    required_resource_keys={"resilientllm", "slack", "airtable", "s3"},
    deps=[AssetKey([f"sandiego", "resilientllm_sd_summary"])],
automation_condition=AutomationCondition.eager()
)
def update_resilientllm_asset(context):
    """
    Calls the ResilientLLM API Forecast Update.
    """
    try:
        slack_resource = context.resources.slack
        llm = context.resources.resilientllm
        llm = context.resources.resilientllm
        name = 'sandiego_epidemiology_sd_update'
        description = '''
                  San Diego Epidemiology Update from ResilientLLM
                  '''
        metadata = store_assets.objectMetadata(name=name, description=description)
        s3_resource = context.resources.s3

        airtable_resource = context.resources.airtable
        update =  llm.execute(llm.update_id)
        content = update['message']['data']['content']
        try:
            RsvPortalRecordId = "rec4NITTQNAONirhd"
            update_table=airtable_resource.getTable('Updates')
            data = {
                        "Portal": [RsvPortalRecordId],
                        "Update": content,
                        "Status": "Published",
                        "Date": datetime.today().strftime('%Y-%m-%d'),
                        #"Portal slug":"CoSD-ILI-Report"
                    }
            record = update_table.create(data)
            # attempt to upsert if this gets run more than once a day
           # records = [{ "fields":data}]
           # key_fields=['Date', 'Portal']
           # key_fields = ['Date', 'Portal slug']
            #record = update_table.batch_upsert(records, key_fields)

        except Exception as e:
            dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
            raise e
        date_path = datetime.today().strftime('%Y%m%d')
        s3_key = f"{s3_output_path}output/llm/{date_path}/update.md"
        store_assets.text_to_s3(content, s3_key, s3_resource
                               , contenttype='text/markdown',
                               metadata=metadata
                               )
        try:


            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"# LLM Update for San Diego : \n {content}")
        except:
            pass
        triggerDeploy()
        return content
    except Exception as e:
        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                         markdown_text=f"Error in update_resilientllm_asset: {e}")
        except:
            pass
        dagster.get_dagster_logger().error(f"Error in update_resilientllm_asset: {e}")
        raise e


def triggerDeploy():
    if FORECAST_NETILFY_PREVIEW_HOOK is None:
        return False
    response = requests.post(f"{FORECAST_NETILFY_PREVIEW_HOOK}?trigger_title=triggered+by+Dagster")
    if response.status_code == 200:
        return True
    else:
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
