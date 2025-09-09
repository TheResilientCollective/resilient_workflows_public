import io
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import re

from dagster import (
    asset,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    AutomationCondition, RunConfig, Config
)
from icecream import ic

from ..resources import minio
from ..utils import store_assets

# S3 paths
S3_FORECAST_BASE_PATH = "seasonal_forecast/api_run/"
S3_OUTPUT_PATH = "health/sandiego_epidemiology_forecasts/output"
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL", "#test")
# File to Airtable table mapping - update these UUIDs with actual Airtable table IDs
FILE_TO_TABLE_MAPPING = {
    # Example mappings - replace with actual file names and table UUIDs
   # "COVID_reports.csv": {"table": "tblvSecYusufGrkDY","keyfields":["Disease", "Date", "Type"] }, # new cases
    "COVID_Rt.csv": {"table": "tbl5nRdKkSJveFOiv",  "keyfields":["Disease", "date", "type"] },     # Infections Epi , Rt Estimates
    "COVIDhosp_reports.csv":{"table":  "tblvSecYusufGrkDY", "keyfields":["Disease", "date", "type"]}, # hosptial Admissions
    "RSV_Rt.csv": {"table": "tbl5nRdKkSJveFOiv", "keyfields": ["Disease", "date", "type"]},
    "RSVhosp_reports.csv": {"table": "tblvSecYusufGrkDY", "keyfields": ["Disease", "date", "type"]},
    "Influenza_Rt.csv": {"table": "tbl5nRdKkSJveFOiv", "keyfields": ["Disease", "date", "type"]},
    "Influenzahosp_reports.csv": {"table": "tblvSecYusufGrkDY", "keyfields": ["Disease", "date", "type"]},
    # hosptial Admissions
   # "COVIDhosp_Rt.csv": "tblMNO345PQR678", # not sure
    # Add more mappings as needed
}


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
       required_resource_keys={"resilientsims", "slack"})
def run_epidemic_simulation(context):
  sims = context.resources.resilientsims
  slack = context.resources.slack
  simulator_key = 1

  # Complete workflow
  result = sims.run_simulator_workflow(
      simulator_pk=simulator_key,
      #config_data={"param1": "value1"},
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

        # List CSV files in ForAirTable directory
        try:
            files = s3_resource.listPath(for_airtable_path)
            csv_files = [f for f in files if f.object_name.endswith('.csv')]
            logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")
        except Exception as e:
            logger.error(f"Error listing files in {for_airtable_path}: {e}")
            return results

        # Process each CSV file that has a mapping
        for csv_file in csv_files:
            object_name = Path(csv_file.object_name).name

            if object_name not in FILE_TO_TABLE_MAPPING:
                logger.info(f"Skipping {object_name} - no Airtable mapping defined")
                continue

            table_id = FILE_TO_TABLE_MAPPING[object_name]["table"]
            keyfields= FILE_TO_TABLE_MAPPING[object_name]["keyfields"]
            file_path = f"{for_airtable_path}{object_name}"

            try:
                # Read CSV from S3
                csv_content = s3_resource.getFile(csv_file.object_name)
                df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

                if df.empty:
                    logger.warning(f"Empty dataframe for {csv_file.object_name}")
                    results["failed_files"].append({
                        "file": csv_file.object_name,
                        "error": "Empty dataframe"
                    })
                    continue

                logger.info(f"Processing {csv_file.object_name}  for {object_name} with {len(df)} rows for table {table_id}")

                # Upsert to Airtable
                upsert_result = airtable_resource.upsert2Table(
                    tableid=table_id,
                    df=df,
                    keyfields=keyfields  # You may want to specify key fields for proper upserts
                )

                results["processed_files"].append({
                    "file": csv_file.object_name,
                    "table_id": table_id,
                    "rows_processed": len(df),
                    "airtable_result": str(upsert_result)
                })

                # Store processed data to S3 for backup/audit


            except Exception as e:
                logger.error(f"Error processing {csv_file.object_name}: {e}")
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
            slack_resource.send_message(slack_message)
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
        run_directories = s3_resource.listPath(S3_FORECAST_BASE_PATH)
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
            files_in_for_airtable = s3_resource.listPath(for_airtable_path)
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
            run_config=RunConfig({"forecast_run_path": run_path})
        )

    except Exception as e:
        logger.error(f"Error in epidemiology_forecasts_sensor: {e}")
        try:
            slack_resource.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                text=f"❌ Error in epidemiology forecasts sensor: {e}")
        except:
            pass
