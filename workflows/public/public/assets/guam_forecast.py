import io
import os
import json

import dagster
import pandas as pd
from pathlib import Path
from typing import Dict, Any
from datetime import datetime

import yaml
from minio.commonconfig import CopySource

from jinja2 import Environment, FileSystemLoader, select_autoescape

from dagster import (
    asset,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    AutomationCondition,
    RunConfig,
    Output,
)

from ..utils import store_assets
from ..utils.resilient_epi_schemas import (
    EpidemiologyValidationError,
    create_statistical_extension_record,
)
from .sandiego_epidemiology_forecasts import (
    DeployConfig,
    trigger_deploy,
    parse_run_id,
    forecastsS3AssetConfig,
    COLUMN_RENAME_MAPPING_NEW,
    COLUMN_RENAME_MAPPING_RT_ESTIMATES,
    COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS,
)

# Guam-specific environment / paths
GUAM_FORECAST_API_RUN_PATH = os.environ.get("GUAM_FORECAST_API_RUN_PATH", "guam_api_run/")
GUAM_FORECAST_OUTPUT_DIRECTORY = os.environ.get(
    "GUAM_FORECAST_OUTPUT_DIRECTORY", "pathogens/guam/guam_forecast/output"
)
FORECAST_BUCKET = os.environ.get("RESILIENTSIMS_BUCKET", "resilientseasonal")

# Re-use shared Netlify / Slack env vars
FORECAST_NETLIFY_PREVIEW_2_HOOK = os.environ.get("FORECAST_NETLIFY_PREVIEW_2_HOOK")
FORECAST_NETLIFY_PRODUCTION_2_HOOK = os.environ.get("FORECAST_NETLIFY_PRODUCTION_2_HOOK")
FORECAST_NETLIFY_PREVIEW_2_URL = os.environ.get("FORECAST_NETLIFY_PREVIEW_2_URL")
FORECAST_NETLIFY_PRODUCTION_2_URL = os.environ.get("FORECAST_NETLIFY_PRODUCTION_2_URL")
FORECAST_NETLIFY_REJECT_2_MESSAGE = os.environ.get(
    "FORECAST_NETLIFY_REJECT_2_MESSAGE", "Editing interface is being worked on."
)
TRIGGER_PREVIEW_HOOK = os.environ.get("TRIGGER_PREVIEW_HOOK")
SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")

s3_output_path = "pathogens/guam/guam_forecast/"
s3_latest_llm_path = "latest/guam/forecast"

# Guam only has Influenza – simplified mapping
GENERIC_FILE_TO_TABLE_MAPPING = {
    "_case_reports.csv": {"mapping": COLUMN_RENAME_MAPPING_NEW},
    "_case_Rt.csv": {"mapping": COLUMN_RENAME_MAPPING_RT_ESTIMATES},
    "_hosp_reports.csv": {"mapping": COLUMN_RENAME_MAPPING_HOSPITAL_ADMISSIONS},
}


# ---------------------------------------------------------------------------
# Asset: Run the epidemic simulation for Guam
# ---------------------------------------------------------------------------
@asset(
    group_name="health",
    key_prefix="guam",
    name="guam_epidemic_simulation",
    required_resource_keys={"resilientsims", "slack", "s3"},
    automation_condition=AutomationCondition.eager(),
)
def guam_run_epidemic_simulation(context):
    """Submit a ResilientSims epidemic simulation run for Guam ILI data."""
    sims = context.resources.resilientsims
    s3_resource = context.resources.s3
    slack = context.resources.slack
    logger = get_dagster_logger()

    # Fixed run-id for Guam (no Tableau extraction dependency)
    run_id = "guam_forecast"

    templateLoader = FileSystemLoader(
        searchpath=[
            "templates",
            "public/templates",
            "public/public/templates",
            "workflows/public/public/templates",
        ]
    )
    jinja = Environment(loader=templateLoader, autoescape=select_autoescape())

    try:
        template_config = jinja.get_template("guam_forecast_config_v2.yaml")
        config_config_str = template_config.render(
            DATE="variables",
            LONG_DATE="here",
            RUNID=run_id,
            sims=sims,
            PUBLIC_BUCKET=os.environ.get("PUBLIC_BUCKET"),
            RESILIENTSIMS_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET"),
        )
        config_config_yaml = yaml.safe_load(config_config_str)
    except Exception as e:
        logger.error(f"Error rendering guam_forecast_config.yaml: {e}")
        raise e

    config_info = sims.create_configuration(sims.RESILIENTSIMS_SIMULATOR_ID, config_config_yaml)

    runs_path = f"sims/configs/guam/config_{run_id}.yaml"
    try:
        store_assets.text_to_s3(json.dumps(config_info, indent=2), runs_path, s3_resource)
    except Exception as e:
        logger.error(f"Error storing guam_forecast_config.yaml to s3: {runs_path} {e}")

    logger.info(f"Created configuration: {config_info.get('id')}")

    template_run = jinja.get_template("guam_forecast_run.yaml")
    config_run_str = template_run.render(
        s3=s3_resource,
        sims=sims,
        CONFIG_ID=config_info.get("id"),
    )
    config_run_yaml = yaml.safe_load(config_run_str)

    result = sims.run_simulator_workflow(
        simulator_pk=sims.RESILIENTSIMS_SIMULATOR_ID,
        run_data=config_run_yaml,
        slack_resource=slack,
    )
    return Output(result)


# ---------------------------------------------------------------------------
# Asset: Process Guam forecast outputs (S3 storage + schema validation only)
# ---------------------------------------------------------------------------
@asset(
    group_name="health",
    key_prefix="guam",
    name="guam_epidemiology_airtable",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def guam_process_epidemiology_forecasts(context, config: forecastsS3AssetConfig) -> Dict[str, Any]:
    """
    Process CSV files from ForAirTable directory.
    Guam version: schema validation + S3 storage only (no Airtable push).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack

    results: Dict[str, Any] = {
        "processed_files": [],
        "failed_files": [],
        "run_info": {},
    }

    try:
        run_key = config.forecast_run_path
        if not run_key:
            logger.warning("No forecast_run_path found in run tags")
            return results

        for_airtable_path = f"{run_key}ForAirTable/"
        logger.info(f"Processing files from: {for_airtable_path}")

        run_datetime, run_id = parse_run_id(run_key)
        results["run_info"] = {
            "run_path": run_key,
            "run_datetime": run_datetime.isoformat() if run_datetime else None,
            "run_id": run_id,
        }

        try:
            files = s3_resource.listPath(for_airtable_path, bucket=FORECAST_BUCKET)
            csv_files = [f for f in files if f.object_name.endswith(".csv")]
            logger.info(f"Found {len(csv_files)} CSV files: {csv_files}")

            matching_files = []
            for csv_file in csv_files:
                object_name = Path(csv_file.object_name).name
                matched_mapping = None
                for file_ending, mapping_info in GENERIC_FILE_TO_TABLE_MAPPING.items():
                    if object_name.endswith(file_ending):
                        matched_mapping = mapping_info
                        break
                if matched_mapping is not None:
                    matching_files.append((csv_file, matched_mapping))

            logger.info(f"Found {len(matching_files)} files matching GENERIC_FILE_TO_TABLE_MAPPING")
            if not matching_files:
                logger.info("No matching files found - skipping processing")
                return results

        except Exception as e:
            logger.error(f"Error listing files in {for_airtable_path}: {e}")
            return results

        for csv_file, matched_mapping in matching_files:
            object_name = Path(csv_file.object_name).name
            mappings = matched_mapping["mapping"]

            try:
                csv_content = s3_resource.getFile(csv_file.object_name, bucket=FORECAST_BUCKET)
                df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

                if df.empty:
                    logger.warning(f"Empty dataframe for {csv_file.object_name}")
                    results["failed_files"].append(
                        {"file": csv_file.object_name, "error": "Empty dataframe"}
                    )
                    continue

                df = df.rename(columns=mappings)
                logger.info(
                    f"Processing {csv_file.object_name} ({object_name}) with {len(df)} rows"
                )

                # --- Schema validation ---
                metric_type = "hospitalizations" if "hosp" in object_name.lower() else "cases"
                if "reports" in object_name.lower():
                    observation_type = "actual"
                elif "Rt" in object_name:
                    observation_type = "prediction"
                else:
                    observation_type = "forecast"

                statistical_extension_records = []
                for _, row in df.iterrows():
                    if "Disease" in row and "Date" in row:
                        disease = row["Disease"]
                        date_str = pd.to_datetime(row["Date"]).strftime("%Y-%m-%d")
                        jurisdiction = f"Guam{disease.replace(' ', '').replace('/', '')}"

                        base_fields = {
                            "jurisdiction": jurisdiction,
                            "date": date_str,
                            "disease": disease,
                            "metric": metric_type,
                            "observation_type": observation_type,
                        }

                        field_mappings = {
                            "Mean": "mean",
                            "Median": "median",
                            "Lower 90": "lower_90",
                            "Upper 90": "upper_90",
                            "Lower 50": "lower_50",
                            "Upper 50": "upper_50",
                            "Lower 20": "lower_20",
                            "Upper 20": "upper_20",
                            "New cases": "count",
                            "Reported hospital admissions": "count",
                            "Estimated mean hospital admissions": "mean",
                        }

                        statistical_fields = {}
                        for df_col, schema_field in field_mappings.items():
                            if df_col in row and pd.notna(row[df_col]):
                                try:
                                    value = float(row[df_col])
                                    if value >= 0:
                                        statistical_fields[schema_field] = value
                                except (ValueError, TypeError):
                                    continue

                        if statistical_fields:
                            try:
                                stat_record = create_statistical_extension_record(
                                    **base_fields, **statistical_fields
                                )
                                if not stat_record.empty:
                                    stat_record["forecast_run_id"] = run_id
                                    stat_record["forecast_file"] = object_name
                                    stat_record["data_source"] = "ResilientSims"
                                    if "Type" in row:
                                        stat_record["forecast_type"] = row["Type"]
                                    if "Variable" in row:
                                        stat_record["forecast_variable"] = row["Variable"]
                                    statistical_extension_records.append(stat_record)
                            except EpidemiologyValidationError as ve:
                                logger.warning(
                                    f"Validation error for {disease} on {date_str}: {ve}"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Error creating statistical record for {disease}: {e}"
                                )

                if statistical_extension_records:
                    combined_statistical = pd.concat(
                        statistical_extension_records, ignore_index=True
                    )
                    logger.info(
                        f"Created {len(combined_statistical)} validated statistical extension records from {object_name}"
                    )

                    stat_filename = f"{s3_output_path}output/validated_epi_schema/forecasts/{run_id}/{object_name.replace('.csv', '_statistical')}"
                    stat_metadata = store_assets.objectMetadata(
                        name=f"guam_forecast_statistical_extension_{object_name}",
                        description=f"Guam epidemiology forecast data in statistical extension schema format from {object_name}",
                        source_url="ResilientSims_Forecast",
                    )
                    store_assets.store_dataframe_to_s3(
                        combined_statistical,
                        stat_filename,
                        object_name,
                        s3_resource,
                        metadata=stat_metadata,
                        formats=["csv", "json"],
                    )
                    logger.info(
                        f"Stored validated statistical extension forecast data: {len(combined_statistical)} rows"
                    )
                else:
                    logger.warning(
                        f"No valid statistical extension records created from {object_name}"
                    )

                results["processed_files"].append(
                    {
                        "file": csv_file.object_name,
                        "rows_processed": len(df),
                    }
                )

            except Exception as e:
                logger.error(f"Error processing {csv_file.object_name}: {e}")
                slack_resource.get_client().chat_postMessage(
                    channel=SLACK_CHANNEL,
                    text=f"Error processing Guam forecast file {csv_file.object_name}: {e}",
                )
                results["failed_files"].append(
                    {"file": csv_file.object_name, "error": str(e)}
                )

        total_processed = len(results["processed_files"])
        total_failed = len(results["failed_files"])
        slack_message = f"""
Guam Epidemiology Forecasts Processed
Run: {run_id}
Processed: {total_processed} files
Failed: {total_failed} files
        """
        try:
            slack_resource.get_client().chat_postMessage(
                channel=SLACK_CHANNEL, text=slack_message
            )
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

    except Exception as e:
        logger.error(f"Error in guam_process_epidemiology_forecasts: {e}")
        results["error"] = str(e)

    return results


# ---------------------------------------------------------------------------
# Asset: Copy Rt to GitHub (disabled for Guam)
# ---------------------------------------------------------------------------
@asset(
    group_name="health",
    key_prefix="guam",
    name="guam_epidemiology_github_rt",
    required_resource_keys={"s3", "slack"},
)
def guam_copy_rt_to_github(context, config: forecastsS3AssetConfig):
    """Disabled for Guam - GitHub integration deferred."""
    logger = get_dagster_logger()
    logger.info("guam_copy_rt_to_github is disabled - GitHub integration deferred for Guam")
    return {"files": [], "message": "GitHub integration disabled for Guam"}


# ---------------------------------------------------------------------------
# Asset: Copy forecast outputs to latest/ path
# ---------------------------------------------------------------------------
@asset(
    group_name="health",
    key_prefix="guam",
    name="guam_epidemiology_forecast_latest",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def guam_copy_forecast_latest(context, config: forecastsS3AssetConfig):
    """Copy Guam forecast outputs to latest/guam_flu_forecast/forecast/."""
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    run_runpath: str = config.forecast_run_path

    bucket_name = FORECAST_BUCKET
    s3_latest_path = "latest/guam_flu_forecast/forecast"

    for_airtable_path = f"{run_runpath}ForAirTable/"
    rt_files = list(s3_resource.listPath(for_airtable_path, bucket=bucket_name))
    logger.info(f"Found {len(rt_files)} files")

    updated_files = []
    s3_client = s3_resource.getClient()
    for rt in rt_files:
        source_object = CopySource(bucket_name, rt.object_name)
        object_name = Path(rt.object_name).name.replace("Influenza", "FLU")
        dest_path = f"{s3_latest_path}/{object_name}"
        s3_client.copy_object(s3_resource.S3_BUCKET, dest_path, source_object)
        updated_files.append(dest_path)

    return {"files": updated_files}


# ---------------------------------------------------------------------------
# Job + Sensor
# ---------------------------------------------------------------------------
guam_forecasts_job = define_asset_job(
    name="guam_forecasts_job",
    selection=[
        AssetKey(["guam", "guam_epidemiology_airtable"]),
        AssetKey(["guam", "guam_epidemiology_github_rt"]),
        AssetKey(["guam", "guam_epidemiology_forecast_latest"]),
    ],
)


@sensor(
    job=guam_forecasts_job,
    name="guam_forecasts_sensor",
    minimum_interval_seconds=600,
    required_resource_keys={"s3", "slack"},
)
def guam_forecasts_sensor(context: SensorEvaluationContext):
    """
    Watch for new Guam forecast runs in S3 path guam_api_run/.
    Triggers when new run directories are found.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack

    try:
        run_directories = s3_resource.listPath(GUAM_FORECAST_API_RUN_PATH, bucket=FORECAST_BUCKET)
        if not run_directories:
            logger.info("No Guam run directories found")
            return

        latest_run = sorted(run_directories, key=lambda x: x.object_name)[-1]
        run_path = f"{latest_run.object_name}"
        logger.info(f"Latest Guam run found: {run_path}")

        last_processed = context.cursor or ""
        if run_path == last_processed:
            logger.info(f"Guam run {run_path} already processed")
            return

        for_airtable_path = f"{run_path}ForAirTable/"
        try:
            files_in_for_airtable = s3_resource.listPath(
                for_airtable_path, bucket=FORECAST_BUCKET
            )
            csv_files = [f for f in files_in_for_airtable if f.object_name.endswith(".csv")]
            if not csv_files:
                logger.info(f"No CSV files found in {for_airtable_path}")
                return
        except Exception as e:
            logger.info(f"ForAirTable directory not found or error accessing it: {e}")
            return

        run_datetime, run_id = parse_run_id(latest_run.object_name)

        try:
            slack_message = f"""
New Guam ILI Forecast Run Detected
Run: {run_id}
Path: {run_path}
CSV Files: {len(csv_files)}
Starting processing...
            """
            slack_resource.get_client().chat_postMessage(
                channel=SLACK_CHANNEL, text=slack_message
            )
        except Exception as e:
            logger.warning(f"Failed to send Slack notification: {e}")

        context.update_cursor(run_path)

        # github_rt_url is unused for Guam but required by forecastsS3AssetConfig
        dummy_github_url = "https://github.com/placeholder/not-used"

        yield RunRequest(
            run_key=f"guam_forecast_run_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            tags={"forecast_run_path": run_path},
            run_config=RunConfig(
                ops={
                    "guam__guam_epidemiology_airtable": {
                        "config": {
                            "forecast_run_path": run_path,
                            "github_rt_url": dummy_github_url,
                        }
                    },
                    "guam__guam_epidemiology_github_rt": {
                        "config": {
                            "forecast_run_path": run_path,
                            "github_rt_url": dummy_github_url,
                        }
                    },
                    "guam__guam_epidemiology_forecast_latest": {
                        "config": {
                            "forecast_run_path": run_path,
                            "github_rt_url": dummy_github_url,
                        }
                    },
                }
            ),
        )

    except Exception as e:
        logger.error(f"Error in guam_forecasts_sensor: {e}")
        try:
            slack_resource.get_client().chat_postMessage(
                channel=SLACK_CHANNEL,
                text=f"Error in Guam forecasts sensor: {e}",
            )
        except:
            pass


# ---------------------------------------------------------------------------
# LLM Asset: By-disease summary (Influenza only)
# ---------------------------------------------------------------------------
@asset(
    group_name="health",
    key_prefix="guam",
    name="guam_resilientllm_disease",
    required_resource_keys={"resilientllm", "slack", "s3"},
    deps=[AssetKey(["guam", "guam_epidemiology_forecast_latest"])],
    automation_condition=AutomationCondition.eager(),
)
def guam_resilientllm_by_disease_asset(context):
    """Generate LLM disease summary for Guam - Influenza only."""
    diseases_config = {
        "Influenza": [
            {
                "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/guam/forecast/FLU_case_reports.csv",
                "description": "reported cases",
            },
            {
                "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/guam/forecast/FLU_hosp_reports.csv",
                "description": "hospitalizations",
            },
            {
                "url": "https://oss.resilientservice.mooo.com/resilentpublic/latest/guam/forecast/FLU_hosp_Rt.csv",
                "description": "Rt for hospitalizations",
            },
        ]
    }

    slack_resource = context.resources.slack
    try:
        llm = context.resources.resilientllm
        s3_resource = context.resources.s3

        name = "guam_epidemiology_llm_generate_content_by_disease"
        description = "Guam Epidemiology Content Generated from ResilientLLM by disease"
        metadata = store_assets.objectMetadata(name=name, description=description)

        llm_response = llm.execute_with_data(llm.webhook_uuid, diseases_config)
        llm_response_json = json.dumps(llm_response, indent=2)

        date_path = datetime.today().strftime("%Y%m%d")
        s3_json_key = f"{s3_output_path}output/llm/{date_path}/forecast_summary.json"
        s3_json_latest_key = f"{s3_latest_llm_path}forecast_summary.json"

        store_assets.text_to_s3(
            llm_response_json,
            s3_json_key,
            s3_resource,
            contenttype="application/json",
            metadata=metadata,
        )
        store_assets.text_to_s3(
            llm_response_json,
            s3_json_latest_key,
            s3_resource,
            contenttype="application/json",
            metadata=metadata,
        )

        languages = llm_response.keys()
        get_dagster_logger().debug("languages in response: {}".format(languages))

        # deploy_config = DeployConfig(
        #     asset_name="guam_resilientllm_disease",
        #     preview_hook=FORECAST_NETLIFY_PREVIEW_2_HOOK,
        #     deploy_hook=FORECAST_NETLIFY_PRODUCTION_2_HOOK,
        #     preview_url=FORECAST_NETLIFY_PREVIEW_2_URL,
        #     deploy_url=FORECAST_NETLIFY_PRODUCTION_2_URL,
        #     reject_message=FORECAST_NETLIFY_REJECT_2_MESSAGE,
        # )
        # trigger_deploy(deploy_config)

        asset_metadata = {"date": date_path, "diseases": list(diseases_config.keys())}
        return Output(llm_response_json, metadata=asset_metadata)

    except Exception as e:
        try:
            slack_resource.get_client().chat_postMessage(
                channel=SLACK_CHANNEL,
                markdown_text=f"Error in guam_resilientllm_by_disease_asset: {e}",
            )
        except:
            pass
        dagster.get_dagster_logger().error(f"Error in guam_resilientllm_by_disease_asset: {e}")
        raise e
