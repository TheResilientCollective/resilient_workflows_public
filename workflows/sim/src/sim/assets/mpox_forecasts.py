import json
import os
import tempfile
from datetime import datetime
from pathlib import Path

import requests
from git import Repo
from github import Github
from minio.commonconfig import CopySource

from dagster import (
    AssetIn,
    AssetKey,
    AutomationCondition,
    EventLogEntry,
    Output,
    RunRequest,
    SensorEvaluationContext,
    asset,
    asset_sensor,
    define_asset_job,
    get_dagster_logger,
)

from resilient_core.utils import store_assets

# ── Environment variables ─────────────────────────────────────────────────────

MPOX_BUCKET = os.environ.get("MPOX_RESILIENTSIMS_BUCKET", "resilientmpox")
MPOX_API_RUN_PATH = os.environ.get("MPOX_API_RUN_PATH", "api_run/")
MPOX_LATEST_PATH = os.environ.get("MPOX_LATEST_PATH", "latest/pathogens/mpox/forecast/")
MPOX_OUTPUT_PATH = os.environ.get("MPOX_OUTPUT_PATH", "pathogens/mpox/output/")
MPOX_GITHUB_URL = os.environ.get(
    "FORECAST_GITHUB_RT",
    "https://github.com/TheResilientCollective/ResilientDataProducts.git",
)
MPOX_GITHUB_PATH = os.environ.get("MPOX_GITHUB_PATH", "mpox_output")
MPOX_GITHUB_TOKEN = os.environ.get("FORECAST_GITHUB_RT_TOKEN")
MPOX_DEPLOY_HOOK = os.environ.get("MPOX_DEPLOY_HOOK")
MPOX_DEPLOY_URL = os.environ.get("MPOX_DEPLOY_URL")
MPOX_RESILIENTLLM_WEBHOOK_UUID = os.environ.get("MPOX_RESILIENTLLM_WEBHOOK_UUID")
MPOX_EMAIL_WEBHOOK = os.environ.get("MPOX_EMAIL_WEBHOOK")
MPOX_PUBLIC_BASE_URL = os.environ.get(
    "MPOX_PUBLIC_BASE_URL", "https://oss.resilientservice.mooo.com/resilentpublic/"
)
SLACK_CHANNEL = os.environ.get("SLACK_SIMS_CHANNEL", "#test")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _find_latest_mpox_run_path(s3_resource) -> str:
    """Return the most recent run directory path in the mpox S3 bucket.

    Uses the same sort-by-name technique as the epidemiology sensor; the
    directory name format ``YYYY-mm-ddTHH-MM-SS_runXX`` sorts correctly
    lexicographically.
    """
    run_objects = list(s3_resource.listPath(MPOX_API_RUN_PATH, bucket=MPOX_BUCKET))
    if not run_objects:
        raise RuntimeError("No mpox run directories found in S3 after simulation completed")
    latest = sorted(run_objects, key=lambda x: x.object_name)[-1]
    return latest.object_name


# ── Assets ────────────────────────────────────────────────────────────────────


@asset(
    group_name="mpox",
    key_prefix="mpox",
    name="mpox_simulation",
    required_resource_keys={"mpox_resilientsims", "slack", "s3"},
    # deps declares lineage without requiring the value to be passed in;
    # the actual trigger comes from mpox_aggregated_sensor.
    deps=[AssetKey(["mpox", "mpox_aggregated"])],
)
def run_mpox_simulation(context) -> Output:
    """Trigger the ResilientSIMS mpox simulator and block until completion.

    Returns the S3 run path (``api_run/<timestamp>_run<id>/``) where the
    simulator wrote its outputs.
    """
    sims = context.resources.mpox_resilientsims
    slack = context.resources.slack
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    model_jurisdictions=[
        "NewYorkCity",
        "Texas",
        "LosAngelesCounty",
        "Florida",
        "Illinois",
        "Georgia",
    "SanDiego",
        "Washington"
    ]
    portal_jurisdictions=model_jurisdictions
    # need a test that portal is a subset of model
    run_data = {
       # "config": sims.RESILIENTSIMS_CONFIGURATION_ID, # this was SIMULATOR
        "config": 49,
        "parameters":{
            "model_jurisdictions": model_jurisdictions,
            "portal_jurisdictions": portal_jurisdictions
        },
        "output_template": {
            "type": "cloud_storage",
            "provider": "s3",
            "bucket": MPOX_BUCKET,
            "key_prefix": f"{MPOX_API_RUN_PATH}{{timestamp}}_run{{run_id}}",
        },
        "cloud_credentials": {
            "s3": {
                "access_key_id": s3_resource.S3_ACCESS_KEY,
                "secret_access_key": s3_resource.S3_SECRET_KEY,
                "endpoint_url": s3_resource.baseUrl(),
            }
        },
    }
    # run_data={
    #       "name": "Mpox CDC Data 2023-2024",
    #       "description": "Weekly Mpox case counts from CDC surveillance for high-incidence U.S. jurisdictions",
    #       "content": {
    #         "mpox_combined_weekly.csv": {
    #           "type": "cloud_storage",
    #           "provider": "s3",
    #           "bucket": MPOX_BUCKET,
    #           "key": "latest/pathogens/mpox/aggregated/mpox_aggregated.csv"
    #         }
    #       },
    #       "cloud_credentials": {
    #         "s3": {
    #           "access_key_id": sims.RESILIENTSIMS_USERNAME,
    #           "secret_access_key": sims.RESILIENTSIMS_PASSWORD,
    #           "endpoint_url": sims.RESILIENTSIMS_SERVER_URL
    #         }
    #       }
    #     }

    logger.info(f"Starting mpox simulation with simulator {sims.RESILIENTSIMS_SIMULATOR_ID}")
    result = sims.run_simulator_workflow(
        simulator_pk=sims.RESILIENTSIMS_SIMULATOR_ID,
        run_data=run_data,
        slack_resource=slack,
        monitor=True,
    )

    run_path = _find_latest_mpox_run_path(s3_resource)
    logger.info(f"Mpox simulation complete. Run path: {run_path}")

    return Output(
        run_path,
        metadata={
            "run_path": run_path,
            "run_info": str(result.get("run_info", {})),
            "final_status": str(result.get("final_status", {})),
        },
    )


@asset(
    group_name="mpox",
    key_prefix="mpox",
    name="mpox_latest",
    required_resource_keys={"s3", "slack"},
    ins={"mpox_simulation": AssetIn(key=AssetKey(["mpox", "mpox_simulation"]))},
    automation_condition=AutomationCondition.eager(),
)
def copy_mpox_latest(context, mpox_simulation: str):
    """Copy the most recent mpox run outputs to the S3 ``latest/`` path."""
    s3_resource = context.resources.s3
    slack_resource = context.resources.slack
    logger = get_dagster_logger()
    run_path = mpox_simulation

    for_airtable_path = f"{run_path}"
    files = list(s3_resource.listPath(for_airtable_path, bucket=MPOX_BUCKET))
    logger.info(f"Found {len(files)} files to copy to {MPOX_LATEST_PATH}")

    s3_client = s3_resource.getClient()
    updated_files = []
    for f in files:
        source_object = CopySource(MPOX_BUCKET, f.object_name)
        object_name = Path(f.object_name).name.replace("mpox_forecasts", "mpox_case_reports")
        dest_path = f"{MPOX_LATEST_PATH}{object_name}"
        s3_client.copy_object(s3_resource.S3_BUCKET, dest_path, source_object)
        updated_files.append(dest_path)

    logger.info(f"Copied {len(updated_files)} files to {MPOX_LATEST_PATH}")

    try:
        slack_resource.get_client().chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Mpox simulation outputs copied to latest/: {len(updated_files)} files",
        )
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")

    return {"files": updated_files, "latest_path": MPOX_LATEST_PATH}


@asset(
    group_name="mpox",
    key_prefix="mpox",
    name="mpox_github",
    required_resource_keys={"s3", "slack"},
    ins={"mpox_simulation": AssetIn(key=AssetKey(["mpox", "mpox_simulation"]))},
    automation_condition=AutomationCondition.eager(),
)
def copy_mpox_to_github(context, mpox_simulation: str):
    """Clone ResilientDataProducts and commit mpox outputs to ``mpox_output/``.

    Reuses ``FORECAST_GITHUB_RT`` and ``FORECAST_GITHUB_RT_TOKEN``; mpox files
    go into a separate ``MPOX_GITHUB_PATH`` subdirectory (default:
    ``mpox_output/``) alongside the existing ``sandiego_rt/`` directory.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    run_path = mpox_simulation

    try:
        g = Github(MPOX_GITHUB_TOKEN)
        user = g.get_user()
        logger.info(f"Authenticated as GitHub user: {user.login}")
    except Exception as e:
        logger.error(f"Error authenticating to GitHub: {e}")
        raise Exception("Cannot authenticate to GitHub") from e

    for_airtable_path = f"{run_path}ForAirTable/"
    all_files = list(s3_resource.listPath(for_airtable_path, bucket=MPOX_BUCKET))
    csv_files = [f for f in all_files if f.object_name.endswith(".csv")]
    logger.info(f"Found {len(csv_files)} CSV files to push to GitHub")

    if not csv_files:
        logger.warning("No CSV files found — skipping GitHub push")
        return {"files": [], "message": "No files to push", "github": MPOX_GITHUB_URL}

    commit_message = f"Add mpox outputs from run {run_path}"
    updated_files = []

    with tempfile.TemporaryDirectory() as tempdir:
        if MPOX_GITHUB_URL.startswith("https://github.com/"):
            auth_url = MPOX_GITHUB_URL.replace(
                "https://github.com/", f"https://{MPOX_GITHUB_TOKEN}@github.com/"
            )
        else:
            raise Exception(f"Unsupported GitHub URL format: {MPOX_GITHUB_URL}")

        try:
            repo = Repo.clone_from(auth_url, tempdir)
            logger.info(f"Cloned repository to {tempdir}")
        except Exception as e:
            logger.error(f"Error cloning repository: {e}")
            raise Exception(f"Failed to clone repository: {MPOX_GITHUB_URL}") from e

        target_dir = os.path.join(tempdir, MPOX_GITHUB_PATH)
        os.makedirs(target_dir, exist_ok=True)

        for f in csv_files:
            object_name = Path(f.object_name).name
            local_file_path = os.path.join(target_dir, object_name)
            try:
                s3_resource.downloadFile(
                    bucket=MPOX_BUCKET, path=f.object_name, filename=local_file_path
                )
                repo.index.add([local_file_path])
                updated_files.append(f.object_name)
                logger.info(f"Added file to git index: {local_file_path}")
            except Exception as e:
                logger.error(f"Error downloading/adding {f.object_name}: {e}")
                continue

        if not updated_files:
            logger.warning("No files were successfully added to the repository")
            return {"files": [], "message": "No files were added", "github": MPOX_GITHUB_URL}

        if repo.index.diff("HEAD") or repo.untracked_files:
            try:
                repo.index.commit(commit_message)
                logger.info(f"Committed: {commit_message}")
                origin = repo.remote(name="origin")
                origin.push()
                logger.info("Pushed changes to GitHub")
            except Exception as e:
                logger.error(f"Error committing/pushing to GitHub: {e}")
                raise Exception("Failed to push changes to GitHub") from e
        else:
            logger.info("No changes to commit")

    return {"files": updated_files, "message": commit_message, "github": MPOX_GITHUB_URL}


@asset(
    group_name="mpox",
    key_prefix="mpox",
    name="mpox_llm",
    required_resource_keys={"resilientllm", "slack", "s3"},
    deps=[AssetKey(["mpox", "mpox_latest"])],
    automation_condition=AutomationCondition.eager(),
)
def mpox_llm_asset(context):
    """Generate an LLM summary for mpox using the latest forecast outputs.

    Skips gracefully if ``MPOX_RESILIENTLLM_WEBHOOK_UUID`` is not configured.
    """
    llm = context.resources.resilientllm
    slack_resource = context.resources.slack
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    if not MPOX_RESILIENTLLM_WEBHOOK_UUID:
        logger.info("MPOX_RESILIENTLLM_WEBHOOK_UUID not configured — skipping LLM step")
        return Output(None, metadata={"skipped": True, "reason": "MPOX_RESILIENTLLM_WEBHOOK_UUID not set"})

    base_url = f"{MPOX_PUBLIC_BASE_URL}{MPOX_LATEST_PATH}"
    diseases_config = {
        "mpox": [
            {"url": f"{base_url}mpox_case_reports.csv", "description": "reported cases"},
            {"url": f"{base_url}mpox_case_Rt.csv", "description": "Rt estimates"},
        ]
    }

    try:
        llm_response = llm.execute_with_data(MPOX_RESILIENTLLM_WEBHOOK_UUID, diseases_config)
        llm_response_json = json.dumps(llm_response, indent=2)

        date_path = datetime.today().strftime("%Y%m%d")
        name = "mpox_llm_forecast_summary"
        description = "Mpox forecast summary generated by ResilientLLM"
        metadata = store_assets.objectMetadata(name=name, description=description)

        s3_json_key = f"{MPOX_OUTPUT_PATH}llm/{date_path}/forecast_summary.json"
        s3_json_latest_key = f"{MPOX_LATEST_PATH}forecast_summary.json"
        store_assets.text_to_s3(
            llm_response_json, s3_json_key, s3_resource,
            contenttype="application/json", metadata=metadata,
        )
        store_assets.text_to_s3(
            llm_response_json, s3_json_latest_key, s3_resource,
            contenttype="application/json", metadata=metadata,
        )

        logger.info("Mpox LLM summary stored to S3")
        return Output(llm_response_json, metadata={"date": date_path, "s3_key": s3_json_key})

    except Exception as e:
        logger.error(f"Error in mpox_llm_asset: {e}")
        try:
            slack_resource.get_client().chat_postMessage(
                channel=SLACK_CHANNEL, text=f"Error in mpox LLM step: {e}"
            )
        except Exception:
            pass
        raise e


@asset(
    group_name="mpox",
    key_prefix="mpox",
    name="mpox_deploy",
    required_resource_keys={"slack"},
    deps=[AssetKey(["mpox", "mpox_latest"])],
    automation_condition=AutomationCondition.eager(),
)
def mpox_website_deploy(context):
    """Trigger the mpox website build, send Slack notification, and
    optionally POST to an email webhook (n8n/Zapier).
    """
    slack_resource = context.resources.slack
    logger = get_dagster_logger()
    date_path = datetime.today().strftime("%Y%m%d")

    # Website build webhook
    if MPOX_DEPLOY_HOOK:
        try:
            response = requests.post(f"{MPOX_DEPLOY_HOOK}?trigger_title=triggered+by+Dagster")
            if response.status_code == 200:
                logger.info("Mpox deploy hook triggered successfully")
            else:
                logger.error(f"Mpox deploy hook returned status {response.status_code}")
        except Exception as e:
            logger.error(f"Error triggering mpox deploy hook: {e}")
    else:
        logger.info("MPOX_DEPLOY_HOOK not configured — skipping website build trigger")

    # Slack notification
    try:
        slack_resource.get_client().chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Mpox simulation pipeline complete ({date_path}). Website build triggered.",
        )
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")

    # Email notification via webhook (n8n/Zapier)
    if MPOX_EMAIL_WEBHOOK:
        try:
            payload = {
                "subject": f"Mpox Simulation Complete ({date_path})",
                "body": (
                    "The mpox simulation pipeline has completed. "
                    "New forecast outputs are available."
                ),
                "date": date_path,
            }
            response = requests.post(MPOX_EMAIL_WEBHOOK, json=payload)
            if response.status_code == 200:
                logger.info("Mpox email webhook triggered successfully")
            else:
                logger.error(f"Mpox email webhook returned status {response.status_code}")
        except Exception as e:
            logger.error(f"Error triggering mpox email webhook: {e}")
    else:
        logger.info("MPOX_EMAIL_WEBHOOK not configured — skipping email notification")

    return {"date": date_path, "deploy_hook_triggered": MPOX_DEPLOY_HOOK is not None}


# ── Job ───────────────────────────────────────────────────────────────────────

mpox_simulation_job = define_asset_job(
    name="mpox_simulation_job",
    selection=[
        AssetKey(["mpox", "mpox_simulation"]),
        AssetKey(["mpox", "mpox_latest"]),
        AssetKey(["mpox", "mpox_github"]),
    ],
)


# ── Sensor ────────────────────────────────────────────────────────────────────


@asset_sensor(
    asset_key=AssetKey(["mpox", "mpox_aggregated"]),
    job=mpox_simulation_job,
    name="mpox_aggregated_sensor",
    minimum_interval_seconds=60,
    required_resource_keys={"slack"},
)
def mpox_aggregated_sensor(context: SensorEvaluationContext, asset_event: EventLogEntry):
    """Fire when ``mpox_aggregated`` (pathogens code location) materializes.

    Triggers ``mpox_simulation_job`` which runs the ResilientSIMS mpox
    simulator. Downstream assets then fan out via ``AutomationCondition.eager()``.
    """
    logger = get_dagster_logger()
    slack_resource = context.resources.slack

    run_key = f"mpox_run_{asset_event.run_id}"
    logger.info(
        f"mpox_aggregated materialized (run_id={asset_event.run_id}), "
        "triggering mpox simulation"
    )

    try:
        slack_resource.get_client().chat_postMessage(
            channel=SLACK_CHANNEL,
            text="mpox_aggregated materialized — starting mpox simulation pipeline",
        )
    except Exception as e:
        logger.warning(f"Slack notification failed: {e}")

    yield RunRequest(run_key=run_key)
