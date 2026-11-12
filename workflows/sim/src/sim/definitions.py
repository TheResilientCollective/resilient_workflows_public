import os

from dagster import (
    AssetKey,
    DefaultSensorStatus,
    Definitions,
    EnvVar,
    RunFailureSensorContext,
    load_assets_from_modules,
    run_failure_sensor,
)
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor
from dagster_openai import OpenAIResource

from resilient_core.resources.minio import S3Resource
from resilient_core.resources.airtable import AirtableResource

from . import assets as assets_pkg
from .resources.resilientsims import ResilientSimsResource
from .resources.resilientllm import ResilientLLMResource
from .source_assets import all_source_assets
from .assets.sandiego_epidemiology_forecasts import send_email

# Same base URL the Slack failure sensor uses for its "View in Dagster UI" button.
WEBSERVER_BASE_URL = f'https://{os.environ.get("SCHED_HOSTNAME", "sched")}.{os.environ.get("HOST", "local")}/'

# Assets whose run failures should also be emailed (in addition to the Slack alert).
EMAIL_ON_FAILURE_ASSET_KEYS = {
    AssetKey(["sandiego", "sandiego_epidemiology_forecast"]),
}


def slack_message_fn(context: RunFailureSensorContext) -> str:
    return (
        f"Job *[{context.dagster_run.job_name}]* failed! "
        f"Error: {context.failure_event.message}"
    )


slack_on_run_failure = make_slack_on_run_failure_sensor(
    os.environ.get("SLACK_CHANNEL_FAILURES", "workflows-failures"),
    os.getenv("SLACK_TOKEN"),
    webserver_base_url=WEBSERVER_BASE_URL,
    text_fn=slack_message_fn,
)


def dagster_run_url(context: RunFailureSensorContext) -> str:
    """Link to the failed run, built the same way dagster_slack builds its button link."""
    return f"{WEBSERVER_BASE_URL}/runs/{context.dagster_run.run_id}"


def failed_asset_keys(context: RunFailureSensorContext) -> set[AssetKey]:
    """Asset keys that were part of the failed run, from the failed steps and the run's selection."""
    keys: set[AssetKey] = set()
    try:
        for event in context.get_step_failure_events():
            step_key = event.step_key
            if step_key:
                keys.add(AssetKey(step_key.split("__")))
    except Exception:
        pass
    if context.dagster_run.asset_selection:
        keys.update(context.dagster_run.asset_selection)
    return keys


def email_forecast_failure(context: RunFailureSensorContext) -> bool:
    """Email the failure if the run touched one of EMAIL_ON_FAILURE_ASSET_KEYS. Returns True if sent."""
    matched = failed_asset_keys(context) & EMAIL_ON_FAILURE_ASSET_KEYS
    if not matched:
        return False
    run = context.dagster_run
    asset_names = ", ".join(sorted(k.to_user_string() for k in matched))
    url = dagster_run_url(context)
    subject = f"[Dagster] {asset_names} failed"
    body = (
        f"Job {run.job_name} failed.\n"
        f"Asset(s): {asset_names}\n"
        f"Run ID: {run.run_id}\n"
        f"Error: {context.failure_event.message}\n\n"
        f"View in Dagster UI: {url}\n"
    )
    html = (
        f"<p><b>Job {run.job_name} failed.</b></p>"
        f"<p>Asset(s): {asset_names}<br/>Run ID: {run.run_id}</p>"
        f"<pre>{context.failure_event.message}</pre>"
        f'<p><a href="{url}">View in Dagster UI</a></p>'
    )
    return send_email(subject, body, html)


@run_failure_sensor(
    name="email_on_forecast_run_failure",
    description="Email when a run containing the San Diego epidemiology forecast asset fails",
    default_status=DefaultSensorStatus.RUNNING,
)
def email_on_forecast_run_failure(context: RunFailureSensorContext):
    email_forecast_failure(context)

all_assets = load_assets_from_modules([assets_pkg])

all_sensors = [
    slack_on_run_failure,
    email_on_forecast_run_failure,
    assets_pkg.epidemiology_forecasts_sensor,
    assets_pkg.mpox_aggregated_sensor,
    assets_pkg.forecast_freshness_alert_sensor,
    assets_pkg.resilientllm_status_sensor,
]

minio = S3Resource(
    S3_BUCKET=os.environ.get("S3_BUCKET"),
    S3_ADDRESS=os.environ.get("S3_ADDRESS"),
    S3_PORT=os.environ.get("S3_PORT"),
    S3_ACCESS_KEY=EnvVar("S3_ACCESS_KEY"),
    S3_SECRET_KEY=EnvVar("S3_SECRET_KEY"),
)
airtable = AirtableResource(
    AIRTABLE_ACCESS_TOKEN=EnvVar("AIRTABLE_ACCESS_TOKEN"),
    AIRTABLE_BASE_ID=EnvVar("AIRTABLE_BASE_ID"),
)
openai_resource = OpenAIResource(
    api_key=EnvVar("OPENAI_API_KEY"),
    base_url=EnvVar("OPENAI_BASE_URL"),
)
resilientsims_config = ResilientSimsResource(
    RESILIENTSIMS_SERVER_URL=os.environ.get("RESILIENTSIMS_SERVER_URL", "https://sims.resilientservice.mooo.com"),
    RESILIENTSIMS_API_PATH=os.environ.get("RESILIENTSIMS_API_PATH", "/api/v1"),
    RESILIENTSIMS_USERNAME=EnvVar("RESILIENTSIMS_USERNAME"),
    RESILIENTSIMS_PASSWORD=EnvVar("RESILIENTSIMS_PASSWORD"),
    RESILIENTSIMS_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET", "resilientseasonal"),
    RESILIENTSIMS_SIMULATOR_ID=EnvVar.int("RESILIENTSIMS_SIMULATOR_ID"),
)
mpox_resilientsims_config = ResilientSimsResource(
    RESILIENTSIMS_SERVER_URL=os.environ.get("RESILIENTSIMS_SERVER_URL", "https://sims.resilientservice.mooo.com"),
    RESILIENTSIMS_API_PATH=os.environ.get("RESILIENTSIMS_API_PATH", "/api/v1"),
    RESILIENTSIMS_USERNAME=EnvVar("RESILIENTSIMS_USERNAME"),
    RESILIENTSIMS_PASSWORD=EnvVar("RESILIENTSIMS_PASSWORD"),
    RESILIENTSIMS_BUCKET=os.environ.get("MPOX_RESILIENTSIMS_BUCKET", "resilientmpox"),
    RESILIENTSIMS_SIMULATOR_ID=int(os.environ.get("MPOX_RESILIENTSIMS_SIMULATOR_ID", "49")),
)
resilientllm_config = ResilientLLMResource(
    token=EnvVar("RESILIENTLLM_API_TOKEN"),
)

base_resources = {
    "s3": minio,
    "airtable": airtable,
    "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
    "openai": openai_resource,
    "resilientsims": resilientsims_config,
    "mpox_resilientsims": mpox_resilientsims_config,
    "resilientllm": resilientllm_config,
}

resources = {"local": base_resources, "production": base_resources}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=[*all_assets, *all_source_assets],
    resources=resources[deployment_name],
    sensors=all_sensors,
    jobs=[assets_pkg.resilientllm_sd_disease_job],
)
