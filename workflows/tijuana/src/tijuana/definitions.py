import os
from dagster import Definitions, load_assets_from_modules, EnvVar, RunFailureSensorContext
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor
from dagster_openai import OpenAIResource
from . import assets as assets_pkg
from resilient_core.resources.minio import S3Resource
from resilient_core.resources.airtable import AirtableResource


def slack_message_fn(context: RunFailureSensorContext) -> str:
    return (
        f"Job *[{context.dagster_run.job_name}]* failed! "
        f"Error: {context.failure_event.message}"
    )


slack_on_run_failure = make_slack_on_run_failure_sensor(
    os.environ.get("SLACK_CHANNEL_FAILURES", "workflows-failures"),
    os.getenv("SLACK_TOKEN"),
    webserver_base_url=f'https://{os.environ.get("SCHED_HOSTNAME", "sched")}.{os.environ.get("HOST", "local")}/',
    text_fn=slack_message_fn,
)

all_assets = load_assets_from_modules([assets_pkg])
all_assets = all_assets + assets_pkg.yearly_assets()

asset_checks = [
    assets_pkg.sd_complaints_freshness_check,
    assets_pkg.current_freshness_check,
]

all_schedules = [
    assets_pkg.beach_waterquality_schedule,
    assets_pkg.apcd_current_schedule,
    assets_pkg.apcd_all_schedule,
    assets_pkg.streamflow_all_schedule,
    assets_pkg.streamflow_yearly_schedule,
    assets_pkg.weather_all_schedule,
    assets_pkg.beachwatch_closure_schedule,
    assets_pkg.purple_air_schedule,
    assets_pkg.spills_historic_schedule,
    assets_pkg.tides_monthly_schedule,
    assets_pkg.tides_hourly_schedule,
    assets_pkg.effluent_flow_current_schedule,
    assets_pkg.effluent_flow_yearly_schedule,
    assets_pkg.apcd_yearly_schedule,
    assets_pkg.wu_weather_daily_schedule,
]

all_sensors = [
    slack_on_run_failure,
    assets_pkg.complaints_data_sensor,
    assets_pkg.beachinfo_updated_sensor,
    assets_pkg.spills_latest_sensor,
    assets_pkg.scripps_pfm_sensor,
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

resources = {
    "local": {
        "s3": minio,
        "airtable": airtable,
        "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
        "openai": OpenAIResource(api_key=EnvVar("OPENAI_API_KEY"), base_url=EnvVar("OPENAI_BASE_URL")),
    },
    "production": {
        "s3": minio,
        "airtable": airtable,
        "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
        "openai": OpenAIResource(api_key=EnvVar("OPENAI_API_KEY"), base_url=EnvVar("OPENAI_BASE_URL")),
    },
}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=all_assets,
    asset_checks=asset_checks,
    resources=resources[deployment_name],
    schedules=all_schedules,
    sensors=all_sensors,
)
