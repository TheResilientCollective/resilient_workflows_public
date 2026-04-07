import os

from dagster import Definitions, EnvVar, RunFailureSensorContext, load_assets_from_modules
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor
from dagster_openai import OpenAIResource

from resilient_core.resources.minio import S3Resource
from resilient_core.resources.airtable import AirtableResource

from . import assets as assets_pkg
from .resources.resilientsims import ResilientSimsResource
from .resources.resilientllm import ResilientLLMResource
from .source_assets import all_source_assets


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

all_sensors = [
    slack_on_run_failure,
    assets_pkg.epidemiology_forecasts_sensor,
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
resilientllm_config = ResilientLLMResource(
    token=EnvVar("RESILIENTLLM_API_TOKEN"),
)

base_resources = {
    "s3": minio,
    "airtable": airtable,
    "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
    "openai": openai_resource,
    "resilientsims": resilientsims_config,
    "resilientllm": resilientllm_config,
}

resources = {"local": base_resources, "production": base_resources}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=[*all_assets, *all_source_assets],
    resources=resources[deployment_name],
    sensors=all_sensors,
)
