import os
from dagster import Definitions, load_assets_from_modules, EnvVar, RunFailureSensorContext
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor
from dagster_openai import OpenAIResource
from resilient_core.resources.minio import S3Resource
from resilient_core.resources.airtable import AirtableResource
from . import assets as assets_pkg
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

asset_checks = [
    assets_pkg.check_mpox_weekly_completeness,
    assets_pkg.check_measles_weekly_completeness,
    assets_pkg.check_mpox_statistical_extension_completeness,
    assets_pkg.check_measles_statistical_extension_completeness,
    assets_pkg.check_mpox_zero_counts_preserved,
    assets_pkg.check_measles_zero_counts_preserved,
    assets_pkg.check_nndss_label_mapping_coverage,
    assets_pkg.check_nndss_all_basis_no_negative_counts,
]

minio = S3Resource(
    S3_BUCKET=os.environ.get('S3_BUCKET'),
    S3_ADDRESS=os.environ.get('S3_ADDRESS'),
    S3_PORT=os.environ.get('S3_PORT'),
    S3_ACCESS_KEY=EnvVar('S3_ACCESS_KEY'),
    S3_SECRET_KEY=EnvVar('S3_SECRET_KEY'),
)
airtable = AirtableResource(
    AIRTABLE_ACCESS_TOKEN=EnvVar('AIRTABLE_ACCESS_TOKEN'),
    AIRTABLE_BASE_ID=EnvVar('AIRTABLE_BASE_ID'),
)
openai_resource = OpenAIResource(
    api_key=EnvVar("OPENAI_API_KEY"),
    base_url=EnvVar("OPENAI_BASE_URL"),
)

base_resources = {
    "s3": minio,
    "airtable": airtable,
    "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
    "openai": openai_resource,
}
resources = {"local": base_resources, "production": base_resources}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")

defs = Definitions(
    assets=[*all_assets, *all_source_assets],
    asset_checks=asset_checks,
    resources=resources[deployment_name],
    jobs=[
        assets_pkg.mpox_aggregated_job,
        assets_pkg.nndss_all_job,
        assets_pkg.nws_weekly_status_job,
        assets_pkg.nws_dashboard_job,
    ],
    schedules=[
        assets_pkg.cdc_nndss_weekly_schedule,
        assets_pkg.cdc_nnds.cdc_nndss_raw_schedule,
        assets_pkg.mpox_counties_weekly_schedule,
        assets_pkg.nndss_all_schedule,
        assets_pkg.nws_weekly_status_schedule,
        assets_pkg.nws_dashboard_schedule,
    ],
    sensors=[slack_on_run_failure, assets_pkg.wahis_upload_sensor, assets_pkg.mpox_upstream_sensor],
)
