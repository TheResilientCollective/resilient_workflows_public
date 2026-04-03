import os
from dagster import Definitions, load_assets_from_modules, EnvVar, RunFailureSensorContext
from dagster_slack import SlackResource, make_slack_on_run_failure_sensor
from dagster_openai import OpenAIResource
from . import assets
from .resources.minio import S3Resource
from .resources.airtable import AirtableResource
from .resources.resilientsims import ResilientSimsResource,ResourceWithResilientSimsConfiguration
from .resources.resilientllm import ResilientLLMResource

def slack_message_fn(context: RunFailureSensorContext) -> str:
    return (
        f"Job *[{context.dagster_run.job_name}]* failed! "
        f"Error: {context.failure_event.message}"
    )
slack_on_run_failure = make_slack_on_run_failure_sensor(
     os.environ.get("SLACK_CHANNEL_FAILURES","workflows-failures"),
    os.getenv("SLACK_TOKEN"),
    webserver_base_url=f'https://{os.environ.get("SCHED_HOSTNAME", "sched")}.{os.environ.get("HOST", "local")}/',
    text_fn=slack_message_fn
)

all_assets = load_assets_from_modules([assets])
asset_checks=[assets.sd_complaints_freshness_check
    , assets.current_freshness_check
    , assets.sde_timeseries_checks
    , assets.mpox_data_checks
              ]
all_schedules = [assets.beach_waterquality_schedule,
              #   assets.complaints_daily_schedule, # now a sensor
                 assets.apcd_current_schedule, assets.apcd_all_schedule,
                 assets.streamflow_all_schedule, assets.weather_all_schedule,
                 assets.cdc_nndss_weekly_schedule, assets.beachwatch_closure_schedule,
                    assets.purple_air_schedule,
                 assets.spills_historic_schedule,
                 assets.cdc_nnds.cdc_nndss_raw_schedule,
                 assets.mpox_counties_weekly_schedule,
                 assets.streamflow_yearly_schedule,
                 assets.tides_monthly_schedule,assets.tides_hourly_schedule,
                 assets.effluent_flow_current_schedule,
                 assets.effluent_flow_yearly_schedule,
                # assets.sandiego_epidemiology_schedule # now a sensor
                 ]
all_sensors=[slack_on_run_failure,
             assets.complaints_data_sensor,
             assets.beachinfo_updated_sensor,
             assets.spills_latest_sensor,
             assets.sandiego_epidemiology_sensor,
             assets.epidemiology_forecasts_sensor,
             assets.mpox_epidemiology_sensor,
             assets.wahis_upload_sensor,
             assets.scripps_pfm_sensor,
             ]
all_jobs=[
    #assets.complaints_daily_job
]
minio=S3Resource(

    S3_BUCKET=os.environ.get('S3_BUCKET'),
    S3_ADDRESS=os.environ.get('S3_ADDRESS'),
    S3_PORT=os.environ.get('S3_PORT'),
    S3_ACCESS_KEY=EnvVar('S3_ACCESS_KEY'), # not shown in UI
    S3_SECRET_KEY=EnvVar('S3_SECRET_KEY'),# not shown in UI


)
airtable=AirtableResource(
    AIRTABLE_ACCESS_TOKEN=EnvVar('AIRTABLE_ACCESS_TOKEN') , # not shown in UI
    AIRTABLE_BASE_ID=EnvVar('AIRTABLE_BASE_ID')
)
openai=OpenAIResource(
   api_key=EnvVar("OPENAI_API_KEY"),
   base_url=EnvVar("OPENAI_BASE_URL")
)
resilentsims_config=ResilientSimsResource(
    RESILIENTSIMS_SERVER_URL=os.environ.get("RESILIENTSIMS_SERVER_URL", "https://sims.resilientservice.mooo.com"),
    RESILIENTSIMS_API_PATH=os.environ.get("RESILIENTSIMS_API_PATH", "/api/v1"),
    RESILIENTSIMS_USERNAME=EnvVar("RESILIENTSIMS_USERNAME"),
    RESILIENTSIMS_PASSWORD=EnvVar("RESILIENTSIMS_PASSWORD"),
    RESILIENTSIMS_BUCKET=os.environ.get("RESILIENTSIMS_BUCKET","resilientseasonal"),
    RESILIENTSIMS_SIMULATOR_ID=EnvVar.int("RESILIENTSIMS_SIMULATOR_ID"),
)
resilientllm_config = ResilientLLMResource(
    token=EnvVar("RESILIENTLLM_API_TOKEN"),

)
# SLACK docker env has prefix
resources ={
    "local": {
        "s3":minio,
        "airtable": airtable,
        "openai":openai,
        "slack": SlackResource(token=EnvVar("SLACK_TOKEN")),
       "resilientsims": resilentsims_config,
        "resilientllm": resilientllm_config,
    },
    "production": {
        "s3":minio,
        "airtable": airtable,
        "openai": openai,
        "slack":SlackResource(token=EnvVar("SLACK_TOKEN")),
       "resilientsims":resilentsims_config,
       "resilientllm": resilientllm_config,
    },
}
deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "local")
all_assets = all_assets + assets.yearly_assets()
defs = Definitions(
    assets=all_assets,
    asset_checks=asset_checks,
    resources=resources[deployment_name],
    schedules=all_schedules,
    sensors=all_sensors,
    jobs=all_jobs,
)

