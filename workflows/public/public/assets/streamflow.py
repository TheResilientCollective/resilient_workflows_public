from urllib.error import HTTPError

import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from dagster import ( asset,
                     get_dagster_logger,
                      define_asset_job,AssetKey,
                      RunRequest,
                      schedule)

from ..resources import minio

boundary_cms='https://waterdata.ibwc.gov/AQWebportal/Export/BulkExport?DateRange=Days30&TimeZone=-8&Calendar=CALENDARYEAR&Interval=Hourly&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Best%20Available%4011013300&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=128&_=1739415189330'

canal_cms="https://waterdata.ibwc.gov/AQWebportal/Export/BulkExport?DateRange=Days30&TimeZone=-8&Calendar=CALENDARYEAR&Interval=Hourly&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Telemetry-ADS-mgd%4011-TIJUANA-CANAL&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=128&_=1739415331096"

s3_output_path = 'tijuana/streamflow/output'

@asset(group_name="tijuana",key_prefix="streamflow",
       name="boundary_cms", required_resource_keys={"s3", "airtable"}
  )
def tj_boundary(context):
    s3_resource = context.resources.s3
    response = requests.get(boundary_cms)
    if response.status_code == 200:
        boundary_df = pd.read_csv(StringIO(response.text), skiprows=3, skipfooter=1, header=1, engine='python')
        boundary_df.set_index('Start of Interval (UTC-08:00)', inplace=True)

        filename = f'{s3_output_path}/boundary_cms.csv'
        s3_resource.putFile_text(data=boundary_df.to_csv( index=False), path=filename)


        return boundary_df
    else:
        get_dagster_logger().error(
            f"Request failed with status {response.status_code}: {response.text}"
        )
        raise HTTPError(response.status_code, response.reason)

@asset(group_name="tijuana",key_prefix="streamflow",
       name="canal_cms", required_resource_keys={"s3", "airtable"}
  )
def tj_canal(context):
    s3_resource = context.resources.s3
    response = requests.get(canal_cms)
    if response.status_code == 200:
        boundary_df = pd.read_csv(StringIO(response.text), skiprows=3, skipfooter=1, header=1, engine='python')
        boundary_df.set_index('Start of Interval (UTC-08:00)', inplace=True)

        filename = f'{s3_output_path}/canal_cms.csv'
        s3_resource.putFile_text(data=boundary_df.to_csv(index=False), path=filename)

        return boundary_df
    else:
        get_dagster_logger().error(
            f"Request failed with status {response.status_code}: {response.text}"
        )
        raise HTTPError(response.status_code, response.reason)

streamflow_all_job = define_asset_job(
    "streamflow_tj_all", selection=[AssetKey(["streamflow", "canal_cms"]), AssetKey(["streamflow", "boundary_cms"])]
)

# daily but at 3 am
@schedule(job=streamflow_all_job, cron_schedule="@hourly", name="streamflow_all")
def streamflow_all_schedule(context):

    return RunRequest(
    )
