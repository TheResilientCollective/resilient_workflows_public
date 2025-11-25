from datetime import datetime
from urllib.error import HTTPError

import requests
import pandas as pd
from io import StringIO
import geopandas as gpd
from dagster import (asset,
                     get_dagster_logger,
                     define_asset_job, AssetKey,
                     RunRequest,
                     schedule, TimeWindowPartitionsDefinition)
from ..utils import store_assets

# Define a yearly partition
start_date_closures=datetime(2020,1,1)
yearly_partitions= TimeWindowPartitionsDefinition(start=start_date_closures,fmt='%Y',
cron_schedule = "@yearly"
)

''' note want to repliace 30 days with
&StartTime=2024-01-01 00:00:00&EndTime=2024-12-31 00:00:00&DateRange=Custom
'''
boundary_cms='https://waterdata.ibwc.gov/AQWebportal/Export/BulkExport?DateRange=Days30&TimeZone=-8&Calendar=CALENDARYEAR&Interval=Hourly&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Best%20Available%4011013300&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=128&_=1739415189330'

canal_cms="https://waterdata.ibwc.gov/AQWebportal/Export/BulkExport?DateRange=Days30&TimeZone=-8&Calendar=CALENDARYEAR&Interval=Hourly&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=Discharge.Telemetry-ADS-mgd%4011-TIJUANA-CANAL&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=128&_=1739415331096"

#export_template_url="https://waterdata.ibwc.gov/AQWebportal/Export/BulkExport?&StartTime=${YEAR}-01-01 00:00:00&EndTime=${YEAR}-12-31 00:00:00&DateRange=Custom&TimeZone=-8&Calendar=CALENDARYEAR&Interval=Hourly&Step=1&ExportFormat=csv&TimeAligned=True&RoundData=True&IncludeGradeCodes=False&IncludeApprovalLevels=False&IncludeQualifiers=False&IncludeInterpolationTypes=False&Datasets[0].DatasetName=${dataset}&Datasets[0].Calculation=Aggregate&Datasets[0].UnitId=12"
#export_template_url="https://waterdata.ibwc.gov/AQWebportal/Export/DataSet?DataSet=${dataset}&Calendar=CALENDARYEAR&StartTime=${YEAR}-01-01 00:00:00&EndTime=${YEAR}-12-31 00:00:00&DateRange=Custom&UnitID=128&Conversion=Aggregate&IntervalPoints=Hourly&ApprovalLevels=False&Qualifiers=False&Step=1&ExportFormat=csv&Compressed=true&RoundData=True&GradeCodes=False&InterpolationTypes=False&Timezone=-8&_=1764010032438"
export_template_url="https://waterdata.ibwc.gov/AQWebportal/Export/DataSet?DataSet=${dataset}&Calendar=CALENDARYEAR&StartTime=${YEAR}-01-01 00:00:00&EndTime=${YEAR}-12-31 00:00:00&DateRange=Custom&UnitID=128&Conversion=Aggregate&IntervalPoints=Hourly&ApprovalLevels=False&Qualifiers=False&Step=1&ExportFormat=csv&Compressed=false&RoundData=True&GradeCodes=False&InterpolationTypes=False&Timezone=-8"

datasets={'boundary_cms':'Discharge.Best%20Available%4011013300',
'canal_cms':'Discharge.Telemetry-ADS-mgd%4011-TIJUANA-CANAL'}
variableMeasured="streamflow_cms"
s3_output_path = 'tijuana/streamflow/output'
s3_latest_path = 'tijuana/streamflow'

@asset(group_name="tijuana",key_prefix="streamflow",
       name="boundary_cms", required_resource_keys={"s3", "airtable"},
       metadata={
           "source": boundary_cms
       ,"description":"Tijuana River at Border Streamflow data from the International Water Boundary Commission"
,"variableMeasured":variableMeasured
}
  )
def tj_boundary(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)
    s3_resource = context.resources.s3
    year = datetime.now().year
    dataset_value = datasets['boundary_cms']
    # Build the URL using the template
    url = export_template_url.replace('${YEAR}', str(year) ).replace('${dataset}', dataset_value)
    response = requests.get(url)
    if response.status_code == 200:
        boundary_df = pd.read_csv(StringIO(response.text), skiprows=3, skipfooter=1, header=1, engine='python')
        boundary_df.set_index('Start of Interval (UTC-08:00)', inplace=True)

        filename = f'{s3_output_path}/boundary_cms/'
       # s3_resource.putFile_text(data=boundary_df.to_csv( index=False), path=filename)
        store_assets.store_dataframe_to_s3(boundary_df, filename, 'boundary_cms',s3_resource, metadata=metadata,
                                           enable_latest_path=True, latestdatasetpath=f'{s3_latest_path}/boundary_cms')

        return boundary_df
    else:
        get_dagster_logger().error(
            f"Request failed with status {response.status_code}: {response.text}"
        )
        raise HTTPError(response.status_code, response.reason)

@asset(group_name="tijuana",key_prefix="streamflow",
       name="canal_cms", required_resource_keys={"s3", "airtable"}
       ,metadata={
           "source": canal_cms
       ,"description":"Tijauanda Canal Streamflow data from the International Water Boundary Commission"
,"variableMeasured":variableMeasured
}
  )
def tj_canal(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")

    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)
    s3_resource = context.resources.s3
    year = datetime.now().year
    dataset_value = datasets['canal_cms']
    # Build the URL using the template
    url = export_template_url.replace('${YEAR}', str(year) ).replace('${dataset}', dataset_value)
    response = requests.get(url)
    if response.status_code == 200:
        boundary_df = pd.read_csv(StringIO(response.text), skiprows=3, skipfooter=1, header=1, engine='python')
        boundary_df.set_index('Start of Interval (UTC-08:00)', inplace=True)

        filename = f'{s3_output_path}/canal_cms/'
       # s3_resource.putFile_text(data=boundary_df.to_csv(index=False), path=filename)
        store_assets.store_dataframe_to_s3(boundary_df, filename,'canal_cms', s3_resource, metadata=metadata,
                                           latestdatasetpath=f'{s3_latest_path}/canal_cms',enable_latest_path=True)
        return boundary_df
    else:
        get_dagster_logger().error(
            f"Request failed with status {response.status_code}: {response.text}"
        )
        raise HTTPError(response.status_code, response.reason)


# Asset factory for yearly streamflow datasets
def create_streamflow_yearly_asset(dataset_name: str, dataset_value: str):
    """
    Asset factory to create yearly partitioned streamflow assets.

    Args:
        dataset_name: Name of the dataset (e.g., 'border_cms_yearly')
        dataset_value: Dataset value for the API call (e.g., 'Discharge.Best%20Available%4011013300')
    """

    @asset(
        name=f'{dataset_name}_yearly',
        group_name="tijuana",
        key_prefix="streamflow",
        partitions_def=yearly_partitions,
        required_resource_keys={"s3", "airtable"},
        metadata={
            "description": f"Yearly {dataset_name} streamflow data from IBWC",
            "variableMeasured": variableMeasured,
            "source": "International Water Boundary Commission"
        }
    )
    def streamflow_yearly_asset(context):
        """Retrieve yearly streamflow data for a specific dataset"""
        logger = get_dagster_logger()

        # Get the partition (year) from context
        partition_key = context.partition_key
        year = partition_key

        # Build the URL using the template
        url = export_template_url.replace('${YEAR}', year).replace('${dataset}', dataset_value)

        logger.info(f"Fetching {dataset_name} data for year {year} from: {url}")

        # Get metadata
        meta = context.assets_def.metadata_by_key[context.asset_key]
        description = meta["description"]
        source_url = meta.get("source")
        variable_measured = meta.get("variableMeasured")

        metadata = store_assets.objectMetadata(
            name=f"{dataset_name}_{year}",
            description=f"{description} - Year {year}",
            source_url=source_url,
            variableMeasured=variable_measured
        )

        s3_resource = context.resources.s3

        try:
            # Make the API request
            response = requests.get(url, timeout=60)

            if response.status_code == 200:
                # Parse the CSV data
                df = pd.read_csv(StringIO(response.text), skiprows=3, skipfooter=1, header=1, engine='python')

                if not df.empty:
                    # Set the time index
                    df.set_index('Start of Interval (UTC-08:00)', inplace=True)

                    # Store as raw CSV file
                    raw_filename = f'tijuana/streamflow/raw/{dataset_name}/{year}'
                    s3_resource.putFile_text(data=response.text, path=f"{raw_filename}.csv")

                    # Store as processed CSV file using store_assets
                    dataset_id= f"{dataset_name}_{year}"
                    output_path = f'{s3_output_path}/{dataset_name}/'
                    latest_path=f'{s3_latest_path}/{dataset_name}'
                    store_assets.store_dataframe_to_s3(df, output_path,dataset_id, s3_resource, metadata=metadata
                                                       ,enable_latest_path=True, latestdatasetpath=latest_path, formats=['csv','parquet'])

                    logger.info(f"Successfully processed {len(df)} records for {dataset_name} year {year}")
                    return df
                else:
                    logger.warning(f"No data returned for {dataset_name} year {year}")
                    return pd.DataFrame()

            else:
                logger.error(f"Request failed with status {response.status_code}: {response.text}")
                raise HTTPError(url, response.status_code, response.reason, None, None)

        except requests.RequestException as e:
            logger.error(f"Request failed for {dataset_name} year {year}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing {dataset_name} year {year}: {str(e)}")
            raise
    return streamflow_yearly_asset

    # return dg.Definitions(
    #     assets=[streamflow_yearly_asset],
    # )



# Create yearly assets for each dataset
#def yearly_assets(datasets):
def yearly_assets():
    yearly_assets = []
    for dataset_name, dataset_value in datasets.items():
        get_dagster_logger().info(f'yearly asset {dataset_name}, {dataset_value}  ')
        yearly_asset = create_streamflow_yearly_asset(dataset_name, dataset_value)
        yearly_assets.append(yearly_asset)
        # Add the asset to the global scope so Dagster can discover it
       # globals()[f"{dataset_name}_asset"] = yearly_asset
    return yearly_assets


# @dg.definitions
# def yearly_defs():
#     get_dagster_logger().info('in yearly defs')
#     return dg.Definitions.merge(*[yearly_assets(datasets)])

# Job for current/recent streamflow data (existing assets)
streamflow_all_job = define_asset_job(
    "streamflow_tj_all", selection=[AssetKey(["streamflow", "canal_cms"]), AssetKey(["streamflow", "boundary_cms"])]
)

# Job for yearly streamflow datasets
yearly_asset_keys = [AssetKey(["streamflow", dataset_name]) for dataset_name in datasets.keys()]
streamflow_yearly_job = define_asset_job(
    "streamflow_yearly_all",
    selection=yearly_asset_keys,
    partitions_def=yearly_partitions
)

# Schedule for current data (hourly)
@schedule(job=streamflow_all_job, cron_schedule="@hourly", name="streamflow_all")
def streamflow_all_schedule(context):
    return RunRequest()

# Schedule for yearly data (monthly on the 1st at 2 AM)
@schedule(job=streamflow_yearly_job, cron_schedule="@yearly", name="streamflow_yearly")
def streamflow_yearly_schedule(context):
    """Schedule to materialize yearly streamflow data monthly"""
    from datetime import datetime

    current_year = datetime.now().year
    # Materialize current year and previous year (in case of late data updates)
    #partitions_to_run = [str(current_year-1), str(current_year - 2)]
    partitions_to_run = yearly_partitions.get_partition_keys()

    return [
        RunRequest(
            run_key=f"streamflow_yearly_{partition_key}",
            partition_key=partition_key
        )
        for partition_key in partitions_to_run
    ]
