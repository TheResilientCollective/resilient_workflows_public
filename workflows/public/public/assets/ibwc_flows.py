from datetime import datetime

import requests
import pandas as pd
from io import StringIO

from dagster import (
    asset,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    RunRequest,
    schedule,
    TimeWindowPartitionsDefinition,
    AutomationCondition
)

from ..utils import store_assets

# Dataset identifier for effluent flow at SBIWTP (South Bay International Wastewater Treatment Plant)
EFFLUENT_DATASET = 'Flow.Plant-Effluent-Flow-MGD%40SBIWTP'

# URL for today's data
EFFLUENT_TODAY_URL = (
    'https://waterdata.ibwc.gov/AQWebportal/Export/DataSet'
    '?DataSet=Flow.Plant-Effluent-Flow-MGD%40SBIWTP'
    '&Calendar=CALENDARYEAR&DateRange=Today&UnitID=111'
    '&Conversion=Instantaneous&IntervalPoints=PointsAsRecorded'
    '&ApprovalLevels=False&Qualifiers=True&Step=1&ExportFormat=csv'
    '&Compressed=false&RoundData=True&GradeCodes=True'
    '&InterpolationTypes=False&Timezone=-8'
)

# URL template for a specific calendar year
EFFLUENT_YEAR_TEMPLATE = (
    'https://waterdata.ibwc.gov/AQWebportal/Export/DataSet'
    '?DataSet=Flow.Plant-Effluent-Flow-MGD%40SBIWTP'
    '&Calendar=CALENDARYEAR'
    '&StartTime=${YEAR}-01-01 00:00:00&EndTime=${YEAR}-12-31 00:00:00'
    '&DateRange=Custom&UnitID=111&Conversion=Instantaneous'
    '&IntervalPoints=PointsAsRecorded&ApprovalLevels=False&Qualifiers=True'
    '&Step=1&ExportFormat=csv&Compressed=false&RoundData=True'
    '&GradeCodes=True&InterpolationTypes=False&Timezone=-8'
)

s3_output_path = 'tijuana/effluent_flow/output'
s3_raw_path = 'tijuana/effluent_flow/raw'
s3_latest_path = 'tijuana/effluent_flow'

variableMeasured = 'effluent_flow_mgd'

start_date_effluent = datetime(2020, 1, 1)
yearly_partitions = TimeWindowPartitionsDefinition(
    start=start_date_effluent,
    fmt='%Y',
    cron_schedule='@yearly'
)


def parse_effluent_csv(text: str) -> pd.DataFrame:
    """Parse IBWC effluent flow CSV export into a DataFrame.

    The portal exports a CSV with 3 header rows; skiprows=3 skips them,
    header=1 uses the second remaining row as column names, and skipfooter=1
    drops the trailing summary row.
    """
    df = pd.read_csv(StringIO(text), skiprows=3, skipfooter=1, header=1, engine='python')
    # Find timestamp column — PointsAsRecorded exports use 'Timestamp (UTC-08:00)'
    time_col = next(
        (c for c in df.columns if 'timestamp' in c.lower() or 'time' in c.lower()),
        None
    )
    if time_col:
        df.set_index(time_col, inplace=True)
    return df


@asset(
    group_name="tijuana",
    key_prefix="effluent_flow",
    name="effluent_flow_today",
    required_resource_keys={"s3", "airtable"},
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": EFFLUENT_TODAY_URL,
        "description": (
            "Today's effluent flow data (MGD) from the South Bay International "
            "Wastewater Treatment Plant (SBIWTP) via the IBWC water data portal."
        ),
        "variableMeasured": variableMeasured,
    }
)
def effluent_flow_today(context):
    """Fetch today's effluent flow readings from SBIWTP."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variable_measured = meta.get("variableMeasured")

    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variable_measured
    )
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    response = requests.get(EFFLUENT_TODAY_URL, timeout=60)
    response.raise_for_status()

    df = parse_effluent_csv(response.text)
    if df.empty:
        logger.warning("No effluent flow data returned for today")
        return df

    logger.info(f"Fetched {len(df)} effluent flow records for today")

    filename = f'{s3_output_path}/effluent_flow_today/'
    store_assets.store_dataframe_to_s3(
        df, filename, 'effluent_flow_today', s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=f'{s3_latest_path}/today',
        formats=['csv']
    )
    return df


@asset(
    group_name="tijuana",
    key_prefix="effluent_flow",
    name="effluent_flow_current_year",
    required_resource_keys={"s3", "airtable"},
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": "IBWC AQWebportal — SBIWTP effluent flow current year",
        "description": (
            "Current calendar year effluent flow data (MGD) from the South Bay "
            "International Wastewater Treatment Plant (SBIWTP) via IBWC."
        ),
        "variableMeasured": variableMeasured,
    }
)
def effluent_flow_current_year(context):
    """Fetch the current calendar year's effluent flow data from SBIWTP."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variable_measured = meta.get("variableMeasured")

    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variable_measured
    )
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    year = datetime.now().year
    url = EFFLUENT_YEAR_TEMPLATE.replace('${YEAR}', str(year))
    logger.info(f"Fetching current year ({year}) effluent flow from: {url}")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    df = parse_effluent_csv(response.text)
    if df.empty:
        logger.warning(f"No effluent flow data returned for {year}")
        return df

    logger.info(f"Fetched {len(df)} effluent flow records for {year}")

    # Store raw response
    raw_path = f'{s3_raw_path}/{year}.csv'
    s3_resource.putFile_text(data=response.text, path=raw_path)

    filename = f'{s3_output_path}/effluent_flow_current_year/'
    store_assets.store_dataframe_to_s3(
        df, filename, f'effluent_flow_{year}', s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=f'{s3_latest_path}/yearly',
        formats=['csv', 'parquet']
    )
    return df


def create_effluent_flow_yearly_asset():
    """Asset factory for yearly partitioned effluent flow data."""

    @asset(
        name='effluent_flow_yearly',
        group_name="tijuana",
        key_prefix="effluent_flow",
        partitions_def=yearly_partitions,
        required_resource_keys={"s3", "airtable"},
        metadata={
            "description": "Yearly effluent flow data (MGD) from SBIWTP via IBWC",
            "variableMeasured": variableMeasured,
            "source": "International Water Boundary Commission — SBIWTP"
        }
    )
    def effluent_flow_yearly_asset(context):
        """Retrieve yearly partitioned effluent flow data from SBIWTP."""
        logger = get_dagster_logger()

        partition_key = context.partition_key
        year = partition_key

        url = EFFLUENT_YEAR_TEMPLATE.replace('${YEAR}', year)
        logger.info(f"Fetching effluent flow for year {year} from: {url}")

        meta = context.assets_def.metadata_by_key[context.asset_key]
        description = meta["description"]
        source_url = meta.get("source")
        variable_measured = meta.get("variableMeasured")

        metadata = store_assets.objectMetadata(
            name=f"effluent_flow_{year}",
            description=f"{description} - Year {year}",
            source_url=source_url,
            variableMeasured=variable_measured
        )
        s3_resource = context.resources.s3

        try:
            response = requests.get(url, timeout=60)

            response.raise_for_status()

            df = parse_effluent_csv(response.text)

            if df.empty:
                logger.warning(f"No effluent flow data returned for year {year}")
                return pd.DataFrame()

            # Store raw CSV
            raw_path = f'{s3_raw_path}/{year}.csv'
            s3_resource.putFile_text(data=response.text, path=raw_path)

            # Store processed data
            output_path = f'{s3_output_path}/effluent_flow_yearly/'
            latest_path = f'{s3_latest_path}/yearly'
            store_assets.store_dataframe_to_s3(
                df, output_path, f'effluent_flow_{year}', s3_resource,
                metadata=metadata,
                enable_latest_path=True,
                latestdatasetpath=latest_path,
                formats=['csv', 'parquet']
            )

            logger.info(f"Successfully stored {len(df)} effluent flow records for {year}")
            return df

        except requests.RequestException as e:
            logger.error(f"Request failed for effluent flow year {year}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing effluent flow for year {year}: {str(e)}")
            raise

    return effluent_flow_yearly_asset


effluent_flow_yearly = create_effluent_flow_yearly_asset()

# Job for current/today assets
effluent_flow_current_job = define_asset_job(
    "effluent_flow_current",
    selection=[
        AssetKey(["effluent_flow", "effluent_flow_today"]),
        AssetKey(["effluent_flow", "effluent_flow_current_year"]),
    ]
)

# Job for yearly partitioned data
effluent_flow_yearly_job = define_asset_job(
    "effluent_flow_yearly_all",
    selection=[AssetKey(["effluent_flow", "effluent_flow_yearly"])],
    partitions_def=yearly_partitions
)

# Hourly schedule for today's data
@schedule(job=effluent_flow_current_job, cron_schedule="@hourly", name="effluent_flow_current")
def effluent_flow_current_schedule(context):
    return RunRequest()

# Yearly schedule for historical partitioned data
@schedule(job=effluent_flow_yearly_job, cron_schedule="@yearly", name="effluent_flow_yearly")
def effluent_flow_yearly_schedule(context):
    partition_keys = yearly_partitions.get_partition_keys()
    return [
        RunRequest(
            run_key=f"effluent_flow_yearly_{pk}",
            partition_key=pk
        )
        for pk in partition_keys
    ]
