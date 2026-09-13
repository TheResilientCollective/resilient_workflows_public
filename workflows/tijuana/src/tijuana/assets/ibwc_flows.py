"""SBIWTP plant flows from the IBWC AQWebportal.

The South Bay International Wastewater Treatment Plant publishes two daily flow
series: what enters the plant (influent) and what it discharges after treatment
(effluent). Both come from the same portal in the same export shape, so one
factory builds the same trio of assets for each:

    ibwc/{kind}_flow_today          — today's readings, refreshed hourly
    ibwc/{kind}_flow_current_year   — the current calendar year, refreshed hourly
    ibwc/{kind}_flow_yearly         — one partition per calendar year since 2020

plus a freshness check on the "today" asset and the jobs and schedules that
drive them. The effluent assets predate the factory; their keys, S3 paths, job
and schedule names are unchanged.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd
from io import StringIO

from dagster import (
    asset,
    asset_check,
    AssetCheckResult,
    AssetCheckExecutionContext,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    RunRequest,
    schedule,
    TimeWindowPartitionsDefinition,
    AutomationCondition
)

from resilient_core.utils import store_assets

_EXPORT_BASE = 'https://waterdata.ibwc.gov/AQWebportal/Export/DataSet'
_EXPORT_OPTIONS = (
    '&UnitID=111'
    '&Conversion=Instantaneous&IntervalPoints=PointsAsRecorded'
    '&ApprovalLevels=False&Qualifiers=True&Step=1&ExportFormat=csv'
    '&Compressed=false&RoundData=True&GradeCodes=True'
    '&InterpolationTypes=False&Timezone=-8'
)

variableMeasured = 'effluent_flow_mgd'

start_date_effluent = datetime(2020, 1, 1)
yearly_partitions = TimeWindowPartitionsDefinition(
    start=start_date_effluent,
    fmt='%Y',
    cron_schedule='@yearly'
)


@dataclass(frozen=True)
class PlantFlowDataset:
    """One SBIWTP flow series on the IBWC portal and where its assets live."""

    kind: str          # 'effluent' or 'influent'; prefixes asset names and S3 paths
    dataset: str       # IBWC dataset identifier, URL-encoded
    label: str         # how the series is described in metadata

    @property
    def today_url(self) -> str:
        return (
            f'{_EXPORT_BASE}?DataSet={self.dataset}'
            '&Calendar=CALENDARYEAR&DateRange=Today'
            f'{_EXPORT_OPTIONS}'
        )

    def year_url(self, year) -> str:
        return (
            f'{_EXPORT_BASE}?DataSet={self.dataset}'
            '&Calendar=CALENDARYEAR'
            f'&StartTime={year}-01-01 00:00:00&EndTime={year}-12-31 00:00:00'
            '&DateRange=Custom'
            f'{_EXPORT_OPTIONS}'
        )

    @property
    def s3_output_path(self) -> str:
        return f'tijuana/{self.kind}_flow/output'

    @property
    def s3_raw_path(self) -> str:
        return f'tijuana/{self.kind}_flow/raw'

    @property
    def s3_latest_path(self) -> str:
        return f'tijuana/{self.kind}_flow'

    @property
    def variable_measured(self) -> str:
        return f'{self.kind}_flow_mgd'


EFFLUENT = PlantFlowDataset(
    kind='effluent',
    dataset='Flow.Plant-Effluent-Flow-MGD%40SBIWTP',
    label='effluent flow',
)
INFLUENT = PlantFlowDataset(
    kind='influent',
    dataset='Flow.Plant-Influent-Flow-MGD%40SBIWTP',
    label='influent flow',
)

# Kept for callers that predate the factory.
EFFLUENT_DATASET = EFFLUENT.dataset
EFFLUENT_TODAY_URL = EFFLUENT.today_url
EFFLUENT_YEAR_TEMPLATE = EFFLUENT.year_url('${YEAR}')
s3_output_path = EFFLUENT.s3_output_path
s3_raw_path = EFFLUENT.s3_raw_path
s3_latest_path = EFFLUENT.s3_latest_path


def parse_flow_csv(text: str) -> pd.DataFrame:
    """Parse an IBWC plant-flow CSV export into a DataFrame.

    The portal exports a CSV with 3 header rows; skiprows=3 skips them,
    header=1 uses the second remaining row as column names, and skipfooter=1
    drops the trailing disclaimer row.

    Returns the DataFrame with the original timestamp column name (usually
    'Timestamp (UTC-08:00)') kept as a regular column, not an index, so the
    schema is consistent across every parquet file written.
    """
    return pd.read_csv(StringIO(text), skiprows=3, skipfooter=1, header=1, engine='python')


# Original name, kept for existing imports.
parse_effluent_csv = parse_flow_csv


def build_plant_flow_assets(spec: PlantFlowDataset):
    """Build the today / current-year / yearly assets and their plumbing for one series.

    Returns a dict keyed by role so callers can bind module-level names that
    Dagster's module loader and `definitions.py` can find.
    """
    kind = spec.kind
    key_today = AssetKey(["ibwc", f"{kind}_flow_today"])
    key_current_year = AssetKey(["ibwc", f"{kind}_flow_current_year"])
    key_yearly = AssetKey(["ibwc", f"{kind}_flow_yearly"])

    def _metadata(context, name, description):
        meta = context.assets_def.metadata_by_key[context.asset_key]
        return store_assets.objectMetadata(
            name=name,
            description=description,
            source_url=meta.get("source"),
            variableMeasured=meta.get("variableMeasured"),
        )

    @asset(
        group_name="tijuana",
        key_prefix="ibwc",
        name=f"{kind}_flow_today",
        required_resource_keys={"s3", "airtable"},
        automation_condition=AutomationCondition.eager(),
        metadata={
            "source": spec.today_url,
            "description": (
                f"Today's {spec.label} data (MGD) from the South Bay International "
                "Wastewater Treatment Plant (SBIWTP) via the IBWC water data portal."
            ),
            "variableMeasured": spec.variable_measured,
        }
    )
    def flow_today(context):
        """Fetch today's readings for this series from SBIWTP."""
        meta = context.assets_def.metadata_by_key[context.asset_key]
        metadata = _metadata(context, str(context.asset_key.path[-1]), meta["description"])
        s3_resource = context.resources.s3
        logger = get_dagster_logger()

        response = requests.get(spec.today_url, timeout=60)
        response.raise_for_status()

        df = parse_flow_csv(response.text)
        if df.empty:
            logger.warning(f"No {spec.label} data returned for today")
            return df

        logger.info(f"Fetched {len(df)} {spec.label} records for today")

        store_assets.store_dataframe_to_s3(
            df, f'{spec.s3_output_path}/{kind}_flow_today/', f'{kind}_flow_today', s3_resource,
            metadata=metadata,
            enable_latest_path=True,
            latestdatasetpath=f'{spec.s3_latest_path}/today',
            formats=['csv']
        )
        return df

    @asset_check(asset=key_today, name=f"{kind}_flow_freshness_check")
    def flow_freshness_check(context: AssetCheckExecutionContext, flow_today):
        """Checks that the most recent reading is no older than six hours.

        The IBWC export timestamps are labelled 'Timestamp (UTC-08:00)' — a fixed
        UTC-8 offset (no daylight saving), so freshness is compared in that offset.
        """
        if flow_today.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "Asset is empty, cannot determine freshness."},
            )

        timestamp_cols = [c for c in flow_today.columns if str(c).startswith("Timestamp")]
        if not timestamp_cols:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": f"No timestamp column found; columns are {list(flow_today.columns)}"},
            )

        fixed_offset = timezone(timedelta(hours=-8))
        timestamps = pd.to_datetime(flow_today[timestamp_cols[0]], errors="coerce")
        timestamps = timestamps.dropna()
        if timestamps.empty:
            return AssetCheckResult(
                passed=False,
                metadata={"reason": "No parseable timestamps, cannot determine freshness."},
            )

        most_recent_datetime = timestamps.max()
        if most_recent_datetime.tzinfo is None:
            most_recent_datetime = most_recent_datetime.tz_localize(fixed_offset)

        current_datetime = datetime.now(tz=fixed_offset)
        time_difference = current_datetime - most_recent_datetime
        passed = time_difference <= timedelta(hours=6)

        metadata = {
            "most_recent_datetime": str(most_recent_datetime),
            "current_datetime": str(current_datetime),
            "time_difference": str(time_difference),
        }
        if not passed:
            metadata["reason"] = f"Most recent {spec.label} reading is older than six hours."
        return AssetCheckResult(passed=passed, metadata=metadata)

    @asset(
        group_name="tijuana",
        key_prefix="ibwc",
        name=f"{kind}_flow_current_year",
        required_resource_keys={"s3", "airtable"},
        automation_condition=AutomationCondition.eager(),
        metadata={
            "source": f"IBWC AQWebportal — SBIWTP {spec.label} current year",
            "description": (
                f"Current calendar year {spec.label} data (MGD) from the South Bay "
                "International Wastewater Treatment Plant (SBIWTP) via IBWC."
            ),
            "variableMeasured": spec.variable_measured,
        }
    )
    def flow_current_year(context):
        """Fetch the current calendar year's readings for this series from SBIWTP."""
        meta = context.assets_def.metadata_by_key[context.asset_key]
        metadata = _metadata(context, str(context.asset_key.path[-1]), meta["description"])
        s3_resource = context.resources.s3
        logger = get_dagster_logger()

        year = datetime.now().year
        url = spec.year_url(year)
        logger.info(f"Fetching current year ({year}) {spec.label} from: {url}")

        response = requests.get(url, timeout=60)
        response.raise_for_status()

        df = parse_flow_csv(response.text)
        if df.empty:
            logger.warning(f"No {spec.label} data returned for {year}")
            return df

        logger.info(f"Fetched {len(df)} {spec.label} records for {year}")

        s3_resource.putFile_text(data=response.text, path=f'{spec.s3_raw_path}/{year}.csv')

        store_assets.store_dataframe_to_s3(
            df, f'{spec.s3_output_path}/{kind}_flow_current_year/', f'{kind}_flow_{year}', s3_resource,
            metadata=metadata,
            enable_latest_path=True,
            latestdatasetpath=f'{spec.s3_latest_path}/yearly',
            formats=['csv', 'parquet']
        )
        return df

    @asset(
        name=f'{kind}_flow_yearly',
        group_name="tijuana",
        key_prefix="ibwc",
        partitions_def=yearly_partitions,
        required_resource_keys={"s3", "airtable"},
        metadata={
            "description": f"Yearly {spec.label} data (MGD) from SBIWTP via IBWC",
            "variableMeasured": spec.variable_measured,
            "source": "International Water Boundary Commission — SBIWTP"
        }
    )
    def flow_yearly(context):
        """Retrieve one calendar-year partition of this series from SBIWTP."""
        logger = get_dagster_logger()
        year = context.partition_key

        url = spec.year_url(year)
        logger.info(f"Fetching {spec.label} for year {year} from: {url}")

        meta = context.assets_def.metadata_by_key[context.asset_key]
        metadata = _metadata(context, f"{kind}_flow_{year}", f"{meta['description']} - Year {year}")
        s3_resource = context.resources.s3

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            df = parse_flow_csv(response.text)
            if df.empty:
                logger.warning(f"No {spec.label} data returned for year {year}")
                return pd.DataFrame()

            s3_resource.putFile_text(data=response.text, path=f'{spec.s3_raw_path}/{year}.csv')

            store_assets.store_dataframe_to_s3(
                df, f'{spec.s3_output_path}/{kind}_flow_yearly/', f'{kind}_flow_{year}', s3_resource,
                metadata=metadata,
                enable_latest_path=True,
                latestdatasetpath=f'{spec.s3_latest_path}/yearly',
                formats=['csv', 'parquet']
            )

            logger.info(f"Successfully stored {len(df)} {spec.label} records for {year}")
            return df

        except requests.RequestException as e:
            logger.error(f"Request failed for {spec.label} year {year}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error processing {spec.label} for year {year}: {str(e)}")
            raise

    current_job = define_asset_job(
        f"{kind}_flow_current",
        selection=[key_today, key_current_year],
    )
    yearly_job = define_asset_job(
        f"{kind}_flow_yearly_all",
        selection=[key_yearly],
    )

    @schedule(job=current_job, cron_schedule="@hourly", name=f"{kind}_flow_current")
    def current_schedule(context):
        return RunRequest()

    @schedule(job=yearly_job, cron_schedule="@yearly", name=f"{kind}_flow_yearly")
    def yearly_schedule(context):
        return [
            RunRequest(run_key=f"{kind}_flow_yearly_{pk}", partition_key=pk)
            for pk in yearly_partitions.get_partition_keys()
        ]

    return {
        "today": flow_today,
        "current_year": flow_current_year,
        "yearly": flow_yearly,
        "freshness_check": flow_freshness_check,
        "current_job": current_job,
        "yearly_job": yearly_job,
        "current_schedule": current_schedule,
        "yearly_schedule": yearly_schedule,
    }


_effluent = build_plant_flow_assets(EFFLUENT)
effluent_flow_today = _effluent["today"]
effluent_flow_current_year = _effluent["current_year"]
effluent_flow_yearly = _effluent["yearly"]
effluent_flow_freshness_check = _effluent["freshness_check"]
effluent_flow_current_job = _effluent["current_job"]
effluent_flow_yearly_job = _effluent["yearly_job"]
effluent_flow_current_schedule = _effluent["current_schedule"]
effluent_flow_yearly_schedule = _effluent["yearly_schedule"]

_influent = build_plant_flow_assets(INFLUENT)
influent_flow_today = _influent["today"]
influent_flow_current_year = _influent["current_year"]
influent_flow_yearly = _influent["yearly"]
influent_flow_freshness_check = _influent["freshness_check"]
influent_flow_current_job = _influent["current_job"]
influent_flow_yearly_job = _influent["yearly_job"]
influent_flow_current_schedule = _influent["current_schedule"]
influent_flow_yearly_schedule = _influent["yearly_schedule"]
