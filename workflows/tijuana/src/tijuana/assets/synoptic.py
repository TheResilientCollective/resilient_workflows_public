"""Synoptic Data (mesonet) weather observations for Tijuana-border stations.

Mirrors the openmeteo pattern: per-station + combined storage to S3 in multiple
formats with a "latest" path, plus recent / current-year / yearly-partitioned
assets and an hourly schedule.

The ``synoptic_recent`` and ``synoptic_current_year`` assets pull live from the
Synoptic API (requires ``SYNOPTIC_API_KEY``). The ``synoptic_historical`` asset
does NOT use the API — the account's token has no access to older history — and
instead reads the bulk CSVs downloaded from the Synoptic web form, uploaded to:
  {s3_bucket}/tijuana/weather/synoptic/{STID}.{date}.csv
(publicly mirrored at
  https://oss.resilientservice.mooo.com/test/tijuana/weather/synoptic/{STID}.{date}.csv)
"""

from datetime import datetime, timezone, timedelta
import io
import os

import pandas as pd
import requests
from tenacity import (retry as tenacity_retry, wait_fixed, stop_after_attempt,
                      retry_if_exception_type)

from dagster import (asset,
                     asset_check,
                     AssetCheckResult,
                     AssetCheckExecutionContext,
                     get_dagster_logger,
                     AssetKey,
                     TimeWindowPartitionsDefinition, AutomationCondition
                     )

from resilient_core.utils import store_assets

s3_output_path = 'tijuana/weather/synoptic'

SYNOPTIC_URL = "https://api.synopticdata.com/v2/stations/timeseries"

# Stations requested by the user, with metadata pulled from the historic CSV headers.
SYNOPTIC_STATIONS = [
    {"stid": "BFDSD", "site_name": "Border Field",
     "lat": 32.54197, "lon": -117.09674, "elevation_ft": 336.0},
    {"stid": "TIXC1", "site_name": "NERRS METEOROLOGICAL SITE AT TIJUANA RIVER IMPERIAL BEACH",
     "lat": 32.57444, "lon": -117.12694, "elevation_ft": 3.0},
    {"stid": "CVXSD", "site_name": "Chula Vista",
     "lat": 32.60159, "lon": -117.05801, "elevation_ft": 135.0},
]

# Variables requested from the API (Synoptic appends a "_set_1" suffix in the response).
SYNOPTIC_VARIABLES = [
    "air_temp",
    "relative_humidity",
    "wind_speed",
    "wind_direction",
    "wind_gust",
    "peak_wind_direction",
]

# Canonical column order so every station frame is concatenable, even when a
# station does not report a given variable (e.g. TIXC1 has no peak_wind_direction).
CANONICAL_COLUMNS = [
    "Station_ID",
    "site_name",
    "Date_Time",
    "air_temp_set_1",
    "relative_humidity_set_1",
    "wind_speed_set_1",
    "wind_direction_set_1",
    "wind_gust_set_1",
    "peak_wind_direction_set_1",
]

START_YEAR = 2024  # earliest year with data for these stations


def get_synoptic_token() -> str:
    token = os.environ.get("SYNOPTIC_API_KEY")
    if not token:
        raise ValueError("SYNOPTIC_API_KEY is not set; cannot query the Synoptic API")
    return token


@tenacity_retry(
    retry=retry_if_exception_type(requests.RequestException),
    wait=wait_fixed(30),
    stop=stop_after_attempt(3),
    reraise=True,
)
def _fetch_timeseries(station: dict, extra_params: dict) -> pd.DataFrame:
    """Fetch one station's observations from Synoptic and return a tidy DataFrame
    with the canonical column set (missing variables filled with NA)."""
    logger = get_dagster_logger()
    stid = station["stid"]

    params = {
        "stid": stid,
        "token": get_synoptic_token(),
        "vars": ",".join(SYNOPTIC_VARIABLES),
        "obtimezone": "local",
        "units": "metric",
        "output": "json",
        **extra_params,
    }

    response = requests.get(SYNOPTIC_URL, params=params, timeout=120)
    response.raise_for_status()
    payload = response.json()

    summary = payload.get("SUMMARY", {})
    if summary.get("RESPONSE_CODE") != 1:
        message = summary.get("RESPONSE_MESSAGE", "")
        # The token tier only grants a limited history window; requests for older
        # periods return this message. Those years are already covered by the
        # seeded historic CSV export, so skip rather than failing the partition.
        if "does not have access to the requested history" in message:
            logger.warning(f"No history access for {stid} in requested range; skipping: {message}")
            return pd.DataFrame(columns=CANONICAL_COLUMNS)
        raise ValueError(f"Synoptic API error for {stid}: {message}")

    stations = payload.get("STATION", [])
    if not stations:
        logger.warning(f"No observations returned for {stid}")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    observations = stations[0].get("OBSERVATIONS", {})
    date_time = observations.get("date_time", [])

    data = {"Date_Time": date_time}
    for var in SYNOPTIC_VARIABLES:
        col = f"{var}_set_1"
        data[col] = observations.get(col, [None] * len(date_time))

    df = pd.DataFrame(data)
    # Coerce measurement columns to float so a station that never reports a
    # variable (e.g. TIXC1 has no peak_wind_direction) yields an all-NaN float
    # column rather than an all-None object column — keeping dtypes consistent
    # across stations when the frames are concatenated.
    for var in SYNOPTIC_VARIABLES:
        col = f"{var}_set_1"
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df.insert(0, "site_name", station["site_name"])
    df.insert(0, "Station_ID", stid)

    # Ensure canonical columns/order regardless of what the station reported.
    df = df.reindex(columns=CANONICAL_COLUMNS)
    logger.info(f"Fetched {len(df)} observations for {stid}")
    return df


def _load_downloaded_station(stid: str, station: dict, s3_resource) -> pd.DataFrame:
    """Read a station's bulk web-form download (``{STID}.{date}.csv`` directly
    under the synoptic prefix) from S3 and return a tidy canonical DataFrame.

    Picks the most recent download when several dated files are present.
    """
    logger = get_dagster_logger()
    prefix = f"{s3_output_path}/"

    candidates = []
    for obj in s3_resource.listPath(path=prefix):
        base = obj.object_name[len(prefix):]
        # Only top-level dated files for this station — skip nested raw/ output/.
        if "/" in base:
            continue
        if base.startswith(f"{stid}.") and base.endswith(".csv"):
            candidates.append(obj.object_name)

    if not candidates:
        logger.warning(f"No downloaded CSV found for {stid} under {prefix}")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    key = sorted(candidates)[-1]  # latest by the YYYY-MM-DD date suffix
    logger.info(f"Loading downloaded history for {stid} from {key}")
    raw = s3_resource.getFile(path=key)

    df = pd.read_csv(io.BytesIO(raw), comment="#", low_memory=False)
    # The row directly under the header carries units and has no Station_ID.
    df = df[df["Station_ID"].notna()].reset_index(drop=True)

    for var in SYNOPTIC_VARIABLES:
        col = f"{var}_set_1"
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["site_name"] = station["site_name"]
    df = df.reindex(columns=CANONICAL_COLUMNS)
    logger.info(f"Loaded {len(df)} downloaded observations for {stid}")
    return df


def _store_stations(context, fetch_fn, subfolder: str, dataset_identifier: str):
    """Store per-station and combined outputs, return the combined DataFrame.
    Shared by the recent / current-year / historical assets. ``fetch_fn`` takes a
    station dict and returns that station's canonical DataFrame."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")

    s3_resource = context.resources.s3
    asset_name = str(context.asset_key.path[-1])

    all_frames = []
    for station in SYNOPTIC_STATIONS:
        stid = station["stid"]
        station_df = fetch_fn(station)
        if station_df.empty:
            continue
        all_frames.append(station_df)

        station_metadata = store_assets.objectMetadata(
            name=f"{asset_name}_{stid}", description=description,
            source_url=source_url, variableMeasured=variableMeasured)
        store_assets.store_dataframe_to_s3(
            station_df, f'{s3_output_path}/raw/{subfolder}/{stid}/', dataset_identifier,
            s3_resource, metadata=station_metadata, enable_latest_path=True,
            latestdatasetpath=f'{s3_output_path}/{stid}',
            formats=['csv', 'parquet'])

    if not all_frames:
        get_dagster_logger().warning("No station data fetched; nothing stored")
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    combined_df = pd.concat(all_frames, ignore_index=True).reindex(columns=CANONICAL_COLUMNS)
    combined_metadata = store_assets.objectMetadata(
        name=asset_name, description=description,
        source_url=source_url, variableMeasured=variableMeasured)
    store_assets.store_dataframe_to_s3(
        combined_df, f'{s3_output_path}/raw/{subfolder}/all/', dataset_identifier,
        s3_resource, metadata=combined_metadata, enable_latest_path=True,
        latestdatasetpath=s3_output_path,
        formats=['csv', 'parquet'])
    return combined_df


@asset(group_name="tijuana", key_prefix="weather",
       name="synoptic_recent", required_resource_keys={"s3", "airtable"},
       automation_condition=AutomationCondition.eager(),
       metadata={
           "source": SYNOPTIC_URL,
           "description": "Recent Synoptic weather observations (last 7 days) for Tijuana-border stations",
       })
def synoptic_recent(context):
    """Rolling window of the most recent observations, refreshed frequently."""
    get_dagster_logger().info("Fetching recent Synoptic observations (last 7 days)")
    return _store_stations(context, lambda st: _fetch_timeseries(st, {"recent": 10080}),
                           "recent", "recent")


# Yearly partition for the historical backfill, matching the openmeteo pattern.
yearly_partitions = TimeWindowPartitionsDefinition(
    start=datetime(START_YEAR, 1, 1), fmt='%Y', cron_schedule="@yearly")


@asset(group_name="tijuana", key_prefix="weather",
       name="synoptic_historical", required_resource_keys={"s3", "airtable"},
       partitions_def=yearly_partitions,
       metadata={
           "source": f"{s3_output_path}/{{STID}}.{{date}}.csv (Synoptic web-form bulk download)",
           "description": "Historical Synoptic weather observations by year, from the bulk web-form download",
       })
def synoptic_historical(context):
    """Historical observations for the partition year, read from the bulk CSV
    download (the API token has no access to older history)."""
    year = context.asset_partition_key_for_output()
    s3_resource = context.resources.s3
    get_dagster_logger().info(f"Loading historical Synoptic observations for {year} from the download")

    def load_year(station):
        full = _load_downloaded_station(station["stid"], station, s3_resource)
        if full.empty:
            return full
        # Date_Time is ISO local time (e.g. 2024-11-01T05:00:00-0700); its leading
        # four characters are the local calendar year the partition is keyed on.
        year_df = full[full["Date_Time"].astype(str).str.slice(0, 4) == str(year)]
        return year_df.reset_index(drop=True)

    return _store_stations(context, load_year, f'yearly/{year}', str(year))


@asset(group_name="tijuana", key_prefix="weather",
       name="synoptic_current_year", required_resource_keys={"s3", "airtable"},
       automation_condition=AutomationCondition.eager(),
       metadata={
           "source": SYNOPTIC_URL,
           "description": "Current-year Synoptic observations, refreshed daily for Tijuana-border stations",
       })
def synoptic_current_year(context):
    """Observations from Jan 1 of the current year through now, refreshed daily."""
    year = datetime.today().strftime('%Y')
    start = f'{year}0101 0000'.replace(' ', '')
    end = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')
    get_dagster_logger().info(f"Fetching current-year Synoptic observations {start} → {end}")

    return _store_stations(context, lambda st: _fetch_timeseries(st, {"start": start, "end": end}),
                           f'yearly/{year}', f'{year}_current')


@asset_check(asset=AssetKey(["weather", "synoptic_recent"]))
def synoptic_recent_freshness_check(context: AssetCheckExecutionContext, synoptic_recent):
    """Checks that the most recent Synoptic observation is no older than six hours."""
    if synoptic_recent.empty:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": "Asset is empty, cannot determine freshness."},
        )

    observations = pd.to_datetime(synoptic_recent["Date_Time"], utc=True, errors="coerce")
    observations = observations.dropna()
    if observations.empty:
        return AssetCheckResult(
            passed=False,
            metadata={"reason": "No parseable Date_Time values, cannot determine freshness."},
        )

    most_recent_datetime = observations.max()
    current_datetime = datetime.now(tz=timezone.utc)
    time_difference = current_datetime - most_recent_datetime
    passed = time_difference <= timedelta(hours=6)

    metadata = {
        "most_recent_datetime": str(most_recent_datetime),
        "current_datetime": str(current_datetime),
        "time_difference": str(time_difference),
    }
    if not passed:
        metadata["reason"] = "Most recent observation is older than six hours."
    return AssetCheckResult(passed=passed, metadata=metadata)
