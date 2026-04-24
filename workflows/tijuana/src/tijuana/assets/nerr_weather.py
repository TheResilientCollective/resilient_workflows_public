"""NERR Tijuana River met station weather data.

Fetches meteorological observations from the Tijuana River NERR "Tidal Linkage"
station (STID: TIXC1, NERRS code: tjrtlmet) for use in H2S forecasting.

Data sources:
- Historical: CDMO SWMP yearly CSV archives (2023+)
    https://cdmo.baruch.sc.edu/waf/swmp_data_archives/tjrtlmet{year}.csv
- Current: Synoptic Data API v2 (~30 days real-time)
    https://api.synopticdata.com/v2/stations/timeseries?stid=TIXC1

The data replaces OpenMeteo weather for the IB CIVIC CTR site in modeldata_h2s.
"""
import os
from datetime import datetime
from io import StringIO

import numpy as np
import pandas as pd
import requests
import requests_cache
from retry_requests import retry

from dagster import (
    asset,
    get_dagster_logger,
    define_asset_job,
    schedule,
    TimeWindowPartitionsDefinition,
    AutomationCondition,
)

from resilient_core.utils import store_assets

# --- CDMO SWMP data archives ---
CDMO_BASE = "https://cdmo.baruch.sc.edu/waf/swmp_data_archives"
CDMO_STATION = "tjrtlmet"

# Map CDMO column names to OpenMeteo-compatible names
CDMO_TO_OPENMETEO = {
    "ATemp": "temperature_2m",        # °C
    "RH": "relative_humidity_2m",     # %
    "BP": "surface_pressure",         # mb (= hPa)
    "WSpd": "wind_speed_10m",         # m/s → km/h
    "MaxWSpd": "wind_gusts_10m",      # m/s → km/h
    "Wdir": "wind_direction_10m",     # degrees
    "TotPrcp": "precipitation",       # mm
}

# --- Synoptic Data API ---
SYNOPTIC_BASE = "https://api.synopticdata.com/v2/stations/timeseries"
STATION_ID = "TIXC1"
SYNOPTIC_VARS = "air_temp,wind_speed,wind_direction,wind_gust,relative_humidity,dew_point_temperature,precip_accum_fifteen_minute"

SYNOPTIC_TO_OPENMETEO = {
    "air_temp_set_1": "temperature_2m",
    "relative_humidity_set_1": "relative_humidity_2m",
    "wind_speed_set_1": "wind_speed_10m",           # m/s → km/h
    "wind_gust_set_1": "wind_gusts_10m",             # m/s → km/h
    "wind_direction_set_1": "wind_direction_10m",
    "dew_point_temperature_set_1d": "dewpoint_2m",
    "precip_accum_fifteen_minute_set_1": "precipitation",
}

SITE_NAME = "IB CIVIC CTR"

s3_output_path = "tijuana/weather_nerr"
LATEST_PATH = "tijuana/weather_nerr"


def _resample_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample 15-min time-indexed DataFrame to hourly with OpenMeteo-compatible output."""
    if df.empty:
        return pd.DataFrame()

    agg = {}
    for col in df.columns:
        if col in ("wind_gusts_10m",):
            agg[col] = "max"
        elif col in ("precipitation",):
            agg[col] = "sum"
        else:
            agg[col] = "mean"

    hourly = df.resample("1h").agg(agg)
    hourly = hourly.reset_index().rename(columns={"time": "date"})

    # Ensure all expected columns exist
    for col in ("cloud_cover", "dewpoint_2m", "visibility",
                "precipitation", "relative_humidity_2m", "surface_pressure",
                "temperature_2m", "wind_speed_10m", "wind_gusts_10m",
                "wind_direction_10m"):
        if col not in hourly.columns:
            hourly[col] = np.nan

    hourly["site_name"] = SITE_NAME
    return hourly


# --- CDMO fetch ---

def fetch_cdmo_year(year: str, session=None) -> pd.DataFrame:
    """Download a yearly CDMO SWMP archive CSV and return hourly DataFrame.

    CDMO provides QA'd 15-min data. Timestamps are US/Pacific local time.
    """
    logger = get_dagster_logger()
    url = f"{CDMO_BASE}/{CDMO_STATION}{year}.csv"
    logger.info(f"Downloading CDMO archive: {url}")

    if session is None:
        session = requests.Session()

    resp = session.get(url, timeout=120)
    resp.raise_for_status()

    df = pd.read_csv(StringIO(resp.text))
    logger.info(f"Loaded {len(df)} records from CDMO for {year}")

    if df.empty:
        return pd.DataFrame()

    # Parse timestamp — CDMO uses MM/DD/YYYY H:MM in US/Pacific
    df["time"] = pd.to_datetime(df["DatetimeStamp"], format="%m/%d/%Y %H:%M")
    df["time"] = df["time"].dt.tz_localize("America/Los_Angeles", ambiguous="NaT", nonexistent="NaT")
    df["time"] = df["time"].dt.tz_convert("UTC")

    # Drop rows where tz conversion failed (DST transitions)
    df = df.dropna(subset=["time"])

    # Rename to OpenMeteo-compatible names
    rename_map = {k: v for k, v in CDMO_TO_OPENMETEO.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Keep only the mapped columns
    keep = ["time"] + [v for v in rename_map.values()]
    df = df[keep]

    # Convert numeric, coercing QC-flagged strings to NaN
    for col in df.columns:
        if col != "time":
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Wind speed m/s → km/h
    if "wind_speed_10m" in df.columns:
        df["wind_speed_10m"] = df["wind_speed_10m"] * 3.6
    if "wind_gusts_10m" in df.columns:
        df["wind_gusts_10m"] = df["wind_gusts_10m"] * 3.6

    df = df.set_index("time").sort_index()
    logger.info(f"Parsed {len(df)} records, range: {df.index.min()} to {df.index.max()}")

    hourly = _resample_to_hourly(df)
    logger.info(f"Resampled to {len(hourly)} hourly records")
    return hourly


# --- Synoptic fetch ---

def fetch_synoptic_recent(recent_minutes: int = 43200, session=None) -> pd.DataFrame:
    """Fetch recent observations from Synoptic Data API for station TIXC1."""
    logger = get_dagster_logger()
    token = os.environ.get("SYNOPTICSWEAHTER_TOKEN", "")
    if not token:
        raise ValueError("SYNOPTICSWEAHTER_TOKEN environment variable not set")

    if session is None:
        session = requests.Session()

    params = {
        "stid": STATION_ID,
        "recent": str(recent_minutes),
        "vars": SYNOPTIC_VARS,
        "units": "metric",
        "obtimezone": "UTC",
        "token": token,
    }

    logger.info(f"Fetching Synoptic data for {STATION_ID}, recent {recent_minutes} min")
    resp = session.get(SYNOPTIC_BASE, params=params, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    summary = data.get("SUMMARY", {})
    if summary.get("RESPONSE_CODE") != 1:
        msg = summary.get("RESPONSE_MESSAGE", "Unknown error")
        raise RuntimeError(f"Synoptic API error: {msg}")

    station = data["STATION"][0]
    obs = station["OBSERVATIONS"]
    logger.info(f"Station: {station['NAME']} ({station['STID']})")

    df = pd.DataFrame({"time": pd.to_datetime(obs["date_time"], utc=True)})
    for synoptic_key, openmeteo_name in SYNOPTIC_TO_OPENMETEO.items():
        if synoptic_key in obs:
            df[openmeteo_name] = obs[synoptic_key]

    df = df.set_index("time").sort_index()
    logger.info(f"Loaded {len(df)} records from {df.index.min()} to {df.index.max()}")

    # Wind speed m/s → km/h
    if "wind_speed_10m" in df.columns:
        df["wind_speed_10m"] = df["wind_speed_10m"] * 3.6
    if "wind_gusts_10m" in df.columns:
        df["wind_gusts_10m"] = df["wind_gusts_10m"] * 3.6

    hourly = _resample_to_hourly(df)
    logger.info(f"Resampled to {len(hourly)} hourly records")
    return hourly


# --- Dagster assets ---

nerr_yearly_partitions = TimeWindowPartitionsDefinition(
    start=datetime(2023, 1, 1), fmt="%Y", cron_schedule="@yearly"
)


@asset(
    group_name="tijuana",
    key_prefix="weather",
    name="nerr_tjr_met_historical",
    required_resource_keys={"s3"},
    partitions_def=nerr_yearly_partitions,
    metadata={
        "source": f"{CDMO_BASE}/{CDMO_STATION}",
        "description": "Historical meteorological data from NERR TJR met station (CDMO SWMP archives)",
    },
)
def nerr_tjr_met_historical(context):
    """Fetch yearly historical NERR met data from CDMO SWMP CSV archives.

    Downloads the full-year 15-min CSV, resamples to hourly, and stores
    with OpenMeteo-compatible column names.
    """
    s3_resource = context.resources.s3
    year = context.asset_partition_key_for_output()
    logger = get_dagster_logger()
    logger.info(f"Fetching NERR TJR met historical data for year: {year}")

    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    metadata = store_assets.objectMetadata(
        name=f"nerr_tjr_met_{year}",
        description=description,
        source_url=source_url,
    )

    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    df = fetch_cdmo_year(year, session=retry_session)
    if df.empty:
        logger.warning(f"No CDMO data for {year}")
        return df

    logger.info(f"Storing {len(df)} hourly records for {year}")
    store_assets.store_dataframe_to_s3(
        df,
        f"{s3_output_path}/raw/yearly/",
        str(year),
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=LATEST_PATH,
        formats=["csv", "parquet"],
    )
    return df


@asset(
    group_name="tijuana",
    key_prefix="weather",
    name="nerr_tjr_met_current_year",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": f"{SYNOPTIC_BASE}?stid={STATION_ID}",
        "description": "NERR TJR met station data via Synoptic Data API, refreshed daily (~30 days)",
    },
)
def nerr_tjr_met_current_year(context):
    """Fetch recent NERR met data from Synoptic Data API.

    Uses the `recent` parameter for up to 30 days of 15-min observations.
    Resampled to hourly with OpenMeteo-compatible column names for modeldata_h2s.
    """
    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    metadata = store_assets.objectMetadata(
        name="nerr_tjr_met_synoptic",
        description=description,
        source_url=source_url,
    )

    cache_session = requests_cache.CachedSession(".cache", expire_after=900)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

    df = fetch_synoptic_recent(recent_minutes=43200, session=retry_session)
    if df.empty:
        logger.warning("No Synoptic data returned")
        return df

    logger.info(f"Storing {len(df)} hourly NERR/Synoptic records")
    store_assets.store_dataframe_to_s3(
        df,
        f"{s3_output_path}/raw/current_year/",
        "current_year",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=LATEST_PATH,
        formats=["csv", "parquet"],
    )
    return df


nerr_weather_job = define_asset_job(
    "nerr_weather_all",
    selection=[
        "weather/nerr_tjr_met_current_year",
    ],
)


@schedule(
    cron_schedule="0 6 * * *",
    job=nerr_weather_job,
    execution_timezone="America/Los_Angeles",
)
def nerr_weather_schedule(_context):
    return {}
