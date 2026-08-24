"""Gridded 10 m wind field over the Tijuana River Valley.

The public map (see `docs/mapinterface_plan.md` in the `tijuana-dispersion`
repo) wants wind drawn as an animated vector field rather than three arrows at
the monitoring stations. That needs wind sampled on a grid, not at points.

Open-Meteo accepts arbitrary coordinates and is free for non-commercial use, so
a small grid over the valley is one batch call per run with no new credentials
and no new vendor. The grid deliberately covers the same bounds the dispersion
service uses for its forward concentration grids
(32.45-32.70 N, -117.25 to -117.00 W) so the two can be overlaid without
resampling.

Two shapes are published:

* a tidy dataframe (csv/parquet) for analysis, one row per point per timestep;
* a u/v JSON in the layout the browser wind-particle layers expect — a header
  describing the grid followed by a flat, row-major array of components. This
  is the same convention `grib2json` emits and `leaflet-velocity` and the
  earth.nullschool renderers consume, so the front end needs no adapter.

Winds here are *modelled*, not observed. The calm nocturnal conditions that
produce the worst H2S episodes are exactly where gridded wind models are
weakest — the dispersion service's own `regime` module makes the same point.
Anything rendering this field should say so.
"""

from datetime import datetime, timezone
import json
import math

import numpy as np
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

from dagster import (
    asset,
    get_dagster_logger,
    define_asset_job,
    AssetKey,
    RunRequest,
    schedule,
)

from resilient_core.utils import store_assets

s3_output_path = 'tijuana/weather'
s3_latest_path = 'tijuana/weather_grid'

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Valley bounds, matching the dispersion service's forward grid frames.
GRID_WEST = -117.25
GRID_EAST = -117.00
GRID_SOUTH = 32.45
GRID_NORTH = 32.70

# 6 x 6 = 36 points, roughly 4.7 km spacing.
#
# This is about as fine as the underlying model supports. Open-Meteo snaps each
# request to its own grid, and measured against the live API a 6 x 6 request
# resolves 34 distinct model cells out of 36 nodes (94%); an 8 x 8 request falls
# to 54 of 64 (84%), i.e. it returns duplicated values dressed up as detail.
# Raising these numbers buys resolution the forecast does not actually have.
GRID_NX = 6
GRID_NY = 6

# +/- 24 h at 15-minute resolution, matching `openmeteo_15min_forecast`.
PAST_INTERVALS = 96
FORECAST_INTERVALS = 96

GRID_VARIABLES = [
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
]


def grid_points(
    west: float = GRID_WEST,
    east: float = GRID_EAST,
    south: float = GRID_SOUTH,
    north: float = GRID_NORTH,
    nx: int = GRID_NX,
    ny: int = GRID_NY,
) -> pd.DataFrame:
    """Grid nodes in the scan order the u/v JSON format requires.

    Velocity layers read their data array row-major starting at the *northwest*
    corner, scanning east, then stepping south. Emitting the points in that
    order here means the published arrays need no reordering later, and the
    dataframe and the JSON cannot drift apart.
    """
    if nx < 2 or ny < 2:
        raise ValueError(f"grid needs at least 2x2 points, got {nx}x{ny}")

    lons = [west + i * (east - west) / (nx - 1) for i in range(nx)]
    lats = [north - j * (north - south) / (ny - 1) for j in range(ny)]

    rows = [
        {"row": j, "col": i, "lat": lat, "lon": lon}
        for j, lat in enumerate(lats)
        for i, lon in enumerate(lons)
    ]
    return pd.DataFrame(rows)


def wind_to_uv(speed: float | None, direction_deg: float | None) -> tuple[float | None, float | None]:
    """Convert speed and meteorological direction to eastward/northward components.

    `wind_direction_10m` is the direction the wind blows *from*, so a 90 degree
    (easterly) wind moves air towards the west and must give a negative u. Hence
    the negated sine and cosine — the sign convention here is the single most
    error-prone line in this module, which is why it has its own test.

    Returns `(None, None)` when either input is missing. Callers must not
    substitute zeros: a missing wind is not a calm wind, and a calm night is
    precisely when H2S accumulates.
    """
    if speed is None or direction_deg is None:
        return None, None
    if isinstance(speed, float) and math.isnan(speed):
        return None, None
    if isinstance(direction_deg, float) and math.isnan(direction_deg):
        return None, None

    rad = math.radians(direction_deg)
    return -speed * math.sin(rad), -speed * math.cos(rad)


def _round(value: float | None, digits: int = 2) -> float | None:
    return None if value is None else round(value, digits)


def uv_grid_json(
    frame: pd.DataFrame,
    ref_time: datetime,
    forecast_time: datetime,
    nx: int = GRID_NX,
    ny: int = GRID_NY,
) -> list[dict]:
    """Build one timestep in the two-record u/v format velocity layers expect.

    `frame` must hold exactly one row per grid node, already in northwest-first
    scan order (as `grid_points` produces).
    """
    expected = nx * ny
    if len(frame) != expected:
        raise ValueError(f"expected {expected} grid nodes for a {nx}x{ny} grid, got {len(frame)}")

    dx = (GRID_EAST - GRID_WEST) / (nx - 1)
    dy = (GRID_NORTH - GRID_SOUTH) / (ny - 1)

    def header(parameter_number: int, parameter_name: str) -> dict:
        return {
            "parameterCategory": 2,          # momentum
            "parameterNumber": parameter_number,
            "parameterNumberName": parameter_name,
            "parameterUnit": "m.s-1",
            "nx": nx,
            "ny": ny,
            "lo1": GRID_WEST,
            "la1": GRID_NORTH,                # scan starts at the NW corner
            "lo2": GRID_EAST,
            "la2": GRID_SOUTH,
            "dx": dx,
            "dy": dy,
            "refTime": ref_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "forecastTime": forecast_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    return [
        {"header": header(2, "eastward_wind"), "data": [_round(v) for v in frame["u"].tolist()]},
        {"header": header(3, "northward_wind"), "data": [_round(v) for v in frame["v"].tolist()]},
    ]


def _frames_payload(df: pd.DataFrame, ref_time: datetime) -> dict:
    """All timesteps in one object, for scrubbing a time slider client-side."""
    times = sorted(df["time"].unique())
    frames = []
    for t in times:
        frame = df[df["time"] == t].sort_values(["row", "col"])
        frames.append({
            "time": pd.Timestamp(t).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "u": [_round(v) for v in frame["u"].tolist()],
            "v": [_round(v) for v in frame["v"].tolist()],
        })

    return {
        "refTime": ref_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "grid": {
            "nx": GRID_NX,
            "ny": GRID_NY,
            "lo1": GRID_WEST,
            "la1": GRID_NORTH,
            "lo2": GRID_EAST,
            "la2": GRID_SOUTH,
            "dx": (GRID_EAST - GRID_WEST) / (GRID_NX - 1),
            "dy": (GRID_NORTH - GRID_SOUTH) / (GRID_NY - 1),
            "scanMode": "northwest-first, east then south",
        },
        "units": "m.s-1",
        "intervalMinutes": 15,
        "note": (
            "Modelled 10 m wind from Open-Meteo, not observations. "
            "null means no value was returned for that node and time; it does not mean calm."
        ),
        "frames": frames,
    }


@asset(
    group_name="tijuana",
    key_prefix="weather",
    name="openmeteo_wind_grid",
    required_resource_keys={"s3"},
    metadata={
        "source": OPEN_METEO_URL,
        "description": (
            "Gridded 10 m wind over the Tijuana River Valley at 15-minute resolution, "
            "past 24 h and next 24 h, as a tidy table and as u/v grids for map animation"
        ),
        "variableMeasured": ["wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "u", "v"],
    },
)
def wind_grid(context):
    """Fetch the valley wind grid in one batch call and publish table + u/v JSON."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    points = grid_points()
    logger.info(
        f"Requesting {len(points)} grid nodes over "
        f"{GRID_SOUTH}-{GRID_NORTH}N, {GRID_WEST}-{GRID_EAST}E"
    )

    cache_session = requests_cache.CachedSession(".cache", expire_after=900)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    params = {
        "latitude": points["lat"].tolist(),
        "longitude": points["lon"].tolist(),
        # Velocity layers work in m/s; ask for it rather than converting from
        # the km/h default and risking a silent unit mismatch.
        "wind_speed_unit": "ms",
        "minutely_15": GRID_VARIABLES,
        "forecast_minutely_15": FORECAST_INTERVALS,
        "past_minutely_15": PAST_INTERVALS,
    }

    responses = openmeteo.weather_api(OPEN_METEO_URL, params=params)
    if len(responses) != len(points):
        raise Exception(
            f"Open-Meteo returned {len(responses)} responses for {len(points)} grid nodes"
        )

    ref_time = datetime.now(timezone.utc).replace(microsecond=0)

    frames = []
    for i, response in enumerate(responses):
        node = points.iloc[i]
        m15 = response.Minutely15()

        timestamps = pd.date_range(
            start=pd.to_datetime(m15.Time(), unit="s", utc=True),
            end=pd.to_datetime(m15.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=m15.Interval()),
            inclusive="left",
        )

        node_data = {"time": timestamps}
        for j, var_name in enumerate(GRID_VARIABLES):
            node_data[var_name] = m15.Variables(j).ValuesAsNumpy()

        node_df = pd.DataFrame(node_data)
        node_df["row"] = int(node["row"])
        node_df["col"] = int(node["col"])
        # The requested coordinates, not the ones Open-Meteo snapped to, so the
        # published grid stays exactly regular and the header stays truthful.
        node_df["lat"] = node["lat"]
        node_df["lon"] = node["lon"]
        node_df["model_lat"] = response.Latitude()
        node_df["model_lon"] = response.Longitude()
        frames.append(node_df)

    df = pd.concat(frames, ignore_index=True)
    if df.empty:
        raise Exception("Open-Meteo returned no wind data for the valley grid")

    uv = [
        wind_to_uv(speed, direction)
        for speed, direction in zip(df["wind_speed_10m"], df["wind_direction_10m"])
    ]
    df["u"] = [pair[0] for pair in uv]
    df["v"] = [pair[1] for pair in uv]

    missing = int(df["u"].isna().sum())
    if missing:
        logger.warning(
            f"{missing} of {len(df)} grid samples had no wind value; "
            "published as null rather than zero"
        )
    if missing == len(df):
        raise Exception("Every grid sample was missing a wind value")

    df = df.sort_values(["time", "row", "col"]).reset_index(drop=True)

    store_assets.store_dataframe_to_s3(
        df,
        f"{s3_output_path}/output/wind_grid/",
        "wind_grid",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=s3_latest_path,
        formats=["csv", "parquet"],
    )

    # The frame nearest the run time, for a map that only wants "now".
    times = pd.Series(sorted(df["time"].unique()))
    nearest = times.iloc[(times - pd.Timestamp(ref_time)).abs().argmin()]
    current = df[df["time"] == nearest].sort_values(["row", "col"])
    latest_json = uv_grid_json(current, ref_time, pd.Timestamp(nearest).to_pydatetime())

    latest_metadata = metadata.copy()
    latest_metadata.name = "wind_grid_latest"
    latest_metadata.description = (
        "Current gridded 10 m wind over the Tijuana River Valley as u/v components"
    )
    store_assets.text_to_s3(
        json.dumps(latest_json),
        f"{store_assets.get_latest_basepath()}/{s3_latest_path}/wind_grid_latest.json",
        s3_resource,
        contenttype="application/json",
        metadata=latest_metadata,
    )

    frames_metadata = metadata.copy()
    frames_metadata.name = "wind_grid_frames"
    frames_metadata.description = (
        "Gridded 10 m wind over the Tijuana River Valley at 15-minute steps, "
        "past 24 h and next 24 h, as u/v components"
    )
    store_assets.text_to_s3(
        json.dumps(_frames_payload(df, ref_time)),
        f"{store_assets.get_latest_basepath()}/{s3_latest_path}/wind_grid_frames.json",
        s3_resource,
        contenttype="application/json",
        metadata=frames_metadata,
    )

    speeds = df["wind_speed_10m"].to_numpy(dtype=float)
    context.add_output_metadata({
        "grid_nodes": len(points),
        "timesteps": int(df["time"].nunique()),
        "rows": len(df),
        "missing_samples": missing,
        "max_wind_ms": float(np.nanmax(speeds)) if len(speeds) else 0.0,
        "mean_wind_ms": float(np.nanmean(speeds)) if len(speeds) else 0.0,
    })

    logger.info(
        f"Published wind grid: {df['time'].nunique()} timesteps x {len(points)} nodes"
    )
    return df


wind_grid_job = define_asset_job(
    "wind_grid", selection=[AssetKey(["weather", "openmeteo_wind_grid"])]
)


@schedule(job=wind_grid_job, cron_schedule="@hourly", name="wind_grid")
def wind_grid_schedule(context):
    return RunRequest()
