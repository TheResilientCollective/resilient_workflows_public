import hashlib
import io
import json
import os
import re
import zipfile
from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests
from dagster import (
    AssetKey,
    AutomationCondition,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    get_dagster_logger,
    sensor,
)
from dagster import RunRequest

from resilient_core.utils import store_assets
from resilient_core.utils.constants import ICONS

SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_UPDATES", "#test")

PFM_PAGE_URL = "https://pfmweb.ucsd.edu/"
PFM_BASE_URL = "https://pfmweb.ucsd.edu"

# The zip now contains these JSON data files plus map/frame_NNN.png rasters
JSON_FILES = ["times.json", "sites.json", "shoreline.json"]

# Risk level encoding used by the PFM model
RISK_COLORS = {0: "#00C853", 1: "#FFD600", 2: "#D50000"}
RISK_LABELS = {0: "low", 1: "moderate", 2: "high"}


def fetch_pfm_zip_url() -> str:
    """
    Fetch https://pfmweb.ucsd.edu/ and extract the hashed zip URL from
    Observable's registerFile() call embedded in the page JavaScript.
    """
    response = requests.get(PFM_PAGE_URL, timeout=30)
    response.raise_for_status()
    pattern = re.compile(
        r'registerFile\(["\']\.\/data\/pfm_his_daily\.zip["\']'
        r'.*?["\']path["\']:\s*["\']([^"\']+)["\']',
        re.DOTALL,
    )
    m = pattern.search(response.text)
    if not m:
        raise ValueError(
            "Could not find pfm_his_daily.zip URL. PFM page structure may have changed."
        )
    # path is like "./_file/data/pfm_his_daily.12e0d4e0.zip" — strip leading "."
    relative_path = m.group(1)
    if relative_path.startswith("."):
        relative_path = relative_path[1:]
    return f"{PFM_BASE_URL}{relative_path}"


def fetch_pfm_files() -> dict:
    """
    Download the PFM zip and return {filename: bytes} for JSON data files.
    Handles arbitrary directory nesting inside the zip by matching on basename.
    """
    zip_url = fetch_pfm_zip_url()
    response = requests.get(zip_url, timeout=120)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        name_map = {os.path.basename(n): n for n in zf.namelist() if not n.endswith("/")}
        missing = [f for f in JSON_FILES if f not in name_map]
        if missing:
            raise ValueError(
                f"Missing files in PFM zip: {missing}. Available: {list(name_map.keys())}"
            )
        return {f: zf.read(name_map[f]) for f in JSON_FILES}


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_site_markers",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_site_markers(context):
    """Scripps PFM monitoring station locations with hour-0 risk levels."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    files = fetch_pfm_files()
    sites = json.loads(files["sites.json"])
    times_data = json.loads(files["times.json"])
    logger.info("Loaded sites.json and times.json from PFM zip")

    s3_resource.putFile_text(
        data=files["sites.json"].decode(),
        path=f"tijuana/oceanmodel/raw/scripps_pfm/{today}/sites.json",
        content_type="application/json",
    )

    n_sites = len(sites["names"])
    hour0_risk = [sites["risk"][0][i] for i in range(n_sites)]
    gdf = gpd.GeoDataFrame(
        {
            "name": sites["names"],
            "lat": sites["lats"],
            "lon": sites["lons"],
            "risk_value": hour0_risk,
            "risk_level": [RISK_LABELS.get(r, "unknown") for r in hour0_risk],
            "forecast_start": times_data["times"][0],
            "Icons": [ICONS.get("beach", "place")] * n_sites,
        },
        geometry=gpd.points_from_xy(sites["lons"], sites["lats"]),
        crs="EPSG:4326",
    )

    metadata = store_assets.objectMetadata(
        name="pfm_site_markers",
        description="Scripps Pathogen Forecast Model monitoring station locations along the Tijuana River and San Diego coast",
        source_url=PFM_PAGE_URL,
    )

    store_assets.store_dataframe_to_s3(
        gdf,
        "tijuana/oceanmodel/output/pfm_site_markers",
        "site_markers",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath="tijuana/oceanmodel/pfm_site_markers",
        formats=["geojson", "csv"],
    )

    context.add_output_metadata({"station_count": len(gdf), "forecast_start": times_data["times"][0]})
    return gdf


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_site_timeseries",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_site_timeseries(context):
    """Scripps PFM hourly risk timeseries per monitoring station (121 hours)."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    files = fetch_pfm_files()
    sites = json.loads(files["sites.json"])
    times_data = json.loads(files["times.json"])
    logger.info("Loaded sites.json and times.json from PFM zip")

    timestamps = times_data["times"]
    names = sites["names"]

    rows = []
    for t_idx, ts in enumerate(timestamps):
        for s_idx in range(len(names)):
            risk_val = sites["risk"][t_idx][s_idx]
            rows.append({
                "timestamp": ts,
                "site": names[s_idx],
                "lat": sites["lats"][s_idx],
                "lon": sites["lons"][s_idx],
                "risk_value": risk_val,
                "risk_level": RISK_LABELS.get(risk_val, "unknown"),
            })
    df = pd.DataFrame(rows)

    s3_resource.putFile_text(
        data=df.to_csv(index=False),
        path=f"tijuana/oceanmodel/raw/scripps_pfm/{today}/site_timeseries.csv",
        content_type="text/csv",
    )

    metadata = store_assets.objectMetadata(
        name="pfm_site_timeseries",
        description="Scripps Pathogen Forecast Model hourly risk timeseries per monitoring station",
        source_url=PFM_PAGE_URL,
    )

    store_assets.store_dataframe_to_s3(
        df,
        "tijuana/oceanmodel/output/pfm_site_timeseries",
        "site_timeseries",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath="tijuana/oceanmodel/pfm_site_timeseries",
        formats=["csv", "json"],
    )

    context.add_output_metadata({"row_count": len(df), "sites": names, "time_steps": len(timestamps)})
    return df


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_dye_contours",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_dye_contours(context):
    """
    Scripps PFM animated forecast frames — stores all 121 hourly PNG images to S3
    and writes manifest.json with Leaflet-ready bounds for client-side rendering.

    PNG frames carry no embedded coordinate metadata. Bounds come from times.json
    and must be applied when displaying frames on a Leaflet map:
      L.imageOverlay(frameUrl, manifest.leaflet_bounds)
      where leaflet_bounds = [[south, west], [north, east]]

    Current bounds (EPSG:4326):
      south: 32.4062, north: 32.7544, west: -117.2781, east: -117.0567
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    zip_url = fetch_pfm_zip_url()
    logger.info(f"Downloading PFM zip from {zip_url}")
    response = requests.get(zip_url, timeout=180)
    response.raise_for_status()

    zip_bytes = response.content
    buf = io.BytesIO(zip_bytes)
    s3_resource.putStream(
        stream=buf,
        length=len(zip_bytes),
        path=f"tijuana/oceanmodel/raw/scripps_pfm/{today}/pfm_his_daily.zip",
        content_type="application/zip",
    )

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        times_data = json.loads(zf.read("times.json"))
        bounds = times_data["bounds"]
        timestamps = times_data["times"]

        leaflet_bounds = [
            [bounds["south"], bounds["west"]],
            [bounds["north"], bounds["east"]],
        ]

        frame_names = sorted(n for n in zf.namelist() if n.startswith("map/frame_") and n.endswith(".png"))
        logger.info(f"Storing {len(frame_names)} PNG frames to S3")

        frame_manifest = []
        for name in frame_names:
            frame_bytes = zf.read(name)
            frame_num = int(os.path.splitext(os.path.basename(name))[0].split("_")[1])
            out_path = f"tijuana/oceanmodel/output/pfm_dye_contours/frame_{frame_num:03d}.png"
            s3_resource.putStream(
                stream=io.BytesIO(frame_bytes),
                length=len(frame_bytes),
                path=out_path,
                content_type="image/png",
            )
            frame_manifest.append({
                "frame": frame_num,
                "timestamp": timestamps[frame_num] if frame_num < len(timestamps) else None,
                "path": out_path,
            })

    manifest = {
        "bounds": bounds,
        "leaflet_bounds": leaflet_bounds,
        "frame_count": len(frame_manifest),
        "frames": frame_manifest,
    }
    manifest_json = json.dumps(manifest)
    for path in [
        "tijuana/oceanmodel/output/pfm_dye_contours/manifest.json",
        "tijuana/oceanmodel/pfm_dye_contours/manifest.json",
    ]:
        s3_resource.putFile_text(data=manifest_json, path=path, content_type="application/json")

    logger.info(f"Stored {len(frame_manifest)} frames and manifest")
    context.add_output_metadata({"frame_count": len(frame_manifest), "bounds": bounds, "date": today})
    return manifest


pfm_job = define_asset_job(
    "scripps_pfm_job",
    selection=[
        AssetKey(["oceanmodel", "pfm_site_markers"]),
        AssetKey(["oceanmodel", "pfm_site_timeseries"]),
        AssetKey(["oceanmodel", "pfm_dye_contours"]),
        AssetKey(["oceanmodel", "pfm_hour0_contours"]),
        AssetKey(["oceanmodel", "pfm_shoreline_hazard"]),
    ],
)


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_hour0_contours",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    deps=[AssetKey(["oceanmodel", "pfm_dye_contours"])],
)
def pfm_hour0_contours(context):
    """
    Scripps PFM current-conditions snapshot — reads the pfm_dye_contours manifest
    and publishes a slim JSON with the hour-0 frame path and Leaflet-ready bounds
    for frontend consumption without loading the full 121-frame manifest.

    Output schema:
      {
        "timestamp": "2026-05-04T00:00:00",
        "frame_path": "tijuana/oceanmodel/output/pfm_dye_contours/frame_000.png",
        "bounds": {"south": 32.4062, "north": 32.7544, "west": -117.2781, "east": -117.0567},
        "leaflet_bounds": [[32.4062, -117.2781], [32.7544, -117.0567]]
      }
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    logger.info("Loading pfm_dye_contours manifest from S3")
    data = s3_resource.getFile("tijuana/oceanmodel/output/pfm_dye_contours/manifest.json")
    manifest = json.loads(data.decode("utf-8"))

    frame0 = manifest["frames"][0]
    current = {
        "timestamp": frame0["timestamp"],
        "frame_path": frame0["path"],
        "bounds": manifest["bounds"],
        "leaflet_bounds": manifest["leaflet_bounds"],
    }
    current_json = json.dumps(current)
    for path in [
        "tijuana/oceanmodel/output/pfm_hour0_contours/current.json",
        "tijuana/oceanmodel/pfm_hour0_contours/current.json",
    ]:
        s3_resource.putFile_text(data=current_json, path=path, content_type="application/json")

    logger.info(f"Published hour-0 frame: {frame0['path']} at {frame0['timestamp']}")
    context.add_output_metadata({
        "timestamp": frame0["timestamp"],
        "frame_path": frame0["path"],
        "bounds": manifest["bounds"],
    })
    return current


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_shoreline_hazard",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_shoreline_hazard(context):
    """
    Scripps PFM shoreline hazard lines — converts 1060 dense shoreline points into
    simplified colored LineStrings by risk level for mobile-friendly visualization.
    Risk values: 0=low (green), 1=moderate (yellow), 2=high (red).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    files = fetch_pfm_files()
    shoreline = json.loads(files["shoreline.json"])
    lats = shoreline["lats"]
    lons = shoreline["lons"]
    risk_hour0 = shoreline["risk"][0]
    logger.info(f"Loaded {len(lats)} shoreline points from PFM zip")

    # Segment contiguous points by risk level, bridging transitions for continuous lines
    all_lines = []
    segment_id = 0
    i = 0
    while i < len(lats):
        current_risk = risk_hour0[i]
        coords = [[lons[i], lats[i]]]
        i += 1
        while i < len(lats):
            next_risk = risk_hour0[i]
            if next_risk != current_risk:
                coords.append([lons[i], lats[i]])
                break
            coords.append([lons[i], lats[i]])
            i += 1
        if len(coords) >= 2:
            all_lines.append({
                "type": "Feature",
                "properties": {
                    "risk_value": int(current_risk),
                    "risk_level": RISK_LABELS.get(int(current_risk), "unknown"),
                    "segment_id": segment_id,
                },
                "geometry": {"type": "LineString", "coordinates": coords},
            })
            segment_id += 1

    gdf = gpd.GeoDataFrame.from_features(all_lines, crs="EPSG:4326")
    logger.info(f"Created {len(gdf)} line segments")
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.00015, preserve_topology=True)
    gdf["color"] = gdf["risk_value"].map(RISK_COLORS)
    gdf["Icons"] = "line"

    metadata = store_assets.objectMetadata(
        name="pfm_shoreline_hazard",
        description="Scripps PFM simplified shoreline hazard lines colored by risk level (0=low/green, 1=moderate/yellow, 2=high/red)",
        source_url=PFM_PAGE_URL,
    )

    store_assets.store_dataframe_to_s3(
        gdf,
        "tijuana/oceanmodel/output/pfm_shoreline_hazard",
        "shoreline_hazard",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath="tijuana/oceanmodel/pfm_shoreline_hazard",
        formats=["geojson", "csv"],
    )

    context.add_output_metadata({
        "line_count": len(gdf),
        "risk_distribution": gdf["risk_value"].value_counts().to_dict(),
        "simplified": True,
    })
    return gdf


@sensor(
    job=pfm_job,
    name="scripps_pfm_sensor",
    minimum_interval_seconds=3600,
    required_resource_keys={"slack"},
)
def scripps_pfm_sensor(context: SensorEvaluationContext):
    """Hourly sensor: fires when pfmweb.ucsd.edu deploys updated forecast data."""
    slack = context.resources.slack
    try:
        zip_url = fetch_pfm_zip_url()
        fingerprint = hashlib.md5(zip_url.encode()).hexdigest()

        last_fingerprint = context.cursor or None
        if fingerprint != last_fingerprint:
            context.update_cursor(fingerprint)
            context.log.info(f"PFM site updated. New fingerprint: {fingerprint}")
            context.log.info(f"PFM site updated. New zip url: {zip_url}")
            slack.get_client().chat_postMessage(
                channel=SLACK_CHANNEL,
                text=f"Scripps PFM forecast updated, running ingest (fingerprint: {fingerprint})",
            )
            return RunRequest(run_key=f"pfm_{fingerprint}")
        else:
            context.log.info(f"No PFM update detected. Fingerprint: {fingerprint}")
            return None
    except Exception as e:
        context.log.error(f"Error checking pfmweb for updates: {e}")
        slack.get_client().chat_postMessage(
            channel=SLACK_CHANNEL,
            text=f"Error checking Scripps PFM site for updates: {e}",
        )
        return None
