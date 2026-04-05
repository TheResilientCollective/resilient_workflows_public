import hashlib
import io
import json
import os
import re
from datetime import datetime
from io import StringIO

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
PFM_FILE_BASE = "https://pfmweb.ucsd.edu/_file/data/pfm_his_daily"
EXPECTED_FILES = [
    "site_markers.json",
    "site_timeseries.csv",
    "computed_shoreline_points.json",
    "computed_dye_contours_0.json",
    "computed_dye_contours_1.json",
    "computed_dye_contours_2.json",
    "computed_dye_contours_3.json",
    "computed_dye_contours_4.json",
]


def fetch_pfm_file_hashes() -> dict:
    """
    Fetch https://pfmweb.ucsd.edu/ and extract hashed file paths from
    Observable's registerFile() calls embedded in the page JavaScript.
    Returns dict mapping logical filename -> full URL with hash.
    """
    response = requests.get(PFM_PAGE_URL, timeout=30)
    response.raise_for_status()
    pattern = re.compile(
        r'registerFile\(["\']\.\/data\/pfm_his_daily\/([\w._-]+)["\']'
        r'.*?["\']path["\']:\s*["\']\./_file\/data\/pfm_his_daily\/([\w._-]+)["\']',
        re.DOTALL,
    )
    hashes = {
        m.group(1): f"{PFM_FILE_BASE}/{m.group(2)}"
        for m in pattern.finditer(response.text)
    }
    missing = [f for f in EXPECTED_FILES if f not in hashes]
    if missing:
        raise ValueError(
            f"Could not find hash URLs for: {missing}. PFM page structure may have changed."
        )
    return hashes


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_site_markers",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_site_markers(context):
    """Scripps PFM monitoring station point locations along the Tijuana River / SD coast."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    hashes = fetch_pfm_file_hashes()
    url = hashes["site_markers.json"]
    logger.info(f"Fetching site markers from {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    # Archive raw dated copy
    raw_path = f"tijuana/oceanmodel/raw/scripps_pfm/{today}/site_markers.geojson"
    s3_resource.putFile_text(data=response.text, path=raw_path, content_type="application/geo+json")

    # Parse double-encoded JSON (server returns JSON-encoded JSON string)
    geojson_str = response.json()
    geojson_data = json.loads(geojson_str)
    gdf = gpd.GeoDataFrame.from_features(geojson_data["features"], crs="EPSG:4326")
    gdf["Icons"] = ICONS.get("beach", "place")

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

    context.add_output_metadata({"station_count": len(gdf), "hash_url": url})
    return gdf


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_site_timeseries",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_site_timeseries(context):
    """Scripps PFM hourly timeseries of modeled concentrations per monitoring station."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    hashes = fetch_pfm_file_hashes()
    url = hashes["site_timeseries.csv"]
    logger.info(f"Fetching site timeseries from {url}")

    response = requests.get(url, timeout=60)
    response.raise_for_status()

    # Archive raw dated copy
    raw_path = f"tijuana/oceanmodel/raw/scripps_pfm/{today}/site_timeseries.csv"
    s3_resource.putFile_text(data=response.text, path=raw_path, content_type="text/csv")

    df = pd.read_csv(StringIO(response.text))
    # Normalize timestamp: first column is the time index
    ts_col = df.columns[0]
    df = df.rename(columns={ts_col: "timestamp"})

    metadata = store_assets.objectMetadata(
        name="pfm_site_timeseries",
        description="Scripps Pathogen Forecast Model hourly timeseries per monitoring station",
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

    context.add_output_metadata({"row_count": len(df), "columns": list(df.columns), "hash_url": url})
    return df


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_dye_contours",
    required_resource_keys={"s3", "slack"},
    automation_condition=AutomationCondition.eager(),
)
def pfm_dye_contours(context):
    """Scripps PFM dye concentration contour polygons (levels 0-4) and shoreline geometry."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    hashes = fetch_pfm_file_hashes()
    stored_paths = {}

    # Shoreline points
    shoreline_url = hashes["computed_shoreline_points.json"]
    logger.info(f"Fetching shoreline points from {shoreline_url}")
    buf = io.BytesIO(requests.get(shoreline_url, timeout=120).content)
    size = buf.getbuffer().nbytes
    raw_path = f"tijuana/oceanmodel/raw/scripps_pfm/{today}/shoreline_points.geojson"
    output_path = "tijuana/oceanmodel/output/pfm_dye_contours/shoreline_points.geojson"
    s3_resource.putStream(stream=buf, length=size, path=raw_path, content_type="application/geo+json")
    buf.seek(0)
    s3_resource.putStream(stream=buf, length=size, path=output_path, content_type="application/geo+json")
    stored_paths["shoreline_points"] = output_path
    logger.info(f"Stored shoreline points ({size} bytes)")

    # Dye contour levels 0-4
    for level in range(5):
        contour_key = f"computed_dye_contours_{level}.json"
        contour_url = hashes[contour_key]
        logger.info(f"Fetching dye contour level {level} from {contour_url}")
        buf = io.BytesIO(requests.get(contour_url, timeout=120).content)
        size = buf.getbuffer().nbytes
        raw_path = f"tijuana/oceanmodel/raw/scripps_pfm/{today}/dye_contours_{level}.geojson"
        output_path = f"tijuana/oceanmodel/output/pfm_dye_contours/dye_contours_{level}.geojson"
        s3_resource.putStream(stream=buf, length=size, path=raw_path, content_type="application/geo+json")
        buf.seek(0)
        s3_resource.putStream(stream=buf, length=size, path=output_path, content_type="application/geo+json")
        stored_paths[f"dye_contours_{level}"] = output_path
        logger.info(f"Stored dye contour level {level} ({size} bytes)")

    context.add_output_metadata({"stored_files": stored_paths, "date": today})
    return stored_paths


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
)
def pfm_hour0_contours(context):
    """
    Scripps PFM hour-0 forecast contours - extracts the initial forecast time
    snapshot from dye_contours_0 for static map tile visualization.

    Provides a simplified, mobile-optimized version of the current forecast
    without needing to animate through 121 time steps.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    today = datetime.now().strftime("%Y%m%d")

    # Fetch dye_contours_0 which contains hours 0-24
    logger.info("Loading dye_contours_0 from S3")
    data = s3_resource.getFile("tijuana/oceanmodel/output/pfm_dye_contours/dye_contours_0.geojson")

    # The file is an array of 25 time snapshots (hours 0-24)
    # Parse the array and extract the first element (hour 0)
    contours_array = json.loads(data.decode("utf-8"))

    if not isinstance(contours_array, list) or len(contours_array) == 0:
        raise ValueError(f"Expected array of contours, got {type(contours_array)}")

    # Parse hour 0 (first element, which is a JSON-encoded FeatureCollection)
    if isinstance(contours_array[0], str):
        hour0_data = json.loads(contours_array[0])
    else:
        hour0_data = contours_array[0]

    logger.info(f"Extracted hour-0 with {len(hour0_data.get('features', []))} concentration contours")

    # Convert to GeoDataFrame for processing
    gdf = gpd.GeoDataFrame.from_features(hour0_data["features"], crs="EPSG:4326")

    # Simplify geometry for mobile (tolerance ~20 meters)
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.0002, preserve_topology=True)

    # Parse concentration range from title property (e.g., "-5.50--5.25")
    # Convert log10 values to actual percentages
    def parse_concentration_range(title):
        if not title or not isinstance(title, str):
            return None
        parts = title.strip().split("--")
        if len(parts) == 2:
            try:
                log_min = float(parts[0])
                log_max = float(parts[1])
                pct_min = 10 ** log_min
                pct_max = 10 ** log_max
                return {
                    "log_min": log_min,
                    "log_max": log_max,
                    "pct_min": pct_min,
                    "pct_max": pct_max,
                    "pct_range": f"{pct_min:.6f}% - {pct_max:.6f}%"
                }
            except:
                return None
        return None

    gdf["concentration"] = gdf["title"].apply(parse_concentration_range)

    # Store raw archive
    raw_path = f"tijuana/oceanmodel/raw/scripps_pfm/{today}/hour0_contours.geojson"
    raw_geojson = json.dumps(hour0_data)
    s3_resource.putFile_text(data=raw_geojson, path=raw_path, content_type="application/geo+json")

    # Store simplified output
    metadata = store_assets.objectMetadata(
        name="pfm_hour0_contours",
        description="Scripps PFM hour-0 forecast: sewage concentration contours at initial forecast time (19 concentration levels from 0.0005% to 10%)",
        source_url=PFM_PAGE_URL,
    )

    store_assets.store_dataframe_to_s3(
        gdf,
        "tijuana/oceanmodel/output/pfm_hour0_contours",
        "hour0_contours",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath="tijuana/oceanmodel/pfm_hour0_contours",
        formats=["geojson", "csv"],
    )

    # Calculate statistics
    original_size = len(raw_geojson)
    simplified_geojson = gdf.to_json()
    simplified_size = len(simplified_geojson)
    reduction = (1 - simplified_size / original_size) * 100

    context.add_output_metadata({
        "contour_count": len(gdf),
        "original_size_mb": round(original_size / 1024 / 1024, 2),
        "simplified_size_mb": round(simplified_size / 1024 / 1024, 2),
        "size_reduction_pct": round(reduction, 1),
    })

    return gdf


@asset(
    group_name="tijuana",
    key_prefix="oceanmodel",
    name="pfm_shoreline_hazard",
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    deps=[AssetKey(["oceanmodel", "pfm_dye_contours"])], # shoreline_points pulled at same time as contours
)
def pfm_shoreline_hazard(context):
    """
    Scripps PFM shoreline hazard lines - converts dense shoreline points into simplified
    colored LineStrings by risk level for mobile-friendly visualization.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    # Fetch the shoreline points from S3
    logger.info("Loading shoreline points from S3")
    data = s3_resource.getFile("tijuana/oceanmodel/output/pfm_dye_contours/shoreline_points.geojson")
    shoreline_data = json.loads(data.decode("utf-8"))

    # shoreline_data is an array of 121 time snapshots (same structure as dye_contours).
    # Use only snapshot 0 (hour-0 / current conditions) to get an ordered south→north
    # sequence of shoreline points without spurious cross-snapshot jumps.
    fc_str = shoreline_data[0]
    if isinstance(fc_str, str):
        feature_collection = json.loads(fc_str)
    else:
        feature_collection = fc_str

    all_points = [
        f for f in feature_collection.get("features", [])
        if f["geometry"]["type"] == "Point"
    ]
    logger.info(f"Using snapshot 0: {len(all_points)} shoreline points")

    # Build line segments: a new segment starts each time risk changes
    all_lines = []
    segment_id = 0
    i = 0
    while i < len(all_points):
        current_risk = all_points[i]["properties"].get("risk", "unknown")
        coords = [all_points[i]["geometry"]["coordinates"]]
        i += 1
        while i < len(all_points):
            next_risk = all_points[i]["properties"].get("risk", "unknown")
            if next_risk != current_risk:
                # Bridge: include this point in the current segment so lines connect
                coords.append(all_points[i]["geometry"]["coordinates"])
                break
            coords.append(all_points[i]["geometry"]["coordinates"])
            i += 1

        if len(coords) >= 2:
            all_lines.append({
                "type": "Feature",
                "properties": {"risk": current_risk, "segment_id": segment_id},
                "geometry": {"type": "LineString", "coordinates": coords},
            })
            segment_id += 1

    # Create GeoDataFrame from LineStrings
    gdf = gpd.GeoDataFrame.from_features(all_lines, crs="EPSG:4326")
    logger.info(f"Created {len(gdf)} line segments from shoreline points")

    # Simplify for mobile (tolerance ~15 meters in degrees, roughly 0.00015 at this latitude)
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.00015, preserve_topology=True)

    # Add color mapping for visualization
    color_map = {"red": "#FF0000", "yellow": "#FFFF00", "green": "#00FF00", "unknown": "#808080"}
    gdf["color"] = gdf["risk"].map(color_map)
    gdf["Icons"] = "line"  # Use line icon for rendering

    metadata = store_assets.objectMetadata(
        name="pfm_shoreline_hazard",
        description="Scripps PFM simplified shoreline hazard lines colored by risk level (red/yellow/green)",
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
        "risk_levels": gdf["risk"].value_counts().to_dict(),
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
        hashes = fetch_pfm_file_hashes()
        fingerprint = hashlib.md5(
            json.dumps(sorted(hashes.items())).encode()
        ).hexdigest()

        last_fingerprint = context.cursor or None
        if fingerprint != last_fingerprint:
            context.update_cursor(fingerprint)
            context.log.info(f"PFM site updated. New fingerprint: {fingerprint}")
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
