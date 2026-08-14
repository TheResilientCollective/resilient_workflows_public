"""VectorSurv (maps.vectorsurv.org) vector-borne disease surveillance assets.

Pulls the public map-data API that backs https://maps.vectorsurv.org/arbo
(``https://mathew.vectorsurv.org/v2``) and produces California / San Diego
focused datasets under the ``pathogens/vectorborne`` S3 path:

* ``vectorsurv_invasive_regions`` — invasive *Aedes* surveillance subcounty
  polygons for California (decoded from Google encoded polylines) with
  per-species detection summaries. Stored as GeoJSON + CSV.
* ``vectorsurv_mosquito_counts`` — weekly invasive-mosquito trap counts by
  species for every California subcounty region, all years (2010–present),
  from the per-region ``invasive/subcounty2`` endpoint. Stored as CSV +
  Parquet.
* ``vectorsurv_dengue_risk`` — dengue transmission-risk subcounty polygons
  for California (GeoJSON) plus the full weekly risk-index time series
  (CSV + Parquet).
* ``vectorsurv_arbovirus_activity`` — arbovirus surveillance activity
  (WNV/SLEV/WEEV/... positive mosquito pools, dead birds, sentinel
  seroconversions) by city and month for all years, filtered to California.
  Stored as CSV + Parquet + a GeoJSON point layer.

Geometry note: VectorSurv serves polygons as ``EncodedPolyline`` geometries
(Google polyline5 encoding, one encoded string per ring). These are decoded
here into standard GeoJSON MultiPolygons so downstream consumers get plain
GeoJSON.

The mosquito-abundance (AMoR) endpoints of the same API are restricted to
VectorSurv Gateway account holders and are intentionally not scraped here;
the invasive weekly counts and arbovirus activity layers are the public
"mosquito counts" sources.
"""

import json
import os
import time
from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests
from dagster import (
    AssetIn,
    AssetKey,
    AutomationCondition,
    Output,
    RunRequest,
    asset,
    define_asset_job,
    get_dagster_logger,
    schedule,
)
from shapely.geometry import MultiPolygon, Point, Polygon

from resilient_core.utils import store_assets

VECTORSURV_API = "https://mathew.vectorsurv.org/v2"
VECTORSURV_MAPS_URL = "https://maps.vectorsurv.org"

S3_RAW_PATH = "pathogens/vectorborne/raw/vectorsurv"
S3_OUTPUT_PATH = "pathogens/vectorborne/output"
VECTORSURV_LATEST_PATH = "pathogens/vectorborne/vectorsurv"

# The API mixes agencies from many states without a state attribute on the
# dengue / invasive layers, so features are filtered spatially to California.
CA_BBOX = {"min_lat": 32.3, "max_lat": 42.1, "min_lon": -124.6, "max_lon": -114.1}

# Optional comma-separated county filter for the (slow) per-region weekly
# counts pull, e.g. "San Diego,Imperial". Empty/unset = all California.
COUNTY_FILTER_ENV = "VECTORSURV_COUNTIES"

REQUEST_TIMEOUT = 120
REQUEST_DELAY_SECONDS = 0.1

# Species keys used by the invasive/subcounty2 weekly records -> full name.
INVASIVE_SPECIES = {
    "aegypti": "Aedes aegypti",
    "albopictus": "Aedes albopictus",
    "atropalpus": "Aedes atropalpus",
    "bahamensis": "Aedes bahamensis",
    "biscaynensis": "Aedes biscaynensis",
    "coronator": "Aedes coronator",
    "declarator": "Aedes declarator",
    "notoscriptus": "Aedes notoscriptus",
    "interrogator": "Aedes interrogator",
    "japonicus": "Aedes japonicus",
    "lactator": "Aedes lactator",
    "panocossa": "Aedes panocossa",
    "scapularis": "Aedes scapularis",
    "squamipennis": "Aedeomyia squamipennis",
}

# Arbo layer key -> (pathogen, indicator). Layers come from /v2/arbo/.
ARBO_LAYERS = {
    "WNVPools": ("West Nile virus", "positive_mosquito_pools"),
    "WNVBirds": ("West Nile virus", "dead_birds"),
    "WNVSentinels": ("West Nile virus", "sentinel_seroconversions"),
    "SLEVPools": ("St. Louis encephalitis virus", "positive_mosquito_pools"),
    "SLEVSentinels": ("St. Louis encephalitis virus", "sentinel_seroconversions"),
    "WEEVPools": ("Western equine encephalitis virus", "positive_mosquito_pools"),
    "WEEVSentinels": ("Western equine encephalitis virus", "sentinel_seroconversions"),
    "CVVPools": ("Cache Valley virus", "positive_mosquito_pools"),
    "LACVPools": ("La Crosse virus", "positive_mosquito_pools"),
    "JCVPools": ("Jamestown Canyon virus", "positive_mosquito_pools"),
    "EEEVPools": ("Eastern equine encephalitis virus", "positive_mosquito_pools"),
    "FLAVPools": ("Flanders virus", "positive_mosquito_pools"),
}


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def decode_polyline(encoded: str):
    """Decode a Google polyline5 string into a list of (lon, lat) tuples."""
    coords = []
    index = lat = lon = 0
    length = len(encoded)
    while index < length:
        for is_lon in (False, True):
            shift = result = 0
            while True:
                b = ord(encoded[index]) - 63
                index += 1
                result |= (b & 0x1F) << shift
                shift += 5
                if b < 0x20:
                    break
            delta = ~(result >> 1) if result & 1 else result >> 1
            if is_lon:
                lon += delta
            else:
                lat += delta
        coords.append((lon * 1e-5, lat * 1e-5))
    return coords


def encoded_polyline_to_multipolygon(geometry: dict) -> MultiPolygon:
    """Convert a VectorSurv ``EncodedPolyline`` geometry to a MultiPolygon.

    ``coordinates`` is a list of polygons, each polygon a list of rings, each
    ring a dict with an encoded ``point`` string (first ring = exterior).
    """
    polygons = []
    for polygon_rings in geometry.get("coordinates", []):
        rings = []
        for ring in polygon_rings:
            points = decode_polyline(ring["point"])
            if len(points) >= 3:
                rings.append(points)
        if rings:
            polygons.append(Polygon(rings[0], rings[1:]))
    return MultiPolygon(polygons)


def in_california(geom) -> bool:
    point = geom.representative_point()
    return (
        CA_BBOX["min_lat"] <= point.y <= CA_BBOX["max_lat"]
        and CA_BBOX["min_lon"] <= point.x <= CA_BBOX["max_lon"]
    )


def _get_json(url: str, logger=None, retries: int = 3):
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:  # noqa: BLE001 - retry any network/JSON error
            if attempt == retries - 1:
                raise
            if logger:
                logger.warning(f"retry {attempt + 1} for {url}: {e}")
            time.sleep(2 ** attempt)
    return None


# ---------------------------------------------------------------------------
# Pure transforms (unit-testable without Dagster/S3)
# ---------------------------------------------------------------------------

def invasive_features_to_gdf(feature_collection: dict) -> gpd.GeoDataFrame:
    """California invasive-Aedes subcounty polygons with detection summaries."""
    rows = []
    geometries = []
    for feature in feature_collection.get("features", []):
        try:
            geom = encoded_polyline_to_multipolygon(feature["geometry"])
        except Exception:
            continue
        if geom.is_empty or not in_california(geom):
            continue
        props = feature.get("properties", {})
        row = {
            "region_id": props.get("id"),
            "agency": props.get("agency"),
            "county": props.get("county"),
            "subcounty": props.get("subcounty"),
            "surveillance_start": props.get("surveillance_start"),
            "website": props.get("website"),
        }
        for key in INVASIVE_SPECIES:
            for suffix in ("detections", "first_found", "last_found"):
                value = props.get(f"{key}_{suffix}")
                row[f"{key}_{suffix}"] = None if value in ("None", "") else value
        rows.append(row)
        geometries.append(geom)
    gdf = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    for key in INVASIVE_SPECIES:
        gdf[f"{key}_detections"] = pd.to_numeric(
            gdf[f"{key}_detections"], errors="coerce"
        ).fillna(0).astype(int)
    return gdf


def subcounty_detail_to_rows(detail: dict, region: dict) -> list:
    """Flatten one invasive/subcounty2 response into weekly count rows.

    The response is quarters -> months -> weeks keyed by index since 2010;
    every level carries an explicit date, so only dates are used. Weeks with
    no reported counts for any species (all null) are skipped — those are
    weeks without surveillance, while 0 means surveyed and none collected.
    """
    rows = []
    for quarter in detail.values():
        for month in quarter.get("months", {}).values():
            for week_index, week in month.get("weeks", {}).items():
                counts = {
                    key: week.get(key)
                    for key in list(INVASIVE_SPECIES) + ["other"]
                }
                if all(value is None for value in counts.values()):
                    continue
                date = pd.to_datetime(week.get("date"), errors="coerce")
                if pd.isna(date):
                    continue
                row = {
                    "week_start": date.date().isoformat(),
                    "year": date.year,
                    "week_index": int(week_index),
                    "week_days": pd.to_numeric(
                        week.get("week_days"), errors="coerce"
                    ),
                    "region_id": region.get("region_id"),
                    "agency": region.get("agency"),
                    "county": region.get("county"),
                    "subcounty": region.get("subcounty"),
                    "aegypti_growth_index": week.get("aegyptiGrowth"),
                    "albopictus_growth_index": week.get("alboGrowth"),
                }
                row.update(counts)
                rows.append(row)
    return rows


def dengue_features_to_gdf_and_series(feature_collection: dict):
    """California dengue-risk polygons (latest risk) plus the weekly series."""
    rows = []
    geometries = []
    series_rows = []
    for feature in feature_collection.get("features", []):
        try:
            geom = encoded_polyline_to_multipolygon(feature["geometry"])
        except Exception:
            continue
        if geom.is_empty or not in_california(geom):
            continue
        props = feature.get("properties", {})
        risk = props.get("risk") or {}
        latest_week = None
        for week_index, entry in risk.items():
            if not isinstance(entry, dict):
                continue
            date = pd.to_datetime(entry.get("date"), errors="coerce")
            if pd.isna(date):
                continue
            series_rows.append(
                {
                    "region_id": props.get("id"),
                    "county": props.get("county"),
                    "subcounty": props.get("subcounty"),
                    "week_index": int(week_index),
                    "week_start": date.date().isoformat(),
                    "year": date.year,
                    "risk_index": entry.get("risk"),
                }
            )
            if latest_week is None or int(week_index) > latest_week[0]:
                latest_week = (int(week_index), date, entry.get("risk"))
        rows.append(
            {
                "region_id": props.get("id"),
                "county": props.get("county"),
                "subcounty": props.get("subcounty"),
                "latest_risk_index": latest_week[2] if latest_week else None,
                "latest_risk_date": (
                    latest_week[1].date().isoformat() if latest_week else None
                ),
            }
        )
        geometries.append(geom)
    gdf = gpd.GeoDataFrame(rows, geometry=geometries, crs="EPSG:4326")
    series = pd.DataFrame(series_rows)
    if not series.empty:
        series = series.sort_values(
            ["county", "subcounty", "week_index"]
        ).reset_index(drop=True)
    return gdf, series


def arbo_payload_to_df(payload: dict, states=("CA",)) -> pd.DataFrame:
    """Flatten /v2/arbo/ into monthly counts per pathogen/indicator/city."""
    rows = []
    for layer, collection in payload.items():
        pathogen, indicator = ARBO_LAYERS.get(layer, (layer, layer))
        for feature in collection.get("features", []):
            props = feature.get("properties", {})
            if states and props.get("state") not in states:
                continue
            geometry = feature.get("geometry", {})
            coords = geometry.get("coordinates", [None, None])
            monthly = {}
            for entry in props.get("collections", []):
                if not isinstance(entry, dict):
                    continue
                for label, value in entry.items():
                    month = pd.to_datetime(label, format="%b %Y", errors="coerce")
                    if pd.isna(month):
                        continue
                    bucket = monthly.setdefault(
                        month, {"count": 0, "records": 0}
                    )
                    bucket["count"] += value or 0
                    bucket["records"] += 1
            for month, bucket in monthly.items():
                rows.append(
                    {
                        "pathogen": pathogen,
                        "indicator": indicator,
                        "layer": layer,
                        "city": props.get("city"),
                        "state": props.get("state"),
                        "longitude": coords[0],
                        "latitude": coords[1],
                        "month_start": month.date().isoformat(),
                        "year": month.year,
                        "month": month.month,
                        "count": bucket["count"],
                        "n_records": bucket["records"],
                    }
                )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(
            ["pathogen", "indicator", "city", "month_start"]
        ).reset_index(drop=True)
    return df


def _metadata(context, name, description=None):
    """schema.org Dataset metadata via store_assets.objectMetadata.

    Defaults to the asset's own description/source; pass ``description`` for
    companion datasets (raw snapshots, weekly series, point layers) stored
    alongside the primary output.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    return store_assets.objectMetadata(
        name=name,
        description=description or meta.get("description"),
        source_url=meta.get("source"),
    )


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------

@asset(
    group_name="vectorborne",
    key_prefix="vectorsurv",
    name="vectorsurv_invasive_regions",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{VECTORSURV_API}/invasive/static2",
        "description": (
            "Invasive Aedes surveillance subcounty regions for California from "
            "VectorSurv (maps.vectorsurv.org/invasive). Polygons decoded from "
            "encoded polylines to GeoJSON, with per-species detection "
            "summaries (Aedes aegypti, albopictus, notoscriptus, ...)."
        ),
    },
)
def vectorsurv_invasive_regions(context) -> Output:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    payload = _get_json(f"{VECTORSURV_API}/invasive/static2", logger)
    store_assets.raw_to_s3(
        json.dumps(payload).encode(),
        f"{S3_RAW_PATH}/invasive_static2.json",
        s3_resource,
        contenttype="application/json",
        metadata=_metadata(
            context,
            "vectorsurv_invasive_static2_raw",
            description=(
                "Raw VectorSurv invasive/static2 payload (all states, encoded "
                "polyline geometries) as fetched from the API."
            ),
        ),
    )

    gdf = invasive_features_to_gdf(payload)
    logger.info(
        f"{len(gdf)} California invasive regions "
        f"({gdf['county'].nunique()} counties)"
    )

    metadata = _metadata(context, "vectorsurv_invasive_regions")
    store_assets.store_dataframe_to_s3(
        gdf,
        f"{S3_OUTPUT_PATH}/vectorsurv_invasive_regions",
        "vectorsurv_invasive_regions",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["geojson", "csv"],
    )
    return Output(
        gdf,
        metadata={
            "regions": len(gdf),
            "counties": gdf["county"].nunique(),
            "aegypti_detections_total": int(gdf["aegypti_detections"].sum()),
        },
    )


@asset(
    group_name="vectorborne",
    key_prefix="vectorsurv",
    name="vectorsurv_mosquito_counts",
    ins={
        "vectorsurv_invasive_regions": AssetIn(
            key=AssetKey(["vectorsurv", "vectorsurv_invasive_regions"])
        )
    },
    required_resource_keys={"s3"},
    metadata={
        "source": f"{VECTORSURV_API}/invasive/subcounty2/",
        "description": (
            "Weekly invasive-mosquito trap counts by species for California "
            "subcounty surveillance regions, all years (2010-present), from "
            "VectorSurv. Weeks with no surveillance are omitted; a 0 means "
            "surveyed with none collected. Includes Aedes aegypti/albopictus "
            "weather-driven growth indexes. Set the VECTORSURV_COUNTIES env "
            "var (comma separated) to restrict the pull, e.g. 'San Diego'."
        ),
    },
    automation_condition=AutomationCondition.eager(),
)
def vectorsurv_mosquito_counts(context, vectorsurv_invasive_regions) -> Output:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    regions = vectorsurv_invasive_regions.drop(columns="geometry")
    county_filter = [
        c.strip()
        for c in os.environ.get(COUNTY_FILTER_ENV, "").split(",")
        if c.strip()
    ]
    if county_filter:
        regions = regions[regions["county"].isin(county_filter)]
        logger.info(f"county filter {county_filter}: {len(regions)} regions")

    all_rows = []
    failures = []
    for _, region in regions.iterrows():
        url = (
            f"{VECTORSURV_API}/invasive/subcounty2/"
            f"{requests.utils.quote(str(region['agency']))}/"
            f"{requests.utils.quote(str(region['subcounty']))}/"
            f"{region['region_id']}"
        )
        try:
            detail = _get_json(url, logger)
            all_rows.extend(subcounty_detail_to_rows(detail, region.to_dict()))
        except Exception as e:  # noqa: BLE001 - keep going, report at the end
            failures.append(f"{region['agency']}/{region['subcounty']}: {e}")
        time.sleep(REQUEST_DELAY_SECONDS)

    if failures:
        logger.warning(
            f"{len(failures)} regions failed: " + "; ".join(failures[:10])
        )
    if not all_rows:
        raise RuntimeError("no weekly mosquito counts extracted from VectorSurv")

    df = pd.DataFrame(all_rows).sort_values(
        ["county", "subcounty", "week_start"]
    ).reset_index(drop=True)
    logger.info(
        f"{len(df)} weekly rows across {df['region_id'].nunique()} regions, "
        f"{df['week_start'].min()} - {df['week_start'].max()}"
    )

    metadata = _metadata(context, "vectorsurv_mosquito_counts")
    store_assets.store_dataframe_to_s3(
        df,
        f"{S3_OUTPUT_PATH}/vectorsurv_mosquito_counts",
        "vectorsurv_mosquito_counts",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    return Output(
        df,
        metadata={
            "rows": len(df),
            "regions": int(df["region_id"].nunique()),
            "first_week": str(df["week_start"].min()),
            "last_week": str(df["week_start"].max()),
            "failed_regions": len(failures),
        },
    )


@asset(
    group_name="vectorborne",
    key_prefix="vectorsurv",
    name="vectorsurv_dengue_risk",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{VECTORSURV_API}/dengue",
        "description": (
            "Dengue transmission-risk subcounty regions for California from "
            "VectorSurv (maps.vectorsurv.org/dengue). GeoJSON polygons carry "
            "the most recent weekly risk index; the companion "
            "vectorsurv_dengue_risk_weekly CSV/Parquet table has the full "
            "weekly risk-index series per region."
        ),
    },
)
def vectorsurv_dengue_risk(context) -> Output:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    payload = _get_json(f"{VECTORSURV_API}/dengue", logger)
    store_assets.raw_to_s3(
        json.dumps(payload).encode(),
        f"{S3_RAW_PATH}/dengue.json",
        s3_resource,
        contenttype="application/json",
        metadata=_metadata(
            context,
            "vectorsurv_dengue_raw",
            description=(
                "Raw VectorSurv dengue payload (all states, encoded polyline "
                "geometries, weekly risk series) as fetched from the API."
            ),
        ),
    )

    gdf, series = dengue_features_to_gdf_and_series(payload)
    logger.info(
        f"{len(gdf)} California dengue regions, {len(series)} weekly risk rows"
    )

    metadata = _metadata(context, "vectorsurv_dengue_risk")
    store_assets.store_dataframe_to_s3(
        gdf,
        f"{S3_OUTPUT_PATH}/vectorsurv_dengue_risk",
        "vectorsurv_dengue_risk",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["geojson", "csv"],
    )
    store_assets.store_dataframe_to_s3(
        series,
        f"{S3_OUTPUT_PATH}/vectorsurv_dengue_risk",
        "vectorsurv_dengue_risk_weekly",
        s3_resource,
        metadata=_metadata(
            context,
            "vectorsurv_dengue_risk_weekly",
            description=(
                "Weekly dengue transmission-risk index time series per "
                "California subcounty surveillance region from VectorSurv "
                "(week_start, risk_index; 0 = no risk)."
            ),
        ),
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    return Output(
        gdf,
        metadata={
            "regions": len(gdf),
            "weekly_rows": len(series),
            "counties": gdf["county"].nunique(),
        },
    )


@asset(
    group_name="vectorborne",
    key_prefix="vectorsurv",
    name="vectorsurv_arbovirus_activity",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{VECTORSURV_API}/arbo/",
        "description": (
            "Arbovirus surveillance activity from VectorSurv "
            "(maps.vectorsurv.org/arbo): monthly counts of positive mosquito "
            "pools, dead birds and sentinel seroconversions by city for WNV, "
            "SLEV, WEEV and other arboviruses, all years, filtered to "
            "California."
        ),
    },
)
def vectorsurv_arbovirus_activity(context) -> Output:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    payload = _get_json(f"{VECTORSURV_API}/arbo/", logger)
    store_assets.raw_to_s3(
        json.dumps(payload).encode(),
        f"{S3_RAW_PATH}/arbo.json",
        s3_resource,
        contenttype="application/json",
        metadata=_metadata(
            context,
            "vectorsurv_arbo_raw",
            description=(
                "Raw VectorSurv arbo payload: per-city point features with "
                "monthly collection counts for 12 arbovirus surveillance "
                "layers (all states), as fetched from the API."
            ),
        ),
    )

    df = arbo_payload_to_df(payload, states=("CA",))
    if df.empty:
        raise RuntimeError("no California arbovirus activity rows extracted")
    logger.info(
        f"{len(df)} monthly rows, {df['year'].min()}-{df['year'].max()}, "
        f"pathogens: {sorted(df['pathogen'].unique())}"
    )

    metadata = _metadata(context, "vectorsurv_arbovirus_activity")
    store_assets.store_dataframe_to_s3(
        df,
        f"{S3_OUTPUT_PATH}/vectorsurv_arbovirus_activity",
        "vectorsurv_arbovirus_activity",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )

    # Point layer aggregated per city/pathogen/indicator for mapping.
    points = (
        df.groupby(
            ["pathogen", "indicator", "layer", "city", "state",
             "longitude", "latitude"],
            as_index=False,
        )
        .agg(
            total_count=("count", "sum"),
            first_year=("year", "min"),
            last_year=("year", "max"),
        )
    )
    points_gdf = gpd.GeoDataFrame(
        points,
        geometry=[Point(xy) for xy in zip(points["longitude"], points["latitude"])],
        crs="EPSG:4326",
    )
    store_assets.store_dataframe_to_s3(
        points_gdf,
        f"{S3_OUTPUT_PATH}/vectorsurv_arbovirus_activity",
        "vectorsurv_arbovirus_activity_points",
        s3_resource,
        metadata=_metadata(
            context,
            "vectorsurv_arbovirus_activity_points",
            description=(
                "GeoJSON point layer of California arbovirus surveillance "
                "activity aggregated per city, pathogen and indicator "
                "(total_count, first_year, last_year) from VectorSurv."
            ),
        ),
        latestdatasetpath=VECTORSURV_LATEST_PATH,
        enable_latest_path=True,
        formats=["geojson", "csv"],
    )
    return Output(
        df,
        metadata={
            "rows": len(df),
            "cities": int(df["city"].nunique()),
            "years": f"{int(df['year'].min())}-{int(df['year'].max())}",
        },
    )


# ---------------------------------------------------------------------------
# Job + weekly schedule
# ---------------------------------------------------------------------------

vectorsurv_weekly_job = define_asset_job(
    "vectorsurv_weekly_job",
    selection=[
        AssetKey(["vectorsurv", "vectorsurv_invasive_regions"]),
        AssetKey(["vectorsurv", "vectorsurv_mosquito_counts"]),
        AssetKey(["vectorsurv", "vectorsurv_dengue_risk"]),
        AssetKey(["vectorsurv", "vectorsurv_arbovirus_activity"]),
    ],
)


@schedule(
    job=vectorsurv_weekly_job,
    cron_schedule="0 6 * * 1",
    name="vectorsurv_weekly_schedule",
    execution_timezone="America/Los_Angeles",
)
def vectorsurv_weekly_schedule(context):
    """VectorSurv refreshes continuously; Monday 6am PT catches the prior week."""
    return RunRequest(run_key=f"vectorsurv_{datetime.now().date().isoformat()}")
