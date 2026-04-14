import io
import os

import geopandas as gpd
import pandas as pd
import requests
from dagster import asset, AutomationCondition, AssetKey, get_dagster_logger
from resilient_core.utils import store_assets

# Direct GeoJSON URLs — no zip extraction needed, readable via requests + BytesIO
NE_STATES_URL = os.environ.get(
    "NE_STATES_URL",
    "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_1_states_provinces.geojson",
)
# TIGERweb REST API returns GeoJSON directly for California (FIPS 06) counties
TIGER_CA_COUNTIES_URL = os.environ.get(
    "TIGER_CA_COUNTIES_URL",
    "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/State_County/MapServer/1/query"
    "?where=STATE%3D%2706%27&outFields=NAME&f=geojson&resultRecordCount=100",
)

# Jurisdictions that are California counties, not states
_COUNTY_JURISDICTIONS = {"San Diego", "Los Angeles", "San Francisco"}


def _fetch_geodataframe(url: str, timeout: int = 60) -> gpd.GeoDataFrame:
    """Fetch a GeoJSON URL and return a GeoDataFrame.

    Uses requests so that both plain GeoJSON files and REST API endpoints
    work reliably without relying on GDAL's /vsicurl/ handler.
    """
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return gpd.read_file(io.BytesIO(resp.content))

s3_aggregated_path = 'pathogens/diseases/mpox/aggregated'
s3_aggregated_latest = 'pathogens/mpox/aggregated'

_MPOX_SOURCES = [
    {
        'name': 'cdc',
        'path': 'pathogens/mpox/usa/mpox_usa_weekly_basic.csv',
        'latest': True,
        'is_base': True,
    },
    {
        'name': 'california',
        'path': 'pathogens/mpox/california/mpox_california_weekly_basic.csv',
        'latest': True,
        'replaces_jurisdiction': 'California',
    },
    {
        'name': 'sandiego',
        'path': 'pathogens/mpox/california/mpox_sd_weekly_basic.csv',
        'latest': True,
    },
    {
        'name': 'los_angeles',
        'path': 'pathogens/mpox/california/mpox_la_weekly_basic.csv',
        'latest': True,
    },
    {
        'name': 'san_francisco',
        'path': 'pathogens/mpox/california/mpox_sf_weekly_basic.csv',
        'latest': True,
    },
]

_COLUMNS = ['Jurisdiction', 'date_week_start', 'date_week_end', 'Week_Number',
            'Year', 'Week_Year', 'Cases', 'Week_Type', 'source_name']


def _load_source(source, s3_resource, latest_base, logger):
    """Load a single source CSV from S3, returning a DataFrame or None."""
    s3_path = f"{latest_base}/{source['path']}" if source.get('latest', True) else source['path']
    try:
        raw = s3_resource.getFile(s3_path)
        df = pd.read_csv(io.BytesIO(raw) if isinstance(raw, bytes) else io.StringIO(raw.decode('utf-8')))
        df['source_name'] = source['name']
        logger.info(f"Loaded {len(df)} rows from {source['name']} ({s3_path})")
        return df
    except Exception as e:
        logger.error(f"Could not load {source['name']} from {s3_path}: {e}")
        return None


@asset(
    group_name="pathogens",
    key_prefix="mpox",
    name="mpox_aggregated",
    deps=[
        AssetKey(["sandiego", "sd_mpox"]),
        AssetKey(["mpox", "mpox_la_weekly"]),
        AssetKey(["mpox", "mpox_sf_weekly"]),
        AssetKey(["cdc", "mpox_california_weekly"]),
        AssetKey(["cdc", "mpox_weekly"]),
    ],
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    description="Aggregated MPOX dataset merging San Diego, Los Angeles, San Francisco, and CDC outputs",
)
def mpox_aggregated(context):
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    latest_base = store_assets.get_latest_basepath()

    # Step 1: Load the base source (CDC) first
    base_source = next(s for s in _MPOX_SOURCES if s.get('is_base'))
    base_df = _load_source(base_source, s3_resource, latest_base, logger)
    if base_df is None:
        raise ValueError("Could not load base CDC data — cannot build mpox_aggregated")

    # Step 2: Process override and additive sources
    other_sources = [s for s in _MPOX_SOURCES if not s.get('is_base')]
    additive_frames = []

    for source in other_sources:
        df = _load_source(source, s3_resource, latest_base, logger)
        if df is None:
            continue

        replaces = source.get('replaces_jurisdiction')
        if replaces:
            # Drop matching jurisdiction rows from base, replace with this source
            n_before = len(base_df)
            base_df = base_df[base_df['Jurisdiction'] != replaces]
            n_dropped = n_before - len(base_df)
            logger.info(f"Replaced {n_dropped} '{replaces}' rows from base with {len(df)} rows from {source['name']}")
        additive_frames.append(df)

    # Step 3: Combine base + all other sources
    combined = pd.concat([base_df] + additive_frames, ignore_index=True)

    # Ensure standard columns exist
    for col in _COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA
    aggregated = combined[_COLUMNS]
    logger.info(f"Aggregated {len(aggregated)} total rows")

    name = 'mpox_aggregated'
    metadata = store_assets.objectMetadata(
        name=name,
        description='Aggregated MPOX dataset combining San Diego, Los Angeles, San Francisco, and CDC weekly case data',
        source_url='https://data.cdc.gov/resource/x9gk-5huc.geojson'
    )

    store_assets.store_dataframe_to_s3(
        df=aggregated,
        path=s3_aggregated_path,
        dataset_identifier=name,
        s3_resource=s3_resource,
        metadata=metadata,
        formats=['csv', 'parquet'],
        enable_latest_path=True,
        latestdatasetpath=s3_aggregated_latest,
    )


@asset(
    group_name="pathogens",
    key_prefix="mpox",
    name="mpox_aggregated_geo",
    deps=[AssetKey(["mpox", "mpox_aggregated"])],
    required_resource_keys={"s3"},
    automation_condition=AutomationCondition.eager(),
    description=(
        "GeoJSON of aggregated MPOX data — all weeks, one polygon feature per "
        "jurisdiction per week. States use Natural Earth 50m boundaries; "
        "San Diego, Los Angeles, and San Francisco use Census TIGERweb county polygons."
    ),
)
def mpox_aggregated_geo(context):
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    latest_base = store_assets.get_latest_basepath()

    # ── Load aggregated mpox data ─────────────────────────────────────────────
    csv_path = f"{latest_base}/{s3_aggregated_latest}/mpox_aggregated.csv"
    raw = s3_resource.publicUrl(csv_path)
    # raw = s3_resource.getFile(csv_path)
    # df = pd.read_csv(
    #     io.BytesIO(raw) if isinstance(raw, bytes) else io.StringIO(raw.decode("utf-8"))
    # )
    df = pd.read_csv(raw)
    logger.info(f"Loaded {len(df)} rows from mpox_aggregated")

    # ── Load state polygons (Natural Earth via GitHub raw GeoJSON) ───────────
    logger.info("Loading US state boundaries from Natural Earth")
    states_gdf = _fetch_geodataframe(NE_STATES_URL)
    us_states = (
        states_gdf[states_gdf["admin"] == "United States of America"][["name", "geometry"]]
        .copy()
        .to_crs("EPSG:4326")
    )
    us_states['name'] = us_states['name'].str.replace(" ", "")  # Normalize for matching
    # ── Load CA county polygons (Census TIGERweb REST → GeoJSON) ─────────────
    logger.info("Loading California county boundaries from TIGERweb")
    ca_counties = (
        _fetch_geodataframe(TIGER_CA_COUNTIES_URL)[["NAME", "geometry"]]
        .copy()
        .rename(columns={"NAME": "name"})
        .to_crs("EPSG:4326")
    )
    ca_counties['name'] = ca_counties['name'].str.replace(" ", "")

    # ── Merge polygons onto mpox data ─────────────────────────────────────────
    county_mask = df["Jurisdiction"].isin(_COUNTY_JURISDICTIONS)
    df_states_geo = df[~county_mask].merge(us_states, left_on="Jurisdiction", right_on="name", how="left")
    df_counties_geo = df[county_mask].merge(ca_counties, left_on="Jurisdiction", right_on="name", how="left")

    combined = pd.concat([df_states_geo, df_counties_geo], ignore_index=True).drop(
        columns=["name"], errors="ignore"
    )

    n_before = len(combined)
    combined = combined.dropna(subset=["geometry"])
    n_missing = n_before - len(combined)
    if n_missing:
        logger.warning(f"{n_missing} rows had no matching geometry and were dropped")

    gdf = gpd.GeoDataFrame(combined, geometry="geometry", crs="EPSG:4326")
    logger.info(f"Created GeoDataFrame with {len(gdf)} features across {gdf['Jurisdiction'].nunique()} jurisdictions")

    # ── Store to S3 ───────────────────────────────────────────────────────────
    geo_metadata = store_assets.objectMetadata(
        name="mpox_aggregated_geo",
        description="Aggregated MPOX data with state and county geometries (all weeks)",
        source_url="https://data.cdc.gov/resource/x9gk-5huc.geojson",
    )
    store_assets.geodataframe_to_s3(
        gdf,
        f"{s3_aggregated_path}_geo/mpox_aggregated_geo",
        s3_resource,
        metadata=geo_metadata,
    )
    store_assets.geodataframe_to_s3(
        gdf,
        f"{s3_aggregated_latest}/mpox_aggregated_geo",
        s3_resource,
        metadata=geo_metadata,
    )

    return gdf
