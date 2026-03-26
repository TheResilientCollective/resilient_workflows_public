import io
import pandas as pd
from dagster import asset, AutomationCondition, AssetKey, get_dagster_logger
from ..utils import store_assets

s3_aggregated_path = 'pathogens/diseases/mpox/aggregated'
s3_aggregated_latest = 'pathogens/mpox/aggregated'

_MPOX_SOURCES = [
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
    {
        'name': 'cdc',
        'path': 'pathogens/mpox/usa/mpox_usa_weekly_basic.csv',
        'latest': True,
    },
    {
        'name': 'california',
        'path': 'pathogens/mpox/california/mpox_california_weekly_basic.csv',
        'latest': True,
    },
]


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

    frames = []
    for source in _MPOX_SOURCES:
        s3_path = f"{latest_base}/{source['path']}" if source.get('latest', True) else source['path']
        try:
            raw = s3_resource.getFile(s3_path)
            df = pd.read_csv(io.BytesIO(raw) if isinstance(raw, bytes) else io.StringIO(raw.decode('utf-8')))
            df['source_name'] = source['name']
            frames.append(df)
            logger.info(f"Loaded {len(df)} rows from {source['name']} ({s3_path})")
        except Exception as e:
            logger.error(f"Could not load {source['name']} from {s3_path}: {e}")

    if not frames:
        raise ValueError("No mpox source data could be loaded — cannot build mpox_aggregated")

    _COLUMNS = ['Jurisdiction', 'date_week_start', 'date_week_end', 'Week_Number',
                'Year', 'Week_Year', 'Cases', 'Week_Type', 'source_name']

    combined = pd.concat(frames, ignore_index=True)
    # Keep only the standard columns; fill any missing ones with NaN
    for col in _COLUMNS:
        if col not in combined.columns:
            combined[col] = pd.NA
    aggregated = combined[_COLUMNS]
    logger.info(f"Aggregated {len(aggregated)} rows from {len(frames)} sources")

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