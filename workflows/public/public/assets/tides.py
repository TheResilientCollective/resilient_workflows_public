import pandas as pd
from noaa_coops import Station
import datetime
from dagster import (asset,
                     get_dagster_logger,
                     define_asset_job, AssetKey,
                     RunRequest,
                     schedule,
                     MonthlyPartitionsDefinition, AssetIn, AutomationCondition, Field, AssetExecutionContext
                     )
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from ..resources import minio
from ..utils import store_assets

# verified is a couple months delayed.
end_date_month= (datetime.datetime.now() - datetime.timedelta(days=60)).strftime('%Y-%m-%d')

# Define monthly partitions starting from a specific date
monthly_partitions = MonthlyPartitionsDefinition (
    cron_schedule="0 0 1 * *", # Start of each month
    start_date="2023-01-01", # Example start
    end_date=end_date_month
)

STATION_ID = "9410170"
s3_output_path = 'tijuana/tides/output'
s3_latest_path = 'tijuana/tides'
##### tide
def refine_state(row):
    # This logic checks if the current hour is a turning point
    # Since data is hourly, the exact slack might be between hours,
    # but we can label the hour closest to the turn or the turning interval.

    # Simple approach:
    # If height increases then decreases -> High (Slack High)
    # If height decreases then increases -> Low (Slack Low)

    current_diff = row['diff']
    next_diff = row['next_diff']

    if pd.isna(current_diff) or pd.isna(next_diff):
        return row['tidal_state']

    # Check for High Slack (transition from flood to ebb)
    if current_diff > 0 and next_diff < 0:
        return "high"

    # Check for Low Slack (transition from ebb to flood)
    elif current_diff < 0 and next_diff > 0:
        return "low"

    return row['tidal_state']

def _derive_tidal_state(heights: pd.Series) -> pd.Series:
    """Classify each hourly tide height as flood, ebb, slack high, or slack low."""
    states = ['ebb'] * len(heights)
    for i in range(len(heights)):
        h = heights.iloc[i]
        h_prev = heights.iloc[i - 1] if i > 0 else h
        h_next = heights.iloc[i + 1] if i < len(heights) - 1 else h
        if i > 0 and i < len(heights) - 1:
            if h >= h_prev and h >= h_next:
                states[i] = 'slack high'
            elif h <= h_prev and h <= h_next:
                states[i] = 'slack low'
            elif h > h_prev:
                states[i] = 'flood'
            # else: ebb (default)
        elif h > h_prev:
            states[i] = 'flood'
    return pd.Series(states, index=heights.index)

@retry(
    wait=wait_exponential(min=300, max=900),  # First retry at 5 minutes, max 15 minutes
    stop=stop_after_attempt(3),  # Retry up to 3 times
    retry=retry_if_exception_type(Exception),
    reraise=True
)
def tidal_data(station_id, start_date, end_date, product="predictions", datum="MLLW",
    interval="h",
    units="metric",
   # time_zone="gmt"
               ):
    start_hist = start_date
    end_hist = end_date
    station = Station(id=station_id)
    print(f"Fetching historic data from {start_hist} to {end_hist}...")
    try:
        if product=='hourly_height':
            # No interval
            df_hist = station.get_data(
                begin_date=start_hist,
                end_date=end_hist,
                product="hourly_height",  # hourly_height

                datum="MLLW",

                units=units,
                time_zone="gmt"
            )
        # Fetch predictions
        elif product=='predictions':
            df_hist = station.get_data(
                begin_date=start_hist,
                end_date=end_hist,
                product="predictions", #hourly_height

                datum="MLLW",
                interval="h",
                units=units,
                time_zone="gmt"
            )
        else:
            get_dagster_logger().error(f"Unsupported product {product}")
            return None
    except Exception as e:
        get_dagster_logger().error(f"Error fetching data: {e}")
        raise e
    try:
    # Process dataframe matching the requested structure
        df_hist = df_hist.reset_index().rename(columns={'t': 'time', 'v': 'tide_height'})
        df_hist['time'] = pd.to_datetime(df_hist['time'], utc=True, unit='ns')
        df_hist['time'] = df_hist['time'].dt.tz_localize(None)
        df_hist['time'] = df_hist['time'].astype('datetime64[ns]')
        df_hist['time'] = df_hist['time'].dt.tz_localize("UTC")
        df_hist["time"] = df_hist["time"].dt.tz_convert("America/Los_Angeles")
        # Calculate differences to determine state
        df_hist['diff'] = df_hist['tide_height'].diff()

        # Initial state assignment
        df_hist['tidal_state'] = df_hist['diff'].apply(lambda x: 'flood' if x > 0 else ('ebb' if x < 0 else 'slack'))

        # Prepare for refinement (looking ahead to find peaks/troughs)
        df_hist['next_diff'] = df_hist['diff'].shift(-1)

        # Refine state using the previously defined function 'refine_state'
        df_hist['tidal_state'] = df_hist.apply(refine_state, axis=1)

        # specific structure requested
        df_hist_final = df_hist[['time', 'tide_height', 'tidal_state']].copy()
    except Exception as e:
        get_dagster_logger().error(f"Error fetching data: {e}")
        raise e

    return df_hist_final

@asset(group_name="tijuana",key_prefix="tides",
       name="tidal_hourly", required_resource_keys={"s3"},
       metadata={
           "source": "https://api.tidesandcurrents.noaa.gov/api/prod/"
       ,"description":"Hourly Tidal Height data from NOAA with ebb and flood states added"
#,"variableMeasured":variableMeasured
       },
)
def tides_hourly(context,):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url)

    dataset_name = context.asset_key.path[-1]
    """ Data is delayed by a couple  months, so we will use monlthy"""
    date =  datetime.datetime.now()
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    start_date = f'{date.strftime('%Y%m')}01'
    end_date = date.strftime('%Y%m%d')
    tidal_df = tidal_data(station_id=STATION_ID, start_date=start_date, end_date=end_date)
    dataset_id = f"{dataset_name}_{date.strftime('%Y%m')}"
    output_path = f'{s3_output_path}'
    latest_path = f'{s3_latest_path}'
    store_assets.store_dataframe_to_s3(tidal_df, output_path, dataset_id, s3_resource, metadata=metadata
                                       , enable_latest_path=True, latestdatasetpath=latest_path,
                                       formats=['csv', 'parquet'])

@asset(
group_name="tijuana",key_prefix="tides",
       name="tidal_monthly", required_resource_keys={"s3"},
       metadata={
           "source": "https://api.tidesandcurrents.noaa.gov/api/prod/"
       ,"description":"Hourly Tidal Height data from NOAA with ebb and flood states added"
#,"variableMeasured":variableMeasured
       },
    partitions_def=monthly_partitions
)
def tides_monthly(context,):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url)

    dataset_name = context.asset_key.path[-1]
    """ Data is delayed by a couple  months, so we will use monlthy"""
    time_window = context.partition_time_window
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    start_date = time_window.start.strftime('%Y%m%d')
    end_date = time_window.end.strftime('%Y%m%d')
    tidal_df = tidal_data(station_id=STATION_ID, start_date=start_date, end_date=end_date)
    month = time_window.start.strftime('%Y%m')
    dataset_id = f"{dataset_name}_{month}"
    output_path = f'{s3_output_path}'
    latest_path = f'{s3_latest_path}'
    store_assets.store_dataframe_to_s3(tidal_df, output_path, dataset_id, s3_resource, metadata=metadata
                                       , enable_latest_path=True, latestdatasetpath=latest_path,
                                       formats=['csv', 'parquet'])
@asset(
    key_prefix="tides",
    group_name="tijuana",
    name="tidal_forecast",
    required_resource_keys={"s3"},
    kinds={"python", "s3"},
    description="Tidal predictions from NOAA CO-OPS API (Station 9410170, San Diego)",
    config_schema={
        "forecast_days":Field(
            int,
            default_value=10,
            description="Number of days forward to fetch tidal predictions",
        ),
        "noaa_station": Field(
            str,
            default_value="9410170",
            description="NOAA CO-OPS station ID (default: San Diego, CA — closest to Tijuana River mouth)",
        ),
    },

    automation_condition=AutomationCondition.eager(),
)
def tidal_forecast(context: AssetExecutionContext) -> pd.DataFrame:
    """Fetch deterministic hourly tidal predictions from NOAA CO-OPS API.

    Station 9410170 (San Diego) is the closest official NOAA gauge to the
    Tijuana River mouth. Derives tidal_state (flood/ebb/slack high/slack low)
    from the slope of the predicted tide height series.
    Returns columns: time, tide_height, tidal_state
    """
    import json as _json
    import urllib.request

    s3_resource = context.resources.s3
    forecast_days = context.op_config["forecast_days"]
    station = context.op_config["noaa_station"]

    now_utc = pd.Timestamp.utcnow()
    begin_date = now_utc.strftime("%Y%m%d")
    end_date = (now_utc + pd.Timedelta(days=forecast_days)).strftime("%Y%m%d")

    api_url = (
        f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        f"?product=predictions&datum=MLLW&interval=h&units=metric&time_zone=gmt"
        f"&format=json&station={station}&begin_date={begin_date}&end_date={end_date}"
    )

    context.log.info(f"Fetching tidal predictions from NOAA CO-OPS (station {station})...")
    context.log.info(f"  Date range: {begin_date} → {end_date}")

    try:
        with urllib.request.urlopen(api_url, timeout=30) as response:
            data = _json.loads(response.read().decode("utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to fetch NOAA tidal predictions: {e}")

    if "error" in data:
        raise RuntimeError(f"NOAA API error: {data['error'].get('message', data['error'])}")

    predictions = data.get("predictions", [])
    if not predictions:
        raise RuntimeError("NOAA API returned no predictions")

    context.log.info(f"✓ Received {len(predictions)} tidal predictions from NOAA")

    tide_df = pd.DataFrame([
        {"time": pd.to_datetime(p["t"]), "tide_height": float(p["v"])}
        for p in predictions
    ]).sort_values("time").reset_index(drop=True)

    tide_df["tidal_state"] = _derive_tidal_state(tide_df["tide_height"])

    state_counts = tide_df["tidal_state"].value_counts().to_dict()
    context.log.info(f"✓ Tide height range: {tide_df['tide_height'].min():.3f} – {tide_df['tide_height'].max():.3f} m")
    context.log.info(f"  Tidal states: {state_counts}")

    # --- Upload to S3 ---
    csv_path = "latest/tijuana/tidal_forecast/latest.csv"
    try:
        csv_bytes = tide_df.to_csv(index=False).encode("utf-8")
        s3_resource.putFile(csv_bytes, csv_path, bucket=s3_resource.S3_BUCKET, content_type="text/csv")
        context.log.info(f"✓ Uploaded tidal forecast to S3: {csv_path}")
    except Exception as e:
        context.log.warning(f"Could not upload tidal forecast to S3: {e}")

    context.add_output_metadata({
        "row_count": len(tide_df),
        "tide_min": float(tide_df["tide_height"].min()),
        "tide_max": float(tide_df["tide_height"].max()),
        "tidal_states": state_counts,
        "station": station,
    })
    return tide_df

tides_hourly_job = define_asset_job(
    "tides_hourly", selection=[AssetKey(["tides", "tidal_hourly"]), AssetKey(["tides", "tidal_forecast"])]
)
@schedule(job=tides_hourly_job, cron_schedule="@hourly", name="tidal_hourly")
def tides_hourly_schedule(context):
    return RunRequest()

tides_all_job = define_asset_job(
    "tides_tj_all", selection=[AssetKey(["tides", "tidal_monthly"]),
                               #AssetKey(["tides", "tides_current"])
                               ],
partitions_def=monthly_partitions
)

@schedule(job=tides_all_job, cron_schedule="@monthly", name="tides_monthly")
def tides_monthly_schedule(context):
    partitions_to_run = monthly_partitions.get_partition_keys()

    return [
        RunRequest(
            run_key=f"tidal_monthly_{partition_key}",
            partition_key=partition_key
        )
        for partition_key in partitions_to_run
    ]
