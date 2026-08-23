from datetime import datetime
from io import StringIO
import numpy as np
import pandas as pd


from dagster import (asset,
                     get_dagster_logger,
                     define_asset_job, AssetKey,
                     RunRequest,
                     schedule,
                     TimeWindowPartitionsDefinition, AssetIn, AutomationCondition
                     )
import duckdb
from resilient_core.resources import minio
from resilient_core.utils import store_assets
from ..utils import astro_calendar, forecast_features
from ..utils.h2s_exceedance import aggregate_exceedances
#from .sd_apcd import s3_output_path as apcd_s3_output_path

OUTPUT_PATH='tijuana/forecast_data/output/'
LATEST='tijuana/forecast_data'

PARQUET_PATTERN='*.parquet'
CSV_PATTERN='*.csv'

H2S_PATH='latest/tijuana/sd_apcd_air/h2s'
WEATHER_BASE='latest/tijuana/weather'
STREAMFLOW_BASE='latest/tijuana/streamflow'
STREAMFLOW_SITE_YEARLY='boundary_cms'
STREAMFLOW_SITE_RECENT=STREAMFLOW_SITE_YEARLY
TIDAL_BASE='latest/tijuana/tides'

EFFLUENT_BASE='latest/tijuana/effluent_flow'
EFFLUENT_CURRENT_YEAR='yearly'
EFFLUENT_TODAY='today'

DIURNAL_FACTORS = {
    0: 0.70, 1: 0.65, 2: 0.60, 3: 0.58, 4: 0.60, 5: 0.70,
    6: 0.85, 7: 1.05, 8: 1.15, 9: 1.20, 10: 1.25, 11: 1.20,
    12: 1.15, 13: 1.10, 14: 1.10, 15: 1.10, 16: 1.15, 17: 1.20,
    18: 1.15, 19: 1.10, 20: 1.00, 21: 0.90, 22: 0.85, 23: 0.75
}
DIURNAL_MEAN = np.mean(list(DIURNAL_FACTORS.values()))


sites_csv = """LongName,site_name,lat,lon,AgencyName
Berry Elementary School,NESTOR - BES, 32.567097, -117.090656,San Diego APCD
Imperial Beach Civic Center,IB CIVIC CTR, 32.576139,  -117.115361,San Diego APCD
El Cajon - Lexington Elementary School,EL CAJON LES, 32.789561,  -116.944222,San Diego APCD
San Ysidro,SAN YSIDRO,	32.552794,	-117.047286,San Diego APCD
        """

def degrees_to_direction(degrees):
    """
    Convert wind direction in degrees to categorical text directions.

    Args:
        degrees: Wind direction in degrees (0-360)

    Returns:
        str: Cardinal/intercardinal direction (N, NE, E, SE, S, SW, W, NW)
    """
    if pd.isna(degrees):
        return None

    # Normalize degrees to 0-360 range
    degrees = degrees % 360

    # Define direction ranges (each direction covers 45 degrees, centered on the cardinal direction)
    directions = [
        (0, 22.5, "N"),      # 337.5-22.5 degrees
        (22.5, 67.5, "NE"),  # 22.5-67.5 degrees
        (67.5, 112.5, "E"),  # 67.5-112.5 degrees
        (112.5, 157.5, "SE"), # 112.5-157.5 degrees
        (157.5, 202.5, "S"),  # 157.5-202.5 degrees
        (202.5, 247.5, "SW"), # 202.5-247.5 degrees
        (247.5, 292.5, "W"),  # 247.5-292.5 degrees
        (292.5, 337.5, "NW"), # 292.5-337.5 degrees
        (337.5, 360, "N")     # 337.5-360 degrees (wrapping to North)
    ]

    for min_deg, max_deg, direction in directions:
        if min_deg <= degrees < max_deg:
            return direction

    return "N"  # Default to North for edge cases


def add_wind_features(df, logger):
    """Add wind direction categorical/trig/encoded, rolling avg/max, and interaction features."""
    if 'wind_direction_10m' in df.columns:
        df['wind_direction_categorical'] = df['wind_direction_10m'].apply(degrees_to_direction)
        wind_direction_rad = np.deg2rad(df['wind_direction_10m'])
        df['wind_direction_sin'] = np.sin(wind_direction_rad)
        df['wind_direction_cos'] = np.cos(wind_direction_rad)
        logger.info("Added sine and cosine components for wind direction")

    logger.info("Calculating rolling wind metrics for 2, 3, and 4 hour windows")

    if 'wind_speed_10m' in df.columns:
        for window, label in [(2, '2h'), (3, '3h'), (4, '4h')]:
            df[f'wind_speed_10m_avg_{label}'] = (
                df.groupby('site_name')['wind_speed_10m']
                .transform(lambda x: x.rolling(window=window, min_periods=1).mean())
            )

    if 'wind_gusts_10m' in df.columns:
        for window, label in [(2, '2h'), (3, '3h'), (4, '4h')]:
            df[f'wind_gusts_10m_max_{label}'] = (
                df.groupby('site_name')['wind_gusts_10m']
                .transform(lambda x: x.rolling(window=window, min_periods=1).max())
            )
        logger.info("Added wind gust rolling maximums for 2, 3, 4 hour windows")
    else:
        logger.warning("wind_gusts_10m column not found - skipping gust calculations")

    # Negative-lag wind direction: wind[t-k] predicts H2S[t]. Cross-correlation on
    # modeldata_h2s_nofill shows |r| peaks at k=6-9h for wind_direction_sin across
    # all APCD sites, consistent with advection transit time from the Tijuana
    # River source. wind_speed/gusts peak at lag 0 (already captured).
    for col in ('wind_direction_sin', 'wind_direction_cos'):
        if col in df.columns:
            for k in (6, 9):
                df[f'{col}_lag_{k}h'] = df.groupby('site_name')[col].shift(k)

    if 'wind_speed_10m' in df.columns and 'temperature_2m' in df.columns:
        df['wind_temp_interaction'] = df['wind_speed_10m'] * df['temperature_2m']

    if 'relative_humidity_2m' in df.columns and 'temperature_2m' in df.columns:
        df['humidity_temp_interaction'] = df['relative_humidity_2m'] * df['temperature_2m']

    if 'wind_direction_categorical' in df.columns:
        wind_direction_mapping = {'N': 0, 'NE': 1, 'E': 2, 'SE': 3, 'S': 4, 'SW': 5, 'W': 6, 'NW': 7}
        df['wind_direction_categorical_encoded'] = (
            df['wind_direction_categorical'].map(wind_direction_mapping).fillna(0).astype(int)
        )
        logger.info("Encoded wind_direction_categorical to integers (N=0, NE=1, ..., NW=7)")

    logger.info("Completed rolling wind calculations")
    return df


def add_tidal_encoding(tidal_df):
    """Add tidal_state_encoded column based on tidal_state.

    Encoding matches feature_engineering.py standard:
        low: 0, ebb: 1, flood: 2, slack: 3, high: 4
    """
    tidal_mapping = {
        'low': 0,
        'ebb': 1,
        'flood': 2,
        'slack': 3,
        'high': 4,
    }
    if 'tidal_state' in tidal_df.columns:
        tidal_df['tidal_state_encoded'] = tidal_df['tidal_state'].map(tidal_mapping).fillna(1).astype(int)
    else:
        tidal_df['tidal_state_encoded'] = 1
    return tidal_df


def add_day_night(df, logger):
    """Add day_night column based on San Diego sunrise/sunset times. Reads from df['time'] column.

    Delegates to the shared astronomical calendar so this asset module and the
    astronomical_day datasets cannot drift apart. Output is unchanged from the
    original per-asset implementation, which is pinned by
    tests/test_astro_calendar.py::test_add_day_night_helper_matches_original.
    """
    df['day_night'] = astro_calendar.label_day_night(df['time'])
    return df


def add_inference_features(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Pre-compute temporal cyclicals, flow transforms, source regime, and stable_atm.

    Valid at both training time and forecast time (no H2S observations required).
    Logic matches prepare_data() in train_models_auto.py exactly.
    """
    hour = df['time'].dt.hour
    month = df['time'].dt.month
    df['hour_sin']  = np.sin(2 * np.pi * hour / 24)
    df['hour_cos']  = np.cos(2 * np.pi * hour / 24)
    df['month_sin'] = np.sin(2 * np.pi * month / 12)
    df['month_cos'] = np.cos(2 * np.pi * month / 12)
    df['is_night']  = (df['day_night'] == 'night').astype(int)

    def source_regime(row):
        if row['day_night'] != 'night': return 0
        wd = row['wind_direction_10m']
        if 22.5 <= wd < 135:           return 1
        elif wd >= 247.5 or wd < 22.5: return 2
        elif 135 <= wd < 247.5:        return 3
        return 0
    df['source_regime'] = df.apply(source_regime, axis=1)

    flow_col = 'Flow (m^3/s)--Border'
    if flow_col in df.columns:
        df['flow_log']  = np.log1p(df[flow_col])
        df['flow_low']  = (df[flow_col] < 1).astype(int)
        df['flow_high'] = (df[flow_col] > 5).astype(int)
    else:
        df['flow_log'] = df['flow_low'] = df['flow_high'] = np.nan

    df['stable_atm'] = ((df['wind_speed_10m'] < 5) & (df['is_night'] == 1)).astype(int)
    logger.info("Added inference features: hour/month cyclicals, is_night, source_regime, flow transforms, stable_atm")
    return df


def add_h2s_lag_features(df: pd.DataFrame, logger) -> pd.DataFrame:
    """Pre-compute per-site H2S and flow lag/rolling features.

    Only valid for training data where H2S measurements exist.
    Logic matches prepare_data() in train_models_auto.py exactly.
    """
    flow_col = 'Flow (m^3/s)--Border'
    lag_cols = ['h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h',
                'h2s_rolling_6h', 'h2s_rolling_24h', 'flow_lag_6h', 'flow_rolling_24h']
    for col in lag_cols:
        df[col] = np.nan
    df = df.sort_values(['site_name', 'time']).reset_index(drop=True)
    for site in df['site_name'].unique():
        m = df['site_name'] == site
        s = df.loc[m].copy()
        s['h2s_lag_1h']      = s['H2S'].shift(1)
        s['h2s_lag_3h']      = s['H2S'].shift(3)
        s['h2s_lag_6h']      = s['H2S'].shift(6)
        s['h2s_rolling_6h']  = s['H2S'].rolling(6,  min_periods=1).mean()
        s['h2s_rolling_24h'] = s['H2S'].rolling(24, min_periods=1).mean()
        if flow_col in df.columns:
            s['flow_lag_6h']      = s[flow_col].shift(6)
            s['flow_rolling_24h'] = s[flow_col].rolling(24, min_periods=1).mean()
        for col in lag_cols:
            df.loc[m, col] = s[col].values
    logger.info(f"Added H2S/flow lag features for {df['site_name'].nunique()} sites")
    return df


def add_sbiwtp_features(df: pd.DataFrame, sbiwtp_daily: pd.Series, logger) -> pd.DataFrame:
    """Merge SBIWTP effluent flow features into df.

    Parameters
    ----------
    df : DataFrame with a 'time' column in America/Los_Angeles timezone.
    sbiwtp_daily : Series indexed by date with daily average MGD values.
    logger : dagster logger.

    Adds columns (Tiers 1-3):
      sbiwtp_flow_mgd       — daily flow lagged 1 day
      sbiwtp_anomaly        — (flow - 30d_mean) / 30d_mean
      sbiwtp_deficit        — max(0, 30d_mean - flow) lagged 1 day
      sbiwtp_flow_x_temp    — sbiwtp_flow_mgd × temperature_2m
      sbiwtp_hourly_mgd     — diurnal-disaggregated hourly flow
      sbiwtp_sli            — Sewage Load Index accumulation-decay
    """
    if sbiwtp_daily.empty:
        logger.warning("SBIWTP daily series is empty — skipping feature engineering")
        for col in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
            df[col] = np.nan
        return df

    # Resample to daily mean
    daily = sbiwtp_daily.resample('D').mean().rename('sbiwtp_flow_mgd')
    logger.info(f"Resampled effluent to daily: {len(daily)} days from {daily.index.min()} to {daily.index.max()}")

    # 30-day rolling mean
    rolling_mean = daily.rolling(30, min_periods=1).mean()

    # Tier 1 features (lagged 1 day)
    lag1 = daily.shift(1)
    lag1_mean = rolling_mean.shift(1)
    anomaly = (lag1 - lag1_mean) / lag1_mean.replace(0, np.nan)
    deficit = (lag1_mean - lag1).clip(lower=0)
    logger.info(f"Created lag1 series: {lag1.notna().sum()} non-null values")

    # Map to each row in df via date
    df = df.copy()

    # Create date column for merging (normalized to midnight in local timezone)
    df['_date'] = df['time'].dt.normalize()
    logger.info(f"DataFrame time range: {df['time'].min()} to {df['time'].max()}")
    logger.info(f"DataFrame normalized dates: {df['_date'].min()} to {df['_date'].max()}")

    def map_by_date(series, series_name):
        """Map daily series values to hourly dataframe by matching dates."""
        s = series.copy()
        # Ensure series index is timezone-aware and normalized to midnight
        if s.index.tz is None:
            logger.info(f"{series_name}: Index has no timezone, localizing to America/Los_Angeles")
            s.index = pd.to_datetime(s.index).tz_localize('America/Los_Angeles')
        s.index = s.index.normalize()

        logger.info(f"{series_name}: Series date range: {s.index.min()} to {s.index.max()}")
        logger.info(f"{series_name}: Series has {s.notna().sum()} non-null values out of {len(s)}")

        # Convert to DataFrame for merge
        s_df = s.to_frame('value')

        # Merge on normalized date
        merged = df[['_date']].merge(s_df, left_on='_date', right_index=True, how='left')
        matched_count = merged['value'].notna().sum()
        logger.info(f"{series_name}: Matched {matched_count} out of {len(df)} rows")
        return merged['value'].values

    df['sbiwtp_flow_mgd'] = map_by_date(lag1, 'lag1')
    df['sbiwtp_anomaly'] = map_by_date(anomaly, 'anomaly')
    df['sbiwtp_deficit'] = map_by_date(deficit, 'deficit')

    # Drop temporary date column
    df = df.drop(columns=['_date'])

    if 'temperature_2m' in df.columns:
        df['sbiwtp_flow_x_temp'] = df['sbiwtp_flow_mgd'] * df['temperature_2m']
    else:
        df['sbiwtp_flow_x_temp'] = np.nan

    # Tier 2 — hourly disaggregation
    hour = df['time'].dt.hour
    diurnal = hour.map(DIURNAL_FACTORS).fillna(1.0)
    df['sbiwtp_hourly_mgd'] = df['sbiwtp_flow_mgd'] * diurnal / DIURNAL_MEAN

    # Tier 3 — Sewage Load Index
    decay = np.exp(-np.log(2) / 2.5)  # half-life 2.5 days
    sli_values = np.zeros(len(df))
    deficit_arr = df['sbiwtp_deficit'].fillna(0).values
    temp_arr = df['temperature_2m'].fillna(20).values if 'temperature_2m' in df.columns else np.full(len(df), 20.0)
    tide_arr = df['tide_height_m'].fillna(0).values if 'tide_height_m' in df.columns else np.zeros(len(df))

    sli = 0.0
    for i in range(len(df)):
        temp_factor = 1 + 0.02 * (temp_arr[i] - 20)
        tide_factor = 1 - 0.1 * tide_arr[i]
        sli = sli * decay * tide_factor + deficit_arr[i] * temp_factor
        sli_values[i] = sli

    df['sbiwtp_sli'] = sli_values

    logger.info(
        f"SBIWTP features added: {df['sbiwtp_flow_mgd'].notna().sum()} non-null flow values, "
        f"mean deficit={df['sbiwtp_deficit'].mean():.2f}, mean SLI={df['sbiwtp_sli'].mean():.2f}"
    )
    return df


def duckdb_connection(s3_resource: minio.S3Resource):

    server=s3_resource.S3_ADDRESS
    bucket= s3_resource.S3_BUCKET
    s3_setup=f"""CREATE OR REPLACE SECRET s3_credentials(
        TYPE s3,    
        ENDPOINT '{server}',
        URL_STYLE 'path'
    );
    """
    con = duckdb.connect()
    con.execute(s3_setup)
    return con

@asset(group_name="tijuana",
       key_prefix="h2sforecast",
       name="hs2_locations",
       required_resource_keys={"s3"},
       metadata={
           "source": "San Diego APCD H2S location data"
           , "description": "Location San Diego Air Pollution Control District Air Quality Monitoring Sites"
       },
       automation_condition=AutomationCondition.eager()
       )
def h2s_locations(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    sites_df = pd.read_csv(StringIO(sites_csv), sep=',', on_bad_lines='warn')
    store_assets.store_dataframe_to_s3( sites_df, OUTPUT_PATH,'h2s_locations', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata)
    return sites_df



@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s",
    required_resource_keys={"s3"},
    deps=[AssetKey(["apcd", 'yearly_aggregated_h2s']),
          AssetKey(['streamflow', 'boundary_cms_yearly']),
          AssetKey(['streamflow', 'boundary_cms']),
          AssetKey(['weather', 'openmeteo_historical']),
          AssetKey(['weather', 'openmeteo_current_year']),
          AssetKey(['ibwc', 'effluent_flow_current_year']),
          AssetKey(['ibwc', 'effluent_flow_today']),
          ],
       metadata={
           "source": "San Diego APCD, IBWC Streamflow and OpenMeteo historical data"
           , "description": "Data for Forecast Modeling of H2S includes Wind Direction, Wind Speed, and complete Tijuana River streamflow (yearly historical + recent 30 days). THIS IS UP TO DATE DAILY. Not hourly. "
           , "variableMeasured": ["H2S", 'Wind Direction', 'Wind Speed', "Streamflow"]
       },
       automation_condition=AutomationCondition.eager()
)
def data_for_models(context):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()
    duckdb_con=duckdb_connection(s3_resource)
    # filename = f'{apcd_s3_output_path}/h2s.csv'
    # apcd_s3_output_path='tijuana/sd_apcd_air/output'
    # h2surl = s3_resource.publicUrl(path=f'{apcd_s3_output_path}/h2s.csv', bucket=s3_resource.S3_BUCKET)
    # dagster_logger.info(f"Downloading {h2surl}")
    # h2s_sensor_data_all = pd.read_csv(h2surl)
    hs2_files = f"s3://{s3_resource.S3_BUCKET}/{H2S_PATH}/{PARQUET_PATTERN}"
    try:
         h2s_sensor_data_all = duckdb_con.read_parquet(hs2_files).df()
    except Exception as e:
        dagster_logger.error(f"Error reading apcd parquet files {hs2_files} {e}")
        raise e
    # using names causes a parsing error, do just drop after loading
    try:
        h2s_sensor_data_all = h2s_sensor_data_all.drop(
            ['Original Value', 'Icons', 'level', 'Parameter', 'LongName', 'Site Name', 'Latitude', 'Longitude',
             'AgencyName'], axis=1)
        h2s_sensor_data_all['time'] = pd.to_datetime(h2s_sensor_data_all['Date with time'], utc=True)
        h2s_sensor_data_all = h2s_sensor_data_all.rename(
            columns={'SiteName': 'site_name', 'Result': 'H2S', 'Qualifier': 'H2S_qualifier'})
        h2s_sensor_data_all["time"] = h2s_sensor_data_all["time"].dt.tz_convert("America/Los_Angeles")
        h2s_sensor_data_all = h2s_sensor_data_all.rename(columns={'Result': 'H2S', 'Qualifier': 'H2S_qualifier'})

        # 2s_sensor_data_all.index = pd.to_datetime(h2s_sensor_data_all['Date with time']).dt.tz_localize('America/Los_Angeles', ambiguous=True)
        h2s_sensor_data_all = h2s_sensor_data_all.drop('Date with time', axis=1)
        h2s_sensor_data_all = h2s_sensor_data_all.set_index(pd.DatetimeIndex(h2s_sensor_data_all['time']))
        h2s_sensor_data_all = h2s_sensor_data_all.drop('time', axis=1)
        h2s_sensor_data_all.index = h2s_sensor_data_all.index.astype("datetime64[ns, America/Los_Angeles]")
        h2s_sensor_data_all = h2s_sensor_data_all.sort_index()
        pre_dedup = h2s_sensor_data_all.shape[0]
        h2s_sensor_data_all = (h2s_sensor_data_all
                               .reset_index()
                               .drop_duplicates(subset=['time', 'site_name'], keep='last')
                               .set_index('time'))
        h2s_sensor_data_all.index = h2s_sensor_data_all.index.astype("datetime64[ns, America/Los_Angeles]")
        dagster_logger.info(f"Deduplicated h2s: {pre_dedup} -> {h2s_sensor_data_all.shape[0]} rows")
    except Exception as e:
        dagster_logger.error(f"Error processing h2s data {e}")
        raise e
    dagster_logger.info(f"Matched {h2s_sensor_data_all.shape[0]} rows")

    # weather_df = pd.DataFrame()
    # for wurl in weather_urls:
    #     wyear_df = pd.read_csv(wurl)
    #     wyear_df['time'] = pd.to_datetime(wyear_df['date'], utc=True)
    #     # forecast_df["time"] = forecast_df["time"].dt.tz_localize("America/Los_Angeles", ambiguous=True)
    #     wyear_df = wyear_df.set_index(pd.DatetimeIndex(wyear_df['time']))
    #     wyear_df = wyear_df.drop(['time', 'date'], axis=1)
    #     weather_df = pd.concat([weather_df, wyear_df], )
    weather_files = f"s3://{s3_resource.S3_BUCKET}/{WEATHER_BASE}/{CSV_PATTERN}"
    try:
        weather_df = duckdb_con.read_csv(weather_files, null_padding=True).df()
    except Exception as e:
        dagster_logger.error(f"Error reading weather csv files {weather_files} {e}")
        raise e
    try:
        weather_df = weather_df.rename(columns={'date': 'time'})
        weather_df["time"] = weather_df["time"].dt.tz_convert("America/Los_Angeles")
        weather_df = weather_df.set_index(pd.DatetimeIndex(weather_df['time']))
        weather_df = weather_df.drop(['time'], axis=1)
        weather_df.index = weather_df.index.astype("datetime64[ns, America/Los_Angeles]")
        weather_df = weather_df.sort_index()
        if 'site_name' in weather_df.columns:
            weather_df['site_name'] = weather_df['site_name'].str.strip()
            # Legacy CSVs (pre-refactor) have no site_name — they were single-site NESTOR-BES data
            missing = weather_df['site_name'].isna()
            if missing.any():
                dagster_logger.warning(f"Filling {missing.sum()} rows with missing site_name as 'unknown' (legacy data)")
                weather_df.loc[missing, 'site_name'] = 'unknown'
        else:
            dagster_logger.warning("site_name column not found in weather data — assigning all rows to 'unknown'")
            weather_df['site_name'] = 'unknown'

        weather_df = add_wind_features(weather_df, dagster_logger)
    except Exception as e:
        dagster_logger.error(f"Error processing weather data {e}")
        raise e

    dagster_logger.info(f"Matched {weather_df.shape[0]} rows")
    # merged_df = pd.merge(h2s_sensor_data_all, weather_df, on="time", how="inner")
    try:
        h2s_reset = h2s_sensor_data_all.reset_index()
        weather_reset = weather_df.reset_index()
        h2s_reset = h2s_reset.sort_values('time')
        weather_reset = weather_reset.sort_values('time')
        matched_df = pd.merge_asof(
            h2s_reset,
            weather_reset,
            on='time',
            by='site_name',
            direction='nearest'
        )
    except Exception as e:
        dagster_logger.error(f"Error merging weather and h2s data {e}")
        raise e
    dagster_logger.info(f"Matched {matched_df.shape[0]} rows")
    # border streamflow - load yearly historical data
    streamflow_border_files = f"s3://{s3_resource.S3_BUCKET}/{STREAMFLOW_BASE}/{STREAMFLOW_SITE_YEARLY}/{PARQUET_PATTERN}"
    try:
        streamflow_border_df = duckdb_con.read_parquet(streamflow_border_files).df()
        dagster_logger.info(f"Loaded {streamflow_border_df.shape[0]} yearly streamflow records")
    except Exception as e:
        dagster_logger.error(f"Error reading streamflow parquet files {streamflow_border_files} {e}")
        raise e
    # also load recent boundary_cms (last 30 days) and combine for a complete flow record
    # try:
    #     streamflow_recent_files = f"s3://{s3_resource.S3_BUCKET}/{STREAMFLOW_BASE}/{STREAMFLOW_SITE_RECENT}/{CSV_PATTERN}"
    #     streamflow_recent_df = duckdb_con.read_csv(streamflow_recent_files).df()
    #     dagster_logger.info(f"Loaded {streamflow_recent_df.shape[0]} recent boundary_cms records")
    #     streamflow_border_df = pd.concat([streamflow_border_df, streamflow_recent_df], ignore_index=True)
    # except Exception as e:
    #     dagster_logger.warning(f"Could not load recent boundary_cms data, continuing with yearly only: {e}")
    try:
        streamflow_border_df['time'] = pd.to_datetime(streamflow_border_df['End of Interval (UTC-08:00)'])
        # Data is UTC-8 fixed offset (no DST) — localize to fixed offset, then convert to LA
        streamflow_border_df["time"] = streamflow_border_df["time"].dt.tz_localize('Etc/GMT+8').dt.tz_convert("America/Los_Angeles")
        streamflow_border_df = streamflow_border_df.rename(columns={'Average (m^3/s)': 'Flow (m^3/s)--Border'})
        streamflow_border_df = streamflow_border_df.set_index(pd.DatetimeIndex(streamflow_border_df['time']))
        streamflow_border_df = streamflow_border_df.drop(
            ['time', 'End of Interval (UTC-08:00)', 'Start of Interval (UTC-08:00)'], axis=1, errors='ignore')
        # deduplicate - recent data takes precedence over yearly
        streamflow_border_df = streamflow_border_df[~streamflow_border_df.index.duplicated(keep='last')]
        streamflow_border_df = streamflow_border_df.sort_index()
        dagster_logger.info(f"Combined streamflow has {streamflow_border_df.shape[0]} records after dedup")
    except Exception as e:
        dagster_logger.error(f"Error processing streamflow files {streamflow_border_files} {e}")
        raise e
    try:
        streamflow_reset = streamflow_border_df.reset_index().sort_values('time')
        matched_df = pd.merge_asof(matched_df, streamflow_reset, on='time', direction='nearest')
    except Exception as e:
        dagster_logger.error(f"Error merging weather and h2s  AND STREAMFLOW data {e}")
        raise e
    # add tides
    try:
        tidal_files = f"s3://{s3_resource.S3_BUCKET}/{TIDAL_BASE}/{PARQUET_PATTERN}"
        tidal_df = duckdb_con.read_parquet(tidal_files).df()
    except Exception as e:
        dagster_logger.error(f"Error reading tidal_files  files {tidal_files} {e}")
        raise e
    try:
        tidal_df['time'] = pd.to_datetime(tidal_df['time'], utc=False)
        tidal_df['time'] = tidal_df['time'].dt.tz_localize(None)
        tidal_df['time'] = tidal_df['time'].astype('datetime64[ns]')
        tidal_df['time'] = tidal_df['time'].dt.tz_localize("UTC")
        tidal_df["time"] = tidal_df["time"].dt.tz_convert("America/Los_Angeles")

        tidal_df = tidal_df.set_index(pd.DatetimeIndex(tidal_df['time']))
        tidal_df = tidal_df.drop(
            ['time'], axis=1)

        tidal_df = add_tidal_encoding(tidal_df)
        tidal_df = tidal_df.sort_index()
    except Exception as e:
        dagster_logger.error(f"Error reading tidals   files {tidal_df} {e}")
        raise e
    try:
        tidal_reset = tidal_df.reset_index().sort_values('time')
        matched_df = pd.merge_asof(matched_df, tidal_reset, on='time', direction='nearest')
    except Exception as e:
        dagster_logger.error(f"Error merging with tidal files data {e}")
        raise e

    # --- SBIWTP effluent flow features ---
    try:
        effluent_files = f"s3://{s3_resource.S3_BUCKET}/{EFFLUENT_BASE}/{EFFLUENT_CURRENT_YEAR}/{PARQUET_PATTERN}"
        effluent_df = duckdb_con.read_parquet(effluent_files, union_by_name=True).df()
        dagster_logger.info(f"Loaded {len(effluent_df)} effluent flow records with columns: {effluent_df.columns.tolist()}")

        # Find the timestamp column (check both columns and index)
        time_col = next(
            (c for c in effluent_df.columns if 'timestamp' in c.lower() or 'time' in c.lower()),
            None
        )
        if time_col:
            effluent_df['time'] = pd.to_datetime(effluent_df[time_col])
            # Data is UTC-8 fixed offset
            effluent_df['time'] = effluent_df['time'].dt.tz_localize('Etc/GMT+8').dt.tz_convert('America/Los_Angeles')
        elif effluent_df.index.name and isinstance(effluent_df.index.name, str) and ('timestamp' in effluent_df.index.name.lower() or 'time' in effluent_df.index.name.lower()):
            # Timestamp is in the index, reset it to a column
            index_name = effluent_df.index.name
            effluent_df = effluent_df.reset_index()
            effluent_df['time'] = pd.to_datetime(effluent_df[index_name])
            effluent_df['time'] = effluent_df['time'].dt.tz_localize('Etc/GMT+8').dt.tz_convert('America/Los_Angeles')
            time_col = index_name  # Track the original column name for exclusion below
        else:
            raise ValueError("No timestamp column found in effluent flow data")

        # Find the MGD value column (first numeric column other than timestamp/time)
        value_col = next(
            (c for c in effluent_df.columns if c not in [time_col, 'time'] and pd.api.types.is_numeric_dtype(effluent_df[c])),
            None
        )
        if value_col is None:
            dagster_logger.error(f"Available columns: {effluent_df.columns.tolist()}")
            dagster_logger.error(f"Column dtypes: {effluent_df.dtypes.to_dict()}")
            raise ValueError("No numeric value column found in effluent flow data")

        dagster_logger.info(f"Using effluent time column: {time_col} -> 'time', value column: {value_col}")
        effluent_series = effluent_df.set_index('time')[value_col].rename('sbiwtp_flow_mgd')
        dagster_logger.info(f"Effluent series: {len(effluent_series)} records from {effluent_series.index.min()} to {effluent_series.index.max()}")
        matched_df = add_sbiwtp_features(matched_df, effluent_series, dagster_logger)
    except Exception as e:
        dagster_logger.error(f"Could not load SBIWTP effluent flow, skipping features: {e}")
        import traceback
        dagster_logger.error(traceback.format_exc())
        for col in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
            matched_df[col] = np.nan

    matched_df = add_day_night(matched_df, dagster_logger)
    matched_df = add_inference_features(matched_df, dagster_logger)
    # H2S lags are computed on pre-fill H2S so that train_models_auto.py can filter
    # filled rows via h2s_measured; the fill step happens below.
    matched_df = add_h2s_lag_features(matched_df, dagster_logger)

    # Fill missing H2S values for each site_name based on min/max time ranges
    # and flag filled values
    dagster_logger.info("Filling missing H2S values for each site_name based on min/max time ranges")

    # Add flag column to track measured vs filled values
    matched_df['h2s_measured'] = True

    # Process each site separately
    filled_dfs = []
    for site_name in matched_df['site_name'].unique():
        site_df = matched_df[matched_df['site_name'] == site_name].copy()

        # Find min/max time where H2S data exists for this site
        h2s_valid_mask = site_df['H2S'].notna()
        if h2s_valid_mask.any():
            min_time = site_df.loc[h2s_valid_mask, 'time'].min()
            max_time = site_df.loc[h2s_valid_mask, 'time'].max()

            # Create time range mask for this site
            time_range_mask = (site_df['time'] >= min_time) & (site_df['time'] <= max_time)

            # Find missing H2S values within the time range
            missing_mask = time_range_mask & site_df['H2S'].isna()

            if missing_mask.any():
                # Fill missing values using forward fill then backward fill
                h2s_series = site_df.loc[time_range_mask, 'H2S'].ffill().bfill()
                site_df.loc[time_range_mask, 'H2S'] = h2s_series

                # Flag the filled values as not measured
                site_df.loc[site_df[missing_mask].index, 'h2s_measured'] = False

                dagster_logger.info(f"Filled {missing_mask.sum()} missing H2S values for site {site_name} between {min_time} and {max_time}")
            else:
                dagster_logger.info(f"No missing H2S values to fill for site {site_name}")
        else:
            dagster_logger.warning(f"No valid H2S data found for site {site_name}")

        filled_dfs.append(site_df)

    # Combine all sites back together
    matched_df = pd.concat(filled_dfs, ignore_index=True).sort_values('time')

    # Log summary of filling operation
    total_filled = (~matched_df['h2s_measured']).sum()
    dagster_logger.info(f"Total H2S values filled across all sites: {total_filled}")

    # H2S risk classification
    matched_df['h2s_risk'] = 'GREEN'
    matched_df.loc[matched_df['H2S'] > 5,  'h2s_risk'] = 'YELLOW_LOW'
    matched_df.loc[matched_df['H2S'] > 10, 'h2s_risk'] = 'YELLOW_HIGH'
    matched_df.loc[matched_df['H2S'] > 30, 'h2s_risk'] = 'ORANGE'

    if 'date_processed' in matched_df.columns:
            matched_df = matched_df.drop(columns=['date_processed'])
    if 'aggregation_year' in matched_df.columns:
        matched_df = matched_df.drop(columns=['aggregation_year'])
    if 'H2S_qualifier' in matched_df.columns:
        matched_df = matched_df.drop(columns=['H2S_qualifier'])
    if 'visibility' in matched_df.columns:
        matched_df = matched_df.drop(columns=['visibility'])
    store_assets.store_dataframe_to_s3( matched_df, OUTPUT_PATH,'modeldata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv', 'parquet'], metadata=metadata )
    return matched_df

@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s_nofill",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", "modeldata_h2s"])],
    ins={
        "modeldata_h2s": AssetIn(
            key=AssetKey(["h2sforecast", "modeldata_h2s"])
        )
    },
    metadata={
        "source": "San Diego APCD, IBWC Streamflow and OpenMeteo historical data"
        , "description": "Model data identical to modeldata_h2s but with filled H2S values reverted to N/A. Only measured H2S values are retained."
        , "variableMeasured": ["H2S", "Wind Direction", "Wind Speed", "Streamflow"]
    },
    automation_condition=AutomationCondition.eager()
)
def modeldata_h2s_nofill(context, modeldata_h2s):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3

    result_df = modeldata_h2s.copy()
    if "h2s_measured" in result_df.columns:
        result_df.loc[~result_df["h2s_measured"], "H2S"] = None

    store_assets.store_dataframe_to_s3(result_df, OUTPUT_PATH, "modeldata_h2s_nofill", s3_resource,
                                       latestdatasetpath=LATEST, enable_latest_path=True,
                                       formats=["csv", "parquet"], metadata=metadata)
    return result_df


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="hysplit_h2s",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast",'modeldata_h2s']),
          ] ,
    ins={
        "data_for_models": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s'])
        )
    },
metadata={
           "source": "San Diego APCD and OpenMeteo historical data"
       ,"description":"Data for Hysplit Model of H2S includes Wind Direction and Wind Speed. This is one day behind."
,"variableMeasured":["H2S",'Wind Direction','Wind Speed']
},
       automation_condition=AutomationCondition.eager()
)
def data_for_hysplit(context, data_for_models):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]  # -> "value"
    source_url = meta.get("source")  # -> "data-eng"
    variableMeasured= meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url,variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    # Include both numeric and categorical wind direction columns
    columns_to_select = ['time', 'site_name', 'H2S', 'wind_speed_10m', 'wind_direction_10m']
    if 'wind_direction_categorical' in data_for_models.columns:
        columns_to_select.append('wind_direction_categorical')
    if 'temperature_2m' in data_for_models.columns:
        columns_to_select.append('temperature_2m')
    if 'h2s_measured' in data_for_models.columns:
        columns_to_select.append('h2s_measured')
    if 'h2s_risk' in data_for_models.columns:
        columns_to_select.append('h2s_risk')

    h2s_df = data_for_models[columns_to_select]
    h2s_df = h2s_df.rename(columns={'wind_speed_10m':'wind_speed', 'wind_direction_10m':'wind_direction'})
    store_assets.store_dataframe_to_s3( h2s_df, OUTPUT_PATH,'hysplitdata_h2s', s3_resource,
                                       latestdatasetpath=LATEST,enable_latest_path=True,
                                       formats=[ 'csv'], metadata=metadata )


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_peaks",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", 'modeldata_h2s_nofill'])],
    ins={
        "modeldata_h2s": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s_nofill'])
        )
    },
    metadata={
        "source": "San Diego APCD H2S data analysis"
        , "description": "Hourly counts of H2S threshold exceedances by day/night periods"
        , "variableMeasured": ["H2S", "Exceedance Counts"]
    },
    automation_condition=AutomationCondition.eager()
)
def h2s_peaks_analysis(context, modeldata_h2s):
    """
    Create hourly counts of H2S exceedances for day and night periods

    Counts hourly occurrences when H2S exceeds 5 ppb and 30 ppb thresholds,
    separated by day (6 AM - 6 PM) and night (6 PM - 6 AM) periods.

    Exceedance counts, max_h2s and mean_h2s cover measured observations only.
    The input is modeldata_h2s_nofill (despite the parameter being named
    modeldata_h2s), where unmeasured H2S is already null, so count_filled is 0 in
    practice. Excluding gap-filled values is enforced in aggregate_exceedances
    regardless, so the counts stay correct if this is ever repointed at the
    filled modeldata_h2s.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()

    try:
        # Work with the H2S model data
        h2s_data = modeldata_h2s.copy()

        if h2s_data.empty:
            dagster_logger.warning("No H2S data available")
            return pd.DataFrame()

        # Ensure we have the time index and site_name
        if 'site_name' not in h2s_data.columns:
            dagster_logger.error("site_name column not found in data")
            return pd.DataFrame()

        # Reset index to work with datetime as a column
        h2s_data = h2s_data.reset_index()

        # Ensure datetime column exists
        if 'time' not in h2s_data.columns:
            dagster_logger.error("time column not found in data")
            return pd.DataFrame()

        # Convert time to datetime if it's not already
        h2s_data['datetime'] = pd.to_datetime(h2s_data['time'])

        # Extract hour for day/night classification
        h2s_data['hour'] = h2s_data['datetime'].dt.hour
        h2s_data['date'] = h2s_data['datetime'].dt.date

        # Classify periods: Day = 6 AM to 6 PM, Night = 6 PM to 6 AM
        h2s_data['period'] = h2s_data['hour'].apply(lambda h: 'day' if 6 <= h < 18 else 'night')

        # Filter for valid H2S measurements only
        h2s_valid = h2s_data[h2s_data['H2S'].notna()].copy()

        if h2s_valid.empty:
            dagster_logger.warning("No valid H2S measurements found")
            return pd.DataFrame()

        # Exceedance counts over measured observations only. Gap-filled values are
        # excluded from the counts and from max/mean; a synthetic reading must not
        # be reported as an observed exceedance. The nofill input already nulls
        # them, so this is a guard rather than a behaviour change. Shared with
        # h2s_peaks_astronomical_day so the two assets cannot diverge on it.
        daily_totals = aggregate_exceedances(
            h2s_valid, group_keys=['site_name', 'date', 'period']
        )
        if daily_totals.empty:
            dagster_logger.warning("No measured H2S observations to summarise")
            return pd.DataFrame()

        # Preserve the original column order; measured_observations is appended.
        daily_totals = daily_totals[[
            'site_name', 'date', 'period',
            'count_exceeds_5', 'count_exceeds_30',
            'total_measurements', 'max_h2s', 'mean_h2s', 'count_filled',
            'measured_observations',
        ]]

        dagster_logger.info(
            f"Processing {len(h2s_valid)} valid H2S rows "
            f"({int(daily_totals['measured_observations'].sum())} measured, "
            f"{int(daily_totals['count_filled'].sum())} gap-filled and excluded from counts)"
        )
        dagster_logger.info(f"Found {int(daily_totals['count_exceeds_5'].sum())} measured exceedances > 5 ppb")
        dagster_logger.info(f"Found {int(daily_totals['count_exceeds_30'].sum())} measured exceedances > 30 ppb")

        daily_totals['summary_type'] = 'daily_by_period'
        daily_totals['date_processed'] = datetime.now().isoformat()

        dagster_logger.info(f"Generated {len(daily_totals)} daily H2S peak records by period")
        dagster_logger.info(f"Day period exceedances > 5: {daily_totals[daily_totals['period']=='day']['count_exceeds_5'].sum()}")
        dagster_logger.info(f"Night period exceedances > 5: {daily_totals[daily_totals['period']=='night']['count_exceeds_5'].sum()}")
        dagster_logger.info(f"Day period exceedances > 30: {daily_totals[daily_totals['period']=='day']['count_exceeds_30'].sum()}")
        dagster_logger.info(f"Night period exceedances > 30: {daily_totals[daily_totals['period']=='night']['count_exceeds_30'].sum()}")

        # Store the results
        store_assets.store_dataframe_to_s3(
            daily_totals,
            OUTPUT_PATH,
            'h2s_peaks',
            s3_resource,
            latestdatasetpath=LATEST,
            enable_latest_path=True,
            formats=['csv', 'parquet'],
            metadata=metadata
        )

        return daily_totals

    except Exception as e:
        dagster_logger.error(f"Error processing H2S peaks: {e}")
        raise e


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_wind_lag_analysis",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", 'modeldata_h2s_nofill'])],
    ins={
        "modeldata_h2s": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s_nofill'])
        )
    },
    metadata={
        "source": "San Diego APCD H2S vs OpenMeteo wind cross-correlation",
        "description": (
            "Per-site Pearson cross-correlation between H2S(t) and wind features at lags 0..12h. "
            "Used to decide whether to add negative-lag wind features to the forecast model "
            "(i.e. wind at t-k predicting H2S at t, reflecting advection transit time from source to sensor)."
        ),
        "variableMeasured": ["H2S", "Wind Speed", "Wind Direction", "Wind Gusts", "Lag (hours)", "Pearson r"],
    },
    automation_condition=AutomationCondition.eager(),
)
def h2s_wind_lag_analysis(context, modeldata_h2s):
    """Compute per-site H2S vs wind cross-correlation over lags 0..12 hours.

    For each site and each wind feature, shift the wind series forward by k hours
    (so wind[t-k] aligns with H2S[t]) and compute Pearson r on the overlap. Only
    rows with measured H2S and valid wind are used. Emits one row per
    (site_name, wind_feature, lag_hours) plus a per-site "best" summary row.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variableMeasured,
    )

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()

    df = modeldata_h2s.copy()
    if df.empty:
        dagster_logger.warning("modeldata_h2s_nofill is empty; skipping lag analysis")
        return pd.DataFrame()

    df = df.reset_index()
    if 'h2s_measured' in df.columns:
        df = df[df['h2s_measured'] == True]  # noqa: E712
    df = df[df['H2S'].notna() & (df['H2S'] <= 500)]

    wind_features = [
        'wind_speed_10m',
        'wind_direction_sin',
        'wind_direction_cos',
        'wind_gusts_10m',
    ]
    wind_features = [c for c in wind_features if c in df.columns]
    if not wind_features:
        dagster_logger.error("No wind feature columns found in modeldata_h2s_nofill")
        return pd.DataFrame()

    lags = list(range(0, 13))
    rows = []
    for site, g in df.groupby('site_name'):
        g = g.sort_values('time').set_index('time')
        # resample to hourly in case of irregular sampling — take mean within each hour
        g_hourly = g[['H2S'] + wind_features].resample('1h').mean()
        y = g_hourly['H2S']
        for feat in wind_features:
            x = g_hourly[feat]
            for k in lags:
                # wind[t-k] vs H2S[t]: shift wind forward by k so that index t carries wind from t-k
                pair = pd.concat([y, x.shift(k)], axis=1).dropna()
                n = len(pair)
                if n < 50:
                    r = np.nan
                else:
                    r = pair.iloc[:, 0].corr(pair.iloc[:, 1])
                rows.append({
                    'site_name': site,
                    'wind_feature': feat,
                    'lag_hours': k,
                    'pearson_r': r,
                    'abs_r': abs(r) if pd.notna(r) else np.nan,
                    'n_samples': n,
                })

    results = pd.DataFrame(rows)
    if results.empty:
        dagster_logger.warning("Lag analysis produced no rows")
        return results

    # per (site, feature) best lag by |r|
    best = (results.dropna(subset=['abs_r'])
                   .sort_values('abs_r', ascending=False)
                   .groupby(['site_name', 'wind_feature'], as_index=False)
                   .first()
                   .rename(columns={'lag_hours': 'best_lag_hours',
                                    'pearson_r': 'best_pearson_r'})
                   [['site_name', 'wind_feature', 'best_lag_hours', 'best_pearson_r', 'n_samples']])
    best['summary_type'] = 'best_lag_by_abs_r'
    results['summary_type'] = 'per_lag'

    dagster_logger.info("Best wind lag per site/feature:\n" + best.to_string(index=False))

    store_assets.store_dataframe_to_s3(
        results, OUTPUT_PATH, 'h2s_wind_lag_analysis', s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True,
        formats=['csv', 'parquet'], metadata=metadata,
    )
    store_assets.store_dataframe_to_s3(
        best, OUTPUT_PATH, 'h2s_wind_lag_best', s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True,
        formats=['csv'], metadata=metadata,
    )
    return results


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_exceedance_periods",
    required_resource_keys={"s3"},
    deps=[AssetKey(["h2sforecast", 'h2s_peaks']), AssetKey(["h2sforecast", 'modeldata_h2s_nofill'])],
    ins={
        "h2s_peaks": AssetIn(
            key=AssetKey(['h2sforecast', 'h2s_peaks'])
        ),
        "modeldata_h2s": AssetIn(
            key=AssetKey(['h2sforecast', 'modeldata_h2s_nofill'])
        )
    },
    metadata={
        "source": "San Diego APCD H2S exceedance analysis"
        , "description": "Filtered datasets for day/night periods with H2S exceedances above thresholds"
        , "variableMeasured": ["H2S Exceedances", "Day/Night Periods"]
    },
    automation_condition=AutomationCondition.eager()
)
def h2s_exceedance_periods_filter(context, h2s_peaks, modeldata_h2s):
    """
    Return hourly model data for periods where H2S exceedances occurred.

    Creates two datasets with full hourly environmental data (H2S, weather, streamflow, tidal):
    1. Hours within day/night periods where H2S exceeded 5 ppb
    2. Hours within day/night periods where H2S exceeded 30 ppb

    Perfect for forecast modeling as it provides all environmental variables.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url, variableMeasured=variableMeasured)

    s3_resource = context.resources.s3
    dagster_logger = get_dagster_logger()

    try:
        if h2s_peaks.empty or modeldata_h2s.empty:
            dagster_logger.warning("No H2S peaks data or model data available")
            return {"h2s_exceeds_5": pd.DataFrame(), "h2s_exceeds_30": pd.DataFrame()}

        # Prepare model data with time information
        model_data = modeldata_h2s.copy().reset_index()
        model_data['datetime'] = pd.to_datetime(model_data['time'])
        model_data['hour'] = model_data['datetime'].dt.hour
        model_data['date'] = model_data['datetime'].dt.date
        model_data['period'] = model_data['hour'].apply(lambda h: 'day' if 6 <= h < 18 else 'night')

        def filter_model_data_by_exceedances(peaks_df, threshold_name):
            """Filter model data to only include hours from exceedance periods"""
            if peaks_df.empty:
                return pd.DataFrame()

            # Create list of (site_name, date, period) tuples for exceedance periods
            exceedance_periods = []
            for _, row in peaks_df.iterrows():
                exceedance_periods.append((row['site_name'], row['date'], row['period']))

            # Filter model data to only include matching periods
            filtered_data = []
            for site_name, date, period in exceedance_periods:
                mask = (
                    (model_data['site_name'] == site_name) &
                    (model_data['date'] == date) &
                    (model_data['period'] == period)
                )
                period_data = model_data[mask].copy()
                if not period_data.empty:
                    period_data['exceedance_threshold'] = threshold_name
                    filtered_data.append(period_data)

            if filtered_data:
                return pd.concat(filtered_data, ignore_index=True)
            else:
                return pd.DataFrame()

        # Filter for periods where H2S exceeded 5 ppb - get hourly model data
        h2s_5_periods = h2s_peaks[h2s_peaks['count_exceeds_5'] > 0]
        h2s_exceeds_5_hourly = filter_model_data_by_exceedances(h2s_5_periods, '5_ppb')

        dagster_logger.info(f"Found {len(h2s_5_periods)} day/night periods with H2S > 5 ppb")
        dagster_logger.info(f"Retrieved {len(h2s_exceeds_5_hourly)} hourly records for H2S > 5 ppb periods")

        # Filter for periods where H2S exceeded 30 ppb - get hourly model data
        h2s_30_periods = h2s_peaks[h2s_peaks['count_exceeds_30'] > 0]
        h2s_exceeds_30_hourly = filter_model_data_by_exceedances(h2s_30_periods, '30_ppb')

        dagster_logger.info(f"Found {len(h2s_30_periods)} day/night periods with H2S > 30 ppb")
        dagster_logger.info(f"Retrieved {len(h2s_exceeds_30_hourly)} hourly records for H2S > 30 ppb periods")

        # Add summary statistics
        if not h2s_exceeds_5_hourly.empty:
            unique_hours_5 = h2s_exceeds_5_hourly.groupby(['site_name', 'date', 'period']).size().sum()
            actual_exceedances_5 = (h2s_exceeds_5_hourly['H2S'] > 5).sum()
            dagster_logger.info(f"H2S > 5 ppb: {unique_hours_5} total hours, {actual_exceedances_5} hours with actual H2S > 5")

        if not h2s_exceeds_30_hourly.empty:
            unique_hours_30 = h2s_exceeds_30_hourly.groupby(['site_name', 'date', 'period']).size().sum()
            actual_exceedances_30 = (h2s_exceeds_30_hourly['H2S'] > 30).sum()
            dagster_logger.info(f"H2S > 30 ppb: {unique_hours_30} total hours, {actual_exceedances_30} hours with actual H2S > 30")

        # Store H2S > 5 ppb hourly model data
        if not h2s_exceeds_5_hourly.empty:
            store_assets.store_dataframe_to_s3(
                h2s_exceeds_5_hourly,
                OUTPUT_PATH,
                'h2s_exceedance_model_data_5ppb',
                s3_resource,
                latestdatasetpath=LATEST,
                enable_latest_path=True,
                formats=['csv', 'parquet'],
                metadata=metadata
            )
            dagster_logger.info(f"✓ Stored H2S > 5 ppb hourly model data for forecast modeling")
        else:
            dagster_logger.info("No H2S > 5 ppb exceedance model data found")

        # Store H2S > 30 ppb hourly model data
        if not h2s_exceeds_30_hourly.empty:
            store_assets.store_dataframe_to_s3(
                h2s_exceeds_30_hourly,
                OUTPUT_PATH,
                'h2s_exceedance_model_data_30ppb',
                s3_resource,
                latestdatasetpath=LATEST,
                enable_latest_path=True,
                formats=['csv', 'parquet'],
                metadata=metadata
            )
            dagster_logger.info(f"✓ Stored H2S > 30 ppb hourly model data for forecast modeling")
        else:
            dagster_logger.info("No H2S > 30 ppb exceedance model data found")

        # Return both hourly model datasets
        return {
            "h2s_exceeds_5": h2s_exceeds_5_hourly,
            "h2s_exceeds_30": h2s_exceeds_30_hourly
        }

    except Exception as e:
        dagster_logger.error(f"Error filtering H2S exceedance model data: {e}")
        raise e


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="model_forecast",
    required_resource_keys={"s3"},
    ins={
        "openmeteo_forecast": AssetIn(key=AssetKey(["weather", "openmeteo_forecast"])),
        "tidal_forecast": AssetIn(key=AssetKey(["tides", "tidal_forecast"])),
        "streamflow_forecast": AssetIn(key=AssetKey(["streamflow", "streamflow_forecast"])),
        "modeldata_h2s_nofill": AssetIn(key=AssetKey(["h2sforecast", "modeldata_h2s_nofill"])),
    },
    metadata={
        "source": "OpenMeteo forecast, NOAA tidal predictions, IBWC streamflow forecast",
        "description": "Combined forecast feature dataset for H2S model inference. No H2S target columns.",
        "variableMeasured": ["Wind Direction", "Wind Speed", "Tide Height", "Streamflow", "SBIWTP Effluent Flow"],
    },
    automation_condition=AutomationCondition.eager(),
)
def model_forecast(context, openmeteo_forecast, tidal_forecast, streamflow_forecast, modeldata_h2s_nofill):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variableMeasured,
    )

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    # --- Weather ---
    weather_df = openmeteo_forecast.copy()
    weather_df = weather_df.rename(columns={'date': 'time'})
    weather_df['time'] = weather_df['time'].dt.tz_convert('America/Los_Angeles')
    weather_df['site_name'] = weather_df['site_name'].str.strip()
    weather_df = weather_df.sort_values('time').reset_index(drop=True)
    if 'visibility' in weather_df.columns:
        weather_df = weather_df.drop(columns=['visibility'])
    if 'wind_gusts_10m' not in weather_df.columns or weather_df['wind_gusts_10m'].isna().all():
        weather_df['wind_gusts_10m'] = weather_df['wind_speed_10m'] * 1.8
        logger.info("Estimated wind_gusts_10m as wind_speed_10m × 1.8 (no forecast gusts available)")
    weather_df = add_wind_features(weather_df, logger)
    now = pd.Timestamp.now(tz='America/Los_Angeles').floor('h')
    weather_df = weather_df[weather_df['time'] >= now].reset_index(drop=True)
    logger.info(f"Weather forecast: {len(weather_df)} rows from {now} onwards, sites: {weather_df['site_name'].unique().tolist()}")

    # --- Tidal ---
    tidal_df = tidal_forecast.copy()
    tidal_df['time'] = pd.to_datetime(tidal_df['time']).dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
    tidal_df = add_tidal_encoding(tidal_df)
    tidal_df = tidal_df.sort_values('time').reset_index(drop=True)
    logger.info(f"Tidal forecast: {len(tidal_df)} rows")

    # --- Streamflow ---
    streamflow_df = streamflow_forecast.copy()
    streamflow_df['time'] = pd.to_datetime(streamflow_df['time']).dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
    streamflow_df = streamflow_df.rename(columns={'Average (m^3/s)': 'Flow (m^3/s)--Border'})
    streamflow_df = streamflow_df.sort_values('time').reset_index(drop=True)
    logger.info(f"Streamflow forecast: {len(streamflow_df)} rows")

    # --- Merge (nearest-neighbor) ---
    merged = pd.merge_asof(weather_df, tidal_df, on='time', direction='nearest')
    merged = pd.merge_asof(merged, streamflow_df, on='time', direction='nearest')
    logger.info(f"Merged forecast: {len(merged)} rows")

    # --- Day/night ---
    merged = add_day_night(merged, logger)

    # --- Inference features (temporal cyclicals, flow transforms, source regime) ---
    # H2S lag features are omitted — no observations at forecast time
    merged = add_inference_features(merged, logger)

    # --- SBIWTP effluent flow persistence forecast ---
    try:
        duckdb_con = duckdb_connection(s3_resource)
        effluent_today_files = f"s3://{s3_resource.S3_BUCKET}/{EFFLUENT_BASE}/{EFFLUENT_TODAY}/{CSV_PATTERN}"
        effluent_today_df = duckdb_con.read_csv(effluent_today_files, null_padding=True).df()

        # Find timestamp column (check both columns and index)
        time_col = next(
            (c for c in effluent_today_df.columns if 'timestamp' in c.lower() or 'time' in c.lower()),
            None
        )
        if not time_col:
            # Check if timestamp is in the index
            index_name = effluent_today_df.index.name
            if index_name and isinstance(index_name, str):
                if 'timestamp' in index_name.lower() or 'time' in index_name.lower():
                    time_col = index_name
                    effluent_today_df = effluent_today_df.reset_index()

        value_col = next(
            (c for c in effluent_today_df.columns if c != time_col
             and pd.api.types.is_numeric_dtype(effluent_today_df[c])),
            None
        )
        if time_col and value_col:
            effluent_today_df['time'] = (
                pd.to_datetime(effluent_today_df[time_col])
                .dt.tz_localize('Etc/GMT+8')
                .dt.tz_convert('America/Los_Angeles')
            )
            today_mgd = effluent_today_df[value_col].mean()
            logger.info(f"SBIWTP today's mean effluent flow: {today_mgd:.2f} MGD")

            # Build a daily-indexed series for persistence forecast
            # hours 0-24: today's value; 24-48: persistence; >48: decay to 30-day mean
            effluent_current_files = f"s3://{s3_resource.S3_BUCKET}/{EFFLUENT_BASE}/{EFFLUENT_CURRENT_YEAR}/{PARQUET_PATTERN}"
            effluent_hist_df = duckdb_con.read_parquet(effluent_current_files, union_by_name=True).df()
            hist_time_col = next(
                (c for c in effluent_hist_df.columns if 'timestamp' in c.lower() or 'time' in c.lower()),
                None
            )
            if not hist_time_col:
                # Check if timestamp is in the index
                hist_index_name = effluent_hist_df.index.name
                if hist_index_name and isinstance(hist_index_name, str):
                    if 'timestamp' in hist_index_name.lower() or 'time' in hist_index_name.lower():
                        hist_time_col = hist_index_name
                        effluent_hist_df = effluent_hist_df.reset_index()

            hist_value_col = next(
                (c for c in effluent_hist_df.columns if c != hist_time_col
                 and pd.api.types.is_numeric_dtype(effluent_hist_df[c])),
                None
            )
            if hist_time_col and hist_value_col:
                effluent_hist_df['time'] = (
                    pd.to_datetime(effluent_hist_df[hist_time_col])
                    .dt.tz_localize('Etc/GMT+8')
                    .dt.tz_convert('America/Los_Angeles')
                )
                hist_series = effluent_hist_df.set_index('time')[hist_value_col].rename('sbiwtp_flow_mgd')
                mean_30d = hist_series.last('30D').mean()
            else:
                hist_series = pd.Series(dtype=float)
                mean_30d = today_mgd

            # Build forecast series aligned to merged['time'] dates
            forecast_dates = merged['time'].dt.normalize().unique()
            today_date = now.normalize()
            mgd_by_date = {}
            for d in forecast_dates:
                hours_ahead = (d - today_date).total_seconds() / 3600
                if hours_ahead <= 24:
                    mgd_by_date[d] = today_mgd
                elif hours_ahead <= 48:
                    mgd_by_date[d] = today_mgd  # persistence (autocorr r=0.77)
                else:
                    days_beyond = (hours_ahead - 48) / 24
                    decay_factor = np.exp(-np.log(2) / 2.5 * days_beyond)
                    mgd_by_date[d] = today_mgd * decay_factor + mean_30d * (1 - decay_factor)

            sbiwtp_forecast_series = pd.Series(
                {d: mgd_by_date.get(d, mean_30d) for d in forecast_dates},
                name='sbiwtp_flow_mgd'
            )
            # Extend hist_series with forecast values so add_sbiwtp_features has full context
            combined_series = pd.concat([hist_series, sbiwtp_forecast_series.rename('sbiwtp_flow_mgd')])
            merged = add_sbiwtp_features(merged, combined_series, logger)
        else:
            logger.warning("Could not parse SBIWTP today CSV — skipping SBIWTP forecast features")
            for col in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                        'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
                merged[col] = np.nan
    except Exception as e:
        logger.warning(f"SBIWTP forecast features failed, continuing without them: {e}")
        for col in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
            merged[col] = np.nan

    # --- Feature Engineering: Add missing lag/persistence features ---
    try:
        # Filter to recent observations (last 48h for lag features)
        obs_df = modeldata_h2s_nofill.copy()
        obs_df = obs_df[(obs_df['h2s_measured'] == True) & (obs_df['H2S'] <= 500)]
        cutoff = now - pd.Timedelta(hours=48)
        obs_df = obs_df[obs_df['time'] >= cutoff]

        logger.info(f"Using {len(obs_df)} observation rows for feature engineering")

        # Engineer all missing features (H2S lags, flow lags, etc.)
        merged = forecast_features.engineer_features(merged, obs_df)

        # Verify all 43 features are present
        missing = [f for f in forecast_features.MODEL_FEATURES if f not in merged.columns]
        if missing:
            logger.warning(f"Still missing features after engineering: {missing}")
        else:
            logger.info("All 43 MODEL_FEATURES successfully populated")

    except Exception as e:
        logger.warning(f"Feature engineering failed, some features may be missing: {e}")
        import traceback
        logger.error(traceback.format_exc())

    store_assets.store_dataframe_to_s3(
        merged, OUTPUT_PATH, 'model_forecast', s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True,
        formats=['csv', 'parquet'], metadata=metadata,
    )
    return merged


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_forecast_15min",
    required_resource_keys={"s3"},
    ins={
        "openmeteo_15min_forecast": AssetIn(key=AssetKey(["weather", "openmeteo_15min_forecast"])),
        "openmeteo_forecast": AssetIn(key=AssetKey(["weather", "openmeteo_forecast"])),
        "tidal_forecast": AssetIn(key=AssetKey(["tides", "tidal_forecast"])),
        "streamflow_forecast": AssetIn(key=AssetKey(["streamflow", "streamflow_forecast"])),
        "modeldata_h2s_nofill": AssetIn(key=AssetKey(["h2sforecast", "modeldata_h2s_nofill"])),
    },
    metadata={
        "source": "OpenMeteo 15-min forecast, NOAA tidal predictions, IBWC streamflow forecast",
        "description": (
            "15-minute resolution forecast feature dataset (±24 h). "
            "Past 24 h for emission source analysis; future 24 h for model predictions."
        ),
        "variableMeasured": ["Wind Direction", "Wind Speed", "Tide Height", "Streamflow"],
    },
    automation_condition=AutomationCondition.eager(),
)
def modeldata_forecast_15min(
    context,
    openmeteo_15min_forecast,
    openmeteo_forecast,
    tidal_forecast,
    streamflow_forecast,
    modeldata_h2s_nofill,
):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variableMeasured,
    )

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    # --- Weather (15-min) ---
    weather_df = openmeteo_15min_forecast.copy()
    weather_df["time"] = weather_df["time"].dt.tz_convert("America/Los_Angeles")
    weather_df["site_name"] = weather_df["site_name"].str.strip()
    # Rename for MODEL_FEATURES compatibility (hourly uses dewpoint_2m)
    if "dew_point_2m" in weather_df.columns:
        weather_df = weather_df.rename(columns={"dew_point_2m": "dewpoint_2m"})
    weather_df = weather_df.sort_values(["site_name", "time"]).reset_index(drop=True)

    # --- Merge surface_pressure and cloud_cover from hourly forecast (nearest-neighbor) ---
    hourly_df = openmeteo_forecast.copy()
    hourly_df = hourly_df.rename(columns={"date": "time"})
    hourly_df["time"] = hourly_df["time"].dt.tz_convert("America/Los_Angeles")
    hourly_df["site_name"] = hourly_df["site_name"].str.strip()
    hourly_cols = ["time", "site_name"] + [
        c for c in ("surface_pressure", "cloud_cover") if c in hourly_df.columns
    ]
    hourly_df = hourly_df[hourly_cols].sort_values(["site_name", "time"]).reset_index(drop=True)
    weather_df = pd.merge_asof(
        weather_df.sort_values("time"),
        hourly_df.sort_values("time"),
        on="time",
        by="site_name",
        direction="nearest",
    )
    logger.info("Merged surface_pressure and cloud_cover from hourly forecast")

    # Wind direction trig/categorical features (add_wind_features uses row-count windows)
    weather_df = add_wind_features(weather_df, logger)

    # Overwrite rolling windows with time-correct values: 8/12/16 rows = 2h/3h/4h at 15-min
    for n_rows, label in [(8, "2h"), (12, "3h"), (16, "4h")]:
        weather_df[f"wind_speed_10m_avg_{label}"] = (
            weather_df.groupby("site_name")["wind_speed_10m"]
            .transform(lambda x: x.rolling(window=n_rows, min_periods=1).mean())
        )
        weather_df[f"wind_gusts_10m_max_{label}"] = (
            weather_df.groupby("site_name")["wind_gusts_10m"]
            .transform(lambda x: x.rolling(window=n_rows, min_periods=1).max())
        )

    # Derive day_night from OpenMeteo is_day flag (avoids re-computing astral sunrise/sunset)
    weather_df["day_night"] = weather_df["is_day"].map({1: "day", 0: "night"}).fillna("night")

    # Guard: wind_direction_10m is required by add_inference_features but may be absent
    # in data materialized before it was added to MINUTELY_15_VARIABLES.
    # NaN wind direction causes source_regime to return 0 (all comparisons with NaN are False).
    if "wind_direction_10m" not in weather_df.columns:
        logger.warning(
            "wind_direction_10m not found in 15-min data — source_regime will default to 0. "
            "Re-materialize openmeteo_15min_forecast to restore full functionality."
        )
        weather_df["wind_direction_10m"] = np.nan

    weather_df = weather_df.sort_values("time").reset_index(drop=True)
    logger.info(
        f"15-min weather: {len(weather_df)} rows "
        f"({weather_df['time'].min()} → {weather_df['time'].max()}), "
        f"sites: {weather_df['site_name'].unique().tolist()}"
    )

    # --- Tidal ---
    tidal_df = tidal_forecast.copy()
    tidal_df["time"] = (
        pd.to_datetime(tidal_df["time"]).dt.tz_localize("UTC").dt.tz_convert("America/Los_Angeles")
    )
    tidal_df = add_tidal_encoding(tidal_df)
    tidal_df = tidal_df.sort_values("time").reset_index(drop=True)
    logger.info(f"Tidal forecast: {len(tidal_df)} rows")

    # --- Streamflow ---
    streamflow_df = streamflow_forecast.copy()
    streamflow_df["time"] = (
        pd.to_datetime(streamflow_df["time"]).dt.tz_localize("UTC").dt.tz_convert("America/Los_Angeles")
    )
    streamflow_df = streamflow_df.rename(columns={"Average (m^3/s)": "Flow (m^3/s)--Border"})
    streamflow_df = streamflow_df.sort_values("time").reset_index(drop=True)
    logger.info(f"Streamflow forecast: {len(streamflow_df)} rows")

    # --- Merge (nearest-neighbor; tidal and streamflow are hourly) ---
    merged = pd.merge_asof(weather_df, tidal_df, on="time", direction="nearest")
    merged = pd.merge_asof(merged, streamflow_df, on="time", direction="nearest")
    logger.info(f"Merged 15-min forecast: {len(merged)} rows")

    # --- Inference features (temporal cyclicals, source regime, flow transforms) ---
    merged = add_inference_features(merged, logger)

    # --- SBIWTP effluent flow persistence forecast ---
    now = pd.Timestamp.now(tz="America/Los_Angeles").floor("15min")
    try:
        duckdb_con = duckdb_connection(s3_resource)
        effluent_today_files = f"s3://{s3_resource.S3_BUCKET}/{EFFLUENT_BASE}/{EFFLUENT_TODAY}/{CSV_PATTERN}"
        effluent_today_df = duckdb_con.read_csv(effluent_today_files, null_padding=True).df()

        time_col = next(
            (c for c in effluent_today_df.columns if "timestamp" in c.lower() or "time" in c.lower()),
            None,
        )
        if not time_col:
            index_name = effluent_today_df.index.name
            if index_name and isinstance(index_name, str) and (
                "timestamp" in index_name.lower() or "time" in index_name.lower()
            ):
                time_col = index_name
                effluent_today_df = effluent_today_df.reset_index()

        value_col = next(
            (
                c for c in effluent_today_df.columns
                if c != time_col and pd.api.types.is_numeric_dtype(effluent_today_df[c])
            ),
            None,
        )
        if time_col and value_col:
            effluent_today_df["time"] = (
                pd.to_datetime(effluent_today_df[time_col])
                .dt.tz_localize("Etc/GMT+8")
                .dt.tz_convert("America/Los_Angeles")
            )
            today_mgd = effluent_today_df[value_col].mean()
            logger.info(f"SBIWTP today's mean effluent flow: {today_mgd:.2f} MGD")

            effluent_current_files = f"s3://{s3_resource.S3_BUCKET}/{EFFLUENT_BASE}/{EFFLUENT_CURRENT_YEAR}/{PARQUET_PATTERN}"
            effluent_hist_df = duckdb_con.read_parquet(effluent_current_files, union_by_name=True).df()
            hist_time_col = next(
                (c for c in effluent_hist_df.columns if "timestamp" in c.lower() or "time" in c.lower()),
                None,
            )
            if not hist_time_col:
                hist_index_name = effluent_hist_df.index.name
                if hist_index_name and isinstance(hist_index_name, str) and (
                    "timestamp" in hist_index_name.lower() or "time" in hist_index_name.lower()
                ):
                    hist_time_col = hist_index_name
                    effluent_hist_df = effluent_hist_df.reset_index()

            hist_value_col = next(
                (
                    c for c in effluent_hist_df.columns
                    if c != hist_time_col and pd.api.types.is_numeric_dtype(effluent_hist_df[c])
                ),
                None,
            )
            if hist_time_col and hist_value_col:
                effluent_hist_df["time"] = (
                    pd.to_datetime(effluent_hist_df[hist_time_col])
                    .dt.tz_localize("Etc/GMT+8")
                    .dt.tz_convert("America/Los_Angeles")
                )
                hist_series = effluent_hist_df.set_index("time")[hist_value_col].rename("sbiwtp_flow_mgd")
                mean_30d = hist_series.last("30D").mean()
            else:
                hist_series = pd.Series(dtype=float)
                mean_30d = today_mgd

            forecast_dates = merged["time"].dt.normalize().unique()
            today_date = now.normalize()
            mgd_by_date = {}
            for d in forecast_dates:
                hours_ahead = (d - today_date).total_seconds() / 3600
                if hours_ahead <= 24:
                    mgd_by_date[d] = today_mgd
                elif hours_ahead <= 48:
                    mgd_by_date[d] = today_mgd
                else:
                    days_beyond = (hours_ahead - 48) / 24
                    decay_factor = np.exp(-np.log(2) / 2.5 * days_beyond)
                    mgd_by_date[d] = today_mgd * decay_factor + mean_30d * (1 - decay_factor)

            sbiwtp_forecast_series = pd.Series(
                {d: mgd_by_date.get(d, mean_30d) for d in forecast_dates},
                name="sbiwtp_flow_mgd",
            )
            combined_series = pd.concat([hist_series, sbiwtp_forecast_series.rename("sbiwtp_flow_mgd")])
            merged = add_sbiwtp_features(merged, combined_series, logger)
        else:
            logger.warning("Could not parse SBIWTP today CSV — skipping SBIWTP forecast features")
            for col in ["sbiwtp_flow_mgd", "sbiwtp_anomaly", "sbiwtp_deficit",
                        "sbiwtp_flow_x_temp", "sbiwtp_hourly_mgd", "sbiwtp_sli"]:
                merged[col] = np.nan
    except Exception as e:
        logger.warning(f"SBIWTP forecast features failed, continuing without them: {e}")
        for col in ["sbiwtp_flow_mgd", "sbiwtp_anomaly", "sbiwtp_deficit",
                    "sbiwtp_flow_x_temp", "sbiwtp_hourly_mgd", "sbiwtp_sli"]:
            merged[col] = np.nan

    # --- Feature engineering: H2S persistence seeded from last observations ---
    try:
        obs_df = modeldata_h2s_nofill.copy()
        obs_df = obs_df[(obs_df["h2s_measured"] == True) & (obs_df["H2S"] <= 500)]
        cutoff = now - pd.Timedelta(hours=48)
        obs_df = obs_df[obs_df["time"] >= cutoff]
        logger.info(f"Using {len(obs_df)} observation rows for feature engineering")
        merged = forecast_features.engineer_features(merged, obs_df)
        missing = [f for f in forecast_features.MODEL_FEATURES if f not in merged.columns]
        if missing:
            logger.warning(f"Still missing features after engineering: {missing}")
        else:
            logger.info("All MODEL_FEATURES successfully populated")
    except Exception as e:
        logger.warning(f"Feature engineering failed, some features may be missing: {e}")
        import traceback
        logger.error(traceback.format_exc())

    store_assets.store_dataframe_to_s3(
        merged, OUTPUT_PATH, "modeldata_forecast_15min", s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True,
        formats=["csv", "parquet"], metadata=metadata,
    )
    return merged


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s_15min_24hour",
    required_resource_keys={"s3"},
    ins={
        "modeldata_forecast_15min": AssetIn(key=AssetKey(["h2sforecast", "modeldata_forecast_15min"])),
    },
    metadata={
        "source": "San Diego APCD H2S sensors, OpenMeteo 15-min forecast",
        "description": (
            "Past 24 h of H2S observations merged with 15-min meteorological context "
            "for emission source analysis. Includes wind regime, atmospheric stability, "
            "tidal state, streamflow, and SBIWTP effluent."
        ),
        "variableMeasured": ["H2S", "Wind Direction", "Wind Speed", "Source Regime"],
    },
    automation_condition=AutomationCondition.eager(),
)
def modeldata_h2s_15min_24hour(context, modeldata_forecast_15min):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=description,
        source_url=source_url,
        variableMeasured=variableMeasured,
    )

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    # --- Step 1: Filter 15-min data to past rows only ---
    # forecast_features.engineer_features converts time to UTC; normalize back to LA for consistency.
    now = pd.Timestamp.now(tz="America/Los_Angeles")
    modeldata_forecast_15min = modeldata_forecast_15min.copy()
    modeldata_forecast_15min["time"] = pd.to_datetime(
        modeldata_forecast_15min["time"], utc=True
    ).dt.tz_convert("America/Los_Angeles")
    past_df = modeldata_forecast_15min[modeldata_forecast_15min["time"] <= now].copy()
    logger.info(f"Past 15-min rows: {len(past_df)} up to {now}")

    # --- Step 2: Load H2S observations from S3 ---
    duckdb_con = duckdb_connection(s3_resource)
    h2s_files = f"s3://{s3_resource.S3_BUCKET}/{H2S_PATH}/{PARQUET_PATTERN}"
    try:
        h2s_raw = duckdb_con.read_parquet(h2s_files).df()
    except Exception as e:
        logger.error(f"Failed to load H2S parquet files {h2s_files}: {e}")
        raise

    try:
        h2s_raw["time"] = pd.to_datetime(h2s_raw["Date with time"], utc=True).dt.tz_convert("America/Los_Angeles")
        h2s_raw = h2s_raw.rename(columns={"SiteName": "site_name", "Result": "H2S"})
        h2s_raw["site_name"] = h2s_raw["site_name"].str.strip()
        cutoff = now - pd.Timedelta(hours=24)
        h2s_raw = h2s_raw[h2s_raw["time"] >= cutoff]
        h2s_raw = (
            h2s_raw.sort_values("time")
            .drop_duplicates(subset=["time", "site_name"], keep="last")
        )
        logger.info(f"H2S observations: {len(h2s_raw)} rows across sites {h2s_raw['site_name'].unique().tolist()}")
    except Exception as e:
        logger.error(f"Failed to process H2S data: {e}")
        raise

    # --- Step 3: Merge H2S onto 15-min met context ---
    merged = pd.merge_asof(
        past_df.sort_values("time"),
        h2s_raw[["time", "site_name", "H2S"]].sort_values("time"),
        on="time",
        by="site_name",
        direction="nearest",
        tolerance=pd.Timedelta("30min"),
    )
    logger.info(f"Merged rows: {len(merged)}, H2S non-null: {merged['H2S'].notna().sum()}")

    # --- Step 4: H2S risk classification ---
    merged["h2s_risk"] = "GREEN"
    merged.loc[merged["H2S"] > 5,  "h2s_risk"] = "YELLOW_LOW"
    merged.loc[merged["H2S"] > 10, "h2s_risk"] = "YELLOW_HIGH"
    merged.loc[merged["H2S"] > 30, "h2s_risk"] = "ORANGE"

    # --- Step 5: Select source-analysis columns (keep only what exists) ---
    desired_cols = [
        "time", "site_name", "H2S", "h2s_risk",
        "day_night", "is_night", "source_regime", "stable_atm",
        "wind_direction_10m", "wind_direction_categorical", "wind_speed_10m", "wind_gusts_10m",
        "temperature_2m", "relative_humidity_2m", "dewpoint_2m", "precipitation",
        "surface_pressure", "cloud_cover",
        "tide_height", "tidal_state_encoded",
        "Flow (m^3/s)--Border",
        "sbiwtp_flow_mgd", "sbiwtp_sli",
        "hour_sin", "hour_cos", "month_sin", "month_cos",
    ]
    output_cols = [c for c in desired_cols if c in merged.columns]
    missing = [c for c in desired_cols if c not in merged.columns]
    if missing:
        logger.warning(f"Columns not available, skipped: {missing}")
    merged = merged[output_cols]

    store_assets.store_dataframe_to_s3(
        merged, OUTPUT_PATH, "modeldata_h2s_15min_24hour", s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True,
        formats=["csv", "parquet"], metadata=metadata,
    )
    return merged
