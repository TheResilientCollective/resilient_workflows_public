#!/usr/bin/env python3
"""
H2S Feature Engineering Reference
==================================

Shows exactly how every derived feature is built from the raw inputs.
Two sections:
  1. Features that go INTO the parquet files (your pipeline builds these)
  2. Features constructed AT TRAIN/FORECAST TIME (the model scripts build these)

RAW INPUTS NEEDED IN BOTH PARQUETS:
  - time, site_name, H2S (obs only), h2s_measured (obs only)
  - temperature_2m, wind_speed_10m, wind_direction_10m
  - wind_gusts_10m, precipitation, relative_humidity_2m
  - surface_pressure, cloud_cover, dewpoint_2m
  - Flow (m^3/s)--Border, tide_height, tidal_state
  - day_night
  - SBIWTP daily effluent flow (for sbiwtp_* features)

Usage:
  # To build features for your observation parquet:
  df = build_obs_features(df, sbiwtp_daily_df)

  # To build features for your forecast parquet:
  df = build_forecast_features(df, sbiwtp_daily_df)
"""

import numpy as np
import pandas as pd


# ============================================================
# SECTION 1: Features that go INTO the parquet files
# (Your pipeline should produce these)
# ============================================================

def build_obs_features(df, sbiwtp_df=None):
    """
    Build all derived features for the observation parquet.

    Parameters
    ----------
    df : DataFrame with raw columns:
        time, site_name, H2S, h2s_measured,
        temperature_2m, wind_speed_10m, wind_direction_10m,
        wind_gusts_10m, precipitation, relative_humidity_2m,
        surface_pressure, cloud_cover, dewpoint_2m,
        Flow (m^3/s)--Border, tide_height, tidal_state,
        day_night

    sbiwtp_df : DataFrame with columns [time, flow_mgd]
        Daily SBIWTP effluent flow. If None, SBIWTP features are set to NaN.

    Returns
    -------
    DataFrame with all features added.
    """
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])

    # ------------------------------------------------------------------
    # WIND DIRECTION (circular decomposition)
    # ------------------------------------------------------------------
    wd = df['wind_direction_10m']
    df['wind_direction_sin'] = np.sin(np.radians(wd))
    df['wind_direction_cos'] = np.cos(np.radians(wd))

    # Categorical (for reference, not used by model directly)
    bins = [0, 22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5, 360]
    labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N2']
    df['wind_direction_categorical'] = pd.cut(wd, bins=bins, labels=labels, ordered=False)
    df['wind_direction_categorical'] = df['wind_direction_categorical'].replace('N2', 'N')
    cat_map = {'N': 0, 'NE': 1, 'E': 2, 'SE': 3, 'S': 4, 'SW': 5, 'W': 6, 'NW': 7}
    df['wind_direction_categorical_encoded'] = df['wind_direction_categorical'].map(cat_map).fillna(0).astype(int)

    # ------------------------------------------------------------------
    # ROLLING WIND FEATURES (per station)
    # ------------------------------------------------------------------
    for site in df['site_name'].unique():
        m = df['site_name'] == site
        df.loc[m, 'wind_speed_10m_avg_2h'] = df.loc[m, 'wind_speed_10m'].rolling(2, min_periods=1).mean()
        df.loc[m, 'wind_speed_10m_avg_3h'] = df.loc[m, 'wind_speed_10m'].rolling(3, min_periods=1).mean()
        df.loc[m, 'wind_speed_10m_avg_4h'] = df.loc[m, 'wind_speed_10m'].rolling(4, min_periods=1).mean()
        df.loc[m, 'wind_gusts_10m_max_2h'] = df.loc[m, 'wind_gusts_10m'].rolling(2, min_periods=1).max()
        df.loc[m, 'wind_gusts_10m_max_3h'] = df.loc[m, 'wind_gusts_10m'].rolling(3, min_periods=1).max()
        df.loc[m, 'wind_gusts_10m_max_4h'] = df.loc[m, 'wind_gusts_10m'].rolling(4, min_periods=1).max()

    # ------------------------------------------------------------------
    # INTERACTION FEATURES
    # ------------------------------------------------------------------
    df['wind_temp_interaction'] = df['wind_speed_10m'] * df['temperature_2m']
    df['humidity_temp_interaction'] = df['relative_humidity_2m'] * df['temperature_2m']

    # ------------------------------------------------------------------
    # TIDAL STATE ENCODING
    # ------------------------------------------------------------------
    tidal_map = {'low': 0, 'ebb': 1, 'flood': 2, 'slack': 3, 'high': 4}
    df['tidal_state_encoded'] = df['tidal_state'].map(tidal_map).fillna(1).astype(int)

    # ------------------------------------------------------------------
    # TIME FEATURES
    # ------------------------------------------------------------------
    df['hour_sin'] = np.sin(2 * np.pi * df['time'].dt.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['time'].dt.hour / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['time'].dt.month / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['time'].dt.month / 12)

    # ------------------------------------------------------------------
    # DAY/NIGHT & SOURCE REGIME
    # ------------------------------------------------------------------
    df['is_night'] = (df['day_night'] == 'night').astype(int)

    def _source_regime(row):
        if row['day_night'] != 'night':
            return 0  # Daytime mixed
        wd_val = row['wind_direction_10m']
        if 22.5 <= wd_val < 135:
            return 1  # East (border crossing)
        elif wd_val >= 247.5 or wd_val < 22.5:
            return 2  # West (estuary/pump station)
        elif 135 <= wd_val < 247.5:
            return 3  # South (cross-border)
        return 0

    df['source_regime'] = df.apply(_source_regime, axis=1)

    # ------------------------------------------------------------------
    # BORDER FLOW FEATURES
    # ------------------------------------------------------------------
    df['flow_log'] = np.log1p(df['Flow (m^3/s)--Border'])
    df['flow_low'] = (df['Flow (m^3/s)--Border'] < 1).astype(int)
    df['flow_high'] = (df['Flow (m^3/s)--Border'] > 5).astype(int)

    # ------------------------------------------------------------------
    # ATMOSPHERIC STABILITY PROXY
    # ------------------------------------------------------------------
    df['stable_atm'] = ((df['wind_speed_10m'] < 5) & (df['is_night'] == 1)).astype(int)

    # ------------------------------------------------------------------
    # H2S LAG FEATURES (per station, for v4 single-model)
    # ------------------------------------------------------------------
    for site in df['site_name'].unique():
        m = df['site_name'] == site
        s = df.loc[m].copy()
        df.loc[m, 'h2s_lag_1h'] = s['H2S'].shift(1).values
        df.loc[m, 'h2s_lag_3h'] = s['H2S'].shift(3).values
        df.loc[m, 'h2s_lag_6h'] = s['H2S'].shift(6).values
        df.loc[m, 'h2s_rolling_6h'] = s['H2S'].rolling(6, min_periods=1).mean().values
        df.loc[m, 'h2s_rolling_24h'] = s['H2S'].rolling(24, min_periods=1).mean().values
        df.loc[m, 'flow_lag_6h'] = s['Flow (m^3/s)--Border'].shift(6).values
        df.loc[m, 'flow_rolling_24h'] = s['Flow (m^3/s)--Border'].rolling(24, min_periods=1).mean().values

    # ------------------------------------------------------------------
    # SBIWTP FEATURES (from daily effluent data, 1-day lag)
    # ------------------------------------------------------------------
    if sbiwtp_df is not None:
        df = _add_sbiwtp_features(df, sbiwtp_df)
    else:
        for c in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                   'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
            df[c] = np.nan

    # ------------------------------------------------------------------
    # RISK CLASSIFICATION (informational, not a model feature)
    # ------------------------------------------------------------------
    df['h2s_risk'] = 'GREEN'
    df.loc[df['H2S'] > 5, 'h2s_risk'] = 'YELLOW'
    df.loc[df['H2S'] > 10, 'h2s_risk'] = 'ORANGE'
    df.loc[df['H2S'] > 30, 'h2s_risk'] = 'RED'

    return df


def build_forecast_features(df, sbiwtp_df=None):
    """
    Build all derived features for the forecast parquet.
    Same as obs features except: no H2S lags (those come from the
    forecast runner using last known obs), and no H2S/h2s_measured.

    Parameters
    ----------
    df : DataFrame with raw forecast columns:
        time, site_name,
        temperature_2m, wind_speed_10m, wind_direction_10m,
        wind_gusts_10m (or estimated as wind_speed * 1.8),
        precipitation, relative_humidity_2m, surface_pressure,
        cloud_cover, dewpoint_2m,
        Flow (m^3/s)--Border, tide_height, tidal_state,
        day_night

    sbiwtp_df : DataFrame with [time, flow_mgd] for SBIWTP persistence

    Returns
    -------
    DataFrame with all base features added.
    H2S lag features are NOT added here — they're filled by the
    forecast runner (engineer_forecast_features.py or forecast_multihorizon.py)
    using the last known observation state.
    """
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])

    # Estimate gusts if missing
    if 'wind_gusts_10m' not in df.columns or df['wind_gusts_10m'].isna().all():
        df['wind_gusts_10m'] = df['wind_speed_10m'] * 1.8

    # Wind direction decomposition
    wd = df['wind_direction_10m']
    df['wind_direction_sin'] = np.sin(np.radians(wd))
    df['wind_direction_cos'] = np.cos(np.radians(wd))

    bins = [0, 22.5, 67.5, 112.5, 157.5, 202.5, 247.5, 292.5, 337.5, 360]
    labels = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW', 'N2']
    df['wind_direction_categorical'] = pd.cut(wd, bins=bins, labels=labels, ordered=False)
    df['wind_direction_categorical'] = df['wind_direction_categorical'].replace('N2', 'N')
    cat_map = {'N': 0, 'NE': 1, 'E': 2, 'SE': 3, 'S': 4, 'SW': 5, 'W': 6, 'NW': 7}
    df['wind_direction_categorical_encoded'] = df['wind_direction_categorical'].map(cat_map).fillna(0).astype(int)

    # Rolling wind (per station)
    for site in df['site_name'].unique():
        m = df['site_name'] == site
        df.loc[m, 'wind_speed_10m_avg_2h'] = df.loc[m, 'wind_speed_10m'].rolling(2, min_periods=1).mean()
        df.loc[m, 'wind_speed_10m_avg_3h'] = df.loc[m, 'wind_speed_10m'].rolling(3, min_periods=1).mean()
        df.loc[m, 'wind_speed_10m_avg_4h'] = df.loc[m, 'wind_speed_10m'].rolling(4, min_periods=1).mean()
        df.loc[m, 'wind_gusts_10m_max_2h'] = df.loc[m, 'wind_gusts_10m'].rolling(2, min_periods=1).max()
        df.loc[m, 'wind_gusts_10m_max_3h'] = df.loc[m, 'wind_gusts_10m'].rolling(3, min_periods=1).max()
        df.loc[m, 'wind_gusts_10m_max_4h'] = df.loc[m, 'wind_gusts_10m'].rolling(4, min_periods=1).max()

    # Interactions
    df['wind_temp_interaction'] = df['wind_speed_10m'] * df['temperature_2m']
    df['humidity_temp_interaction'] = df['relative_humidity_2m'] * df['temperature_2m']

    # Tidal encoding
    tidal_map = {'low': 0, 'ebb': 1, 'flood': 2, 'slack': 3, 'high': 4}
    df['tidal_state_encoded'] = df['tidal_state'].map(tidal_map).fillna(1).astype(int)

    # Time features
    df['hour_sin'] = np.sin(2 * np.pi * df['time'].dt.hour / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['time'].dt.hour / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['time'].dt.month / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['time'].dt.month / 12)

    # Day/night and source regime
    df['is_night'] = (df['day_night'] == 'night').astype(int)

    def _source_regime(row):
        if row['day_night'] != 'night':
            return 0
        wd_val = row['wind_direction_10m']
        if 22.5 <= wd_val < 135:
            return 1
        elif wd_val >= 247.5 or wd_val < 22.5:
            return 2
        elif 135 <= wd_val < 247.5:
            return 3
        return 0

    df['source_regime'] = df.apply(_source_regime, axis=1)

    # Flow features
    df['flow_log'] = np.log1p(df['Flow (m^3/s)--Border'])
    df['flow_low'] = (df['Flow (m^3/s)--Border'] < 1).astype(int)
    df['flow_high'] = (df['Flow (m^3/s)--Border'] > 5).astype(int)

    # Stability
    df['stable_atm'] = ((df['wind_speed_10m'] < 5) & (df['is_night'] == 1)).astype(int)

    # SBIWTP
    if sbiwtp_df is not None:
        df = _add_sbiwtp_features(df, sbiwtp_df)
    else:
        for c in ['sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
                   'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli']:
            df[c] = np.nan

    # Placeholder H2S lag columns (filled by forecast runner from obs state)
    for c in ['h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h',
              'h2s_rolling_6h', 'h2s_rolling_24h',
              'flow_lag_6h', 'flow_rolling_24h']:
        if c not in df.columns:
            df[c] = np.nan

    return df


# ============================================================
# SECTION 2: SBIWTP Feature Engineering
# ============================================================

def _add_sbiwtp_features(df, sbiwtp_df):
    """
    Add all 6 SBIWTP features to an hourly DataFrame.

    Parameters
    ----------
    df : hourly DataFrame (obs or forecast) with 'time' column
    sbiwtp_df : DataFrame with columns:
        time : daily timestamps (one per day)
        flow_mgd : daily effluent flow in MGD

    The SBIWTP data is applied with a 1-day lag: yesterday's plant
    throughput predicts today's H2S.
    """
    sb = sbiwtp_df.copy()
    sb['time'] = pd.to_datetime(sb['time'])
    sb = sb.sort_values('time').reset_index(drop=True)

    # --- sbiwtp_flow_mgd (raw daily, 1-day lag) ---
    sb['sbiwtp_flow_mgd'] = sb['flow_mgd']

    # --- sbiwtp_anomaly (deviation from 30-day rolling mean) ---
    sb['_30d'] = sb['flow_mgd'].rolling(30, min_periods=7).mean()
    sb['sbiwtp_anomaly'] = (sb['flow_mgd'] - sb['_30d']) / sb['_30d']

    # --- sbiwtp_deficit (how far below expected) ---
    sb['sbiwtp_deficit'] = (sb['_30d'] - sb['flow_mgd']).clip(lower=0)

    # --- Sewage Load Index (accumulation with tidal decay) ---
    # SLI(t) = SLI(t-1) * decay + deficit(t)
    # Half-life ~2.5 days
    decay = np.exp(-np.log(2) / 2.5)
    sli = np.zeros(len(sb))
    deficit_vals = sb['sbiwtp_deficit'].fillna(0).values
    for i in range(1, len(sb)):
        sli[i] = sli[i-1] * decay + deficit_vals[i]
    sb['sbiwtp_sli'] = sli

    # Apply 1-day lag: shift date forward so "yesterday's SBIWTP" aligns with "today"
    sb['date'] = sb['time'] + pd.Timedelta(days=1)

    # Merge daily SBIWTP onto hourly data
    df_date = df['time'].dt.normalize()
    if df['time'].dt.tz is not None:
        df_date = df_date.dt.tz_localize(None)

    merge_cols = ['date', 'sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit', 'sbiwtp_sli']
    sb_merge = sb[merge_cols].copy()

    df['_date'] = df_date
    df = df.merge(sb_merge.rename(columns={'date': '_date'}), on='_date', how='left',
                  suffixes=('_old', ''))

    # Drop any old versions and the temp column
    for c in ['sbiwtp_flow_mgd_old', 'sbiwtp_anomaly_old', 'sbiwtp_deficit_old', 'sbiwtp_sli_old']:
        if c in df.columns:
            df = df.drop(columns=c)
    df = df.drop(columns='_date')

    # --- sbiwtp_flow_x_temp (interaction: treatment × temperature) ---
    df['sbiwtp_flow_x_temp'] = df['sbiwtp_flow_mgd'] * df['temperature_2m']

    # --- sbiwtp_hourly_mgd (diurnal disaggregation) ---
    # Wastewater treatment diurnal profile (fraction of daily mean)
    diurnal = {
        0: 0.70, 1: 0.65, 2: 0.60, 3: 0.58, 4: 0.60, 5: 0.70,
        6: 0.85, 7: 1.05, 8: 1.15, 9: 1.20, 10: 1.25, 11: 1.20,
        12: 1.15, 13: 1.10, 14: 1.10, 15: 1.10, 16: 1.15, 17: 1.20,
        18: 1.15, 19: 1.10, 20: 1.00, 21: 0.90, 22: 0.85, 23: 0.75,
    }
    mean_factor = np.mean(list(diurnal.values()))
    hour = df['time'].dt.hour
    hourly_factor = hour.map(diurnal).fillna(1.0) / mean_factor
    df['sbiwtp_hourly_mgd'] = df['sbiwtp_flow_mgd'] * hourly_factor

    return df


# ============================================================
# SECTION 3: Multi-Horizon Features
# (Built at train/forecast time, NOT stored in parquet)
# ============================================================
# These are constructed by train_multihorizon.py and
# forecast_multihorizon.py from the base features above.
# Listed here for reference only.

MULTI_HORIZON_FEATURES = """
CONSTRUCTED AT TRAIN TIME (from H2S observations):
  h2s_lag_{offset}h      — H2S value {offset} hours before target
                           0-6h uses: 1, 3, 6
                           6-24h uses: 6, 12, 24
                           24-48h uses: 24, 48
                           48-72h uses: 48, 72

  h2s_roll_{window}h_at_{offset}h — Rolling mean of H2S over {window}
                                     hours, ending {offset} hours before
                           0-6h: roll_6h_at_1h, roll_24h_at_1h
                           6-24h: roll_6h_at_6h, roll_24h_at_6h
                           24-48h: roll_24h_at_24h
                           48-72h: roll_24h_at_48h

  flow_lag_{offset}h     — Border flow {offset} hours before
  flow_roll_24h_at_{offset}h — 24h rolling flow mean

CONSTRUCTED AT TRAIN TIME (from daily H2S stats):
  yest_max               — Yesterday's max H2S
  yest_mean              — Yesterday's mean H2S
  yest_std               — Yesterday's H2S std dev
  twoday_max/mean/std    — Two days ago stats (48-72h only)

  exceed_rate_7d         — Fraction of hours >5 ppb in last 7 days
  spill_active           — 1 if max H2S > 100 ppb in last 72 hours
  crisis_days            — Count of days with max>30 ppb in last 7 days

AT FORECAST TIME:
  H2S lags seeded from last observation, decayed exponentially
  Daily stats from most recent complete day of observations
  spill_active and crisis_days from recent observation history
"""


# ============================================================
# CONVENIENCE: Column checklists
# ============================================================

# Minimum raw columns needed BEFORE feature engineering
RAW_COLUMNS_OBS = [
    'time', 'site_name', 'H2S', 'h2s_measured',
    'temperature_2m', 'wind_speed_10m', 'wind_direction_10m',
    'wind_gusts_10m', 'precipitation', 'relative_humidity_2m',
    'surface_pressure', 'cloud_cover', 'dewpoint_2m',
    'Flow (m^3/s)--Border', 'tide_height', 'tidal_state',
    'day_night',
]

RAW_COLUMNS_FORECAST = [
    'time', 'site_name',
    'temperature_2m', 'wind_speed_10m', 'wind_direction_10m',
    'precipitation', 'relative_humidity_2m',
    'surface_pressure', 'cloud_cover', 'dewpoint_2m',
    'Flow (m^3/s)--Border', 'tide_height', 'tidal_state',
    'day_night',
    # wind_gusts_10m is optional (estimated as 1.8x wind_speed if missing)
]

# All columns that should be in the obs parquet AFTER feature engineering
ENGINEERED_COLUMNS_OBS = RAW_COLUMNS_OBS + [
    'wind_direction_sin', 'wind_direction_cos',
    'wind_direction_categorical', 'wind_direction_categorical_encoded',
    'wind_speed_10m_avg_2h', 'wind_speed_10m_avg_3h', 'wind_speed_10m_avg_4h',
    'wind_gusts_10m_max_2h', 'wind_gusts_10m_max_3h', 'wind_gusts_10m_max_4h',
    'wind_temp_interaction', 'humidity_temp_interaction',
    'tidal_state_encoded',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'is_night', 'source_regime',
    'flow_log', 'flow_low', 'flow_high',
    'stable_atm',
    'h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h',
    'h2s_rolling_6h', 'h2s_rolling_24h',
    'flow_lag_6h', 'flow_rolling_24h',
    'sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli',
    'h2s_risk',
]


if __name__ == '__main__':
    print("Feature Engineering Reference")
    print(f"  Raw obs columns needed: {len(RAW_COLUMNS_OBS)}")
    print(f"  Raw forecast columns needed: {len(RAW_COLUMNS_FORECAST)}")
    print(f"  Engineered obs columns: {len(ENGINEERED_COLUMNS_OBS)}")
    print(f"\nMulti-horizon features (built at train/forecast time):")
    print(MULTI_HORIZON_FEATURES)
