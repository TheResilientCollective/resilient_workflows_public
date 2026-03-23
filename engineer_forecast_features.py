#!/usr/bin/env python3
"""
engineer_forecast_features.py

Takes a forecast parquet (weather/tides/flow/SBIWTP placeholders)
and an observation parquet (recent H2S measurements), and produces
a forecast-ready DataFrame with all 43 model features populated.

Usage:
    python engineer_forecast_features.py \
        --forecast model_forecast.parquet \
        --obs modeldata_h2s_nofill.parquet \
        --output forecast_ready.parquet

Can also be imported:
    from engineer_forecast_features import engineer_features
    df = engineer_features(forecast_df, obs_df)
"""

import argparse
import numpy as np
import pandas as pd


# Full ordered feature list (must match training)
MODEL_FEATURES = [
    'temperature_2m', 'wind_speed_10m', 'wind_direction_sin', 'wind_direction_cos',
    'wind_gusts_10m', 'precipitation', 'relative_humidity_2m', 'surface_pressure',
    'cloud_cover', 'dewpoint_2m',
    'wind_speed_10m_avg_2h', 'wind_speed_10m_avg_3h', 'wind_speed_10m_avg_4h',
    'wind_gusts_10m_max_2h', 'wind_gusts_10m_max_3h', 'wind_gusts_10m_max_4h',
    'tide_height', 'tidal_state_encoded',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'is_night', 'source_regime',
    'flow_log', 'flow_low', 'flow_high',
    'wind_temp_interaction', 'humidity_temp_interaction', 'stable_atm',
    'h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h',
    'h2s_rolling_6h', 'h2s_rolling_24h',
    'flow_lag_6h', 'flow_rolling_24h',
    'sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli',
]


def get_last_known_state(obs_df, site_name):
    """
    Extract the most recent observation state for a station.
    Used to seed persistence/decay features into the forecast.
    """
    ss = obs_df[obs_df['site_name'] == site_name].sort_values('time')
    if len(ss) == 0:
        return {
            'h2s': 0, 'h2s_6h': 0, 'h2s_24h': 0,
            'flow': 2.0, 'flow_24h': 2.0,
            'sbiwtp_mgd': 23.0, 'sbiwtp_sli': 5.0,
            'sbiwtp_anomaly': 0, 'sbiwtp_deficit': 0,
            'sbiwtp_hourly_mgd': 23.0,
        }

    # H2S state
    h2s_last = ss.iloc[-1]['H2S']
    h2s_6h = ss.tail(6)['H2S'].mean()
    h2s_24h = ss.tail(24)['H2S'].mean()

    # Flow state
    flow_last = ss.iloc[-1]['Flow (m^3/s)--Border']
    flow_24h = ss.tail(24)['Flow (m^3/s)--Border'].mean()

    # SBIWTP state (last non-null values)
    def last_valid(col):
        vals = ss[col].dropna()
        return vals.iloc[-1] if len(vals) > 0 else None

    return {
        'h2s': h2s_last,
        'h2s_6h': h2s_6h,
        'h2s_24h': h2s_24h,
        'flow': flow_last,
        'flow_24h': flow_24h,
        'sbiwtp_mgd': last_valid('sbiwtp_flow_mgd') or 23.0,
        'sbiwtp_sli': last_valid('sbiwtp_sli') or 5.0,
        'sbiwtp_anomaly': last_valid('sbiwtp_anomaly') or 0.0,
        'sbiwtp_deficit': last_valid('sbiwtp_deficit') or 0.0,
        'sbiwtp_hourly_mgd': last_valid('sbiwtp_hourly_mgd') or 23.0,
    }


def engineer_station_features(sfc, last_state):
    """
    Fill all missing features for a single station's forecast slice.

    Parameters
    ----------
    sfc : DataFrame
        Forecast rows for one station, sorted by time.
    last_state : dict
        Output of get_last_known_state().

    Returns
    -------
    DataFrame with all MODEL_FEATURES columns populated.
    """
    df = sfc.copy()
    n = len(df)
    h = np.arange(n)
    ls = last_state

    # ------------------------------------------------------------------
    # 1. WIND GUSTS (estimate as 1.8× sustained if missing)
    # ------------------------------------------------------------------
    if 'wind_gusts_10m' not in df.columns or df['wind_gusts_10m'].isna().all():
        df['wind_gusts_10m'] = df['wind_speed_10m'] * 1.8

    for window, col in [(2, 'wind_gusts_10m_max_2h'),
                        (3, 'wind_gusts_10m_max_3h'),
                        (4, 'wind_gusts_10m_max_4h')]:
        if col not in df.columns or df[col].isna().all():
            df[col] = df['wind_gusts_10m'].rolling(window, min_periods=1).max()

    # ------------------------------------------------------------------
    # 2. H2S PERSISTENCE (exponential decay from last known)
    #    12h e-folding for short lags, 36h for 24h rolling
    #    These degrade gracefully: strong signal hours 1-12, weak by 36-48
    # ------------------------------------------------------------------
    decay_fast = np.exp(-h / 12)
    decay_slow = np.exp(-h / 36)

    lh = ls['h2s']
    df['h2s_lag_1h'] = np.concatenate([[lh], lh * decay_fast[:-1]])
    df['h2s_lag_3h'] = np.concatenate([[lh] * min(3, n), (lh * decay_fast)[:max(n - 3, 0)]])
    df['h2s_lag_6h'] = np.concatenate([[lh] * min(6, n), (lh * decay_fast)[:max(n - 6, 0)]])
    df['h2s_rolling_6h'] = ls['h2s_6h'] * decay_fast
    df['h2s_rolling_24h'] = ls['h2s_24h'] * decay_slow

    # ------------------------------------------------------------------
    # 3. FLOW PERSISTENCE (last known value — changes slowly)
    # ------------------------------------------------------------------
    df['flow_lag_6h'] = ls['flow']
    df['flow_rolling_24h'] = ls['flow_24h']

    # ------------------------------------------------------------------
    # 4. SBIWTP (fill nulls from last known, recompute interaction)
    # ------------------------------------------------------------------
    df['sbiwtp_flow_mgd'] = df['sbiwtp_flow_mgd'].fillna(ls['sbiwtp_mgd'])
    df['sbiwtp_anomaly'] = df['sbiwtp_anomaly'].fillna(ls['sbiwtp_anomaly'])
    df['sbiwtp_deficit'] = df['sbiwtp_deficit'].fillna(ls['sbiwtp_deficit'])
    df['sbiwtp_hourly_mgd'] = df['sbiwtp_hourly_mgd'].fillna(ls['sbiwtp_hourly_mgd'])
    df['sbiwtp_sli'] = df['sbiwtp_sli'].fillna(ls['sbiwtp_sli'])

    # Recompute interaction (always, since temp comes from forecast)
    df['sbiwtp_flow_x_temp'] = df['sbiwtp_flow_mgd'] * df['temperature_2m']

    # ------------------------------------------------------------------
    # 5. VERIFY all features present
    # ------------------------------------------------------------------
    missing = [f for f in MODEL_FEATURES if f not in df.columns]
    if missing:
        raise ValueError(f"Still missing after engineering: {missing}")

    return df


def engineer_features(forecast_df, obs_df):
    """
    Main entry point. Takes raw forecast + observation DataFrames,
    returns a forecast DataFrame with all 43 model features populated.

    Parameters
    ----------
    forecast_df : DataFrame
        From model_forecast.parquet. Must have: time, site_name,
        temperature_2m, wind_speed_10m, wind_direction_10m, etc.
    obs_df : DataFrame
        From modeldata_h2s_nofill.parquet (h2s_measured==True, H2S<=500).
        Used only to extract last known state for persistence features.

    Returns
    -------
    DataFrame ready for model.predict(), with MODEL_FEATURES columns.
    """
    forecast_df = forecast_df.copy()
    forecast_df['time'] = pd.to_datetime(forecast_df['time'], utc=True)

    obs_df = obs_df.copy()
    obs_df['time'] = pd.to_datetime(obs_df['time'], utc=True)

    results = []
    for site_name in forecast_df['site_name'].unique():
        sfc = forecast_df[forecast_df['site_name'] == site_name].sort_values('time').reset_index(drop=True)
        last_state = get_last_known_state(obs_df, site_name)
        sfc_ready = engineer_station_features(sfc, last_state)
        results.append(sfc_ready)

    return pd.concat(results, ignore_index=True)


# ============================================================
# CLI
# ============================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Engineer missing forecast features')
    parser.add_argument('--forecast', required=True, help='Path to model_forecast.parquet')
    parser.add_argument('--obs', required=True, help='Path to modeldata_h2s_nofill.parquet')
    parser.add_argument('--output', default='forecast_ready.parquet', help='Output path')
    args = parser.parse_args()

    print("Loading forecast...")
    fc = pd.read_parquet(args.forecast)

    print("Loading observations...")
    obs = pd.read_parquet(args.obs)
    obs = obs[(obs['h2s_measured'] == True) & (obs['H2S'] <= 500)].copy()

    print("Engineering features...")
    result = engineer_features(fc, obs)

    # Verify
    for f in MODEL_FEATURES:
        nulls = result[f].isna().sum()
        if nulls > 0:
            print(f"  WARNING: {f} has {nulls} nulls")

    print(f"Output: {len(result)} rows, {len(MODEL_FEATURES)} features")
    result.to_parquet(args.output, index=False)
    print(f"Saved to {args.output}")

    # Summary
    for site in result['site_name'].unique():
        ss = result[result['site_name'] == site]
        print(f"\n{site}: {len(ss)} hours")
        print(f"  h2s_lag_1h: {ss['h2s_lag_1h'].iloc[0]:.1f} -> {ss['h2s_lag_1h'].iloc[-1]:.1f}")
        print(f"  sbiwtp_flow_mgd: {ss['sbiwtp_flow_mgd'].mean():.1f}")
        print(f"  sbiwtp_sli: {ss['sbiwtp_sli'].mean():.1f}")
