"""
forecast_features.py

Feature engineering utilities for H2S forecasting.

Takes a forecast DataFrame (weather/tides/flow/SBIWTP) and an observation
DataFrame (recent H2S measurements), and produces a forecast-ready DataFrame
with the model features populated.

This module ensures train/inference feature parity by applying the same
transformations used during model training. That parity is the point: it used to
synthesise h2s_lag_* / h2s_rolling_* as an exponential decay from the last
observation, because the model was trained on real H2S history that does not
exist at forecast time. Training on one distribution and serving another made the
published skill a nowcast number -- see docs/tj_data_basis.md. Those features are
now excluded from the model and no longer produced here.

Flow persistence is retained: streamflow is separately forecast and changes
slowly, so carrying the last known value forward is a defensible approximation
rather than a stand-in for something unknowable.
"""

import pandas as pd


# Full ordered feature list (must match training)
MODEL_FEATURES = [
    'temperature_2m', 'wind_speed_10m', 'wind_direction_sin', 'wind_direction_cos',
    'wind_gusts_10m', 'precipitation', 'relative_humidity_2m', 'surface_pressure',
    'cloud_cover', 'dewpoint_2m',
    'wind_speed_10m_avg_2h', 'wind_speed_10m_avg_3h', 'wind_speed_10m_avg_4h',
    'wind_gusts_10m_max_2h', 'wind_gusts_10m_max_3h', 'wind_gusts_10m_max_4h',
    'wind_direction_sin_lag_6h', 'wind_direction_sin_lag_9h',
    'wind_direction_cos_lag_6h', 'wind_direction_cos_lag_9h',
    'tide_height', 'tidal_state_encoded',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'is_night', 'source_regime',
    'flow_log', 'flow_low', 'flow_high',
    'wind_temp_interaction', 'humidity_temp_interaction', 'stable_atm',
    'flow_lag_6h', 'flow_rolling_24h',
    'sbiwtp_flow_mgd', 'sbiwtp_anomaly', 'sbiwtp_deficit',
    'sbiwtp_flow_x_temp', 'sbiwtp_hourly_mgd', 'sbiwtp_sli',
]


def get_last_known_state(obs_df, site_name):
    """
    Extract the most recent observation state for a station.
    Used to seed persistence/decay features into the forecast.

    Parameters
    ----------
    obs_df : DataFrame
        Observation data with columns: time, site_name, H2S, Flow (m^3/s)--Border,
        and optionally SBIWTP features.
    site_name : str
        Station name to extract state for.

    Returns
    -------
    dict
        Last known values for H2S, flow, and SBIWTP features.
        Returns sensible defaults if no observations exist.
    """
    ss = obs_df[obs_df['site_name'] == site_name].sort_values('time')
    if len(ss) == 0:
        return {
            'h2s': 0, 'h2s_6h': 0, 'h2s_24h': 0,
            'flow': 2.0, 'flow_24h': 2.0,
            'sbiwtp_mgd': 23.0, 'sbiwtp_sli': 5.0,
            'sbiwtp_anomaly': 0, 'sbiwtp_deficit': 0,
            'sbiwtp_hourly_mgd': 23.0,
            'wd_sin': 0.0, 'wd_cos': 0.0,
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

    wd_sin_last = last_valid('wind_direction_sin')
    wd_cos_last = last_valid('wind_direction_cos')

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
        'wd_sin': wd_sin_last if wd_sin_last is not None else 0.0,
        'wd_cos': wd_cos_last if wd_cos_last is not None else 0.0,
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

    # Lagged wind direction: add_wind_features produced shift(k) on the training
    # path, but at forecast time the first k rows of each site series are NaN
    # (wind before forecast start is unobserved here). Fall back to the last
    # known observed wind_direction_sin/cos from obs_df.
    for col, fallback in (
        ('wind_direction_sin_lag_6h', ls['wd_sin']),
        ('wind_direction_sin_lag_9h', ls['wd_sin']),
        ('wind_direction_cos_lag_6h', ls['wd_cos']),
        ('wind_direction_cos_lag_9h', ls['wd_cos']),
    ):
        if col in df.columns:
            df[col] = df[col].fillna(fallback)
        else:
            df[col] = fallback

    # ------------------------------------------------------------------
    # 2. H2S PERSISTENCE -- removed.
    #    This block used to synthesise h2s_lag_1h/3h/6h and h2s_rolling_6h/24h as
    #    exp(-h/12) and exp(-h/36) decays from the last observation. By 24h out
    #    they were 14% of the seed and by 36h, 5%. They were the model's highest
    #    importance features, so it was effectively told "recent H2S was near
    #    zero" and predicted accordingly -- a 0.7-3.2 ppb low bias that
    #    under-warned. The features are out of the model; nothing is synthesised.
    # ------------------------------------------------------------------

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
        From model_forecast asset. Must have: time, site_name,
        temperature_2m, wind_speed_10m, wind_direction_10m, etc.
    obs_df : DataFrame
        From modeldata_h2s_nofill (h2s_measured==True, H2S<=500).
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
