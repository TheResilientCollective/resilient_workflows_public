# Feature Engineering Comparison

## Current Implementation vs New feature_engineering.py

### Tidal State Encoding MISMATCH ⚠️

**Current (hysplit_forecasting.py line 138-148):**
```python
{
    'low': 0, 'slack low': 0,
    'rising': 1, 'flood': 1,
    'high': 2, 'slack high': 2,
    'falling': 3, 'ebb': 3,
}
```

**New (feature_engineering.py line 97-98):**
```python
{
    'low': 0, 'ebb': 1, 'flood': 2, 'slack': 3, 'high': 4
}
```

**Action Required:** Update `add_tidal_encoding()` in hysplit_forecasting.py to match the new encoding

---

## Feature Checklist

### Features Currently Generated in `modeldata_h2s` ✓

| Feature | Source Function | Present |
|---------|----------------|---------|
| **Raw Weather** | | |
| temperature_2m, wind_speed_10m, wind_direction_10m | Merged from weather | ✓ |
| wind_gusts_10m, precipitation, relative_humidity_2m | Merged from weather | ✓ |
| surface_pressure, cloud_cover, dewpoint_2m | Merged from weather | ✓ |
| **Raw Other** | | |
| Flow (m^3/s)--Border | Merged from streamflow | ✓ |
| tide_height, tidal_state | Merged from tides | ✓ |
| H2S, h2s_measured | From APCD data | ✓ |
| site_name, time | Core columns | ✓ |
| **Wind Features** | add_wind_features() | |
| wind_direction_sin, wind_direction_cos | ✓ | ✓ |
| wind_direction_categorical | ✓ | ✓ |
| wind_direction_categorical_encoded | ✓ | ✓ |
| wind_speed_10m_avg_2h/3h/4h | ✓ | ✓ |
| wind_gusts_10m_max_2h/3h/4h | ✓ | ✓ |
| wind_temp_interaction | ✓ | ✓ |
| humidity_temp_interaction | ✓ | ✓ |
| **Tidal** | add_tidal_encoding() | |
| tidal_state_encoded | ✓ | ⚠️ WRONG MAPPING |
| **Time Features** | add_inference_features() | |
| hour_sin, hour_cos | ✓ | ✓ |
| month_sin, month_cos | ✓ | ✓ |
| is_night | ✓ | ✓ |
| day_night | add_day_night() | ✓ |
| **Flow Features** | add_inference_features() | |
| flow_log, flow_low, flow_high | ✓ | ✓ |
| **Other** | | |
| source_regime | add_inference_features() | ✓ |
| stable_atm | add_inference_features() | ✓ |
| **H2S Lag Features** | add_h2s_lag_features() | |
| h2s_lag_1h, h2s_lag_3h, h2s_lag_6h | ✓ | ✓ |
| h2s_rolling_6h, h2s_rolling_24h | ✓ | ✓ |
| flow_lag_6h, flow_rolling_24h | ✓ | ✓ |
| **SBIWTP Features** | add_sbiwtp_features() | |
| sbiwtp_flow_mgd, sbiwtp_anomaly, sbiwtp_deficit | ✓ | ✓ |
| sbiwtp_flow_x_temp, sbiwtp_hourly_mgd, sbiwtp_sli | ✓ | ✓ |
| **Risk** | | |
| h2s_risk | Lines 684 | ✓ (log-logistic formula) |

### Features in `model_forecast` ✓

All same as `modeldata_h2s` EXCEPT:
- ✗ No H2S column
- ✗ No h2s_measured column
- ⚠️ h2s_lag_* and flow_lag_* features are filled by forecast_features.engineer_features() from utils/forecast_features.py
- ✗ No h2s_risk (informational only, not needed for forecast)

---

## Issues Found

### 1. ⚠️ CRITICAL: Tidal State Encoding Mismatch

The current implementation uses a different mapping than feature_engineering.py.

**Impact:** If the model was trained with the new encoding but inference uses the old one, predictions will be wrong.

**Fix Required:**
```python
# In hysplit_forecasting.py, line 138-148, change to:
def add_tidal_encoding(tidal_df):
    """Add tidal_state_encoded column based on tidal_state."""
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
```

### 2. ✓ H2S Risk Calculation Different

**Current (hysplit_forecasting.py line 684):**
```python
matched_df['h2s_risk'] = matched_df['H2S'].pow(1.23) / (matched_df['H2S'].pow(1.23) + 5**1.23)
```

**New (feature_engineering.py lines 166-169):**
```python
df['h2s_risk'] = 'GREEN'
df.loc[df['H2S'] > 5, 'h2s_risk'] = 'YELLOW'
df.loc[df['H2S'] > 10, 'h2s_risk'] = 'ORANGE'
df.loc[df['H2S'] > 30, 'h2s_risk'] = 'RED'
```

**Impact:** Current implementation uses a continuous risk score (0-1), new uses categorical. These serve different purposes.

**Recommendation:** Keep both:
- Current `h2s_risk` (continuous) for numerical analysis
- Add new categorical version as `h2s_risk_category` if needed

---

## Recommendations

### Immediate Actions

1. **Fix tidal_state_encoded mapping** to match feature_engineering.py
2. **Verify which encoding was used during model training**
3. **Test model predictions** after fixing encoding to ensure accuracy

### Optional Improvements

1. **Consolidate feature engineering** - Replace current functions with calls to feature_engineering.py to ensure consistency
2. **Add categorical h2s_risk** if needed for reporting
3. **Document** which tidal states actually appear in your data

---

## Feature Count Summary

| Dataset | Current Features | Required by feature_engineering.py |
|---------|-----------------|-----------------------------------|
| modeldata_h2s | ~55+ columns | 53 (ENGINEERED_COLUMNS_OBS) |
| model_forecast | ~55+ columns | Same minus H2S, h2s_measured, h2s_risk |

**Status:** ✓ All required features present, but ⚠️ tidal encoding mismatch must be fixed
