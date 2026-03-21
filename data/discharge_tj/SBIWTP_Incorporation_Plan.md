# SBIWTP Effluent Data: Analysis and Model Incorporation Plan

## Data Summary

The South Bay International Wastewater Treatment Plant (SBIWTP) effluent flow data covers January 2003 through March 2026 at daily resolution (8,475 records). The plant processes an average of 23.5 MGD (1.03 m³/s) with a range of 8.8–54.3 MGD. Autocorrelation is high (0.77 at 1-day lag), meaning flow conditions persist.

## Key Finding: SBIWTP Flow is an *Inverse* Predictor

The most important discovery is that **higher SBIWTP throughput is associated with *lower* H₂S** across all three stations. This is counterintuitive at first — you might expect more wastewater processing to mean more off-gassing. But it makes mechanistic sense once you realize what SBIWTP flow measures:

| Metric | Correlation with NESTOR P(>5 ppb) | Interpretation |
|--------|-----------------------------------|----------------|
| SBIWTP flow (lag 1d) | **r = -0.47** | More treatment → less untreated sewage in river |
| SBIWTP anomaly (lag 1d) | r = -0.33 | Below-normal throughput → capacity problems |
| SBIWTP deficit (lag 1d) | r = +0.28 | Under-treatment accumulating |
| Border flow gauge | r = -0.04 | Weak predictor (different measurement point) |

**SBIWTP flow is a treatment capacity indicator, not a discharge indicator.** When the plant processes more, less sewage bypasses into the river channel. When throughput drops (maintenance, capacity exceedance, infrastructure failure), more untreated wastewater pools in the drainage network, generating H₂S.

The correlation at NESTOR-BES is remarkably strong: r = -0.47 for P(>5 ppb) at 1-day lag. For context, this is comparable to the strongest meteorological predictors already in the model.

## Temperature Interaction: The Worst-Case Combination

The most dangerous conditions occur when **low SBIWTP flow coincides with warm temperatures**:

| Condition | n days | Mean max H₂S | P(>30 ppb) |
|-----------|--------|--------------|------------|
| Low flow + Warm | 29 | **104.4 ppb** | **55%** |
| Low flow + Cool | 96 | 75.5 ppb | 55% |
| High flow + Warm | 220 | 33.1 ppb | 21% |
| High flow + Cool | 154 | 29.4 ppb | 24% |

Low SBIWTP flow triples the mean H₂S regardless of temperature. Adding warmth pushes the expected concentration to extreme levels.

---

## Incorporation Plan

### Tier 1: Direct Features (Immediate, High Impact)

These features can be added to the model data directly using the daily SBIWTP value assigned to all 24 hours of that day.

1. **`sbiwtp_flow_mgd`** — Raw daily effluent flow in MGD. The single strongest new predictor available. Apply with **1-day lag** (yesterday's SBIWTP flow predicts today's H₂S).

2. **`sbiwtp_anomaly`** — (flow - 30-day rolling mean) / rolling mean. Captures deviation from expected throughput. Negative values signal capacity problems. Use lag 1 day.

3. **`sbiwtp_deficit`** — max(0, expected_flow - actual_flow). Quantifies how many MGD of treatment capacity are "missing." Use lag 1 day.

4. **`sbiwtp_flow_x_temp`** — Interaction: sbiwtp_flow × temperature_2m. Captures the compound effect of low treatment + warm decomposition conditions. Low values (low flow × low temp still gives low product; low flow × high temp gives moderate product; but since flow is the inverse predictor, the interaction needs careful sign handling — use -1/flow × temp or just include both and let the tree model learn the interaction).

### Tier 2: Hourly Disaggregation

Daily SBIWTP data needs to be distributed across 24 hours. Wastewater treatment plants have a well-characterized diurnal pattern:

**Typical Diurnal Profile (fraction of daily mean):**
```
Hour (local):  00   01   02   03   04   05   06   07   08   09   10   11
Factor:       0.70 0.65 0.60 0.58 0.60 0.70 0.85 1.05 1.15 1.20 1.25 1.20

Hour (local):  12   13   14   15   16   17   18   19   20   21   22   23
Factor:       1.15 1.10 1.10 1.10 1.15 1.20 1.15 1.10 1.00 0.90 0.85 0.75
```

Apply as: `sbiwtp_hourly = sbiwtp_daily × factor[hour] / mean(factors)`

This creates an hourly SBIWTP feature that peaks during the morning/evening high-flow periods and troughs overnight — matching when the plant is processing the most (or least) wastewater.

**Important caveat:** This diurnal pattern is an assumption based on typical wastewater treatment profiles. If hourly SBIWTP data becomes available, use it directly.

### Tier 3: Accumulation-Decay Index

This is the most physically motivated feature. The idea: untreated sewage accumulates in the river channel when SBIWTP throughput is low, and decays through tidal flushing and biological processing.

**Sewage Load Index:**
```
SLI(t) = SLI(t-1) × decay × tidal_factor + deficit(t) × temp_factor
```

Where:
- `decay` = exp(-ln(2) / half_life), with half_life calibrated (start with 2-3 days)
- `tidal_factor` = lower during spring tides (more flushing), higher during neap
  - spring tides: factor = 0.85
  - neap tides: factor = 1.15
- `deficit(t)` = max(0, expected_sbiwtp - actual_sbiwtp) in MGD
- `temp_factor` = temperature-dependent H₂S generation rate
  - Arrhenius-style: factor = exp(0.07 × (temp - 15)) (doubles every 10°C above 15°C)

This index rises when: SBIWTP throughput drops, temperatures are warm, and tidal flushing is weak. It decays when: treatment capacity returns, temperatures cool, or spring tides flush the channel.

**Calibration approach:** Fit the half_life and factor weights by maximizing correlation between SLI and daily max H₂S at NESTOR-BES. Start with the 2-day half-life that showed the strongest raw correlation.

### Tier 4: Predictive (Forecast Integration)

For the 48-hour forecast, SBIWTP data availability is the constraint:
- **Hours 0-24:** Use today's SBIWTP flow (available from USIBWC with ~1 day latency)
- **Hours 24-48:** Use persistence (yesterday's flow) with high confidence (autocorrelation 0.77)
- **Extended outlook:** Decay toward 30-day mean with e-folding time of 7 days

If USIBWC can provide near-real-time SBIWTP data, this becomes the single most impactful forecast input after meteorology.

---

## Implementation Steps

### Step 1: Add to Training Data
```python
# Load SBIWTP
sbiwtp = pd.read_csv('sbiwtp_effluent.csv', ...)
sbiwtp['flow_mgd'] = ...
sbiwtp['flow_30d_mean'] = sbiwtp['flow_mgd'].rolling(30, min_periods=7).mean()
sbiwtp['sbiwtp_anomaly'] = (sbiwtp['flow_mgd'] - sbiwtp['flow_30d_mean']) / sbiwtp['flow_30d_mean']
sbiwtp['sbiwtp_deficit'] = (sbiwtp['flow_30d_mean'] - sbiwtp['flow_mgd']).clip(lower=0)

# Merge with hourly model data (1-day lag)
sbiwtp['date_lag1'] = sbiwtp['date'] + pd.Timedelta(days=1)
model_data = model_data.merge(sbiwtp[['date_lag1','flow_mgd','sbiwtp_anomaly','sbiwtp_deficit']],
                               left_on='date', right_on='date_lag1', how='left')
```

### Step 2: Hourly Disaggregation
```python
diurnal_factors = {0:0.70, 1:0.65, 2:0.60, 3:0.58, 4:0.60, 5:0.70,
                   6:0.85, 7:1.05, 8:1.15, 9:1.20, 10:1.25, 11:1.20,
                   12:1.15, 13:1.10, 14:1.10, 15:1.10, 16:1.15, 17:1.20,
                   18:1.15, 19:1.10, 20:1.00, 21:0.90, 22:0.85, 23:0.75}
mean_factor = np.mean(list(diurnal_factors.values()))

model_data['sbiwtp_hourly'] = model_data.apply(
    lambda r: r['sbiwtp_daily_mgd'] * diurnal_factors.get(r['local_hour'], 1.0) / mean_factor,
    axis=1
)
```

### Step 3: Sewage Load Index
```python
def compute_sewage_load_index(daily_deficit, tide_height, temperature, 
                               half_life=2.5, temp_ref=15):
    decay = np.exp(-np.log(2) / half_life)
    sli = np.zeros(len(daily_deficit))
    for i in range(1, len(sli)):
        # Tidal flushing: higher tides = more flushing
        tidal_factor = 1.0 - 0.15 * (tide_height[i] - 0.8)  # 0.8m = mean
        tidal_factor = np.clip(tidal_factor, 0.7, 1.3)
        # Temperature: Arrhenius H2S generation
        temp_factor = np.exp(0.07 * (temperature[i] - temp_ref))
        # Accumulate
        sli[i] = sli[i-1] * decay * tidal_factor + daily_deficit[i] * temp_factor
    return sli
```

### Step 4: Retrain Models
Add the new features to the MODEL_FEATURES list and retrain with `train_models_auto.py`. Expected improvement: 5-15% reduction in MAE based on the correlation strength.

---

## Expected Impact

| Feature | Expected Improvement | Confidence |
|---------|---------------------|------------|
| sbiwtp_flow_mgd (lag 1d) | 8-12% reduction in NESTOR MAE | High (r=-0.47) |
| sbiwtp_anomaly | 3-5% additional (captures short-term drops) | Medium |
| sbiwtp_deficit | Marginal standalone; strengthens ensemble | Medium |
| sbiwtp × temp interaction | 5-8% in warm-season episodes | High for extreme events |
| Sewage Load Index | 5-10% if well-calibrated | Medium (needs tuning) |

The largest single gain will come from the raw SBIWTP flow with 1-day lag. It's a strong, readily available, independent predictor that the model currently lacks entirely.

## Data Access for Operational Use

SBIWTP effluent data appears to be available from USIBWC's data portal with approximately 1-day latency. For the daily operational system:
- Pull yesterday's SBIWTP flow daily
- Use persistence for 48-hour forecast (reliable given r=0.77 autocorrelation)
- Flag when SBIWTP drops below the 25th percentile (~20 MGD) as an early warning of capacity issues that will likely produce elevated H₂S within 24-48 hours
