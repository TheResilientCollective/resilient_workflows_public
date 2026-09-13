# SBIWTP plant balance

Three daily measures of the South Bay International Wastewater Treatment Plant,
published as `ibwc/plant_balance` and carried into the H2S model data as
candidate features. Code: `workflows/tijuana/src/tijuana/assets/plant_balance.py`.

## Inputs

| Series | Asset | Source | Native unit |
|---|---|---|---|
| Plant influent | `ibwc/influent_flow_current_year` (new; `_today` and `_yearly` alongside) | IBWC `Flow.Plant-Influent-Flow-MGD@SBIWTP` | MGD, daily |
| Plant effluent | `ibwc/effluent_flow_current_year` | IBWC `Flow.Plant-Effluent-Flow-MGD@SBIWTP` | MGD, daily |
| River at the border | `streamflow/boundary_cms` | IBWC `Discharge.Best Available@11013300` | m³/s, hourly |

The influent assets are built by the same factory as the effluent assets
(`ibwc_flows.build_plant_flow_assets`) and land at `tijuana/influent_flow/…`
with the same layout. The effluent keys, paths, jobs and schedules are unchanged.

All three exports stamp a fixed UTC-8 offset. They are parsed as such, not as
Pacific local time, so the plant's day boundary holds year-round. The border
gauge is converted at 1 m³/s = 22.824 MGD.

## The three measures

**1. Net flow — influent minus effluent.** `net_mgd` and `net_fraction`
(net over influent). Positive means more entered the plant than it discharged
that day. Days present in one series but not the other are kept with NaN so a
gap in one source does not hide the other.

A reported influent below 1 MGD while the effluent ran above 5 MGD is a meter
or reporting outage, not the plant. Such days are flagged `influent_suspect`,
the raw figure is kept in `influent_reported_mgd`, and `influent_mgd` is
treated as missing so the day is not scored as a large negative net flow. The
2026 record has 56 such days, 4 May to 1 July, all reading 0.00 against an
effluent near 34 MGD.

**2. Capacity exceedance.** `influent_over_capacity`, `effluent_over_capacity`,
`over_capacity` (either), the excess above the rating in MGD for each, and
`capacity_utilisation` (influent over capacity). Flags are nullable booleans: a
day with no reading is *unknown*, never "within capacity".

The rating is `PLANT_CAPACITY_MGD = 35.0`. It is expected to change. When it
does, append `(effective_date, new_mgd)` to `CAPACITY_CHANGES` rather than
editing the constant, so days already published keep the rating of their day.
Only the present rating is asserted; days before the first entry are judged
against it, so historical flags describe today's plant. A single run can also
override the rating through the asset's `capacity_mgd` config.

**3. Border flow against effluent.** The daily mean of the hourly gauge
(`border_flow_mgd`), its difference from and ratio to the day's effluent, the
daily-mean flag `border_over_effluent`, and an hourly count:
`border_over_effluent_hours` out of `border_hours_reported`, with the fraction.
The hourly count matters because the river is far spikier than the plant: a day
whose mean sits below the effluent can still spend several hours above it.

## Outputs

- `tijuana/plant_balance/output/plant_balance/plant_balance.{csv,parquet}`
  and the same under `latest/tijuana/plant_balance/daily/`.
- `latest/tijuana/plant_balance/daily/plant_balance_current.json` — the most
  recent day with both plant flows, for the portal. No lag is applied to the
  published series; it describes each day as it was.

## Candidate model features

`add_plant_balance_features` adds seven columns to `h2sforecast/modeldata_h2s`.
Daily plant quantities are lagged one day and mapped by date, exactly as the
existing SBIWTP features are; the border comparison uses the same hour's gauge
reading against the lagged daily effluent, which is the plant's most recent
known outflow at forecast time.

| Column | Meaning |
|---|---|
| `sbiwtp_influent_mgd` | daily influent, lagged 1 day |
| `sbiwtp_net_mgd` | influent − effluent, lagged 1 day |
| `sbiwtp_capacity_mgd` | rating in force on the lagged day |
| `sbiwtp_over_capacity` | 1.0 when influent or effluent exceeded the rating, lagged 1 day |
| `border_flow_mgd` | the hour's border gauge reading in MGD |
| `border_minus_effluent_mgd` | `border_flow_mgd` − lagged daily effluent |
| `border_over_effluent` | 1.0 when the gauge exceeds the lagged effluent |

`MODEL_FEATURES` is unchanged. These are candidates for the same evaluation the
astronomical features went through (see `tj_data_basis.md`, "Model-feature
evaluation"); nothing here changes what the served models see. If any input is
missing the columns are present and NaN, so downstream assets are unaffected.

## First look at the 2026 record

Provisional IBWC data, 1 January to 19 August 2026, pulled 13 September
(231 days with both plant flows before the suspect-influent rule; the border
gauge runs to the day of the pull):

| | |
|---|---|
| Median net flow (influent − effluent), 175 clean days | 3.02 MGD (10th–90th percentile 1.8–5.2) |
| Days influent above 35 MGD | 70 |
| Days effluent above 35 MGD | 15 |
| Days either above 35 MGD | 79 |
| Days border daily mean above effluent | 103 of 231 |
| Median fraction of hours border above effluent | 0.17 |

Capacity and border counts above include the 56 suspect-influent days, since
the effluent and border readings on those days are sound; the net-flow median
excludes them. The influent outage is the first thing to raise with IBWC
(WA-Data@ibwc.gov) before the net-flow feature is evaluated: two months of the
2026 training record carry no usable influent.
