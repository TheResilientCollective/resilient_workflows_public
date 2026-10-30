# Tijuana River H2S prediction redo — planning

Branch: `feature/tj_prediction_redo`. Status: **planning**. Nothing in the
deployed pipeline changes on this branch until the sessions below have produced
decisions.

## Why

San Diego APCD reviewed the H2S forecast. They like the idea, and asked us to
focus on **predicting H2S levels**, in particular doing better at **30 ppb and
above**. This document frames a short series of planning sessions, records what
the data says going in, and lists the decisions each session has to make.

The data review lives in three marimo notebooks under
`notebooks/tj_h2s_review/` (see the README there for how to run them). Every
number quoted below was computed from the production bucket on 2026-09-13 and
can be reproduced from the notebooks.

## What the deployed product does today

Worth stating plainly, because it explains APCD's comment.

- The trained models (`data/discharge_tj/train_models_auto.py`) are a
  regressor on the hourly H2S value plus classifiers at **5 ppb and 10 ppb**.
  There is no 30 ppb classifier. Training also drops every row above 500 ppb,
  which removes the largest events in the record before the model sees them.
- The served products (`forecast_data/products_latest`) carry `p5`, `p10` and
  `p30`; `p30` is populated for one station only.
- The published skill report over the 2026-06-16 to 2026-09-12 validation window
  shows **recall of 30 ppb hours of zero at every lead time**, for every product
  variant. Rank correlation with observed H2S is 0.27 for the forecast and 0.36
  to 0.42 for the nowcast.
- The earlier horizon analysis (`docs/tj_data_basis.md`) found the regressor's
  headline skill came from H2S lag features that do not exist at forecast time;
  served, they are replaced by an exponential decay, which biases the forecast
  low by 1–3 ppb. Under-prediction is the wrong failure direction for a warning.

So "focus on the levels" is a fair reading: the product is tuned to 5 ppb and
effectively never calls a 30 ppb night.

## Data review — what we know going in

### How much data is there at 30 and 70 ppb? (`01_h2s_levels.py`)

The review's tiers are 5, 10, 30, 50 and 70 ppb (75 and 200 ppb were dropped
on 2026-10-26; the figures in this section are on the record to
2026-10-22). Hourly record: 50,068 measured station-hours
from 2023-11-20.

| station | measured hours | > 5 | > 30 | > 50 | > 70 | > 500 | max |
|---|---|---|---|---|---|---|---|
| SAN YSIDRO | 19,645 | 2,394 | 109 | 43 | 21 | 1 | 703 |
| NESTOR - BES | 16,757 | 3,195 | 786 | 501 | 361 | 7 | 915 |
| IB CIVIC CTR | 13,666 | 1,005 | 121 | 56 | 31 | 0 | 264 |

Nights (astronomical, at least half the hours measured):

| station | nights | peak > 30 | peak > 50 | peak > 70 |
|---|---|---|---|---|
| SAN YSIDRO | 832 | 53 | 19 | 9 |
| NESTOR - BES | 710 | 230 | 174 | 136 |
| IB CIVIC CTR | 580 | 56 | 23 | 15 |

Nights above 30 ppb by year: NESTOR 24 / 107 / 99 (2024 / 2025 / 2026 to date);
above 70 ppb: 8 / 61 / 67.

What that means:

- **30 ppb is a NESTOR-first problem with usable data at all three stations.**
  NESTOR has 229 positive nights, a 35% base rate. The other two have 53–56
  positive nights, enough for a classifier but not for fine calibration.
- **70 ppb is a NESTOR-only problem.** 136 positive nights there (19% base
  rate) is workable; the other stations have 9 and 15. A 70 ppb product for
  SAN YSIDRO or IB CIVIC CTR cannot be evaluated, let alone trained.
- **Events are short.** Median run above 30 ppb is 1–2 consecutive hours, the
  longest 11. An hourly product that misses the onset misses the event; a
  nightly peak is the more forgiving target.
- **Strongly seasonal and nocturnal.** 65% of hours above 30 ppb fall in
  March–May; almost none in July–September. 95% fall between 19:00 and 07:00.
- **Persistence is strong.** At NESTOR, P(peak > 30 tonight | > 30 last night)
  is 0.65 against a 0.32 base rate; at 70 ppb, 0.54 against 0.19.

### At what H2S levels do complaints occur? (`02_complaints_vs_h2s.py`)

7,761 SDAPCD complaints, 6,561 mentioning odour, 5,248 of those inside a
Tijuana River Valley bounding box. Two caveats govern everything:

- **Complaint location is APCD's "source adjacent location", not the
  complainant's.** 4,837 odour complaints sit on one intersection, Caspian Way
  and N McCoy Trail, with 22 distinct coordinates. Complaints cannot be matched
  to a station; every join is by time only.
- **Complaints are stamped when received, and arrive in the morning.** The
  largest block is 06:00–09:00, hours after the night peak (which sits
  mid-night). A morning complaint is evidence about the previous night.

Matching each TRV odour complaint to NESTOR's reading in the hour received:
median 5.4 ppb, upper quartile 29 ppb, 90th percentile 113 ppb. Taking the
highest reading in the previous 3 hours: median 10 ppb, upper quartile 60 ppb.

Normalised by exposure (complaints per 100 measured NESTOR hours in each bin):

| H2S bin (ppb) | ≤1 | 1–5 | 5–10 | 10–30 | 30–50 | 50–70 | >70 |
|---|---|---|---|---|---|---|---|
| complaints / 100 h | 5 | 12 | 29 | 46 | 60 | 96 | 146 |

(Recomputed 2026-10-26 on the record to 2026-10-22; the low bins fell as the
summer's quiet hours accrued.)

The rate rises monotonically with no step at 30 ppb. Nightly, Spearman between
the night's peak and the complaint count is 0.57–0.59 at NESTOR and IB, 0.39
at SAN YSIDRO. Treating five or more complaints as a "complaint night": 72% of
NESTOR nights above 30 ppb are complaint nights (35% of nights below), and
53% of complaint nights had a NESTOR peak above 30 ppb.

Reading: 30 ppb is a defensible line, not a natural one; complaints already run
at three times the background rate in the 5–10 ppb bin. The complaint record
is a candidate *target* in its own right ("will tonight be a complaint night")
and the natural verification for a nightly product.

### Temperature, border flow, SBIWTP (`03_drivers.py`)

Spearman with the night's peak:

| driver | SAN YSIDRO | NESTOR - BES | IB CIVIC CTR |
|---|---|---|---|
| temperature (night mean) | +0.31 | −0.23 | −0.31 |
| temperature anomaly within month | +0.29 | +0.12 | +0.06 |
| border flow (night mean) | −0.15 | +0.13 | 0.00 |
| SBIWTP effluent (MGD) | −0.15 | −0.31 | −0.46 |
| wind speed | −0.30 | −0.19 | −0.20 |
| previous night's peak | — | strongest of all, see persistence above | — |

- **Temperature.** Across the year the sign is negative at two stations because
  the bad season is spring, which is cool. Once the seasonal cycle is removed
  the within-month effect is small (+0.06 to +0.12) except at SAN YSIDRO. It is
  not the headline driver.
- **Border flow.** Weak everywhere. The IBWC gauge record also has quality
  problems: runs of identical hourly values, and dry-season monthly medians
  that differ by an order of magnitude between years (July 2023: 0.04 m³/s;
  July 2024: 2.06; July 2025: 0.14). It needs a quality flag before it is used.
- **SBIWTP effluent** is the strongest physical driver in the set, and it is
  *negative*: less treated means more H2S. This confirms the earlier
  incorporation plan (`data/discharge_tj/SBIWTP_Incorporation_Plan.md`).
- **SBIWTP vs border flow.** Daily Spearman −0.25 overall, but the sign flips
  with season: +0.5 to +0.6 in January–March, −0.5 to −0.8 in June–September.
  They are not substitutes for each other; both, with season, are needed.
- The 2024 canal gauge is strongly anti-correlated with the border gauge
  (−0.8), consistent with it measuring diverted flow.

### Choosing the upper threshold (`04_upper_threshold.py`)

Is there a better break than 100 ppb? Three readings on the NESTOR record,
the only station with a usable sample above 50 ppb (figures computed
2026-10-24; the record now runs to 2026-10-21).

**Sample size.** Positive NESTOR nights fall smoothly: 230 at 30 ppb, 174 at
50, 136 at 70, 109 at 100, 74 at 150. SAN YSIDRO and IB CIVIC CTR
have 19–23 positive nights at 50 ppb and 5–10 at 100.

**Predictability.** A walk-forward nightly RandomForest (features known the
evening before plus the night's observed weather, so an upper bound), scored
on 425 NESTOR test nights:

| threshold | positives | AP (90% CI) | recall at precision 0.5 (90% CI) | persistence |
|---|---|---|---|---|
| 30 | 122 | 0.81 (0.75–0.86) | 0.91 (0.85–0.96) | 0.68 |
| 50 | 99 | 0.75 (0.68–0.81) | 0.90 (0.81–0.95) | 0.66 |
| **70** | 79 | 0.72 (0.65–0.80) | 0.92 (0.70–0.97) | 0.58 |
| 100 | 67 | 0.65 (0.56–0.73) | 0.69 (0.49–0.83) | 0.54 |
| 150 | 47 | 0.56 (0.44–0.69) | 0.64 (0.40–0.79) | 0.47 |

AUC is flat (0.85–0.90) at every threshold; what degrades is precision,
because positives thin out. Skill holds through 70 ppb and steps down at
100. Adjacent thresholds are within each other's intervals; the trend is
what is reliable, not any single pairwise difference.

**Natural breaks.** The density of NESTOR nightly peaks (log scale) has modes
near 19, 50 and 150 ppb and valleys at 34 and **101 ppb**. So 100 ppb is not
arbitrary: it separates two populations of high nights, and it approximates
the WHO 24-hour guideline (150 µg/m³, about 107 ppb). The complaint response
breaks elsewhere: odour complaints per 100 measured hours go 51 (30–40 ppb),
72 (40–50), 106 (50–75), 142 (75–100), then plateau at 120–160; nights with
ten or more complaints rise from about 1 in 10 below 50 ppb to 3 in 10 at
50–100 and 6 in 10 above 150.

**Reading.** 50 ppb is where the public response steps up and keeps nearly
the skill of 30 ppb. 100 ppb is a real break in the concentration
distribution but a poor place to train a classifier. 150 ppb is where nights
become reliably complaint-heavy, with 74 examples.

**Decision: 70 ppb.** It is a published health reference (the ATSDR acute
inhalation MRL, 0.07 ppm) rather than a data-derived number, and it sits on
the skill plateau: 136 positive NESTOR nights (8 / 61 / 67 by year), AP 0.72
and recall 0.92 at 50% precision, against 0.65 and 0.69 at 100 ppb. It lies
just below the 101 ppb density valley, so it takes in the whole upper
population of nights, and above the 50 ppb complaint step. At SAN YSIDRO and
IB CIVIC CTR it has 9 and 15 positive nights; a 70 ppb probability at those
stations will be poorly calibrated until the record grows and should be
labelled as such in the product.

One caveat to carry into the documentation and the portal text: the MRL is a
screening level for exposures lasting 1 to 14 days, not a 1-hour health
effect threshold. Using it as an hourly classifier threshold is a policy
choice. The honest phrasing is that an hour above 70 ppb is above a level
ATSDR considers without appreciable risk for a fortnight of continuous
exposure.

## Planning sessions

Each session has a notebook to look at, decisions to make, and an output.

### Session 1 — Target and scope (uses `01_h2s_levels.py`)

Decide what "predict H2S levels at 30 ppb and above" means as a target.

- Hourly value, nightly peak, or nightly hours above threshold? Recommendation
  going in: **nightly peak per station, issued for the coming astronomical
  night**, with hourly detail as a secondary product.
- Threshold set. **Decided:** the 5 and 10 ppb classifiers stay as they are
  (2026-10-24); the County alerts only on 30 ppb (2026-10-24); the upper
  classifier is set at **70 ppb, the ATSDR acute inhalation Minimal Risk
  Level** (2026-10-25, see *Choosing the upper threshold* and *References*).
  `CLASSIFIER_THRESHOLDS = [5, 10, 30, 70]` in
  `data/discharge_tj/train_models_auto.py` carries the decision; a classifier
  with too few exceedances in its train or test block is skipped and the
  reason recorded in the training report. The script has not been re-run for
  deployment. A check run on the 2026-10-22 record trained 30 ppb models at
  SAN YSIDRO and NESTOR and a 70 ppb model at NESTOR only: the chronological
  80/20 split leaves the summer-autumn 2026 test block with no 30 ppb hours
  at IB CIVIC CTR and no 70 ppb hours anywhere but NESTOR. That is the
  Session 2 problem in miniature, and its AUC figures are nowcast numbers
  with true lags, not operational skill. The review notebooks use the same
  tiers, 5 / 10 / 30 / 50 / 70 ppb (75 and 200 ppb were dropped on
  2026-10-26); 100 ppb survives only as the density break noted under
  *Choosing the upper threshold*.
- Station scope for 100 ppb (see the counts above).
- Whether the 500 ppb training filter stays. Recommendation: remove it.
- Which lead times matter to APCD: same evening, 24 h, 48 h?

Output: a one-page target definition that the evaluation protocol is written
against.

### Session 2 — Evaluation protocol (uses `01_h2s_levels.py`, `02_complaints_vs_h2s.py`)

Agree how success is measured before any model is trained.

- Walk-forward, season-aware folds; positives in every test block.
- Baselines every model must beat: climatology by month, and **persistence**
  (last night's peak).
- Metrics at 30 and 100 ppb: recall at a fixed false-alarm rate, Brier score,
  reliability, and event-level (nightly) hit / miss / false alarm counts.
- Whether complaint nights are a second verification target.
- Served-time features only: nothing the model sees in training may be
  unavailable at issue time (the lag-collapse lesson).

Output: an evaluation any candidate model is scored with, replacing the 80/20
split in `train_models_auto.py`. **Delivered** as the
`h2sforecast/h2s_backtest_30ppb` asset; see *Backtest of the 30 ppb
classifier* below.

### Session 3 — Features and drivers (uses `03_drivers.py`)

- SBIWTP: keep the existing features; decide on latency handling for forecast
  use (1-day persistence is already implemented).
- Border flow: add a gauge quality flag; decide whether to use it at all.
- Temperature: keep as a feature, drop the "warm is worse" narrative.
- Persistence and recent history as features *at the lead time they exist*:
  last night's peak is available for tonight's forecast and is the single
  strongest predictor.
- Wind, stability and tide as today.
- Open: Synoptic / on-site meteorology (`latest/tijuana/weather/synoptic`) vs
  OpenMeteo model temperature.

Output: a feature list with, for each feature, its availability at issue time.

### Session 4 — Model approach for rare high levels

- Classifier per threshold vs one quantile / distributional regressor from
  which any threshold probability is read off. The latter gives 30 and 100 ppb
  from one model and keeps them consistent.
- Class imbalance handling and probability calibration.
- Whether a two-stage model (will there be an event → how big) suits the short,
  spiky event structure.
- Station pooling: one model with station as a feature, or one per station.

Output: two or three candidate approaches to run through the Session 2
protocol.

### Session 5 — Operational integration

- Which Dagster assets change (`model_forecast`, products, skill report),
  and where the model inference actually runs (it is not in this repository;
  `products_latest.model_version` names it).
- What the portal shows: probabilities at 30 and 100 ppb, nightly framing.
- What we report back to APCD, and what we ask them for (see below).

## Backtest of the 30 ppb classifier (`h2sforecast/h2s_backtest_30ppb`)

The Session 2 evaluation protocol now exists as a Dagster asset. It re-trains
the 30 ppb classifier walk-forward over the production record and issues it
every afternoon at 15:00 local for the following 18 hours, exactly as the
forecast is served, then verifies each station-night as an event. It
publishes per-hour predictions (csv, parquet), a metrics JSON and a
self-contained HTML evaluation page under
`latest/tijuana/forecast_data/backtest/` (`h2s_backtest_30ppb.html`). Logic in
`workflows/tijuana/src/tijuana/utils/h2s_backtest.py`, unit-tested; the assets
run weekly and take a `source_bucket` so a development run can read
production data while writing to the development bucket. A second asset,
`h2s_backtest_70ppb`, runs the same backtest for the 70 ppb tier (below).

Six lines are scored. *Deployed*: trained on true H2S history, issued with
that history decayed from the last observation (the production path, pinned
to `forecast_features.engineer_station_features` by a parity test).
*Served-time*: no H2S history at all. *Served-time + history*: the history
that genuinely exists at issue time (last reading, 24 h and 72 h maxima,
hours since the last exceedance, lead hour), held fixed over the horizon and
trained in that form. *Night-target*: one row per station per astronomical
night from `h2s_nightly_summary` (peak above 30 ppb) with the night's
meteorology, flow, SBIWTP and tide, the previous nights' peaks and the same
issue-time history, fit on the nights before each fold's test block and
verified on the same events as the hourly variants. *Persistence*: the 24 h
before issue reached 30 ppb. *Climatology*: the training base rate. Weather, tide and flow inputs are
observed values, so every number is an upper bound on operational skill.

First run, 2026-10-30, on the record to 2026-10-22: 1,062 station-nights
scored from 2025-05-04, 196 of them above 30 ppb.

| variant | nightly AUC | nightly avg precision | Brier skill | warned of 196 events at cutoff 0.5 | false alarms |
|---|---|---|---|---|---|
| deployed | 0.79 | 0.56 | −0.09 | **3** | 0 |
| served-time | 0.70 | 0.37 | +0.07 | 23 | 15 |
| served-time + history | 0.82 | 0.57 | +0.22 | 27 | 9 |
| **night-target** | **0.86** | **0.63** | **+0.32** | **97** | 50 |
| persistence | 0.75 | 0.43 | +0.02 | 118 (POD 0.60) | FAR 0.40 |
| climatology | 0.63 | 0.29 | −0.16 | – | – |

What it says:

- **The deployed model ranks nights reasonably but never warns.** Its
  probabilities sit below 0.1 on 1,026 of 1,062 nights; at any cutoff from
  0.2 to 0.7 it flags 2 to 7 of the 196 event nights. This is the lag decay
  measured end to end: the model is trained to expect real recent H2S and is
  served a value that has decayed toward zero. APCD's experience that the
  forecast does not call 30 ppb nights is reproduced exactly.
- **Issue-time history is half the fix.** The hourly variant that uses the
  history available at 15:00 ranks well and is calibrated, but at a 0.2
  cutoff it still trades 118 hits for 90 false alarms.
- **Predicting the night is the other half.** Trained on the nightly target,
  the model reaches nightly AUC 0.86 and Brier skill +0.32, the best on every
  measure. At cutoff 0.5 it warns of 97 of 196 event nights with 50 false
  alarms; at 0.4, 128 with 90; at 0.3, 147 with 135. At NESTOR it warns of
  72% of event nights with a 28% false-alarm rate, against 69% and 30% for
  persistence; at the two smaller stations it does not yet beat persistence.
  Its probabilities are also the ones that read as probabilities: 0.5 means
  about half.
- **Persistence is hard to beat on hits.** "Last 24 h reached 30 ppb" warns
  of 60% of event nights with a 40% false-alarm rate; at NESTOR, 69% and 30%.
  Any product must show it clears this line, and the cutoff table on the page
  is where the County's false-alarm tolerance gets turned into a setting.
- **Hourly class weighting depresses nightly probabilities.** The hourly
  classifiers are trained with balanced class weights and the night is scored
  by its maximum hour, which is why even the calibrated hourly variant needs a
  cutoff near 0.2–0.3. The night-target model does not have this problem,
  which is the practical argument for Session 4 to build the product on it.

### The same backtest at 70 ppb (`h2sforecast/h2s_backtest_70ppb`)

The asset is built by a factory, one per classifier threshold, so the 70 ppb
tier is scored by the same code and published as `h2s_backtest_70ppb.html`
alongside the 30 ppb page. First run, 2026-10-30: 607 station-nights scored
from 2025-10-22, 83 of them above 70 ppb, all at NESTOR. The scoring starts
later than at 30 ppb because a fold is skipped until its training block holds
20 exceedance hours, and SAN YSIDRO and IB CIVIC CTR never reach that, so
they are scored only as non-event nights.

| variant | nightly AUC | Brier skill | warned of 83 events at cutoff 0.5 | false alarms |
|---|---|---|---|---|
| deployed | 0.90 | −0.05 | 2 | 0 |
| served-time + history | 0.88 | +0.14 | 4 | 8 |
| **night-target** | **0.92** | **+0.39** | **43** | 18 |
| persistence | 0.77 | +0.11 | 50 (POD 0.60) | 31 (FAR 0.38) |

At cutoff 0.3 the night-target model warns of 70 of 83 event nights with 58
false alarms; at 0.4, 51 with 39. The pattern is the same as at 30 ppb, only
sharper: the deployed hourly model ranks well and never warns, the
night-target model is the only one with calibrated probabilities, and
persistence is a strong line on hits. The 70 ppb product is a NESTOR
product for now, as the data review predicted.

## References

- Agency for Toxic Substances and Disease Registry (2016). *Toxicological
  Profile for Hydrogen Sulfide and Carbonyl Sulfide.* U.S. Department of
  Health and Human Services. Acute-duration inhalation Minimal Risk Level
  0.07 ppm (1–14 days, respiratory end point, uncertainty factor 27, final,
  cover date 11/2016); intermediate-duration 0.02 ppm.
  https://www.atsdr.cdc.gov/toxprofiles/tp114.pdf — MRL list:
  https://wwwn.cdc.gov/TSP/MRLS/mrlsListing.aspx (verified 2026-10-25).
- California Air Resources Board. California Ambient Air Quality Standards:
  hydrogen sulfide 0.03 ppm (42 µg/m³), 1-hour average.
  https://ww2.arb.ca.gov/resources/california-ambient-air-quality-standards
- World Health Organization (2000). *Air Quality Guidelines for Europe*, 2nd
  edition. Hydrogen sulfide guideline value 150 µg/m³, 24-hour average
  (about 0.1 ppm).

## Questions for APCD

- Which levels they act on operationally, and at what lead time.
- Whether the complaint record's location field could carry the complainant's
  area (zip is present for many records) so complaints can be tied to stations.
- Whether sub-hourly H2S is available; the hourly record smooths peaks.
- Whether a fourth monitor is planned; the spatial coverage is three points.

## Open items from the review

- The hourly record's `border_cms` and the raw IBWC export disagree in
  places; establish which is authoritative and whether the 2.1 m³/s
  dry-season plateau in 2024 is real.
- `sd_complaints` has 22 distinct coordinates for one intersection; find out
  whether those encode anything.
- Model inference code is outside this repository; locate it before Session 5.
