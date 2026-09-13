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

### How much data is there at 30 and 100 ppb? (`01_h2s_levels.py`)

Hourly record: 47,260 measured station-hours, 2023-11-20 to 2026-09-12.

| station | measured hours | > 5 | > 30 | > 50 | > 100 | > 200 | > 500 | max |
|---|---|---|---|---|---|---|---|---|
| SAN YSIDRO | 18,710 | 2,323 | 109 | 43 | 8 | 1 | 1 | 703 |
| NESTOR - BES | 15,792 | 3,154 | 785 | 501 | 259 | 88 | 7 | 915 |
| IB CIVIC CTR | 12,758 | 992 | 121 | 56 | 18 | 2 | 0 | 264 |

Nights (astronomical, at least half the hours measured):

| station | nights | peak > 30 | peak > 100 | peak > 200 |
|---|---|---|---|---|
| SAN YSIDRO | 773 | 53 | 5 | 0 |
| NESTOR - BES | 651 | 229 | 109 | 51 |
| IB CIVIC CTR | 523 | 56 | 10 | 1 |

Nights above 30 ppb by year: NESTOR 24 / 107 / 98 (2024 / 2025 / 2026 to date);
above 100 ppb: 5 / 47 / 57.

What that means:

- **30 ppb is a NESTOR-first problem with usable data at all three stations.**
  NESTOR has 229 positive nights, a 35% base rate. The other two have 53–56
  positive nights, enough for a classifier but not for fine calibration.
- **100 ppb is a NESTOR-only problem.** 109 positive nights there (17% base
  rate) is workable; the other stations have 5 and 10. A 100 ppb product for
  SAN YSIDRO or IB CIVIC CTR cannot be evaluated, let alone trained.
- **Events are short.** Median run above 30 ppb is 1–2 consecutive hours, the
  longest 11. An hourly product that misses the onset misses the event; a
  nightly peak is the more forgiving target.
- **Strongly seasonal and nocturnal.** 65% of hours above 30 ppb fall in
  March–May; almost none in July–September. 95% fall between 19:00 and 07:00.
- **Persistence is strong.** At NESTOR, P(peak > 30 tonight | > 30 last night)
  is 0.65 against a 0.35 base rate; at 100 ppb, 0.50 against 0.17.

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

| H2S bin (ppb) | ≤1 | 1–5 | 5–10 | 10–30 | 30–50 | 50–100 | 100–200 | >200 |
|---|---|---|---|---|---|---|---|---|
| complaints / 100 h | 10 | 17 | 33 | 48 | 62 | 119 | 139 | 163 |

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

## Planning sessions

Each session has a notebook to look at, decisions to make, and an output.

### Session 1 — Target and scope (uses `01_h2s_levels.py`)

Decide what "predict H2S levels at 30 ppb and above" means as a target.

- Hourly value, nightly peak, or nightly hours above threshold? Recommendation
  going in: **nightly peak per station, issued for the coming astronomical
  night**, with hourly detail as a secondary product.
- Threshold set: 5 / 10 / 30 / 100? Recommendation: keep 5 and 10 for
  continuity, add **30 at all stations** and **100 at NESTOR only**.
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

Output: an evaluation script in `data/discharge_tj/` that any candidate model
is scored with, replacing the 80/20 split in `train_models_auto.py`.

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
