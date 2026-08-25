# Astronomical-day reframing of the TJ modeling dataset

## Goal

Reframe the Tijuana River H2S modeling data from a calendar-day basis to a
**year / week / day-of-year** basis anchored on an **astronomical day** rather
than midnight. H2S events occur predominantly at night; a midnight boundary
splits every event across two calendar days, which makes per-event aggregation,
exceedance counting, and night-relative feature engineering awkward and biased.

Deliverable: a reusable astronomical calendar dataset, plus `_astronomical_day`
variants of the core training and forecast datasets that carry the new frame as
additional columns.

## Status

- **Phase 1 (calendar generator + tests) — complete.** `workflows/tijuana/src/tijuana/utils/astro_calendar.py`, `workflows/tijuana/tests/test_astro_calendar.py`.
- **Phase 2 (calendar asset) — complete.** `workflows/tijuana/src/tijuana/assets/astronomical_day.py`, registered in `assets/__init__.py`. Materialized to the `test` bucket and verified against the US Naval Observatory. 36 tests passing.
- **Phase 3 (reframed training + forecast data) — complete.** `add_day_night()` now delegates to the shared calendar; both `_astronomical_day` assets built and materialized. 44 tests passing.
- **Phase 4 (validation) — complete.** Three Dagster asset checks registered and passing; the reframing verified against real H2S events. 53 tests passing.
- **Pass 2 — complete.** `h2s_peaks_astronomical_day`, `h2s_nightly_summary` and `h2s_exceedance_periods_astronomical_day` built, materialized and checked. 73 tests passing.

Pass 1 and pass 2 are done. Model-feature evaluation is deferred; see the end of this document.

## Data basis for the figures below

All measured figures in this document were recomputed against the **production**
bucket (`resilentpublic`) on **2026-08-25**, after the astronomical assets were
first materialized there. Earlier revisions quoted development-bucket (`test`)
numbers over a shorter, sparser record — production carries 53,476 hourly rows
against 17,212, covering 2023-11-20 to 2026-08-24. The direction of every finding
was unchanged; several got stronger. Where a number is quoted below it is the
production one.

## Decisions made

| Question | Decision | Rationale |
|---|---|---|
| Day boundary | **Sunset → sunset**, labelled by the calendar date of the opening sunset | Every night is contained whole inside exactly one unit; never split at midnight. Unit reads naturally as "the night of Aug 22". |
| Night boundary event | **Geometric sunset/sunrise** as primary, with civil/nautical/astronomical dusk & dawn carried as extra columns | Matches the existing `add_day_night()` semantics, so `day_night` / `is_night` / `source_regime` and the trained models keep their current meaning. Twilight columns let us test alternative boundaries later without regenerating anything. |
| Calendar reuse | **Per-year generated calendar over the full data range**, not a single day-of-year template | A one-year template is wrong by a few minutes year to year, breaks on leap years (DOY alignment shifts by one), and cannot represent DST transitions, whose dates move. Cost of doing it exactly is trivial (see below). |
| Scope, pass 1 | Calendar asset + `_astronomical_day` variants of `modeldata_h2s_nofill` and `modeldata_forecast_15min` | Covers training and inference. Analysis assets follow in pass 2. |
| Compatibility | **Purely additive.** No existing column is renamed, redefined, or removed | Trained models and downstream consumers depend on the current schema. |

### Correction to the original premise

> "Since this is the same for an entire year, we can create one dataset and reuse this for all years."

This does not hold:

- Sunrise/sunset at a fixed lat/lon drift by a few minutes from year to year
  (the tropical year is not 365 days).
- Leap years shift day-of-year alignment by one day after Feb 28.
- DST transition dates move, so `America/Los_Angeles` has one 23-hour and one
  25-hour local day per year, in different day-of-year slots each year.

Generating the real calendar per year costs ~8,760 hourly rows (~35,040 at
15-min) per year. Over 2015–2030 that is 140,256 hourly / 561,024 15-min rows,
built in 5.4 seconds (measured). It is still "one dataset that everything joins
to", just year-aware.

Phase 1 quantified both failure modes: day-of-year 100 is April 10 in 2023 but
April 9 in 2024, so a shared template would attach the wrong date's sun times to
every row after February; and even at matched calendar dates, sunset drifts
~20 seconds between years.

## Data under change

Produced in `workflows/tijuana/src/tijuana/assets/hysplit_forecasting.py`:

- `h2sforecast/modeldata_h2s` — hourly observations, tz-aware `America/Los_Angeles`
- `h2sforecast/modeldata_h2s_nofill` — same, with unmeasured H2S nulled (training input)
- `h2sforecast/modeldata_forecast_15min` — 15-minute forecast grid, same tz
- `h2sforecast/h2s_peaks` — currently uses a **hardcoded 6AM–6PM day/night split** (pass 2)
- `h2sforecast/h2s_exceedance_periods` — exceedance windowing (pass 2)

Existing astronomical logic: `add_day_night()` at `hysplit_forecasting.py:165`
builds a per-date `astral` sun dict and labels each row `day`/`night`. This
becomes a thin wrapper over the new calendar join.

## Design

### 1. Astronomical calendar asset

New module `workflows/tijuana/src/tijuana/utils/astro_calendar.py` (pure
functions, no Dagster imports, unit-testable), plus a new asset module
`workflows/tijuana/src/tijuana/assets/astronomical_day.py`.

Asset: `h2sforecast/astronomical_calendar`

- Location: San Diego, `32.7157, -117.1611`, `America/Los_Angeles` — reuse the
  `LocationInfo` currently inlined in `add_day_night()`, promoted to a module
  constant so there is exactly one definition.
- Range: `START_YEAR = 2015` through `current_year + 1`, so forecast timestamps
  are always covered. **Confirmed.** The APCD H2S record only begins in 2024, so
  the pre-2024 span is unused headroom — kept deliberately cheap so other
  Tijuana sources with longer histories can join the same frame later without
  regenerating it.
- Grain: generated at **15 minutes**; the hourly frame is the exact subset where
  `minute == 0`. One generator, two published grains — guarantees the hourly and
  15-min datasets can never disagree.
- Output: **parquet only** (`formats=["parquet"]`) to
  `tijuana/forecast_data/output/astronomical_day/` with
  `enable_latest_path=True` → `latest/tijuana/forecast_data/astronomical_day/`,
  dataset identifiers `astronomical_calendar_hourly` and
  `astronomical_calendar_15min`. **Confirmed:** no CSV rendering of the
  calendar. It is a machine-side join table, never read by hand, and its eleven
  tz-aware timestamp columns make CSV several times larger than the 30.6 MB
  parquet. The reframed *data* assets keep `["csv", "parquet"]` as today.
- Automation: `AutomationCondition.eager()`, but the asset is effectively static
  within a year — it only needs to rematerialize when the year rolls over.

### 2. Calendar schema

Join key:

| column | type | notes |
|---|---|---|
| `time` | datetime64[ns, America/Los_Angeles] | exact join key against existing assets |

Astronomical-day frame:

| column | type | definition |
|---|---|---|
| `astro_day_start` | tz-aware ts | sunset opening the cycle this row falls in |
| `astro_day_end` | tz-aware ts | next sunset |
| `astro_day_date` | date | calendar date of `astro_day_start` — the unit label |
| `astro_day_complete` | bool | False for the two astro days truncated by the grid edges; per-night aggregation must exclude these |
| `astro_year` | int | year of `astro_day_date` |
| `astro_day_of_year` | int 1–366 | ordinal of `astro_day_date` |
| `astro_week_of_year` | int 1–53 | ISO week of `astro_day_date` |
| `astro_iso_year` | int | ISO year (differs from `astro_year` at year boundaries) |
| `night_of_year` | int | sequential night index within `astro_year`; equals `astro_day_of_year` by construction, kept as an explicit name |
| `hours_into_astro_day` | float | elapsed hours since `astro_day_start`, computed in **UTC** so DST never creates a gap or a repeat |

Solar position and phase:

| column | type | definition |
|---|---|---|
| `sunrise`, `sunset` | tz-aware ts | for the calendar date of the row |
| `solar_noon` | tz-aware ts | |
| `dawn_civil`, `dusk_civil` | tz-aware ts | sun 6° below horizon |
| `dawn_nautical`, `dusk_nautical` | tz-aware ts | 12° |
| `dawn_astronomical`, `dusk_astronomical` | tz-aware ts | 18° |
| `day_night` | `'day'` / `'night'` | **identical semantics to today's `add_day_night()`** |
| `is_night` | int 0/1 | as today |
| `hours_since_sunset` | float | hours since the most recent sunset, UTC-elapsed. Identical to `hours_into_astro_day` under the sunset→sunset boundary; kept under its physical name and would diverge if the boundary ever changed |
| `hours_to_sunrise` | float | signed, UTC-elapsed: positive during the night segment (time until sunrise), negative during the day segment (time since it) |
| `night_length_hours` | float | sunset → next sunrise |
| `day_length_hours` | float | sunrise → sunset |
| `night_fraction` | float 0–1 | position within the night, `NaN` during day |
| `solar_elevation_deg` | float | continuous; see note below |
| `solar_azimuth_deg` | float | |

Cyclic encodings (so models never see an artificial 366→1 or 53→1 discontinuity):

`doy_sin`, `doy_cos`, `week_sin`, `week_cos`, `night_fraction_sin`,
`night_fraction_cos`.

`night_fraction` is a bounded 0→1 phase, not a wrapping angle, so its
encoding is a **half** cycle: `sin(π·nf)` peaks at mid-night, `cos(π·nf)` runs
monotonically +1 at sunset to −1 at sunrise. A full cycle would wrongly map
sunset and sunrise to the same point. ISO week uses a period of 52.1775 so
week 53 → week 1 stays continuous.

Calendar bookkeeping:

| column | type | notes |
|---|---|---|
| `is_dst` | bool | |
| `utc_offset_hours` | float | −8 or −7 |
| `dst_transition` | `'spring_forward'` / `'fall_back'` / `None` | flags the 23h and 25h local days |

#### On `night_fraction` and `solar_elevation_deg`

These are the two additions most likely to beat the existing features:

- Night length at 32°N ranges from roughly 9.8h to 14.2h. `hours_since_sunset`
  therefore means something different in June than in December.
  `night_fraction` (0 at sunset, 1 at sunrise) is scale-free and comparable
  across the year. Keep both — raw hours matter for physical processes with an
  absolute timescale (inversion build-up, transit time), the fraction matters
  for phase-of-night patterns.
- `solar_elevation_deg` is a single continuous variable that subsumes
  `hour_sin`/`hour_cos`, `month_sin`/`month_cos`, and the `day_night` binary. It
  is worth including as a candidate feature, though it does **not** replace the
  existing ones in pass 1.

`astral.sun.elevation()` / `azimuth()` are per-timestamp Python calls. At 15-min
grain that is ~35k calls per year — a few seconds per year, acceptable in a
once-per-year asset. If it proves slow, compute at 15-min and interpolate.

### 3. Joining onto existing datasets

New helper in `astro_calendar.py`:

```python
def attach_astro_frame(df: pd.DataFrame, calendar: pd.DataFrame,
                       time_col: str = "time") -> pd.DataFrame:
    """Left-join the astronomical frame onto df on an exact tz-aware timestamp."""
```

- Exact merge on `time`, not `merge_asof`. Both `modeldata_h2s_nofill` (hourly)
  and `modeldata_forecast_15min` (15-min) already sit on clean tz-aware
  `America/Los_Angeles` grids, so an exact join is correct and makes coverage
  gaps loud instead of silently snapping to a neighbour.
- Row count must be preserved exactly. Assert it.
- Any unmatched row is an error, not a warning — it means the calendar range or
  grain is wrong.
- Existing columns win on name collision: the calendar's `day_night` /
  `is_night` are dropped at join time when the frame already carries them, and
  a check asserts the two agree. That check is the regression test proving the
  new calendar reproduces `add_day_night()`.

### 4. New assets

| Asset key | Input | Output dataset |
|---|---|---|
| `h2sforecast/astronomical_calendar` | — | `astronomical_calendar_hourly`, `astronomical_calendar_15min` |
| `h2sforecast/modeldata_h2s_nofill_astronomical_day` | `modeldata_h2s_nofill` + calendar | `modeldata_h2s_nofill_astronomical_day` |
| `h2sforecast/modeldata_forecast_15min_astronomical_day` | `modeldata_forecast_15min` + calendar | `modeldata_forecast_15min_astronomical_day` |

All written under `tijuana/forecast_data/output/astronomical_day/` with
`latestdatasetpath` → `latest/tijuana/forecast_data/astronomical_day/`,
`formats=["csv", "parquet"]`, and schema.org metadata via
`store_assets.objectMetadata()` following the existing pattern in the file.

The reframed assets are thin: load upstream dataframe → `attach_astro_frame()` →
validate → store. No feature logic is duplicated.

## Implementation phases

### Phase 1 — calendar generator ✅ complete

1. Create `workflows/tijuana/src/tijuana/utils/astro_calendar.py`:
   - `SAN_DIEGO = LocationInfo(...)` module constant.
   - `build_astro_calendar(start_year, end_year, freq="15min") -> pd.DataFrame`.
   - `attach_astro_frame(df, calendar, time_col="time")`.
   - `validate_astro_calendar(df) -> list[str]` returning failed checks.
   - `to_hourly(calendar)` — the hourly frame as the exact `minute == 0` subset.
2. Handle DST explicitly: build the timestamp grid in UTC, convert to
   `America/Los_Angeles`, and derive all elapsed-time columns from the UTC
   values. Never subtract two tz-aware local timestamps directly.
3. Unit tests (`workflows/tijuana/tests/`), no S3, no Dagster:
   - Leap year 2024 has 366 distinct `astro_day_date` values.
   - Spring-forward day has 23 hourly rows, fall-back has 25.
   - `hours_into_astro_day` is strictly increasing within each `astro_day_date`,
     with no gap or repeat across DST transitions.
   - `night_fraction` is monotone 0→1 across a night and `NaN` during day.
   - `day_night` matches the current `add_day_night()` output on a sample year,
     compared against a frozen verbatim copy of the original function.
   - `attach_astro_frame` preserves row count, rejects off-grid and
     out-of-range timestamps, and raises when an existing `day_night` disagrees.

**Measured on the real build:**

| | |
|---|---|
| Full range 2015–2030 at 15-min | 5.4 s, 561,024 rows, 30.6 MB parquet |
| Hourly subset | 140,256 rows, 10.2 MB parquet |
| Single leap year at 15-min | 0.5 s, 35,136 rows |
| Tests | 34 passing, ~1.1 s |

Two findings that changed the design:

- **Grid-edge astro days are truncated.** A Jan1–Dec31 grid touches 367 astro
  days: rows before the first sunset belong to the previous year's last day, and
  the final sunset opens a day closing outside the range. Added
  `astro_day_complete` so per-night aggregation drops them rather than silently
  under-counting. Over the full 2015–2030 build only two days are affected.
- **Parquet is 30.6 MB at 15-min, not the "few MB" first estimated** — the frame
  carries eleven tz-aware timestamp columns. Still trivial to store, but worth
  deciding whether the 15-min grain needs a CSV rendering at all, since CSV will
  be several times larger. **Resolved:** the calendar ships parquet-only.

### Phase 2 — calendar asset ✅ complete

4. Create `workflows/tijuana/src/tijuana/assets/astronomical_day.py` with
   `astronomical_calendar`, registered in the tijuana `Definitions`.
5. Materialize and inspect:
   ```bash
   uv run dagster asset materialize --select h2sforecast/astronomical_calendar -m tijuana
   ```
6. Sanity-check a solstice, an equinox, and both DST transition days by hand
   against a published sunrise/sunset table for San Diego.

**Materialization result** (run 2026-08-22, `test` bucket):

| | |
|---|---|
| Range generated | 2015–2027 (`START_YEAR` .. current year + 1) |
| Rows | 455,808 at 15-min, 113,952 hourly |
| Runtime | ~36 s end to end, ~5 s of it calendar construction |
| Objects written | `astronomical_calendar_15min` and `..._hourly`, `.parquet` + `.metadata.json`, each to both the output path and the `latest/` path |

**External verification.** Every internal invariant would still pass with a
mis-signed longitude or the wrong timezone, so the sun times were checked
against the US Naval Observatory
(`aa.usno.navy.mil/api/rstt/oneday`, coords `32.7157,-117.1611`) on five dates:
both solstices, the March equinox, and both 2026 DST transition days. Worst
deviation across all 25 compared values was **0.7 minutes**, against a reference
that tabulates to whole minutes — i.e. within rounding. USNO independently
reported 2026-03-08 as daylight time and 2026-11-01 as standard time, matching
our local wall clock, which is what pins down the timezone handling.

Those reference values are frozen into `test_sun_times_match_usno_reference` so
the check stays offline and runs on every suite.

### Phase 3 — reframe training and forecast data ✅ complete

7. Add `modeldata_h2s_nofill_astronomical_day` and
   `modeldata_forecast_15min_astronomical_day`.
8. Refactor `add_day_night()` (`hysplit_forecasting.py:165`) to delegate to the
   shared calendar rather than rebuilding an `astral` dict per asset run —
   output must be byte-identical; the Phase 1 test is the guard. Done: it now
   calls `astro_calendar.label_day_night()`, pinned by
   `test_add_day_night_helper_matches_original`. The re-materialized
   `modeldata_h2s` produced an identical row count, and the now-unused `astral`
   imports were dropped from `hysplit_forecasting.py`.
9. Materialize the chain and diff row counts against the source assets.

**Materialization result** (run 2026-08-22, `test` bucket):

| dataset | rows | columns | astro days | nights |
|---|---|---|---|---|
| `modeldata_h2s_nofill_astronomical_day` | 17,212 → 17,212 | 59 → 93 | 493 | 492 |
| `modeldata_forecast_15min_astronomical_day` | 576 → 576 | 57 → 92 | 3 | 2 |

Row counts preserved exactly, zero nulls in the frame keys, `night_fraction`
spanning the full 0→1 range, and all 37 frame columns present in both. The
column delta differs by one only because the upstream training parquet carries a
`__index_level_0__` pandas artifact; no real column is lost.

The training data actually begins **2023-11-19**, slightly earlier than the
"2024 and beyond" assumption — it changes nothing, but the frame covers it.

**Two findings:**

- **The two datasets derived `day_night` from different solar models.**
  `modeldata_h2s_nofill` used astral via `add_day_night()`;
  `modeldata_forecast_15min` used OpenMeteo's `is_day` flag. They agreed on 100%
  of observed data, but were free to disagree at the interval straddling
  sunrise/sunset, which would have put a train/serve skew into `is_night`,
  `source_regime` and `stable_atm` — features the model consumes directly.
  **Now unified** (see below); at the time this was handled with a tolerance on
  the join.
- **Materializing the forecast chain from the CLI needs an explicit selection.**
  `--select "+h2sforecast/modeldata_forecast_15min_astronomical_day"` pulls in
  `sd_apcd/yearly_aggregated_all`, which is partitioned and fails with
  "Cannot access partition_key for a non-partitioned run". List the chain
  explicitly instead, and include `streamflow/boundary_cms` — omitting it fails
  `streamflow_forecast` and cascades. Both are pre-existing constraints,
  unrelated to this work.

### Phase 4 — validation ✅ complete

10. Invariants are enforced in two places:

    - **At join time**, `attach_astro_frame()` hard-raises on a changed row count,
      any unmatched timestamp, and `day_night` disagreement beyond the allowed
      tolerance. These cannot be materialized past.
    - **As Dagster asset checks** (severity `ERROR`), registered in
      `definitions.py` alongside the existing freshness checks:

      | check | asset |
      |---|---|
      | `astronomical_calendar_check` | `h2sforecast/astronomical_calendar` |
      | `modeldata_h2s_nofill_astronomical_day_check` | the reframed training data |
      | `modeldata_forecast_15min_astronomical_day_check` | the reframed forecast data |

    The reframed checks run `astro_calendar.validate_reframed()`: no nulls in the
    frame keys, every row inside its own astro day bounds, `hours_into_astro_day`
    in `[0, 25)`, `is_night` consistent with `day_night`, `night_fraction` present
    exactly on night rows and within `[0, 1]`, and — the one that matters most —
    **no night group spanning longer than its own night**, which is what a split
    night would look like. Coverage figures ride along as check metadata.

    All three pass on real data:

    | check | rows | astro days | nights | result |
    |---|---|---|---|---|
    | calendar | 455,808 | 4,749 (4,747 complete) | — | passed |
    | training | 53,476 | 1,009 | 1,008 | passed |
    | complaints | 7,689 | 1,002 | — | passed |
    | nightly summary | 2,243 | 1,008 | — | passed |

11. **Spot-check against real H2S events.** The largest observation in the record
    is **915 ppb at 2026-06-02 01:00 at NESTOR - BES** — an hour past midnight, so
    a midnight-anchored frame splits the event from the evening that produced it.
    Under the new frame it sits at `night_fraction` 0.52, mid-night of astro day
    **2026-06-01**.

    Across the whole record:

    - **57.3%** of night observations (13,179 of 23,018) fall after midnight and
      would be attributed to the following calendar day;
    - **1,935 of 1,964** night groups — 98.5% — would be split in two by a
      midnight frame.

    That is the quantified case for the reframing.

## Pass 2 ✅ complete

**Decision: additive, not a rewrite.** The clock-based `h2s_peaks` and
`h2s_exceedance_periods` are left exactly as they are; the astronomical versions
are published alongside them. Nothing currently consuming those datasets changes.

New assets, all writing to `tijuana/forecast_data/output/astronomical_day/`:

| asset | dataset | grain |
|---|---|---|
| `h2sforecast/h2s_peaks_astronomical_day` | `h2s_peaks_astronomical_day` | site × astro day × day/night segment |
| `h2sforecast/h2s_nightly_summary` | `h2s_nightly_summary` | site × astronomical night |
| `h2sforecast/h2s_exceedance_periods_astronomical_day` | `h2s_exceedance_model_data_{5,30}ppb_astronomical_day` | hourly rows of exceedance segments |

Logic lives in `tijuana/utils/night_analysis.py` (pure functions, unit-tested);
`h2s_nightly_summary_check` is registered as a fourth asset check.

### Before / after on the exceedance counts

The astronomical counts differ from `h2s_peaks` for exactly **one** reason: the
day/night boundary. Exceedance-hours on identical current data:

| variant | >5 night | >5 day | >30 night | >30 day |
|---|---|---|---|---|
| clock 6–18 *(`h2s_peaks`)* | 4,937 | 1,500 | 908 | 107 |
| true sun boundary *(new asset)* | 5,223 | 1,214 | 947 | 68 |

The clock split systematically under-counts night exceedances, because it calls
06:00–06:59 "day" year-round and 18:00–19:59 "night" year-round. 8.9% of all
valid hours are reclassified (4,081 of 45,969, 8.9%); the net effect is 286
exceedance-hours moving from day to night at 5 ppb and 39 at 30 ppb.

> **Correction.** An earlier revision of this document, and PR #58 as originally
> opened, claimed a second cause: that `h2s_peaks` counts gap-filled values as
> exceedances, inflating its >5 ppb figures by ~6%. **That was wrong.**
> `h2s_peaks` takes `modeldata_h2s_nofill` as its input — the `AssetIn` points
> there even though the parameter is named `modeldata_h2s` — so unmeasured
> values are already null and `count_filled` is 0 in the published data. The
> mistaken figures came from computing the comparison against the filled
> `modeldata_h2s` dataset. There was no gap-fill defect. The only difference
> between the clock and astronomical counts is the boundary.

Both assets now share `utils/h2s_exceedance.aggregate_exceedances()`, which
excludes gap-filled values from the counts and from `max_h2s` / `mean_h2s`. This
is a **guard, not a fix**: it is behaviour-neutral on the current nofill inputs
(verified — the refactored `h2s_peaks` reproduces the published 1,391 rows and
2,627 / 362 exceedance totals exactly). It exists so the counts stay correct if
the asset is ever repointed at the filled dataset, and so the two peaks assets
cannot diverge on the question. `h2s_peaks` gains one appended column,
`measured_observations`; no existing column changes meaning or value.

### What the nightly summary gives you

2,243 night × site rows over 1,008 nights, 34 columns. Peak H2S and its timing, hours
above 5/30 ppb, vector-mean wind, and the night's flow, effluent, tide and
meteorology. Timing is reported as `peak_night_fraction` as well as raw hours,
because night length here ranges from 9.7 to 14.0 hours and raw hours are not
comparable across seasons.

Wind is averaged as a **vector**, not a bearing — a scalar mean of 350° and 10°
gives 180°, exactly backwards. `wind_steadiness` (vector mean ÷ scalar mean)
reports how constant the direction was: 1.0 for a steady night, near 0 when the
wind boxes the compass.

First results, none of which were straightforward to compute under the old frame:

- Peaks cluster **mid-night**: for the 342 nights above 30 ppb, median
  `peak_night_fraction` 0.48, IQR 0.33–0.71.
- The worst nights concentrate in **ISO weeks 12–21** (spring).
- High-H2S nights are *less* directionally steady than quiet ones (0.67 vs 0.79),
  at a similar mean bearing (~190°).
- Four of the five worst nights sat at the **2.1 m³/s dry-season baseline flow**;
  the exception (703 ppb at SAN YSIDRO, 2025-01-26) came during a 25.7 m³/s storm
  flow, so high H2S is not confined to low-flow conditions.

## Open items

- Whether the reframed datasets should also be published as year-partitioned
  parquet for efficient DuckDB scans, matching the `H2S_PATH` pattern.
- `astro_day_date` is a `datetime.date` in memory but round-trips through parquet
  as a midnight timestamp, and cannot be written to geojson at all (geopandas
  converts datetime64 columns to ISO strings but leaves object-dtype dates alone).
  `sd_complaints_astronomical_day` casts it to a string for the stored copy only,
  keeping real dates on the returned frame so downstream joins still work. Worth
  normalizing the dtype at source if more geo-bearing datasets are reframed.


## Resolved

- `START_YEAR = 2015`. H2S observations begin in 2024; the earlier span is
  intentional headroom for other sources.
- Calendar is published as parquet only; no CSV.

## Deferred

- **Retiring the clock-based assets.** `h2s_peaks` and `h2s_exceedance_periods`
  are now **marked deprecated** (see below) but still published and still
  updating. Actually withdrawing them needs an answer to "does anything outside
  this repo read them?" — inside the repo the only consumer of `h2s_peaks` is
  `h2s_exceedance_periods_filter`, so the real consumers, if any, are the portal,
  dashboards or notebooks not visible from here.

## Unified `day_night` source

`modeldata_forecast_15min` previously derived `day_night` from OpenMeteo's
`is_day` flag, making it the only one of the three producers not using astral —
`data_for_models` (`hysplit_forecasting.py:627`) and `model_forecast`
(`:1255`) already called `add_day_night()`. It now calls the same helper, so
every `day_night` in the pipeline resolves to one function over one calendar.

Why it mattered even though the two agreed: `is_night`, `source_regime` and
`stable_atm` are all derived from `day_night` and are consumed directly by the
model. A boundary-interval disagreement would have meant the model seeing a
subtly different definition at serve time than it was trained on — the kind of
skew that is invisible in aggregate metrics and hard to trace later.

Measured impact: **zero rows change**. On the current forecast window the old
`is_day` rule and astral agree on all 576 rows, so this removes a latent
divergence rather than correcting live values.

With one source, `modeldata_forecast_15min_astronomical_day` no longer needs the
15-minute tolerance and joins **strictly**. That is the stronger guarantee: any
disagreement now fails the asset, so reintroducing a second solar source cannot
pass silently. `attach_astro_frame`'s `day_night_tolerance_minutes` parameter
remains available, and tested, for any future input genuinely on a different
solar model.

## Deprecation of the clock-based assets

`h2s_peaks` and `h2s_exceedance_periods` keep publishing unchanged, but are now
marked deprecated so no new consumer picks the weaker dataset by accident. The
marking is in three places:

- the **Dagster asset description**, prefixed `DEPRECATED - superseded by ...`,
  naming the replacement and quantifying the difference;
- a Dagster **tag** `deprecated=true` plus metadata keys `deprecated` and
  `superseded_by`, so they can be filtered in the UI;
- the **schema.org `.metadata.json` sidecar published next to the data in S3** —
  the one an external consumer actually sees, since it travels with the file
  rather than living in the orchestrator.

Nothing about the data changes. This is option 2 of four considered: leave both,
deprecate in place, switch the values behind the existing names, or stop
publishing. Options 3 and 4 both need the consumer question answered first.

Migration note for consumers: the astronomical tables are not drop-in. The key
column is `astro_day_date` rather than `date`, with the frame's descriptors
alongside.

## Complaints on the astronomical day

`sd_complaints` is the first **event** dataset on the frame, and it needed two
prerequisites before framing was even possible.

### The source field was date-only

`sd_complaints` requested `date_received`, which carries **no time of day** — every
value converts to exactly 00:00 local. Framing on it would have put every
complaint at local midnight: always "night", always in the *previous* astro day.
A uniform off-by-one, not an analysis.

The source layer already exposes `date_and_time_received`, with real times across
the full 24 hours. The pipeline now requests it and falls back to `date_received`
only where it is missing, flagging which is which in a new `time_of_day_known`
column so date-only rows can be excluded from sub-daily work. After backfilling
the year partitions, **7,705 of 7,705 rows have a real time of day**.

### A timezone bug

`sd_complaints.py` did `to_datetime(unit='ms')` then `tz_localize('US/Pacific')`.
ArcGIS epoch-ms is UTC, so this *relabelled* rather than converted, publishing
every complaint at a spurious **07:00/08:00 local**. The `date` column survived it
(both land on the same calendar date), but any hour-of-day analysis on the
published `datetime` was reading an artifact. Now `tz_localize('UTC')` then
`tz_convert`.

### Framing events rather than grids

Events are irregular, so the exact-join path in `attach_astro_frame()` does not
apply. `frame_for_timestamps()` computes the frame directly from solar geometry
per timestamp — exact, rather than snapping to the nearest grid row — and
`attach_astro_frame_to_events()` attaches it preserving row count and order.
Both share `_frame_for_index()` with `build_astro_calendar()`, so the grid path
and the event path cannot disagree about the same instant; a test asserts it.

### New assets

| asset | contents |
|---|---|
| `complaints/sd_complaints_astronomical_day` | 7,689 complaints on the frame, 1,002 astronomical days |
| `h2sforecast/h2s_nightly_summary_with_complaints` | 2,243 night-site rows; 2,045 have at least one complaint in the same astronomical day |

Complaint counts are night-wide, not per-site: a complaint's location is not
matched to a monitoring station, so the count is attached to every site row for
that night. This is the one modelling choice in the linkage and it is recorded in
the dataset description.

### What it shows

**Odour is a night phenomenon; the other complaint types are not.**

| nature_of_complaint | n | % at night |
|---|---|---|
| Odor | 6,363 | **44.4%** |
| Smoke | 142 | 34.5% |
| Asbestos | 213 | 9.4% |
| Dust | 475 | 6.3% |

Night complaints arrive early in the night (median `night_fraction` 0.30) —
notably earlier than H2S peaks, which sit at 0.42.

**Complaints track H2S nights**, at NESTOR - BES:

| that night's H2S peak | nights | complaints/night | at night |
|---|---|---|---|
| ≤ 5 ppb | 181 | 2.5 | 0.6 |
| 5–30 | 244 | 5.1 | 2.2 |
| 30–100 | 120 | 7.4 | 3.4 |
| > 100 | 111 | **14.8** | 7.6 |

Spearman **0.59** between nightly H2S peak and complaint count (0.54 between
hours above 30 ppb and night-time complaints). None of this was
reachable while the timestamps were date-only.

### Note

`sd_complaints_freshness_check` currently fails: the newest complaint is 6 days
old against a 4-day threshold. That is source lag, unrelated to this work — it
fails identically on the old and new timestamp fields.

## Model-feature evaluation — answered: do not change `FEATURES`

Run via `data/discharge_tj/evaluate_astro_features.py`.

**Method.** `train_models_auto.py` uses a single chronological 80/20 split, which
is fine for shipping a model but too fragile for comparing features: one number,
no uncertainty, and its test block is one particular season in data whose
exceedances concentrate in ISO weeks 6–15. Instead: walk-forward CV (expanding
window, 4 contiguous test blocks, no fold trains on data following its test
block), 3 seeds, 3 stations, every arm on identical folds so differences are
paired. Seeds are averaged **before** testing — they are repeated fits on the
same site-fold, not independent samples, and treating all 36 as independent
would overstate significance.

`night_fraction` is null by construction during the day, and `prepare_data()`
ends with `dropna(subset=FEATURES)`, so adding it raw would have silently deleted
every daytime row and made the arms incomparable. It enters as `night_phase`:
the fraction at night, −1 during the day.

**Result** (n = 12 site-folds per comparison):

| arm | vs | metric | delta | 95% CI | p | verdict |
|---|---|---|---|---|---|---|
| +astro | baseline | R² | −0.0138 | [−0.0365, +0.0088] | 0.26 | no effect |
| +astro | baseline | RMSE | +0.028 | [−0.006, +0.062] | 0.13 | no effect |
| +astro | baseline | AUC | −0.0003 | [−0.0011, +0.0006] | 0.53 | no effect |
| replace cyclicals | baseline | R² | −0.0043 | [−0.0094, +0.0009] | 0.13 | no effect |
| replace cyclicals | baseline | RMSE | +0.037 | [−0.024, +0.099] | 0.26 | no effect |
| +astro, no H2S lags | no-lag baseline | R² | +0.043 | [−0.036, +0.122] | 0.31 | no effect |
| +astro, no H2S lags | no-lag baseline | AUC | +0.0011 | [−0.0013, +0.0036] | 0.38 | no effect |

**Every comparison is null.** Adding `night_fraction` and `solar_elevation_deg`
changes nothing, in the full model or in the harder no-lag regime.

> **A finding that did not replicate.** On the smaller development record,
> replacing the hour/month cyclicals looked *significantly worse* (R² −0.0086,
> p=0.015; RMSE +0.118, p=0.025). On the full production record the same
> comparison is null (p=0.13 and 0.26). That earlier result was a marginal
> p-value on roughly a third of the data and did not survive. The practical
> conclusion is unchanged — there is still no reason to swap the cyclicals out,
> since nothing is gained — but the claim that removing them *hurts* is
> withdrawn.

So: **`FEATURES` should stay as it is**, on the grounds that the astronomical
features add nothing measurable, not that the alternative is harmful.

**Why, and it is not that the model ignores them.** With both present,
`solar_elevation_deg` ranks **#5–#9** of 39 features and `night_phase` #6–#13 —
*above* `hour_sin`, `month_sin`/`month_cos` and `is_night` (#33–34). The model
prefers them; it just gains nothing, because they re-express information the
baseline already carries. That the cyclicals still cannot be removed without cost
suggests they hold a little the solar features do not — plausibly wall-clock
rather than solar effects, since some complaint and activity patterns follow the
clock, not the sun. That reading is a hypothesis, not a measurement.

**A separate finding about the model.** Dropping the H2S lag/rolling features
collapses skill from R² 0.515 to **−1.85** and AUC 0.956 to **0.822**. This model
is overwhelmingly an autocorrelation nowcast, leaning on recent H2S rather than
on meteorology. Investigated below — it turned out to be the more consequential
issue.

**Limitations.** XGBoost is not installed in this environment and is not a
declared dependency, so this covers RandomForest only; `train_models_auto.py`
auto-selects between the two, and the conclusion may not transfer to XGBoost.
Twelve site-folds is modest power — enough to bound the effect at the level
above, not to resolve differences of ~0.002 R².

## The H2S lag collapse — the published skill is a nowcast number

Run via `data/discharge_tj/evaluate_forecast_horizon.py`.

### What is happening

`train_models_auto.py` trains on **true** H2S lags, and reports R² ≈ 0.36 and
AUC ≈ 0.93. But those features do not exist at forecast time.
`forecast_features.engineer_station_features()` synthesises them as an
exponential decay from the last observation:

```
h2s_lag_1h      = last_H2S      * exp(-h/12)
h2s_rolling_24h = last_24h_mean * exp(-h/36)
```

At 24 hours out that is 14% of the seed value; at 36 hours, 5%. These are the
model's two highest-importance features (≈0.20–0.23 each). So the model is
trained on one distribution and served another, and the headline metrics
describe a nowcast the product does not actually make.

### Skill by lead time

Simulating forecasts exactly as they are issued — last-known state, decayed lags,
walk-forward folds, 2 seeds:

| lead | R² served | R² true-lag | R² no-lag | AUC served | AUC no-lag | bias served | bias no-lag |
|---|---|---|---|---|---|---|---|
| 1–6h | 0.066 | 0.533 | 0.060 | 0.803 | 0.791 | −0.7 | +0.9 |
| 7–12h | 0.106 | 0.648 | **0.179** | 0.731 | **0.779** | −2.8 | **+0.1** |
| 13–24h | 0.094 | 0.499 | 0.083 | 0.767 | **0.787** | −2.3 | **+0.9** |
| 25–36h | 0.055 | 0.600 | **0.120** | 0.757 | **0.783** | −3.2 | **+0.7** |
| 37–48h | 0.020 | 0.466 | **0.062** | 0.725 | **0.784** | −3.0 | **+1.0** |

Three things follow.

1. **The published R² overstates forecast skill by roughly 5–25×**, at *every*
   lead including the shortest. Served R² is 0.02–0.11 against a reported 0.47–0.65.
2. **Predictions are biased low by 0.7–3.2 ppb**, and the mechanism is exactly the
   decay: the lag features tell the model recent H2S was near zero, so it predicts
   low. For an exceedance-warning product that is the dangerous direction — it
   under-warns.
3. **Dropping the lag features is better on the measures that matter.** AUC is
   higher at four of five leads, and the bias essentially disappears (−3.2…−0.7
   becomes +0.1…+1.0, i.e. a slight over-prediction, the safe direction for a
   warning product). On R² it is mixed — better at 7–12h, 25–36h and 37–48h,
   about equal at 1–6h and 13–24h. A no-lag model also has no horizon dependence,
   because it never depended on a decaying seed.

The classifier is more robust than the regressor throughout: even served, AUC
holds at 0.68–0.76, well above chance. If the product is "will it exceed
tonight", that is the component to lean on.

### Recommendation

Train the forecast model on the features that exist at forecast time — drop
`h2s_lag_*` and `h2s_rolling_*` from the deployed forecast model — and publish the
operational metrics rather than the nowcast ones. On the production record the
case rests on **bias and AUC rather than R²**: removing the decayed lags takes the
systematic 0.7–3.2 ppb under-prediction to a slight over-prediction and raises AUC
at four of five leads, while R² is mixed. For an exceedance-warning product that
trade is worth taking, because the current failure direction is under-warning.
This is a change to a deployed model and is not made here.

### Limitations

Every non-lag feature in this experiment is the **observed** weather, tide and
flow at the target hour. A real forecast uses forecast weather, with its own
error, so these curves are an upper bound on operational skill rather than an
estimate of it — the true served numbers are lower than the table shows.
RandomForest only, as before.
