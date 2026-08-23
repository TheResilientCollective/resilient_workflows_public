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

- **The two datasets derive `day_night` from different solar models.**
  `modeldata_h2s_nofill` uses astral via `add_day_night()`;
  `modeldata_forecast_15min` uses OpenMeteo's `is_day` flag
  (`hysplit_forecasting.py:1467`). On current published data they agree on
  **100%** of rows (0 of 17,212 and 0 of 576), but they are not guaranteed to,
  and a strict equality check would hard-fail the scheduled forecast pipeline the
  first time OpenMeteo flips `is_day` one interval early. `attach_astro_frame`
  therefore takes a `day_night_tolerance_minutes` argument: the forecast asset
  allows disagreement within 15 minutes (one grid step) of sunrise/sunset and
  raises beyond it, so a genuine bug far from a boundary is still caught. The
  dataset's own labels always win; the calendar's are dropped.
  Worth deciding separately whether that train/serve difference should be
  eliminated at the source.
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
    | training | 17,212 | 493 | 492 | passed |
    | forecast | 576 | 3 | 2 | passed |

11. **Spot-check against real H2S events.** The largest observation in the record
    is **602 ppb at 2026-03-30 00:00 at NESTOR - BES** — the worst possible case
    for a midnight-anchored frame, since the peak lands exactly on the boundary.
    Under the new frame it sits at `night_fraction` 0.424, mid-night of astro day
    **2026-03-29**, whose night runs 20:00 → 06:00 across both calendar dates as
    a single unit.

    Across the whole training record:

    - **4 of the top 5** nightly peaks occur on nights spanning two calendar dates;
    - **56.5%** of all night observations (4,688 of 8,292) fall after midnight and
      would be attributed to the following calendar day;
    - **669 of 712** night groups would be split in two by a midnight frame.

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

The new counts differ from `h2s_peaks` for **two** independent reasons, separated
here on identical current data (exceedance-hours):

| variant | >5 night | >5 day | >30 night | >30 day |
|---|---|---|---|---|
| 1. clock 6–18 + gap-filled *(original `h2s_peaks`)* | 2,011 | 782 | 311 | 51 |
| 2. clock 6–18, measured only | 1,945 | 682 | 311 | 51 |
| 3. true sun boundary + gap-filled | 2,136 | 657 | 332 | 30 |
| 4. true sun boundary, measured only *(new asset)* | 2,075 | 552 | 332 | 30 |

- **Boundary effect** (1→3): moves 125 exceedance-hours from day to night at
  5 ppb, and 21 at 30 ppb. The clock split systematically under-counts night
  exceedances, because it calls 06:00–06:59 "day" year-round and 18:00–19:59
  "night" year-round; 8.7% of all valid hours are reclassified.
- **Gap-fill effect** (3→4): the original counts gap-filled values as
  exceedances. 166 of its >5 ppb exceedance-hours (about 6%) are synthetic. No
  gap-filled value ever exceeds 30 ppb, which is why that column is unaffected.

Consumers comparing the two datasets need to be aware of both, not just the
boundary change.

### What the nightly summary gives you

750 night × site rows over 492 nights, 34 columns. Peak H2S and its timing, hours
above 5/30 ppb, vector-mean wind, and the night's flow, effluent, tide and
meteorology. Timing is reported as `peak_night_fraction` as well as raw hours,
because night length here ranges from 9.7 to 14.0 hours and raw hours are not
comparable across seasons.

Wind is averaged as a **vector**, not a bearing — a scalar mean of 350° and 10°
gives 180°, exactly backwards. `wind_steadiness` (vector mean ÷ scalar mean)
reports how constant the direction was: 1.0 for a steady night, near 0 when the
wind boxes the compass.

First results, none of which were straightforward to compute under the old frame:

- Peaks cluster **mid-night**: for the 124 nights above 30 ppb, median
  `peak_night_fraction` 0.42, IQR 0.27–0.65.
- The worst nights concentrate in **ISO weeks 6–15** (late winter into spring).
- High-H2S nights are *less* directionally steady than quiet ones (0.67 vs 0.80),
  at a similar mean bearing (~180°).
- All five worst nights sat at the **2.1 m³/s dry-season baseline flow**, not at
  high flow.

## Open items

- Whether the reframed datasets should also be published as year-partitioned
  parquet for efficient DuckDB scans, matching the `H2S_PATH` pattern.
- `astro_day_date` is a `datetime.date` in memory but round-trips through parquet
  as a midnight timestamp. Harmless — it is midnight-normalized either way — but
  worth normalizing if it ever becomes a join key on the consumer side.
- The train/serve `day_night` difference (astral vs OpenMeteo `is_day`) is
  tolerated rather than eliminated. Worth deciding whether to unify at the
  source.

## Resolved

- `START_YEAR = 2015`. H2S observations begin in 2024; the earlier span is
  intentional headroom for other sources.
- Calendar is published as parquet only; no CSV.

## Deferred

- **Model-feature evaluation.** Whether `solar_elevation_deg` and
  `night_fraction` improve model skill enough to enter `MODEL_FEATURES`, and
  whether they can retire `hour_sin`/`hour_cos`/`month_sin`/`month_cos`. This
  means retraining `data/discharge_tj/train_models_auto.py` and comparing skill —
  separate work with its own success criteria, not a pipeline change.
- **Retiring the clock-based assets.** `h2s_peaks` and `h2s_exceedance_periods`
  still publish the 6 AM / 6 PM split. Now that the astronomical versions exist
  side by side, decide whether and when to deprecate them.
