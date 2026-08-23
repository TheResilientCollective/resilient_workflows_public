"""Unit tests for the astronomical-day calendar.

No S3, no Dagster, no network. Loaded by file path because importing the
``tijuana`` package pulls in the full asset graph, which requires API-key
environment variables.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from astral import LocationInfo
from astral.sun import sun

_MODULE_PATH = Path(__file__).resolve().parents[1] / "src" / "tijuana" / "utils" / "astro_calendar.py"
_spec = importlib.util.spec_from_file_location("astro_calendar", _MODULE_PATH)
ac = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ac)

TZ = ac.TZ

# 2024 is a leap year and carries both DST transitions, so it exercises every
# calendar edge case in one build.
LEAP_YEAR = 2024
SPRING_FORWARD = "2024-03-10"
FALL_BACK = "2024-11-03"


@pytest.fixture(scope="module")
def cal() -> pd.DataFrame:
    return ac.build_astro_calendar(LEAP_YEAR, LEAP_YEAR, "15min")


@pytest.fixture(scope="module")
def hourly(cal: pd.DataFrame) -> pd.DataFrame:
    return ac.to_hourly(cal)


# --------------------------------------------------------------------------
# Grid shape and reusability
# --------------------------------------------------------------------------


def test_calendar_passes_its_own_invariants(cal):
    assert ac.validate_astro_calendar(cal) == []


def test_hourly_is_exact_subset_of_15min(cal, hourly):
    assert len(hourly) == 366 * 24  # leap year, DST hours cancel out over the year
    merged = hourly.merge(cal, on="time", how="inner", suffixes=("_h", "_q"))
    assert len(merged) == len(hourly)
    for col in ("astro_day_date", "night_fraction", "hours_into_astro_day"):
        pd.testing.assert_series_equal(
            merged[f"{col}_h"], merged[f"{col}_q"], check_names=False
        )


def test_leap_year_astro_day_coverage(cal):
    """A calendar-year grid touches 367 astro days, two of them truncated.

    Sunset-anchored days do not align with a Jan1-Dec31 grid: the rows before the
    first sunset belong to the previous year's last astro day, and the rows after
    the final sunset open an astro day that closes outside the range. Both are
    flagged incomplete so per-night aggregation can drop them. Whole days run
    2024-01-01 .. 2024-12-30, i.e. 365 in a 366-day year.
    """
    assert cal["astro_day_date"].nunique() == 367
    assert cal.loc[cal["astro_day_complete"], "astro_day_date"].nunique() == 365
    partial = sorted(set(cal.loc[~cal["astro_day_complete"], "astro_day_date"]))
    assert partial == [dt.date(2023, 12, 31), dt.date(2024, 12, 31)]


def test_non_leap_year_astro_day_coverage():
    non_leap = ac.build_astro_calendar(2023, 2023, "1h", with_solar_position=False)
    assert non_leap["astro_day_date"].nunique() == 366
    assert non_leap.loc[non_leap["astro_day_complete"], "astro_day_date"].nunique() == 364


def test_one_day_of_year_template_cannot_be_reused_across_years():
    """The premise that a single day-of-year table serves every year does not hold.

    Two independent failure modes, the leap year being by far the larger:
      1. day-of-year 100 is April 10 in 2023 but April 9 in 2024 -- a whole-day
         misalignment, so a shared template would attach the wrong date's sun
         times to every row after February;
      2. even at matched calendar dates the sun times drift tens of seconds
         between years.
    """
    y2023 = ac.build_astro_calendar(2023, 2023, "1h", with_solar_position=False)
    y2024 = ac.build_astro_calendar(2024, 2024, "1h", with_solar_position=False)
    y2025 = ac.build_astro_calendar(2025, 2025, "1h", with_solar_position=False)

    def date_at_doy(cal, doy):
        return cal.loc[cal["astro_day_of_year"] == doy, "astro_day_date"].iloc[0]

    # 1. leap-year misalignment
    assert date_at_doy(y2023, 100) == dt.date(2023, 4, 10)
    assert date_at_doy(y2024, 100) == dt.date(2024, 4, 9)

    # 2. inter-year drift at the same calendar date
    def sunset_clock(cal, doy):
        s = cal.loc[cal["astro_day_of_year"] == doy, "sunset"].iloc[0]
        return s.hour * 3600 + s.minute * 60 + s.second

    md = lambda d: (d.month, d.day)
    assert md(date_at_doy(y2023, 100)) == md(date_at_doy(y2025, 100))  # both non-leap
    assert abs(sunset_clock(y2023, 100) - sunset_clock(y2025, 100)) > 10


# --------------------------------------------------------------------------
# DST -- the main correctness trap
# --------------------------------------------------------------------------


def test_spring_forward_day_has_23_hours(hourly):
    day = hourly[hourly["time"].dt.date.astype(str) == SPRING_FORWARD]
    assert len(day) == 23
    assert (day["dst_transition"] == "spring_forward").all()


def test_fall_back_day_has_25_hours(hourly):
    day = hourly[hourly["time"].dt.date.astype(str) == FALL_BACK]
    assert len(day) == 25
    assert (day["dst_transition"] == "fall_back").all()


def test_only_two_dst_transition_days_per_year(cal):
    labelled = cal.loc[cal["dst_transition"].notna(), "time"].dt.date
    assert sorted(set(labelled.astype(str))) == [SPRING_FORWARD, FALL_BACK]


def test_elapsed_hours_have_no_gap_or_repeat_across_dst(hourly):
    """hours_into_astro_day must advance by exactly 1.0 per hourly step.

    This is the check that fails if elapsed time is computed by subtracting
    tz-aware local timestamps instead of UTC ones: spring forward would show a
    2-hour jump and fall back a repeat.
    """
    for date in (SPRING_FORWARD, FALL_BACK):
        window = hourly[
            hourly["time"].dt.date.astype(str).isin(
                [
                    str(pd.Timestamp(date).date() - dt.timedelta(days=1)),
                    date,
                    str(pd.Timestamp(date).date() + dt.timedelta(days=1)),
                ]
            )
        ]
        steps = window["hours_into_astro_day"].diff().dropna()
        # Steps reset negative at each sunset boundary; every other step is 1h.
        forward = steps[steps > 0]
        assert np.allclose(forward, 1.0), f"{date}: irregular steps {sorted(set(forward))}"


def test_utc_offset_and_is_dst_agree(cal):
    assert set(cal["utc_offset_hours"].unique()) == {-8.0, -7.0}
    assert (cal["is_dst"] == (cal["utc_offset_hours"] == -7.0)).all()


# --------------------------------------------------------------------------
# Astronomical day structure
# --------------------------------------------------------------------------


def test_every_night_is_contained_in_exactly_one_astro_day(cal):
    """The whole point of the reframing: no night is split at midnight."""
    night = cal[cal["day_night"] == "night"]
    # Walk contiguous runs of night rows; each run must carry one astro_day_date.
    run_id = (night.index.to_series().diff() != 1).cumsum()
    per_run = night.groupby(run_id.values)["astro_day_date"].nunique()
    assert (per_run == 1).all(), f"{int((per_run > 1).sum())} nights span two astro days"


def test_night_runs_cross_midnight(cal):
    """Sanity: those single-astro-day nights really do span two calendar dates."""
    night = cal[cal["day_night"] == "night"]
    run_id = (night.index.to_series().diff() != 1).cumsum()
    dates_per_run = night.groupby(run_id.values)["time"].apply(lambda s: s.dt.date.nunique())
    # Interior runs span two calendar dates; only the truncated grid edges do not.
    assert (dates_per_run == 2).sum() >= 364


def test_hours_into_astro_day_is_monotonic_within_each_day(cal):
    grouped = cal.groupby("astro_day_date")["hours_into_astro_day"]
    assert grouped.apply(lambda s: s.is_monotonic_increasing).all()
    assert (cal["hours_into_astro_day"] >= 0).all()
    # A full cycle is one solar day, plus/minus the DST hour.
    spans = grouped.max() - grouped.min()
    assert spans.max() < 25.0


def test_astro_day_boundaries_are_sunsets(cal):
    day = cal[cal["astro_day_date"] == dt.date(2024, 6, 20)]
    start, end = day["astro_day_start"].iloc[0], day["astro_day_end"].iloc[0]
    observer = ac.SAN_DIEGO.observer
    assert abs((start - sun(observer, dt.date(2024, 6, 20), tzinfo=TZ)["sunset"]).total_seconds()) < 1
    assert abs((end - sun(observer, dt.date(2024, 6, 21), tzinfo=TZ)["sunset"]).total_seconds()) < 1


def test_night_and_day_segments_sum_to_the_astro_day(cal):
    complete = cal[cal["astro_day_complete"]]
    total = complete["night_length_hours"] + complete["day_length_hours"]
    span = ac._hours_between(complete["astro_day_end"], complete["astro_day_start"])
    assert np.allclose(total, span)


def test_night_length_varies_seasonally(cal):
    summer = cal.loc[cal["astro_day_date"] == dt.date(2024, 6, 20), "night_length_hours"].iloc[0]
    winter = cal.loc[cal["astro_day_date"] == dt.date(2024, 12, 20), "night_length_hours"].iloc[0]
    assert 9.0 < summer < 10.5
    assert 13.5 < winter < 14.5
    # This spread is exactly why night_fraction exists alongside hours_since_sunset.
    assert winter - summer > 4.0


# --------------------------------------------------------------------------
# Night phase
# --------------------------------------------------------------------------


def test_night_fraction_runs_0_to_1_across_a_night(cal):
    night = cal[
        (cal["astro_day_date"] == dt.date(2024, 6, 20)) & (cal["day_night"] == "night")
    ].sort_values("time")
    nf = night["night_fraction"]
    assert nf.is_monotonic_increasing
    assert nf.iloc[0] < 0.05
    assert nf.iloc[-1] > 0.95
    assert nf.between(0, 1).all()


def test_night_fraction_is_null_exactly_during_the_day(cal):
    is_day = cal["day_night"] == "day"
    assert cal.loc[is_day, "night_fraction"].isna().all()
    assert cal.loc[~is_day, "night_fraction"].notna().all()


def test_night_fraction_cyclicals_are_half_cycle(cal):
    """sin peaks at mid-night; cos runs monotonically +1 at sunset to -1 at sunrise."""
    night = cal[
        (cal["astro_day_date"] == dt.date(2024, 6, 20)) & (cal["day_night"] == "night")
    ].sort_values("time")
    assert night["night_fraction_cos"].is_monotonic_decreasing
    assert night["night_fraction_cos"].iloc[0] > 0.99
    assert night["night_fraction_cos"].iloc[-1] < -0.99
    assert night["night_fraction_sin"].max() > 0.999


def test_hours_to_sunrise_is_signed_around_sunrise(cal):
    day = cal[cal["astro_day_date"] == dt.date(2024, 6, 20)]
    assert (day.loc[day["day_night"] == "night", "hours_to_sunrise"] > 0).all()
    assert (day.loc[day["day_night"] == "day", "hours_to_sunrise"] < 0).all()


def test_solar_elevation_is_negative_at_night_positive_by_day(cal):
    # Refraction makes elevation slightly positive at geometric sunrise/sunset,
    # so allow a small band around the horizon.
    day = cal[cal["day_night"] == "day"]
    night = cal[cal["day_night"] == "night"]
    assert (day["solar_elevation_deg"] > -1.0).all()
    assert (night["solar_elevation_deg"] < 1.0).all()
    assert cal["solar_azimuth_deg"].between(0, 360).all()


def test_twilight_events_are_ordered(cal):
    row = cal.iloc[0]
    assert (
        row["dawn_astronomical"]
        < row["dawn_nautical"]
        < row["dawn_civil"]
        < row["sunrise"]
        < row["solar_noon"]
        < row["sunset"]
        < row["dusk_civil"]
        < row["dusk_nautical"]
        < row["dusk_astronomical"]
    )


# --------------------------------------------------------------------------
# Regression: the calendar must reproduce the original add_day_night()
# --------------------------------------------------------------------------


def _original_add_day_night(df, logger=None):
    """Verbatim copy of add_day_night() from hysplit_forecasting.py:165.

    Kept here as the frozen reference. Phase 3 refactors the real function to
    delegate to the calendar; this test is what proves that is safe.
    """
    san_diego_location = LocationInfo(
        name="San Diego",
        region="USA",
        timezone="America/Los_Angeles",
        latitude=32.7157,
        longitude=-117.1611,
    )
    unique_dates = df["time"].dt.date.unique()
    daily_sun_times = {}
    for date in unique_dates:
        s = sun(san_diego_location.observer, date=date, tzinfo=san_diego_location.timezone)
        daily_sun_times[date] = {"sunrise": s["sunrise"], "sunset": s["sunset"]}

    def get_day_night(timestamp, sun_times_dict):
        date_only = timestamp.date()
        if date_only in sun_times_dict:
            sun_info = sun_times_dict[date_only]
            if sun_info["sunrise"] <= timestamp < sun_info["sunset"]:
                return "day"
            else:
                return "night"
        return "unknown"

    df["day_night"] = df["time"].apply(lambda x: get_day_night(x, daily_sun_times))
    return df


def test_day_night_matches_original_implementation(hourly):
    reference = _original_add_day_night(hourly[["time"]].copy())
    mismatches = (reference["day_night"].values != hourly["day_night"].values).sum()
    assert mismatches == 0, f"{mismatches} of {len(hourly)} rows disagree"


def test_is_night_is_consistent_with_day_night(cal):
    assert (cal["is_night"] == (cal["day_night"] == "night").astype(int)).all()


# --------------------------------------------------------------------------
# attach_astro_frame
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def obs(hourly) -> pd.DataFrame:
    """A stand-in for modeldata_h2s_nofill: two sites on the hourly LA grid."""
    times = hourly["time"].iloc[:240]
    frames = []
    for site in ("NESTOR - BES", "SAN YSIDRO"):
        frames.append(pd.DataFrame({"time": times, "site_name": site, "H2S": 1.0}))
    return pd.concat(frames, ignore_index=True)


def test_attach_preserves_row_count_and_adds_the_frame(obs, hourly):
    out = ac.attach_astro_frame(obs, hourly)
    assert len(out) == len(obs)
    assert out["astro_day_date"].notna().all()
    for col in ("night_of_year", "astro_week_of_year", "night_fraction", "hours_since_sunset"):
        assert col in out.columns


def test_attach_rejects_timestamps_outside_the_calendar(hourly):
    df = pd.DataFrame({"time": pd.to_datetime(["2099-01-01 00:00"]).tz_localize(TZ)})
    with pytest.raises(ValueError, match="no calendar match"):
        ac.attach_astro_frame(df, hourly)


def test_attach_rejects_off_grid_timestamps(hourly):
    """An exact join makes grain mismatches loud instead of snapping to a neighbour."""
    df = pd.DataFrame({"time": [hourly["time"].iloc[10] + pd.Timedelta(minutes=7)]})
    with pytest.raises(ValueError, match="no calendar match"):
        ac.attach_astro_frame(df, hourly)


def test_attach_requires_tz_aware_time(hourly):
    df = pd.DataFrame({"time": pd.to_datetime(["2024-01-01 00:00"])})
    with pytest.raises(TypeError, match="tz-aware"):
        ac.attach_astro_frame(df, hourly)


def test_attach_accepts_a_different_timezone(obs, hourly):
    utc = obs.copy()
    utc["time"] = utc["time"].dt.tz_convert("UTC")
    out = ac.attach_astro_frame(utc, hourly)
    assert len(out) == len(obs)
    assert out["astro_day_date"].notna().all()


def test_attach_keeps_existing_day_night_and_verifies_agreement(obs, hourly):
    existing = _original_add_day_night(obs.copy())
    out = ac.attach_astro_frame(existing, hourly)
    assert (out["day_night"] == existing["day_night"]).all()
    # Only one day_night column survives the join.
    assert sum(c.startswith("day_night") for c in out.columns) == 1


def test_attach_raises_when_existing_day_night_disagrees(obs, hourly):
    corrupted = _original_add_day_night(obs.copy())
    corrupted.loc[corrupted.index[:50], "day_night"] = "day"
    corrupted.loc[corrupted.index[:50], "day_night"] = np.where(
        _original_add_day_night(obs.copy()).loc[corrupted.index[:50], "day_night"] == "day",
        "night",
        "day",
    )
    with pytest.raises(ValueError, match="disagrees with existing column"):
        ac.attach_astro_frame(corrupted, hourly)


# --------------------------------------------------------------------------
# validate_astro_calendar
# --------------------------------------------------------------------------


def test_validate_reports_missing_columns():
    failures = ac.validate_astro_calendar(pd.DataFrame({"time": []}))
    assert failures and "missing columns" in failures[0]


def test_validate_catches_a_split_astro_day(cal):
    scrambled = pd.concat([cal.iloc[:100], cal.iloc[-100:], cal.iloc[100:200]])
    failures = ac.validate_astro_calendar(scrambled)
    assert any("more than one run" in f or "monotonically" in f for f in failures)


def test_validate_catches_nulls(cal):
    broken = cal.copy()
    broken.loc[broken.index[:5], "astro_day_date"] = None
    assert any("astro_day_date has 5 nulls" in f for f in ac.validate_astro_calendar(broken))


# --------------------------------------------------------------------------
# External verification against the US Naval Observatory
# --------------------------------------------------------------------------

# Fetched once from https://aa.usno.navy.mil/api/rstt/oneday for
# coords=32.7157,-117.1611 and frozen here so the test stays offline.
# Values are local wall clock, whole minutes: (dawn_civil, sunrise, solar_noon,
# sunset, dusk_civil). Includes both 2026 DST transition days -- USNO reported
# them as DT and ST respectively, which is what pins down the timezone handling.
USNO_2026 = {
    "2026-06-20": ("05:12", "05:41", "12:50", "20:00", "20:28"),  # summer solstice
    "2026-12-21": ("06:20", "06:47", "11:47", "16:47", "17:14"),  # winter solstice
    "2026-03-20": ("06:28", "06:52", "12:56", "19:00", "19:25"),  # equinox
    "2026-03-08": ("06:43", "07:08", "12:59", "18:51", "19:16"),  # spring forward
    "2026-11-01": ("05:41", "06:06", "11:32", "16:58", "17:23"),  # fall back
}
USNO_FIELDS = ("dawn_civil", "sunrise", "solar_noon", "sunset", "dusk_civil")


def test_sun_times_match_usno_reference():
    """Independent check that the observer, longitude sign, and timezone are right.

    Every internal invariant would still pass with a mis-signed longitude or the
    wrong timezone; only an external reference catches that. USNO tabulates to
    whole minutes, so 1 minute is the tightest meaningful tolerance.
    """
    cal = ac.build_astro_calendar(2026, 2026, "1h", with_solar_position=False)
    for date, expected in USNO_2026.items():
        day = cal[cal["time"].dt.date.astype(str) == date]
        assert not day.empty, date
        for field, want in zip(USNO_FIELDS, expected):
            got = day[field].iloc[0]
            want_h, want_m = (int(x) for x in want.split(":"))
            drift_min = abs(
                (got.hour * 60 + got.minute + got.second / 60) - (want_h * 60 + want_m)
            )
            assert drift_min < 1.0, f"{date} {field}: {got:%H:%M:%S} vs USNO {want}"


def test_dst_transitions_land_on_the_usno_dst_dates():
    cal = ac.build_astro_calendar(2026, 2026, "1h", with_solar_position=False)
    hourly_dates = cal["time"].dt.date.astype(str)
    assert sorted(set(hourly_dates[cal["dst_transition"].notna()])) == [
        "2026-03-08",
        "2026-11-01",
    ]
    assert (hourly_dates == "2026-03-08").sum() == 23
    assert (hourly_dates == "2026-11-01").sum() == 25


# --------------------------------------------------------------------------
# label_day_night -- the shared helper add_day_night() now delegates to
# --------------------------------------------------------------------------


def test_add_day_night_helper_matches_original(hourly):
    """Pins the Phase 3 refactor of hysplit_forecasting.add_day_night().

    That function now delegates to label_day_night(); this asserts the delegation
    reproduces the frozen original byte for byte.
    """
    df = hourly[["time"]].copy()
    reference = _original_add_day_night(df.copy())
    assert (ac.label_day_night(df["time"]).values == reference["day_night"].values).all()


def test_label_day_night_matches_the_calendar_column(hourly):
    assert (ac.label_day_night(hourly["time"]).values == hourly["day_night"].values).all()


def test_label_day_night_handles_non_local_timezones(hourly):
    utc = hourly["time"].dt.tz_convert("UTC")
    assert (ac.label_day_night(utc).values == hourly["day_night"].values).all()


def test_label_day_night_rejects_naive_timestamps():
    with pytest.raises(TypeError, match="tz-aware"):
        ac.label_day_night(pd.Series(pd.to_datetime(["2024-01-01 12:00"])))


# --------------------------------------------------------------------------
# day_night disagreement tolerance
# --------------------------------------------------------------------------


def _flip_at(df: pd.DataFrame, mask) -> pd.DataFrame:
    out = df.copy()
    out.loc[mask, "day_night"] = np.where(out.loc[mask, "day_night"] == "day", "night", "day")
    return out


def test_no_mismatches_when_labels_agree(obs, hourly):
    labelled = _original_add_day_night(obs.copy())
    assert ac.day_night_mismatches(labelled, hourly).empty


def test_mismatch_report_measures_distance_from_the_boundary(obs, hourly):
    labelled = _original_add_day_night(obs.copy())
    # Flip the row sitting at a sunrise: its distance to the boundary is small.
    joined = labelled.merge(hourly[["time", "sunrise"]].drop_duplicates("time"), on="time")
    at_sunrise = (joined["time"] - joined["sunrise"]).abs().idxmin()
    flipped = _flip_at(labelled, labelled.index == at_sunrise)

    report = ac.day_night_mismatches(flipped, hourly)
    assert len(report) == 1
    assert report["minutes_from_boundary"].iloc[0] < 60


def test_boundary_disagreement_is_tolerated_when_allowed(obs, hourly):
    """A forecast source using a different solar model may flip labels at a boundary."""
    labelled = _original_add_day_night(obs.copy())
    joined = labelled.merge(hourly[["time", "sunrise"]].drop_duplicates("time"), on="time")
    at_sunrise = (joined["time"] - joined["sunrise"]).abs().idxmin()
    flipped = _flip_at(labelled, labelled.index == at_sunrise)

    with pytest.raises(ValueError, match="disagrees with existing column"):
        ac.attach_astro_frame(flipped, hourly)  # strict by default

    out = ac.attach_astro_frame(flipped, hourly, day_night_tolerance_minutes=90.0)
    assert len(out) == len(flipped)
    # The dataset's own labels survive; the calendar's are dropped.
    assert (out["day_night"] == flipped["day_night"]).all()


def test_midday_disagreement_always_raises(obs, hourly):
    """Tolerance must not mask a genuine bug far from any boundary."""
    labelled = _original_add_day_night(obs.copy())
    joined = labelled.merge(hourly[["time", "solar_noon"]].drop_duplicates("time"), on="time")
    at_noon = (joined["time"] - joined["solar_noon"]).abs().idxmin()
    flipped = _flip_at(labelled, labelled.index == at_noon)

    with pytest.raises(ValueError, match="disagrees with existing column"):
        ac.attach_astro_frame(flipped, hourly, day_night_tolerance_minutes=60.0)


# --------------------------------------------------------------------------
# validate_reframed -- the invariants the Phase 4 asset checks surface
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reframed(obs, hourly) -> pd.DataFrame:
    return ac.attach_astro_frame(_original_add_day_night(obs.copy()), hourly)


def test_reframed_data_passes_its_invariants(reframed):
    assert ac.validate_reframed(reframed) == []


def test_validate_reframed_rejects_a_frameless_dataset(obs):
    failures = ac.validate_reframed(obs)
    assert failures and "missing frame columns" in failures[0]


def test_validate_reframed_rejects_an_empty_dataset(reframed):
    assert ac.validate_reframed(reframed.iloc[:0]) == ["dataset is empty"]


def test_validate_reframed_catches_null_frame_keys(reframed):
    broken = reframed.copy()
    broken.loc[broken.index[:4], "astro_day_of_year"] = None
    assert any("astro_day_of_year has 4 nulls" in f for f in ac.validate_reframed(broken))


def test_validate_reframed_catches_rows_outside_their_astro_day(reframed):
    broken = reframed.copy()
    broken.loc[broken.index[:3], "astro_day_start"] = broken["astro_day_start"] + pd.Timedelta(
        days=2
    )
    assert any("outside their own astro day bounds" in f for f in ac.validate_reframed(broken))


def test_validate_reframed_catches_inconsistent_is_night(reframed):
    broken = reframed.copy()
    broken.loc[broken.index[:3], "is_night"] = 1 - broken.loc[broken.index[:3], "is_night"]
    assert any("is_night disagrees" in f for f in ac.validate_reframed(broken))


def test_validate_reframed_catches_a_split_night(reframed):
    """The regression that would undo the whole point of the reframing.

    Relabelling part of a night with a neighbouring astro_day_date makes that
    group span far longer than a single night, which is what the check detects.
    """
    broken = reframed.copy()
    night = broken[broken["day_night"] == "night"]
    target = night["astro_day_date"].iloc[0]
    victim = night.index[night["astro_day_date"] == target][:2]
    broken.loc[victim, "astro_day_date"] = night["astro_day_date"].max()
    assert any("span longer than the night itself" in f for f in ac.validate_reframed(broken))


def test_validate_reframed_catches_bad_night_fraction(reframed):
    broken = reframed.copy()
    night_idx = broken.index[broken["day_night"] == "night"][0]
    broken.loc[night_idx, "night_fraction"] = 4.2
    assert any("night_fraction out of" in f for f in ac.validate_reframed(broken))

    dropped = reframed.copy()
    dropped.loc[night_idx, "night_fraction"] = None
    assert any("null night_fraction" in f for f in ac.validate_reframed(dropped))


# --------------------------------------------------------------------------
# reframed_frame_summary
# --------------------------------------------------------------------------


def test_summary_counts_nights_that_cross_midnight(reframed):
    summary = ac.reframed_frame_summary(reframed)
    assert summary["rows"] == len(reframed)
    assert summary["night_rows"] + summary["day_rows"] == len(reframed)
    # Every full night at this latitude crosses midnight -- that is precisely the
    # split a midnight-anchored frame would have introduced.
    assert summary["nights_crossing_midnight"] > 0
    assert summary["nights_crossing_midnight"] <= summary["nights_with_data"] * 2
