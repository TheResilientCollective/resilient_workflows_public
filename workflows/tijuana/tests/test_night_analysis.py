"""Unit tests for the night-centred analysis helpers.

Loaded by file path for the same reason as test_astro_calendar.py: importing the
``tijuana`` package pulls in the full asset graph.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_UTILS = Path(__file__).resolve().parents[1] / "src" / "tijuana" / "utils"

# The utils modules import each other relatively, so they have to be loaded
# inside a package rather than as bare files.
_PKG = "_tijuana_utils_under_test"
if _PKG not in sys.modules:
    _pkg = types.ModuleType(_PKG)
    _pkg.__path__ = [str(_UTILS)]
    sys.modules[_PKG] = _pkg


def _load(name):
    qualified = f"{_PKG}.{name}"
    if qualified in sys.modules:
        return sys.modules[qualified]
    spec = importlib.util.spec_from_file_location(qualified, _UTILS / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[qualified] = mod
    spec.loader.exec_module(mod)
    return mod


ac = _load("astro_calendar")
na = _load("night_analysis")

SITES = ("NESTOR - BES", "SAN YSIDRO")


@pytest.fixture(scope="module")
def hourly() -> pd.DataFrame:
    return ac.to_hourly(ac.build_astro_calendar(2024, 2024, "15min", with_solar_position=False))


@pytest.fixture(scope="module")
def reframed(hourly) -> pd.DataFrame:
    """A synthetic month of hourly observations on the astronomical frame.

    H2S is driven to a mid-night maximum so the peak-timing columns have a known
    answer, and wind is held at a constant bearing so steadiness must be 1.
    """
    window = hourly[
        (hourly["time"] >= pd.Timestamp("2024-01-01", tz=ac.TZ))
        & (hourly["time"] < pd.Timestamp("2024-02-01", tz=ac.TZ))
    ]
    frames = []
    for i, site in enumerate(SITES):
        f = window.copy()
        f["site_name"] = site
        # Peak at mid-night (night_fraction 0.5), zero during the day.
        f["H2S"] = np.where(
            f["day_night"] == "night", (10 + 40 * np.sin(np.pi * f["night_fraction"])) * (i + 1), 0.5
        )
        f["h2s_measured"] = True
        f["wind_speed_10m"] = 3.0
        f["wind_direction_10m"] = 90.0
        f["wind_gusts_10m"] = 7.5
        f[na.FLOW_COL] = 2.0
        f["tide_height"] = 1.0
        f["temperature_2m"] = 15.0
        frames.append(f)
    return pd.concat(frames, ignore_index=True)


# --------------------------------------------------------------------------
# exceedances_by_segment
# --------------------------------------------------------------------------


def test_segments_are_keyed_by_site_astro_day_and_period(reframed):
    seg = na.exceedances_by_segment(reframed)
    assert not seg.duplicated(["site_name", "astro_day_date", "period"]).any()
    assert set(seg["period"]) == {"day", "night"}
    assert set(seg["site_name"]) == set(SITES)


def test_segment_counts_match_a_direct_count(reframed):
    seg = na.exceedances_by_segment(reframed)
    assert seg["count_exceeds_5"].sum() == int((reframed["H2S"] > 5).sum())
    assert seg["count_exceeds_30"].sum() == int((reframed["H2S"] > 30).sum())
    assert seg["total_measurements"].sum() == len(reframed)


def test_exceedances_land_at_night_in_this_fixture(reframed):
    seg = na.exceedances_by_segment(reframed)
    day = seg[seg["period"] == "day"]
    assert day["count_exceeds_5"].sum() == 0


def test_segments_carry_frame_descriptors(reframed):
    seg = na.exceedances_by_segment(reframed)
    for col in ("astro_year", "astro_week_of_year", "night_of_year", "night_length_hours"):
        assert col in seg.columns
        assert seg[col].notna().all()


def test_segments_handle_all_null_h2s(reframed):
    blank = reframed.copy()
    blank["H2S"] = np.nan
    assert na.exceedances_by_segment(blank).empty


# --------------------------------------------------------------------------
# summarize_nights
# --------------------------------------------------------------------------


def test_one_row_per_night_per_site(reframed):
    s = na.summarize_nights(reframed)
    assert not s.duplicated(["astro_day_date", "site_name"]).any()
    nights = reframed.loc[reframed["day_night"] == "night", "astro_day_date"].nunique()
    assert len(s) == nights * len(SITES)


def test_peak_timing_lands_mid_night(reframed):
    """The fixture puts the H2S maximum at night_fraction 0.5 by construction."""
    s = na.summarize_nights(reframed)
    full = s[s["h2s_observations"] > 4]
    assert full["peak_night_fraction"].between(0.3, 0.7).all()
    # Raw hours after sunset vary with season even though the phase does not.
    assert full["peak_hours_after_sunset"].std() > 0


def test_peak_time_is_inside_its_own_night(reframed):
    s = na.summarize_nights(reframed)
    have = s[s["peak_time"].notna()]
    assert (have["peak_time"] >= have["night_start"]).all()
    end = have["night_start"] + pd.to_timedelta(have["night_length_hours"], unit="h")
    assert (have["peak_time"] <= end).all()


def test_hours_above_threshold_never_exceeds_observations(reframed):
    s = na.summarize_nights(reframed)
    assert (s["hours_above_5"] <= s["h2s_observations"]).all()
    assert (s["hours_above_30"] <= s["hours_above_5"]).all()


def test_per_site_scaling_is_preserved(reframed):
    """Site 2's H2S is twice site 1's, so its nightly maximum must be too."""
    s = na.summarize_nights(reframed).dropna(subset=["h2s_max"])
    a = s[s["site_name"] == SITES[0]].set_index("astro_day_date")["h2s_max"]
    b = s[s["site_name"] == SITES[1]].set_index("astro_day_date")["h2s_max"]
    common = a.index.intersection(b.index)
    assert np.allclose(b.loc[common], 2 * a.loc[common])


def test_constant_wind_direction_gives_steadiness_one(reframed):
    s = na.summarize_nights(reframed)
    assert np.allclose(s["wind_steadiness"].dropna(), 1.0)
    assert np.allclose(s["wind_resultant_direction"].dropna(), 90.0)
    assert np.allclose(s["wind_speed_mean"].dropna(), 3.0)


def test_nights_with_no_h2s_still_produce_a_row(reframed):
    blank = reframed.copy()
    blank["H2S"] = np.nan
    s = na.summarize_nights(blank)
    assert len(s) > 0
    assert (s["h2s_observations"] == 0).all()
    assert s["h2s_max"].isna().all()
    assert (s["hours_above_5"] == 0).all()
    # Non-H2S conditions are still summarised.
    assert s["wind_speed_mean"].notna().all()


def test_summary_is_empty_when_there_are_no_nights(reframed):
    assert na.summarize_nights(reframed[reframed["day_night"] == "day"]).empty


# --------------------------------------------------------------------------
# vector wind
# --------------------------------------------------------------------------


def test_opposing_winds_cancel_to_near_zero_steadiness():
    speed = pd.Series([5.0, 5.0])
    direction = pd.Series([0.0, 180.0])
    out = na._resultant_wind(speed, direction)
    assert out["wind_speed_mean"] == pytest.approx(5.0)
    assert out["wind_resultant_speed"] == pytest.approx(0.0, abs=1e-9)
    assert out["wind_steadiness"] == pytest.approx(0.0, abs=1e-9)


def test_wind_bearing_averages_across_north_without_flipping_south():
    """A scalar mean of 350 and 10 degrees would give 180 -- exactly backwards."""
    out = na._resultant_wind(pd.Series([4.0, 4.0]), pd.Series([350.0, 10.0]))
    assert out["wind_resultant_direction"] == pytest.approx(0.0, abs=1e-6) or out[
        "wind_resultant_direction"
    ] == pytest.approx(360.0, abs=1e-6)


def test_all_null_wind_returns_nans():
    out = na._resultant_wind(pd.Series([np.nan]), pd.Series([np.nan]))
    assert all(np.isnan(v) for v in out.values())


# --------------------------------------------------------------------------
# filter_exceedance_segments
# --------------------------------------------------------------------------


def test_filter_returns_only_segments_that_exceeded(reframed):
    seg = na.exceedances_by_segment(reframed)
    out = na.filter_exceedance_segments(reframed, seg, 30)
    assert not out.empty
    hits = set(
        map(tuple, seg.loc[seg["count_exceeds_30"] > 0, ["site_name", "astro_day_date", "period"]].values)
    )
    got = set(map(tuple, out[["site_name", "astro_day_date", "period"]].drop_duplicates().values))
    assert got == hits
    assert (out["exceedance_threshold"] == "30_ppb").all()


def test_filter_keeps_whole_segments_not_just_exceeding_hours(reframed):
    """Downstream modelling needs the full environmental context of the event."""
    seg = na.exceedances_by_segment(reframed)
    out = na.filter_exceedance_segments(reframed, seg, 30)
    assert (out["H2S"] <= 30).any()


def test_higher_threshold_is_a_subset_of_the_lower(reframed):
    seg = na.exceedances_by_segment(reframed)
    low = na.filter_exceedance_segments(reframed, seg, 5)
    high = na.filter_exceedance_segments(reframed, seg, 30)
    key = ["site_name", "astro_day_date", "period"]
    low_keys = set(map(tuple, low[key].drop_duplicates().values))
    high_keys = set(map(tuple, high[key].drop_duplicates().values))
    assert high_keys <= low_keys


def test_filter_is_empty_when_nothing_exceeds(reframed):
    quiet = reframed.copy()
    quiet["H2S"] = 0.1
    seg = na.exceedances_by_segment(quiet)
    assert na.filter_exceedance_segments(quiet, seg, 5).empty


# --------------------------------------------------------------------------
# Gap-filled values must never be counted as observed exceedances
# --------------------------------------------------------------------------

hx = _load("h2s_exceedance")


def _mixed_frame() -> pd.DataFrame:
    """Four rows in one group: two measured, two gap-filled, all above 5 ppb."""
    return pd.DataFrame(
        {
            "site_name": ["A"] * 4,
            "date": [dt.date(2024, 1, 1)] * 4,
            "period": ["night"] * 4,
            "H2S": [10.0, 20.0, 40.0, 50.0],
            "h2s_measured": [True, True, False, False],
        }
    )


def test_filled_values_are_excluded_from_counts_and_stats():
    out = hx.aggregate_exceedances(_mixed_frame(), ["site_name", "date", "period"])
    row = out.iloc[0]
    assert row["count_exceeds_5"] == 2          # not 4
    assert row["count_exceeds_30"] == 0          # the two >30 values are synthetic
    assert row["max_h2s"] == 20.0                # not 50.0
    assert row["mean_h2s"] == pytest.approx(15.0)
    assert row["count_filled"] == 2
    assert row["measured_observations"] == 2
    assert row["total_measurements"] == 4        # keeps its original all-non-null meaning


def test_counts_and_stats_stay_mutually_consistent():
    """max_h2s must never imply an exceedance the counts do not report."""
    out = hx.aggregate_exceedances(_mixed_frame(), ["site_name", "date", "period"])
    row = out.iloc[0]
    assert (row["max_h2s"] > 30) == (row["count_exceeds_30"] > 0)
    assert (row["max_h2s"] > 5) == (row["count_exceeds_5"] > 0)


def test_totals_decompose(): 
    out = hx.aggregate_exceedances(_mixed_frame(), ["site_name", "date", "period"])
    row = out.iloc[0]
    assert row["total_measurements"] == row["measured_observations"] + row["count_filled"]


def test_a_group_of_only_filled_values_still_produces_a_row():
    df = _mixed_frame()
    df["h2s_measured"] = False
    out = hx.aggregate_exceedances(df, ["site_name", "date", "period"])
    assert len(out) == 1
    row = out.iloc[0]
    assert row["count_exceeds_5"] == 0
    assert row["measured_observations"] == 0
    assert row["count_filled"] == 4
    assert pd.isna(row["max_h2s"])


def test_missing_measured_column_treats_everything_as_measured():
    """Correct for nofill-style inputs, where unmeasured values are already null."""
    df = _mixed_frame().drop(columns=["h2s_measured"])
    out = hx.aggregate_exceedances(df, ["site_name", "date", "period"])
    row = out.iloc[0]
    assert row["count_exceeds_5"] == 4
    assert row["count_filled"] == 0
    assert row["max_h2s"] == 50.0


def test_nofill_style_input_is_unaffected_by_the_guard(reframed):
    """h2s_peaks reads modeldata_h2s_nofill, so the guard is a no-op there."""
    with_flag = reframed.copy()
    with_flag["h2s_measured"] = True
    a = na.exceedances_by_segment(reframed)
    b = na.exceedances_by_segment(with_flag)
    pd.testing.assert_frame_equal(
        a.drop(columns=["count_filled", "measured_observations"], errors="ignore"),
        b.drop(columns=["count_filled", "measured_observations"], errors="ignore"),
    )
