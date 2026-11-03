"""Unit tests for serving the night-target H2S model.

Loaded by file path, like the other utils tests, so the full asset graph is not
imported. The point is parity with training — tonight's features are built
the way ``h2s_backtest.prepare_nightly`` builds the training rows — and the
bookkeeping around the issue time, not model skill.
"""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_UTILS = Path(__file__).resolve().parents[1] / "src" / "tijuana" / "utils"
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


ff = _load("forecast_features")
bt = _load("h2s_backtest")
nf = _load("h2s_night_forecast")

TZ = "America/Los_Angeles"
SITES = ("NESTOR - BES", "SAN YSIDRO")
FLOW = "Flow (m^3/s)--Border"


def _hourly(days=40, seed=0):
    rng = np.random.default_rng(seed)
    times = pd.date_range("2025-03-01", periods=days * 24, freq="h", tz=TZ)
    frames = []
    for site in SITES:
        df = pd.DataFrame({"time": times, "site_name": site})
        for f in ff.MODEL_FEATURES:
            df[f] = rng.normal(size=len(times))
        df[FLOW] = rng.uniform(0.1, 3, size=len(times))
        night = (times.hour >= 20) | (times.hour <= 5)
        df["H2S"] = np.where(night & (rng.uniform(size=len(times)) > 0.85), 45.0, rng.uniform(0, 8, size=len(times)))
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _nightly(hourly):
    """A nightly table in the shape of h2s_nightly_summary, one row per site per date."""
    rng = np.random.default_rng(1)
    rows = []
    for (site, day), g in hourly.assign(day=hourly["time"].dt.normalize().dt.tz_localize(None)).groupby(["site_name", "day"]):
        row = {"site_name": site, "astro_day_date": day, "astro_day_complete": True, "h2s_coverage": 0.9,
               "h2s_max": float(g["H2S"].max()), "wind_resultant_direction": float(rng.uniform(0, 360))}
        for f in bt.NIGHT_SUMMARY_FEATURES:
            row[f] = float(rng.normal())
        rows.append(row)
    return pd.DataFrame(rows)


def _forecast_frame(day, sites=SITES, freq="15min"):
    """Forecast inference rows on the astronomical day, as modeldata_forecast_15min_astronomical_day carries them."""
    start = pd.Timestamp(day).tz_localize(TZ) + pd.Timedelta(hours=18)
    times = pd.date_range(start, periods=12 * (4 if freq == "15min" else 1), freq=freq)
    frames = []
    for i, site in enumerate(sites):
        df = pd.DataFrame({"time": times, "site_name": site})
        df["astro_day_date"] = pd.Timestamp(day)
        df["day_night"] = "night"
        df["astro_day_start"] = start
        df["night_length_hours"] = 12.0
        df["astro_day_complete"] = True
        df["night_fraction"] = np.linspace(0, 1, len(times))
        df["hours_into_astro_day"] = np.arange(len(times)) / (4 if freq == "15min" else 1)
        df["wind_speed_10m"] = 3.0 + i
        df["wind_direction_10m"] = 270.0
        df["wind_gusts_10m"] = 6.0
        df[FLOW] = 1.5
        df["sbiwtp_flow_mgd"] = 25.0
        df["tide_height"] = np.linspace(0.5, 1.5, len(times))
        df["temperature_2m"] = 15.0
        df["relative_humidity_2m"] = 80.0
        df["surface_pressure"] = 1013.0
        df["stable_atm"] = 1
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def prepared():
    cfg = bt.BacktestConfig(stations=SITES, n_estimators=20)
    hourly = bt.prepare_hourly(_hourly(), cfg)
    nightly = _nightly(hourly)
    rows = bt.prepare_nightly(nightly, hourly, cfg)
    return cfg, hourly, nightly, rows


def test_night_of_covers_the_coming_night():
    assert nf.night_of(pd.Timestamp("2026-11-03 10:45", tz=TZ)) == pd.Timestamp("2026-11-03")
    assert nf.night_of(pd.Timestamp("2026-11-03 19:45", tz=TZ)) == pd.Timestamp("2026-11-03")
    # in the small hours the night in progress is the one named by yesterday's sunset
    assert nf.night_of(pd.Timestamp("2026-11-04 02:00", tz=TZ)) == pd.Timestamp("2026-11-03")


def test_forecast_features_use_hourly_rows_so_hour_counts_match_training():
    day = pd.Timestamp("2025-04-15")
    q = nf.forecast_night_features(_forecast_frame(day, freq="15min"), day, SITES)
    h = nf.forecast_night_features(_forecast_frame(day, freq="h"), day, SITES)
    assert len(q) == len(SITES)
    # 15-minute input must not count four stable hours per hour
    assert list(q["stable_atm_hours"]) == list(h["stable_atm_hours"]) == [12, 12]
    assert q["wind_resultant_direction"].round(6).eq(270.0).all()
    # another night's rows are ignored
    assert nf.forecast_night_features(_forecast_frame(day), day + pd.Timedelta(days=1), SITES).empty


def test_previous_peaks_match_training_rows(prepared):
    cfg, _, nightly, rows = prepared
    for site in SITES:
        r = rows[rows["site_name"] == site].iloc[-1]
        p = nf.previous_peaks(nightly, r["astro_day_date"], cfg).set_index("site_name").loc[site]
        for c in ("prev1_peak_log", "prev3_peak_log", "prev7_peak_log"):
            assert p[c] == pytest.approx(r[c])


def test_history_at_issue_matches_training_rows(prepared):
    cfg, hourly, _, rows = prepared
    r = rows[rows["site_name"] == "SAN YSIDRO"].iloc[-1]
    h = nf.history_at_issue(hourly, r["issue_time"], cfg).set_index("site_name").loc["SAN YSIDRO"]
    for c in bt.NIGHT_HISTORY_FEATURES:
        assert h[c] == pytest.approx(r[c])


def test_recent_readings_extend_history_and_previous_peaks(prepared):
    cfg, hourly, nightly, _ = prepared
    last = hourly["time"].max()
    new_times = pd.date_range(last + pd.Timedelta(hours=1), periods=30, freq="h")
    recent = pd.DataFrame({"site_name": "NESTOR - BES", "time": new_times, "H2S": 50.0})
    ext = nf.extend_with_recent(hourly, pd.concat([hourly[["site_name", "time", "H2S"]].tail(5), recent]))
    assert len(ext) == len(hourly) + len(recent)  # older readings are not duplicated
    issue = new_times[-1] + pd.Timedelta(minutes=30)
    h = nf.history_at_issue(ext, issue, cfg).set_index("site_name")
    assert h.loc["NESTOR - BES", "hist_last_h2s"] == 50.0
    assert h.loc["NESTOR - BES", "hist_hours_since_exceedance"] == pytest.approx(0.5)

    framed = recent.assign(astro_day_date=new_times.tz_localize(None).normalize() - pd.Timedelta(days=1),
                           day_night="night", night_length_hours=12.0)
    peaks = nf.recent_night_peaks(framed, issue)
    assert set(peaks["astro_day_date"]) and peaks["astro_day_complete"].map(bool).all()
    tonight = nf.night_of(issue)
    # the nightly table trails the monitors: it stops a night before the recent readings end
    trailing = nightly[nightly["astro_day_date"] < peaks["astro_day_date"].max()]
    prev = nf.previous_peaks(trailing, tonight, cfg, peaks).set_index("site_name")
    assert prev.loc["NESTOR - BES", "prev1_peak_log"] == pytest.approx(np.log1p(50.0))


def test_forecast_returns_probabilities_and_skips_incomplete_rows(prepared):
    cfg, hourly, nightly, rows = prepared
    models = nf.train(rows, cfg)
    day = pd.Timestamp(hourly["time"].max().tz_localize(None).normalize())
    issue = (day + pd.Timedelta(hours=15)).tz_localize(TZ)
    feats = nf.tonight_features(_forecast_frame(day), nightly, hourly, issue, cfg)
    assert set(cfg.features_night) <= set(feats.columns)
    feats.loc[feats["site_name"] == "SAN YSIDRO", "tide_height_mean"] = np.nan
    out = nf.forecast(models, feats, cfg).set_index("site_name")
    p = out.loc["NESTOR - BES", "probability"]
    assert 0.0 <= p <= 1.0
    assert out.loc["NESTOR - BES", "warn_30"] == (p >= 0.3)
    assert np.isnan(out.loc["SAN YSIDRO", "probability"]) and out.loc["SAN YSIDRO", "warn_50"] is None
    assert "tide_height_mean" in out.loc["SAN YSIDRO", "missing_features"]


def test_train_refuses_a_single_class(prepared):
    cfg, _, _, rows = prepared
    with pytest.raises(ValueError):
        nf.train(rows.assign(y_night=0), cfg)
