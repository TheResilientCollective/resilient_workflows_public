"""Unit tests for the H2S classifier backtest.

Loaded by file path, like the other utils tests, so the full asset graph is not
imported. The synthetic record is small and the forests tiny; the point is the
bookkeeping — no leakage, operational lags identical to the forecast path,
nightly verification — not model skill.
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

TZ = "America/Los_Angeles"


def _synthetic_record(days: int = 120, sites=("NESTOR - BES", "SAN YSIDRO"), seed: int = 0) -> pd.DataFrame:
    """Hourly rows with every production feature, and H2S that spikes at night when a 'signal' feature is high."""
    rng = np.random.default_rng(seed)
    times = pd.date_range("2025-01-01", periods=days * 24, freq="h", tz=TZ)
    frames = []
    for site in sites:
        n = len(times)
        df = pd.DataFrame({"time": times, "site_name": site})
        for f in ff.MODEL_FEATURES:
            df[f] = rng.normal(size=n)
        df["Flow (m^3/s)--Border"] = rng.uniform(0.1, 3, size=n)
        night = ((times.hour >= 20) | (times.hour <= 6)).astype(float)
        signal = df["temperature_2m"].to_numpy()  # stands in for a real driver
        h2s = np.exp(1.0 + 1.5 * night * (signal > 0.8) + rng.normal(scale=0.4, size=n)) * 3
        h2s[night * (signal > 1.2) > 0] *= 8  # clear exceedances on strong-signal nights
        df["H2S"] = h2s
        df["h2s_measured"] = True
        df.loc[rng.random(n) < 0.03, "H2S"] = np.nan  # a few gaps
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


@pytest.fixture(scope="module")
def cfg():
    return bt.BacktestConfig(threshold=30, n_folds=2, initial_train_fraction=0.5, n_estimators=25, min_train_positives=5)


@pytest.fixture(scope="module")
def hourly(cfg):
    return bt.prepare_hourly(_synthetic_record(), cfg)


@pytest.fixture(scope="module")
def preds(hourly, cfg):
    return bt.simulate(hourly, cfg)


def test_prepare_drops_unmeasured_and_recomputes_lags(cfg):
    raw = _synthetic_record(days=10)
    out = bt.prepare_hourly(raw, cfg)
    assert out["H2S"].notna().all()
    assert set(out["y"].unique()) <= {0, 1}
    one = out[out["site_name"] == "NESTOR - BES"].reset_index(drop=True)
    # lag_1h is the previous retained row's H2S, by construction
    assert np.allclose(one["h2s_lag_1h"].iloc[1:].to_numpy(), one["H2S"].iloc[:-1].to_numpy())
    assert len(out) < len(raw)  # NaN rows (gaps and the first lag rows) are gone


def test_operational_lags_match_forecast_path():
    """The backtest must synthesise the lag features exactly as the forecast does."""
    state = {"h2s": 40.0, "h2s_6h": 25.0, "h2s_24h": 12.0, "flow": 1.7, "flow_24h": 2.2}
    n = 18
    sfc = pd.DataFrame({f: np.zeros(n) for f in ff.MODEL_FEATURES})
    sfc["time"] = pd.date_range("2025-03-01 16:00", periods=n, freq="h", tz=TZ)
    full_state = {**state, "sbiwtp_mgd": 23.0, "sbiwtp_sli": 5.0, "sbiwtp_anomaly": 0.0,
                  "sbiwtp_deficit": 0.0, "sbiwtp_hourly_mgd": 23.0, "wd_sin": 0.0, "wd_cos": 0.0}
    reference = ff.engineer_station_features(sfc, full_state)
    ours = bt.operational_lag_features(state, np.arange(1, n + 1))
    for col in bt.H2S_LAG_FEATURES + bt.FLOW_LAG_FEATURES:
        assert np.allclose(ours[col], reference[col].to_numpy()), col
    # and a non-contiguous subset of leads indexes the same curves
    sub = bt.operational_lag_features(state, np.array([1, 5, 18]))
    assert np.allclose(sub["h2s_rolling_24h"], reference["h2s_rolling_24h"].to_numpy()[[0, 4, 17]])


def test_simulate_shape_and_no_leakage(preds, hourly, cfg):
    assert not preds.empty
    assert {"site_name", "fold", "issue_time", "target_time", "lead_hour", "y", "p_deployed", "p_served_time",
            "p_persistence", "p_climatology"} <= set(preds.columns)
    # every target lies strictly after its issue, within the horizon
    lead = (preds["target_time"] - preds["issue_time"]) / pd.Timedelta(hours=1)
    assert (lead >= 1).all() and (lead <= cfg.horizon_hours).all()
    assert (preds["lead_hour"] == lead.round().astype(int)).all()
    # issues happen at the configured local hour
    assert (preds["issue_time"].dt.tz_convert(TZ).dt.hour == cfg.issue_hour).all()
    # no fold scores a row that was in its own training block
    for site, g in preds.groupby("site_name"):
        s = hourly[hourly["site_name"] == site].sort_values("time").reset_index(drop=True)
        for fold, start, _ in bt._folds(len(s), cfg):
            scored = g[g["fold"] == fold]["target_time"]
            assert scored.min() > s["time"].iloc[start - 1]
    _p = preds[[f"p_{v}" for v in bt.VARIANTS]]
    assert ((_p >= 0) & (_p <= 1)).all().all()
    assert set(preds["p_persistence"].unique()) <= {0.0, 1.0}


def test_nightly_events_aggregate_per_issue(preds, cfg):
    nights = bt.nightly_events(preds, cfg)
    assert len(nights) == preds.groupby(["site_name", "issue_time"]).ngroups
    one = nights.iloc[0]
    rows = preds[(preds["site_name"] == one["site_name"]) & (preds["issue_time"] == one["issue_time"])]
    assert one["event"] == rows["y"].max()
    assert one["p_deployed"] == rows["p_deployed"].max()
    assert one["peak_h2s"] == rows["H2S"].max()


def test_score_and_render(preds, cfg):
    metrics = bt.score(preds, cfg)
    for v in bt.VARIANTS:
        assert metrics["nightly"][v]["n"] == metrics["record"]["nights_scored"]
    c = next(x for x in metrics["cutoffs"]["deployed"] if x["cutoff"] == cfg.cutoff)
    assert c["hits"] + c["misses"] == metrics["nightly"]["deployed"]["positives"]
    assert c["hits"] + c["misses"] + c["false_alarms"] + c["correct_negatives"] == metrics["record"]["nights_scored"]
    assert len(metrics["events"]) == metrics["nightly"]["deployed"]["positives"]
    # the synthetic signal is learnable: the model beats climatology on average precision
    assert metrics["nightly"]["deployed"]["average_precision"] > metrics["nightly"]["deployed"]["base_rate"]
    page = bt.render_html(metrics, preds, cfg, generated_at="test")
    assert page.startswith("<!doctype html>")
    for marker in ("Overall skill", "By station", "Choosing a cutoff", "Every event night", "data:image/png;base64,"):
        assert marker in page
    import json
    json.dumps(metrics, default=str)  # serialisable for the S3 sidecar


def test_perfect_and_empty_edge_cases(cfg):
    y = np.array([0, 1, 0, 1]); p = np.array([0.1, 0.9, 0.2, 0.8])
    m = bt._prob_metrics(y, p)
    assert m["auc"] == 1.0 and m["average_precision"] == 1.0
    assert bt._prob_metrics(np.array([0, 0]), np.array([0.1, 0.2]))["auc"] is None
    strict = bt.BacktestConfig(threshold=30, n_folds=2, n_estimators=5, min_train_positives=10_000)
    empty = bt.simulate(bt.prepare_hourly(_synthetic_record(days=10), strict), strict)
    assert empty.empty  # every fold skipped for too few training positives
