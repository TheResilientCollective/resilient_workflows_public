"""Walk-forward backtest of the H2S exceedance classifier, as it is issued.

The question the backtest answers is operational, not statistical: *if the
30 ppb classifier had been trained and issued every afternoon over the record,
how often would it have warned of the nights that went above 30 ppb, and how
often would it have cried wolf?*

Design
------
- **Walk-forward.** The record is split chronologically into an initial
  training block and ``n_folds`` contiguous test blocks. For each test block a
  model is fit on everything before it and then *issued* once per day at
  ``issue_hour`` local time for the next ``horizon_hours``. No fold ever trains
  on data after its own test block begins.
- **Issued as served.** The deployed model is trained on true H2S lag features
  but, at forecast time, those are synthesised from the last observation by
  exponential decay (``forecast_features.engineer_station_features``). The
  backtest reproduces that exactly (``operational_lag_features``), so the
  ``deployed`` variant scores the product as it actually runs. A second
  ``served_time`` variant drops the H2S lag features altogether. A third,
  ``history``, replaces them with what genuinely exists at issue time: the last
  reading, the 24 h and 72 h maxima, hours since the last exceedance, and the
  lead hour, all fixed at issue and used for training in exactly that form.
  A fourth, ``night``, changes the target: one row per station per
  astronomical night from ``h2s_nightly_summary`` (peak above the threshold),
  trained on the night's own meteorology, flow, SBIWTP and tide plus the same
  issue-time history and the previous nights' peaks. ``persistence`` (the 24 h
  before issue reached the threshold) and ``climatology`` (training base rate)
  are the baselines.
- **Nightly verification.** The County acts on nights, not hours. Each issue
  window is scored as an event: observed if any hour in the window exceeded the
  threshold, forecast with the window's maximum probability.
- **Perfect-prognosis weather.** Every meteorological, tide and flow feature is
  the *observed* value at the target hour, not a forecast. Skill here is
  therefore an upper bound on operational skill.

Pure functions only; no Dagster, no S3. ``simulate`` -> ``score`` ->
``render_html`` is the whole pipeline.
"""

from __future__ import annotations

import base64
import html
import io
import json
from dataclasses import asdict, dataclass, field

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
)

from .forecast_features import MODEL_FEATURES

STATIONS = ("SAN YSIDRO", "NESTOR - BES", "IB CIVIC CTR")

#: H2S history features that do not exist at forecast time and are synthesised
#: by decay from the last observation.
H2S_LAG_FEATURES = ("h2s_lag_1h", "h2s_lag_3h", "h2s_lag_6h", "h2s_rolling_6h", "h2s_rolling_24h")
FLOW_LAG_FEATURES = ("flow_lag_6h", "flow_rolling_24h")
SERVED_TIME_FEATURES = tuple(f for f in MODEL_FEATURES if f not in H2S_LAG_FEATURES)
#: H2S history as it exists at issue time, held fixed over the horizon.
HISTORY_FEATURES = ("hist_last_h2s", "hist_max_24h", "hist_mean_24h", "hist_max_72h", "hist_hours_since_exceedance", "lead_hour")
HISTORY_VARIANT_FEATURES = SERVED_TIME_FEATURES + HISTORY_FEATURES

#: Night-target model: columns of h2s_nightly_summary used as features ...
NIGHT_SUMMARY_FEATURES = (
    "night_length_hours", "temperature_2m_mean", "wind_speed_mean", "wind_resultant_speed", "wind_steadiness",
    "wind_gust_max", "relative_humidity_2m_mean", "surface_pressure_mean", "tide_height_mean", "tide_height_max",
    "stable_atm_hours", "flow_mean", "flow_max", "sbiwtp_flow_mgd_mean",
)
#: ... plus what is derived at issue time: season, wind direction, previous nights, hourly history.
NIGHT_DERIVED_FEATURES = ("m_sin", "m_cos", "wd_sin", "wd_cos", "prev1_peak_log", "prev3_peak_log", "prev7_peak_log")
NIGHT_HISTORY_FEATURES = tuple(f for f in HISTORY_FEATURES if f != "lead_hour")
NIGHT_FEATURES = NIGHT_SUMMARY_FEATURES + NIGHT_DERIVED_FEATURES + NIGHT_HISTORY_FEATURES

MODEL_VARIANTS = ("deployed", "served_time", "history", "night")
VARIANTS = MODEL_VARIANTS + ("persistence", "climatology")
VARIANT_LABELS = {
    "deployed": "deployed model (decayed H2S lags)",
    "served_time": "served-time model (no H2S history)",
    "history": "served-time model + issue-time history",
    "night": "night-target model (nightly summary + history)",
    "persistence": "persistence (24 h before issue reached threshold)",
    "climatology": "climatology (training base rate)",
}
LEAD_BUCKETS = ((1, 6), (7, 12), (13, 24))
CUTOFFS = (0.2, 0.3, 0.4, 0.5, 0.6, 0.7)


@dataclass(frozen=True)
class BacktestConfig:
    threshold: float = 30.0
    #: local hour at which the evening forecast is issued
    issue_hour: int = 15
    #: hours covered by each issue: 16:00 through 09:00 next morning by default
    horizon_hours: int = 18
    n_folds: int = 4
    initial_train_fraction: float = 0.5
    seeds: tuple = (42,)
    n_estimators: int = 300
    max_depth: int = 20
    min_samples_leaf: int = 5
    stations: tuple = STATIONS
    #: a fold is skipped when its training block has fewer positives than this
    min_train_positives: int = 20
    #: operating cutoff reported in the headline; the cutoff table shows the rest
    cutoff: float = 0.5
    features_deployed: tuple = tuple(MODEL_FEATURES)
    features_served: tuple = SERVED_TIME_FEATURES
    features_history: tuple = HISTORY_VARIANT_FEATURES
    features_night: tuple = NIGHT_FEATURES
    #: nightly rows need at least this share of their hours measured to train on
    min_night_coverage: float = 0.5
    #: cap on hours since the last exceedance, so a long quiet spell saturates
    max_hours_since_exceedance: float = 240.0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["features_deployed"] = list(self.features_deployed)
        d["features_served"] = list(self.features_served)
        d["features_history"] = list(self.features_history)
        d["features_night"] = list(self.features_night)
        d["seeds"] = list(self.seeds)
        d["stations"] = list(self.stations)
        return d


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------

def prepare_hourly(df: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    """Measured hourly rows with the lag features recomputed from the measured record.

    Mirrors ``train_models_auto.prepare_data``: only measured H2S, clipped at
    zero, lags and rolling means built per station from the retained rows, and
    rows with any missing feature dropped.
    """
    d = df.copy()
    d["time"] = pd.to_datetime(d["time"])
    if d["time"].dt.tz is None:
        d["time"] = d["time"].dt.tz_localize("America/Los_Angeles")
    if "h2s_measured" in d.columns:
        d = d[d["h2s_measured"].astype(bool)]
    d = d[d["H2S"].notna() & d["site_name"].isin(cfg.stations)].copy()
    d["H2S"] = d["H2S"].clip(lower=0)
    d = d.sort_values(["site_name", "time"]).reset_index(drop=True)
    flow = "Flow (m^3/s)--Border"
    parts = []
    for _, s in d.groupby("site_name", sort=False):
        s = s.copy()
        s["h2s_lag_1h"] = s["H2S"].shift(1)
        s["h2s_lag_3h"] = s["H2S"].shift(3)
        s["h2s_lag_6h"] = s["H2S"].shift(6)
        s["h2s_rolling_6h"] = s["H2S"].rolling(6, min_periods=1).mean()
        s["h2s_rolling_24h"] = s["H2S"].rolling(24, min_periods=1).mean()
        if flow in s.columns:
            s["flow_lag_6h"] = s[flow].shift(6)
            s["flow_rolling_24h"] = s[flow].rolling(24, min_periods=1).mean()
        parts.append(s)
    d = pd.concat(parts, ignore_index=True)
    needed = list(dict.fromkeys(list(cfg.features_deployed) + [flow]))
    missing = [c for c in needed if c not in d.columns]
    if missing:
        raise ValueError(f"backtest input is missing feature columns: {missing}")
    d = d.dropna(subset=needed).reset_index(drop=True)
    d["y"] = (d["H2S"] > cfg.threshold).astype(int)
    return d


# ---------------------------------------------------------------------------
# Operational feature synthesis (parity with forecast_features)
# ---------------------------------------------------------------------------

def last_known_state(history: pd.DataFrame, issue=None, cfg: BacktestConfig | None = None) -> dict:
    """What ``forecast_features.get_last_known_state`` would see at issue time,
    plus the issue-time history features (``hist_*``) when ``issue`` and ``cfg`` are given."""
    flow = "Flow (m^3/s)--Border"
    h2s = history["H2S"]
    state = {
        "h2s": float(h2s.iloc[-1]),
        "h2s_6h": float(h2s.tail(6).mean()),
        "h2s_24h": float(h2s.tail(24).mean()),
        "flow": float(history[flow].iloc[-1]),
        "flow_24h": float(history[flow].tail(24).mean()),
    }
    if issue is not None and cfg is not None:
        t = history["time"]
        last24 = h2s[t > issue - pd.Timedelta(hours=24)]
        last72 = h2s[t > issue - pd.Timedelta(hours=72)]
        over = t[h2s > cfg.threshold]
        since = (issue - over.iloc[-1]) / pd.Timedelta(hours=1) if len(over) else cfg.max_hours_since_exceedance
        state.update({
            "hist_last_h2s": state["h2s"],
            "hist_max_24h": float(last24.max()) if len(last24) else state["h2s"],
            "hist_mean_24h": float(last24.mean()) if len(last24) else state["h2s"],
            "hist_max_72h": float(last72.max()) if len(last72) else state["h2s"],
            "hist_hours_since_exceedance": float(min(since, cfg.max_hours_since_exceedance)),
        })
    return state


def _issue_times(s: pd.DataFrame, cfg: BacktestConfig) -> list:
    """Wall-clock issue times, one per calendar day in the record (DST-safe)."""
    issue_td = pd.Timedelta(hours=cfg.issue_hour)
    out = []
    for day in sorted(set(s["time"].dt.normalize())):
        out.append((day.tz_localize(None) + issue_td).tz_localize(day.tz))
    return out


def issue_states(s: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    """One row per issue time with the last-known state at that moment.

    ``hist_end`` is the number of record rows at or before the issue, so a
    caller can slice the history without recomputing the search.
    """
    times = s["time"].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
    rows = []
    for issue in _issue_times(s, cfg):
        hist_end = int(np.searchsorted(times, np.datetime64(issue.tz_convert("UTC").tz_localize(None)), side="right"))
        if hist_end == 0:
            continue
        st = last_known_state(s.iloc[:hist_end], issue, cfg)
        st["issue_time"] = issue
        st["hist_end"] = hist_end
        rows.append(st)
    return pd.DataFrame(rows)


def attach_issue_history(s: pd.DataFrame, states: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    """Training view for the history variant: each row gets the state of the most
    recent issue before it and its lead hour; rows outside the horizon are dropped."""
    if states.empty:
        return s.iloc[0:0].assign(**{c: np.nan for c in HISTORY_FEATURES})
    st = states.sort_values("issue_time")[["issue_time"] + [c for c in HISTORY_FEATURES if c != "lead_hour"]]
    merged = pd.merge_asof(s.sort_values("time"), st, left_on="time", right_on="issue_time", direction="backward")
    merged = merged.dropna(subset=["issue_time"])
    merged["lead_hour"] = ((merged["time"] - merged["issue_time"]) / pd.Timedelta(hours=1)).round().astype(int)
    return merged[(merged["lead_hour"] >= 1) & (merged["lead_hour"] <= cfg.horizon_hours)]


def prepare_nightly(nightly: pd.DataFrame, hourly: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    """Night-target training rows from ``h2s_nightly_summary`` plus issue-time history.

    One row per station per astronomical night with ``issue_time`` (the
    configured hour on the night's date), ``y_night`` (peak above threshold) and
    every column in ``cfg.features_night``. Previous-night features come from the
    nightly table itself; the ``hist_*`` features from the hourly record at the
    issue time, so nothing after the issue leaks in. Nights that are truncated or
    under-measured are dropped.
    """
    n = nightly.copy()
    n["astro_day_date"] = pd.to_datetime(n["astro_day_date"]).dt.tz_localize(None).dt.normalize()
    n = n[n["site_name"].isin(cfg.stations)]
    if "astro_day_complete" in n.columns:
        n = n[n["astro_day_complete"].astype(bool)]
    if "h2s_coverage" in n.columns:
        n = n[n["h2s_coverage"] >= cfg.min_night_coverage]
    n = n[n["h2s_max"].notna()].sort_values(["site_name", "astro_day_date"]).reset_index(drop=True)
    tz = hourly["time"].dt.tz
    n["issue_time"] = (n["astro_day_date"] + pd.Timedelta(hours=cfg.issue_hour)).dt.tz_localize(tz)
    n["y_night"] = (n["h2s_max"] > cfg.threshold).astype(int)
    n["m_sin"] = np.sin(2 * np.pi * n["astro_day_date"].dt.month / 12)
    n["m_cos"] = np.cos(2 * np.pi * n["astro_day_date"].dt.month / 12)
    n["wd_sin"] = np.sin(np.radians(n["wind_resultant_direction"]))
    n["wd_cos"] = np.cos(np.radians(n["wind_resultant_direction"]))
    g = n.groupby("site_name", sort=False)["h2s_max"]
    n["prev1_peak_log"] = np.log1p(g.shift(1))
    n["prev3_peak_log"] = np.log1p(g.shift(1).rolling(3, min_periods=1).mean().reset_index(level=0, drop=True))
    n["prev7_peak_log"] = np.log1p(g.shift(1).rolling(7, min_periods=1).mean().reset_index(level=0, drop=True))
    parts = []
    for site, ns in n.groupby("site_name", sort=False):
        s = hourly[hourly["site_name"] == site].sort_values("time").reset_index(drop=True)
        if s.empty:
            continue
        st = issue_states(s, cfg)[["issue_time"] + list(NIGHT_HISTORY_FEATURES)]
        parts.append(ns.merge(st, on="issue_time", how="inner"))
    if not parts:
        return n.iloc[0:0]
    out = pd.concat(parts, ignore_index=True)
    missing = [c for c in cfg.features_night if c not in out.columns]
    if missing:
        raise ValueError(f"nightly summary is missing feature columns: {missing}")
    return out.dropna(subset=list(cfg.features_night)).sort_values(["site_name", "issue_time"]).reset_index(drop=True)


def operational_lag_features(state: dict, leads: np.ndarray) -> dict[str, np.ndarray]:
    """The lag features the forecast path synthesises, for 1-based lead hours.

    Reproduces ``engineer_station_features`` exactly: with ``h = lead - 1``,
    ``h2s_lag_1h = last * exp(-h/12)`` shifted by one, the 3 h and 6 h lags held
    at the last value for the first 3 and 6 hours, the rolling means decayed with
    12 h and 36 h e-folding, and the flow lags held at the last observation.
    """
    leads = np.asarray(leads, dtype=int)
    h = leads - 1
    n = int(h.max()) + 1 if len(h) else 0
    hh = np.arange(n)
    fast = np.exp(-hh / 12)
    slow = np.exp(-hh / 36)
    lh = state["h2s"]
    lag1 = np.concatenate([[lh], lh * fast[:-1]]) if n else np.array([])
    lag3 = np.concatenate([[lh] * min(3, n), (lh * fast)[: max(n - 3, 0)]])
    lag6 = np.concatenate([[lh] * min(6, n), (lh * fast)[: max(n - 6, 0)]])
    return {
        "h2s_lag_1h": lag1[h],
        "h2s_lag_3h": lag3[h],
        "h2s_lag_6h": lag6[h],
        "h2s_rolling_6h": state["h2s_6h"] * fast[h],
        "h2s_rolling_24h": state["h2s_24h"] * slow[h],
        "flow_lag_6h": np.full(len(h), state["flow"]),
        "flow_rolling_24h": np.full(len(h), state["flow_24h"]),
    }


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def _folds(n_rows: int, cfg: BacktestConfig):
    start = int(n_rows * cfg.initial_train_fraction)
    block = max((n_rows - start) // cfg.n_folds, 1)
    for i in range(cfg.n_folds):
        s = start + i * block
        e = s + block if i < cfg.n_folds - 1 else n_rows
        if s < e:
            yield i, s, e


def _fit(X, y, features, cfg: BacktestConfig):
    models = []
    for seed in cfg.seeds:
        rf = RandomForestClassifier(
            n_estimators=cfg.n_estimators,
            max_depth=cfg.max_depth,
            min_samples_leaf=cfg.min_samples_leaf,
            max_features="sqrt",
            class_weight="balanced",
            n_jobs=-1,
            random_state=seed,
        )
        rf.fit(X[features].to_numpy(), y)
        models.append(rf)
    return models


def _predict(models, X, features) -> np.ndarray:
    p = np.zeros(len(X))
    for m in models:
        p += m.predict_proba(X[features].to_numpy())[:, 1]
    return p / len(models)


def simulate(hourly: pd.DataFrame, cfg: BacktestConfig, nightly: pd.DataFrame | None = None) -> pd.DataFrame:
    """Issue the classifier walk-forward and return one row per forecast hour.

    Columns: ``site_name, fold, issue_time, target_time, lead_hour, H2S, y,
    last_h2s, p_<variant>`` for every variant. ``nightly`` is the output of
    :func:`prepare_nightly`; when given, the night-target model is trained on
    the nights before each fold's test block (the same boundary the hourly
    variants use) and its probability is attached to every hour of the night.
    ``night_model_used`` records where a night had no nightly row and the
    history variant stood in.
    """
    out = []
    for site in cfg.stations:
        s = hourly[hourly["site_name"] == site].sort_values("time").reset_index(drop=True)
        if s.empty:
            continue
        site_nights = nightly[nightly["site_name"] == site] if nightly is not None else None
        # naive UTC datetime64 so searchsorted works against tz-aware issue times
        times = s["time"].dt.tz_convert("UTC").dt.tz_localize(None).to_numpy()
        states = issue_states(s, cfg)
        for fold, start, end in _folds(len(s), cfg):
            train = s.iloc[:start]
            if train["y"].sum() < cfg.min_train_positives:
                continue
            deployed = _fit(train, train["y"].to_numpy(), list(cfg.features_deployed), cfg)
            served = _fit(train, train["y"].to_numpy(), list(cfg.features_served), cfg)
            train_hist = attach_issue_history(train, states[states["hist_end"] <= start], cfg)
            history = _fit(train_hist, train_hist["y"].to_numpy(), list(cfg.features_history), cfg) if train_hist["y"].sum() >= cfg.min_train_positives else None
            p_clim = float(train["y"].mean())
            night_p: dict = {}
            if site_nights is not None and not site_nights.empty:
                fold_start = s["time"].iloc[start]
                train_n = site_nights[site_nights["issue_time"] < fold_start]
                if train_n["y_night"].sum() >= min(cfg.min_train_positives, 10):
                    night_models = _fit(train_n, train_n["y_night"].to_numpy(), list(cfg.features_night), cfg)
                    test_n = site_nights[site_nights["issue_time"] >= fold_start]
                    if not test_n.empty:
                        night_p = dict(zip(test_n["issue_time"], _predict(night_models, test_n, list(cfg.features_night))))

            test = s.iloc[start:end]
            # an issue belongs to exactly one fold: the one whose test block contains the issue time
            block_start = test["time"].iloc[0]
            block_end = s["time"].iloc[end] if end < len(s) else None
            rows = []
            for st in states.itertuples(index=False):
                issue = st.issue_time
                if issue < block_start or (block_end is not None and issue >= block_end):
                    continue
                hist_end = int(st.hist_end)
                state = st._asdict()
                tgt_lo = hist_end
                horizon_end = issue + pd.Timedelta(hours=cfg.horizon_hours)
                tgt_hi = int(np.searchsorted(times, np.datetime64(horizon_end.tz_convert("UTC").tz_localize(None)), side="right"))
                tgt = s.iloc[tgt_lo:tgt_hi]
                tgt = tgt[(tgt.index >= start)]  # never score a fold on its own training rows (targets may run into the next block)
                if tgt.empty:
                    continue
                lead = ((tgt["time"] - issue) / pd.Timedelta(hours=1)).round().astype(int).to_numpy()
                keep = (lead >= 1) & (lead <= cfg.horizon_hours)
                tgt, lead = tgt[keep], lead[keep]
                if tgt.empty:
                    continue
                X = tgt.copy()
                for col, vals in operational_lag_features(state, lead).items():
                    X[col] = vals
                for col in HISTORY_FEATURES:
                    X[col] = lead if col == "lead_hour" else state[col]
                rows.append(
                    pd.DataFrame(
                        {
                            "site_name": site,
                            "fold": fold,
                            "issue_time": issue,
                            "target_time": tgt["time"].to_numpy(),
                            "lead_hour": lead,
                            "H2S": tgt["H2S"].to_numpy(),
                            "y": tgt["y"].to_numpy(),
                            "last_h2s": state["h2s"],
                            "p_deployed": _predict(deployed, X, list(cfg.features_deployed)),
                            "p_served_time": _predict(served, X, list(cfg.features_served)),
                            "p_history": _predict(history, X, list(cfg.features_history)) if history is not None else np.nan,
                            "p_night": night_p.get(issue, np.nan),
                            "p_persistence": float(state["hist_max_24h"] > cfg.threshold),
                            "p_climatology": p_clim,
                        }
                    )
                )
            if rows:
                out.append(pd.concat(rows, ignore_index=True))
    if not out:
        return pd.DataFrame(columns=["site_name", "fold", "issue_time", "target_time", "lead_hour", "H2S", "y", "last_h2s"] + [f"p_{v}" for v in VARIANTS])
    preds = pd.concat(out, ignore_index=True)
    preds["issue_time"] = pd.to_datetime(preds["issue_time"])
    preds["target_time"] = pd.to_datetime(preds["target_time"])
    # a fold without a history model falls back to the served-time model so every variant is scored
    preds["p_history"] = preds["p_history"].fillna(preds["p_served_time"])
    # nights without a usable nightly row (or without a trained night model) fall back to the history variant
    preds["night_model_used"] = preds["p_night"].notna()
    preds["p_night"] = preds["p_night"].fillna(preds["p_history"])
    return preds


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def nightly_events(preds: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    """One row per station × issue: observed event and maximum probability per variant."""
    agg = {"H2S": "max", "y": "max", "lead_hour": "size", "last_h2s": "first"}
    for v in VARIANTS:
        agg[f"p_{v}"] = "max"
    g = preds.groupby(["site_name", "issue_time"], sort=True).agg(agg).reset_index()
    g = g.rename(columns={"H2S": "peak_h2s", "lead_hour": "hours_scored", "y": "event"})
    g["peak_time"] = preds.loc[preds.groupby(["site_name", "issue_time"])["H2S"].idxmax(), "target_time"].to_numpy()
    g["night_of"] = g["issue_time"].dt.date
    return g


def _safe_auc(y, p):
    y = np.asarray(y)
    if len(y) < 2 or y.min() == y.max():
        return None
    return float(roc_auc_score(y, p))


def _prob_metrics(y, p) -> dict:
    y = np.asarray(y, dtype=int)
    p = np.asarray(p, dtype=float)
    if len(y) == 0:
        return {"n": 0, "positives": 0}
    base = float(y.mean())
    out = {"n": int(len(y)), "positives": int(y.sum()), "base_rate": base, "auc": _safe_auc(y, p)}
    if 0 < y.sum() < len(y):
        out["average_precision"] = float(average_precision_score(y, p))
        bs = float(brier_score_loss(y, p))
        bs_clim = float(brier_score_loss(y, np.full(len(y), base)))
        out["brier"] = bs
        out["brier_skill_score"] = float(1 - bs / bs_clim) if bs_clim > 0 else None
    return out


def _contingency(y, p, cutoff: float) -> dict:
    y = np.asarray(y, dtype=int)
    f = np.asarray(p) >= cutoff
    hits = int((f & (y == 1)).sum())
    misses = int((~f & (y == 1)).sum())
    false_alarms = int((f & (y == 0)).sum())
    correct_negatives = int((~f & (y == 0)).sum())
    pod = hits / (hits + misses) if hits + misses else None
    far = false_alarms / (hits + false_alarms) if hits + false_alarms else None
    csi = hits / (hits + misses + false_alarms) if hits + misses + false_alarms else None
    pofd = false_alarms / (false_alarms + correct_negatives) if false_alarms + correct_negatives else None
    return {
        "cutoff": cutoff, "hits": hits, "misses": misses, "false_alarms": false_alarms,
        "correct_negatives": correct_negatives, "pod": pod, "far": far, "csi": csi, "pofd": pofd,
        "alerts": hits + false_alarms,
    }


def _reliability(y, p, bins: int = 10) -> list[dict]:
    y = np.asarray(y, dtype=int)
    p = np.asarray(p, dtype=float)
    edges = np.linspace(0, 1, bins + 1)
    idx = np.clip(np.digitize(p, edges[1:-1]), 0, bins - 1)
    rows = []
    for b in range(bins):
        m = idx == b
        if m.any():
            rows.append({"bin_lo": float(edges[b]), "bin_hi": float(edges[b + 1]), "n": int(m.sum()),
                         "mean_forecast": float(p[m].mean()), "observed_frequency": float(y[m].mean())})
    return rows


def score(preds: pd.DataFrame, cfg: BacktestConfig) -> dict:
    """Everything the evaluation page reports, as plain JSON-serialisable data."""
    nights = nightly_events(preds, cfg)
    result = {
        "config": cfg.to_dict(),
        "record": {
            "hours_scored": int(len(preds)),
            "nights_scored": int(len(nights)),
            "first_issue": str(preds["issue_time"].min()) if len(preds) else None,
            "last_issue": str(preds["issue_time"].max()) if len(preds) else None,
            "folds": sorted(int(f) for f in preds["fold"].unique()) if len(preds) else [],
            "night_model_share": float(preds["night_model_used"].mean()) if "night_model_used" in preds and len(preds) else 0.0,
        },
        "hourly": {}, "nightly": {}, "by_station": {}, "by_lead": {}, "cutoffs": {},
        "reliability": {}, "monthly": {}, "events": [],
    }
    for v in VARIANTS:
        result["hourly"][v] = _prob_metrics(preds["y"], preds[f"p_{v}"])
        result["nightly"][v] = _prob_metrics(nights["event"], nights[f"p_{v}"])
        result["reliability"][v] = _reliability(nights["event"], nights[f"p_{v}"])
        result["cutoffs"][v] = [_contingency(nights["event"], nights[f"p_{v}"], c) for c in CUTOFFS]
    for site, g in nights.groupby("site_name", sort=False):
        result["by_station"][site] = {
            v: {**_prob_metrics(g["event"], g[f"p_{v}"]), "at_cutoff": _contingency(g["event"], g[f"p_{v}"], cfg.cutoff)}
            for v in VARIANTS
        }
    for lo, hi in LEAD_BUCKETS:
        m = (preds["lead_hour"] >= lo) & (preds["lead_hour"] <= hi)
        if m.any():
            result["by_lead"][f"{lo}-{hi}h"] = {v: _prob_metrics(preds.loc[m, "y"], preds.loc[m, f"p_{v}"]) for v in VARIANTS}
    # month-by-month event count and what the deployed/served model did at the cutoff
    nights["month"] = pd.to_datetime(nights["issue_time"]).dt.strftime("%Y-%m")
    for month, g in nights.groupby("month", sort=True):
        result["monthly"][month] = {
            "nights": int(len(g)), "events": int(g["event"].sum()),
            **{v: _contingency(g["event"], g[f"p_{v}"], cfg.cutoff) for v in MODEL_VARIANTS + ("persistence",)},
        }
    ev = nights[nights["event"] == 1].sort_values(["issue_time", "site_name"])
    for _, r in ev.iterrows():
        result["events"].append({
            "site_name": r["site_name"], "night_of": str(r["night_of"]), "peak_h2s": float(r["peak_h2s"]),
            "peak_time": str(r["peak_time"]), "last_h2s_at_issue": float(r["last_h2s"]),
            **{f"p_{v}": float(r[f"p_{v}"]) for v in VARIANTS},
            **{f"warned_{v}": bool(r[f"p_{v}"] >= cfg.cutoff) for v in MODEL_VARIANTS},
        })
    return result


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

_COLORS = {"deployed": "#2a78d6", "served_time": "#eb6834", "history": "#4a3aa7", "night": "#e87ba4", "persistence": "#1baf7a", "climatology": "#898781"}


def _png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    import matplotlib.pyplot as plt

    plt.close(fig)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _figures(metrics: dict, preds: pd.DataFrame, cfg: BacktestConfig) -> dict[str, str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figs = {}
    nights = nightly_events(preds, cfg)
    y = nights["event"].to_numpy()

    # 1. precision-recall, nightly
    fig, ax = plt.subplots(figsize=(5.2, 4))
    for v in MODEL_VARIANTS:
        if 0 < y.sum() < len(y):
            pr, rc, _ = precision_recall_curve(y, nights[f"p_{v}"])
            ax.plot(rc, pr, lw=2, color=_COLORS[v], label=VARIANT_LABELS[v])
    if 0 < y.sum() < len(y):
        c = _contingency(y, nights["p_persistence"], 0.5)
        if c["pod"] is not None and c["far"] is not None:
            ax.plot([c["pod"]], [1 - c["far"]], "o", ms=8, color=_COLORS["persistence"], label=VARIANT_LABELS["persistence"])
    ax.axhline(y.mean() if len(y) else 0, color=_COLORS["climatology"], ls="--", lw=1, label="base rate")
    ax.set_xlabel("recall (share of event nights warned)")
    ax.set_ylabel("precision (share of warnings that verified)")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1.02)
    ax.set_title(f"Nightly events above {cfg.threshold:g} ppb")
    ax.legend(fontsize=8, loc="upper right"); ax.grid(alpha=0.25)
    figs["pr"] = _png(fig)

    # 2. reliability
    fig, ax = plt.subplots(figsize=(5.2, 4))
    ax.plot([0, 1], [0, 1], color="#c3c2b7", lw=1)
    for v in MODEL_VARIANTS:
        r = metrics["reliability"].get(v, [])
        if r:
            ax.plot([b["mean_forecast"] for b in r], [b["observed_frequency"] for b in r], "o-", lw=2, ms=5, color=_COLORS[v], label=VARIANT_LABELS[v])
            for b in r:
                ax.annotate(str(b["n"]), (b["mean_forecast"], b["observed_frequency"]), textcoords="offset points", xytext=(4, 4), fontsize=7, color="#52514e")
    ax.set_xlabel("forecast probability (night maximum)"); ax.set_ylabel("observed frequency")
    ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.set_title("Reliability (labels: nights per bin)")
    ax.legend(fontsize=8, loc="upper left"); ax.grid(alpha=0.25)
    figs["reliability"] = _png(fig)

    # 3. skill by lead
    fig, ax = plt.subplots(figsize=(5.2, 3.6))
    leads = list(metrics["by_lead"].keys())
    for v in MODEL_VARIANTS + ("persistence",):
        vals = [metrics["by_lead"][k][v].get("average_precision") for k in leads]
        ax.plot(leads, [np.nan if x is None else x for x in vals], "o-", lw=2, color=_COLORS[v], label=VARIANT_LABELS[v])
    base = [metrics["by_lead"][k]["climatology"].get("base_rate") for k in leads]
    ax.plot(leads, [np.nan if x is None else x for x in base], "--", color=_COLORS["climatology"], label="base rate")
    ax.set_ylim(0, 1); ax.set_ylabel("average precision (hourly)"); ax.set_xlabel("lead time after the afternoon issue")
    ax.set_title("Hourly skill by lead time"); ax.legend(fontsize=8); ax.grid(alpha=0.25)
    figs["lead"] = _png(fig)

    # 4. monthly hits / misses / false alarms for the deployed model
    months = list(metrics["monthly"].keys())
    fig, ax = plt.subplots(figsize=(max(6, 0.32 * len(months)), 3.6))
    hits = [metrics["monthly"][m]["deployed"]["hits"] for m in months]
    misses = [metrics["monthly"][m]["deployed"]["misses"] for m in months]
    fa = [metrics["monthly"][m]["deployed"]["false_alarms"] for m in months]
    x = np.arange(len(months))
    ax.bar(x, hits, color="#2a78d6", label="hits")
    ax.bar(x, misses, bottom=hits, color="#d03b3b", label="misses")
    ax.bar(x, fa, bottom=np.add(hits, misses), color="#fab219", label="false alarms")
    ax.set_xticks(x); ax.set_xticklabels(months, rotation=60, fontsize=7)
    ax.set_ylabel("nights"); ax.set_title(f"Deployed model at cutoff {cfg.cutoff:g}, by month"); ax.legend(fontsize=8)
    figs["monthly"] = _png(fig)
    return figs


def _fmt(x, nd=2):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return "–"
    if isinstance(x, float):
        return f"{x:.{nd}f}"
    return str(x)


def render_html(metrics: dict, preds: pd.DataFrame, cfg: BacktestConfig, generated_at: str | None = None) -> str:
    """A self-contained evaluation page (inline CSS and base64 figures)."""
    figs = _figures(metrics, preds, cfg) if len(preds) else {}
    t = cfg.threshold
    rec = metrics["record"]
    gen = generated_at or pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC")
    esc = html.escape

    def row(cells, header=False):
        tag = "th" if header else "td"
        return "<tr>" + "".join(f"<{tag}>{esc(str(c))}</{tag}>" for c in cells) + "</tr>"

    # headline per station at the operating cutoff
    station_rows = [row(["station", "nights", "event nights", "variant", "AUC", "avg precision", "Brier skill", "warned", "hits", "misses", "false alarms", "POD", "FAR"], True)]
    for site, d in metrics["by_station"].items():
        for v in MODEL_VARIANTS + ("persistence",):
            m = d[v]; c = m["at_cutoff"]
            station_rows.append(row([site, m["n"], m["positives"], VARIANT_LABELS[v], _fmt(m.get("auc")), _fmt(m.get("average_precision")),
                                     _fmt(m.get("brier_skill_score")), c["alerts"], c["hits"], c["misses"], c["false_alarms"], _fmt(c["pod"]), _fmt(c["far"])]))
    cutoff_rows = [row(["cutoff", "variant", "warnings", "hits", "misses", "false alarms", "POD (recall)", "FAR", "CSI"], True)]
    for c in CUTOFFS:
        for v in MODEL_VARIANTS:
            k = next(x for x in metrics["cutoffs"][v] if x["cutoff"] == c)
            cutoff_rows.append(row([f"{c:.1f}", VARIANT_LABELS[v], k["alerts"], k["hits"], k["misses"], k["false_alarms"], _fmt(k["pod"]), _fmt(k["far"]), _fmt(k["csi"])]))
    overall_rows = [row(["variant", "nightly AUC", "nightly avg precision", "nightly Brier skill", "hourly AUC", "hourly avg precision"], True)]
    for v in VARIANTS:
        n = metrics["nightly"][v]; h = metrics["hourly"][v]
        overall_rows.append(row([VARIANT_LABELS[v], _fmt(n.get("auc")), _fmt(n.get("average_precision")), _fmt(n.get("brier_skill_score")), _fmt(h.get("auc")), _fmt(h.get("average_precision"))]))
    event_rows = [row(["night of", "station", "peak ppb", "peak time", "H2S at issue", "P deployed", "P served-time", "P + history", "P night model", "persistence", "warned (deployed)?"], True)]
    for e in metrics["events"]:
        warned = "yes" if e["warned_deployed"] else "no"
        event_rows.append(row([e["night_of"], e["site_name"], f"{e['peak_h2s']:.0f}", e["peak_time"][5:16], f"{e['last_h2s_at_issue']:.1f}",
                               f"{e['p_deployed']:.2f}", f"{e['p_served_time']:.2f}", f"{e['p_history']:.2f}", f"{e['p_night']:.2f}", "yes" if e["p_persistence"] >= 0.5 else "no", warned]))
    n_ev = len(metrics["events"]); n_warned = sum(e["warned_deployed"] for e in metrics["events"])
    n_warned_hist = sum(e["warned_history"] for e in metrics["events"])
    n_warned_night = sum(e["warned_night"] for e in metrics["events"])
    night_c = next(x for x in metrics["cutoffs"]["night"] if x["cutoff"] == cfg.cutoff)
    night_share = metrics["record"].get("night_model_share", 0.0)
    dep = metrics["nightly"]["deployed"]; srv = metrics["nightly"]["served_time"]
    dep_c = next(x for x in metrics["cutoffs"]["deployed"] if x["cutoff"] == cfg.cutoff)
    srv_c = next(x for x in metrics["cutoffs"]["served_time"] if x["cutoff"] == cfg.cutoff)
    per_c = next(x for x in metrics["cutoffs"]["persistence"] if x["cutoff"] == 0.5)

    def img(k, alt):
        return f'<img src="{figs[k]}" alt="{esc(alt)}">' if k in figs else f"<p><em>{esc(alt)}: no figure (no scored nights)</em></p>"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>H2S {t:g} ppb backtest</title>
<style>
body{{margin:0;padding:1.5rem 20px 3rem;background:#f3f4f1;color:#151a1e;font:16px/1.5 system-ui,-apple-system,"Segoe UI",sans-serif}}
main{{max-width:1080px;margin:0 auto}} h1{{font-size:1.9rem;margin:.2rem 0 .3rem}} h2{{font-size:1.3rem;margin:2.2rem 0 .6rem;border-bottom:1px solid #d9ddd6;padding-bottom:.3rem}}
p{{max-width:72ch}} .meta{{font-family:ui-monospace,monospace;font-size:.8rem;color:#7d8791}}
.tiles{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:.8rem;margin:1rem 0}}
.tile{{background:#fff;border:1px solid #d9ddd6;border-radius:6px;padding:.8rem 1rem}} .tile b{{display:block;font-size:1.7rem;font-variant-numeric:tabular-nums}} .tile span{{font-size:.82rem;color:#52514e}}
.figs{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem}} img{{max-width:100%;background:#fff;border:1px solid #d9ddd6;border-radius:6px}}
.tw{{overflow-x:auto}} table{{border-collapse:collapse;font-size:.85rem;font-variant-numeric:tabular-nums;width:100%;background:#fff}}
th,td{{padding:.35rem .6rem;border-bottom:1px solid #e1e0d9;text-align:right;white-space:nowrap}} th{{font-size:.74rem;text-transform:uppercase;letter-spacing:.04em;color:#7d8791;font-weight:500}}
th:first-child,td:first-child{{text-align:left}} .note{{background:#fbf6e6;border-left:3px solid #a97a06;padding:.7rem 1rem;max-width:72ch}}
details summary{{cursor:pointer;color:#52514e}}
</style></head><body><main>
<div class="meta">Tijuana River H2S forecast · backtest · generated {esc(gen)}</div>
<h1>How well would the {t:g} ppb classifier have warned?</h1>
<p>The classifier was re-trained and issued every afternoon at {cfg.issue_hour:02d}:00 local for the following {cfg.horizon_hours} hours, walk-forward over {rec['nights_scored']:,} station-nights ({rec['hours_scored']:,} hours) from {esc(str(rec['first_issue'])[:10])} to {esc(str(rec['last_issue'])[:10])}. A night counts as an event if any hour in the window measured above {t:g} ppb; the forecast for the night is the highest hourly probability. Nothing in any fold's training data comes after its test block begins.</p>
<div class="tiles">
<div class="tile"><b>{n_ev}</b><span>event nights above {t:g} ppb</span></div>
<div class="tile"><b>{n_warned} / {n_ev}</b><span>warned by the deployed model at cutoff {cfg.cutoff:g}</span></div>
<div class="tile"><b>{dep_c['false_alarms']}</b><span>false alarms (deployed, cutoff {cfg.cutoff:g})</span></div>
<div class="tile"><b>{_fmt(dep.get('auc'))}</b><span>nightly AUC, deployed</span></div>
<div class="tile"><b>{n_warned_hist} / {n_ev}</b><span>warned by the served-time + history model at cutoff {cfg.cutoff:g}</span></div>
<div class="tile"><b>{n_warned_night} / {n_ev}</b><span>warned by the night-target model at cutoff {cfg.cutoff:g} ({night_c['false_alarms']} false alarms)</span></div>
<div class="tile"><b>{_fmt(metrics['nightly']['night'].get('auc'))}</b><span>nightly AUC, night-target model</span></div>
<div class="tile"><b>{_fmt(per_c['pod'])} / {_fmt(per_c['far'])}</b><span>persistence POD / FAR</span></div>
</div>
<div class="note"><p><b>Read this first.</b> Weather, tide and flow inputs are the <em>observed</em> values at each target hour, not forecasts, so every number here is an upper bound on operational skill. The "deployed" variant is the product as it runs: trained on true H2S history, issued with history decayed from the last observation. The "served-time" variant trains without any H2S history; "served-time + history" adds the history that does exist at issue time (last reading, 24 h and 72 h maxima, hours since the last exceedance, lead hour), fixed at issue and trained in that same form. The "night-target" model is trained on one row per astronomical night from the nightly summary (peak above {t:g} ppb) with the night's meteorology, flow, SBIWTP and tide, the previous nights' peaks and the same issue-time history; it covers {night_share:.0%} of scored hours, the rest falling back to the history variant. Persistence ("the 24 hours before issue reached {t:g} ppb") and climatology are the bars to clear.</p></div>

<h2>Overall skill</h2>
<div class="tw"><table>{''.join(overall_rows)}</table></div>
<div class="figs">{img('pr', 'precision-recall curve, nightly')}{img('reliability', 'reliability diagram, nightly')}</div>

<h2>By station, at cutoff {cfg.cutoff:g}</h2>
<p>POD is the share of event nights that were warned; FAR is the share of warnings that did not verify.</p>
<div class="tw"><table>{''.join(station_rows)}</table></div>

<h2>Choosing a cutoff</h2>
<p>Lowering the cutoff catches more events at the price of more false alarms. CSI (critical success index) balances the two; the County's tolerance for false alarms decides where to sit.</p>
<div class="tw"><table>{''.join(cutoff_rows)}</table></div>

<h2>By lead time and by month</h2>
<div class="figs">{img('lead', 'skill by lead time')}{img('monthly', 'monthly hits, misses, false alarms')}</div>

<h2>Every event night</h2>
<p>All {n_ev} station-nights that went above {t:g} ppb, with what the models said that afternoon.</p>
<details><summary>Show the {n_ev} nights</summary><div class="tw"><table>{''.join(event_rows)}</table></div></details>

<h2>Method</h2>
<p>Folds: initial training block of {cfg.initial_train_fraction:.0%} of each station's measured record, then {cfg.n_folds} contiguous test blocks; each fold's model is fit on everything before its block. Classifier: RandomForest, {cfg.n_estimators} trees, depth {cfg.max_depth}, balanced class weights, {len(cfg.seeds)} seed(s). Features: the production feature list for the deployed variant ({len(cfg.features_deployed)}), the same list without H2S history for the served-time variant ({len(cfg.features_served)}), that list plus the issue-time history features for the third variant ({len(cfg.features_history)}), and {len(cfg.features_night)} nightly features for the night-target model. Lags at issue time are synthesised exactly as <code>forecast_features.engineer_station_features</code> does. Folds whose training block has fewer than {cfg.min_train_positives} exceedance hours are skipped.</p>
<details><summary>Configuration</summary><pre>{esc(json.dumps(cfg.to_dict(), indent=2))}</pre></details>
</main></body></html>
"""
