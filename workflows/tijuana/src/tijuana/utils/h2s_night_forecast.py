"""Serve the night-target H2S model: tonight's chance that a monitor's night peak passes the threshold.

The night-target model is the one the backtest found worth building on
(``h2s_backtest``, variant ``night``; docs/tj_prediction_redo/README.md): one
row per station per astronomical night, trained on the night's meteorology,
flow, SBIWTP and tide, the previous nights' peaks and the H2S history that
exists at the afternoon issue. Serving uses exactly the same feature code:

- the training rows are ``h2s_backtest.prepare_nightly`` over the whole record;
- tonight's meteorology, flow, SBIWTP and tide come from the forecast inference
  data reframed onto the astronomical day (``modeldata_forecast_15min_astronomical_day``),
  reduced to the night with the same ``night_analysis.summarize_nights`` that
  builds the training table, on the on-the-hour rows so hour counts match;
- the previous nights' peaks repeat ``prepare_nightly``'s shifts on the same
  filtered nightly table, and the ``hist_*`` features come from
  ``h2s_backtest.last_known_state`` at the issue time.

The one difference from the backtest is the one that matters operationally:
here the night's weather is a forecast, there it was observed.

Pure functions only; no Dagster, no S3.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from . import h2s_backtest as bt
from .night_analysis import summarize_nights

#: Warning cutoffs reported with each forecast, as compared in the review.
REVIEW_CUTOFFS = (0.3, 0.4, 0.5)


def train(nightly_rows: pd.DataFrame, cfg: bt.BacktestConfig):
    """Fit the night-target model on every prepared station-night."""
    if nightly_rows.empty or nightly_rows["y_night"].nunique() < 2:
        raise ValueError("night-target training needs both event and non-event nights")
    return bt._fit(nightly_rows, nightly_rows["y_night"].to_numpy(), list(cfg.features_night), cfg)


def night_of(issue: pd.Timestamp) -> pd.Timestamp:
    """The astronomical night a forecast issued at ``issue`` covers: the coming night,
    named by the date of its sunset. An issue before 05:00 local covers the night in
    progress; any later issue, from the morning run on, covers the night that follows."""
    local = issue.tz_convert("America/Los_Angeles")
    day = local.normalize().tz_localize(None)
    return day - pd.Timedelta(days=1) if local.hour < 5 else day


def forecast_night_features(forecast: pd.DataFrame, astro_day: pd.Timestamp, stations) -> pd.DataFrame:
    """The ``NIGHT_SUMMARY_FEATURES`` (plus wind direction) for one night from forecast data."""
    f = forecast.copy()
    f["time"] = pd.to_datetime(f["time"])
    f = f[f["time"].dt.minute == 0]  # hourly, as the training record is
    f["astro_day_date"] = pd.to_datetime(f["astro_day_date"]).dt.tz_localize(None).dt.normalize()
    f = f[(f["astro_day_date"] == astro_day) & f["site_name"].isin(stations)]
    if "H2S" not in f.columns:
        f["H2S"] = np.nan
    s = summarize_nights(f)
    if s.empty:
        return s
    keep = ["site_name", "night_start"] + list(bt.NIGHT_SUMMARY_FEATURES) + ["wind_resultant_direction"]
    return s[[c for c in keep if c in s.columns]]


def recent_night_peaks(recent: pd.DataFrame, issue: pd.Timestamp) -> pd.DataFrame:
    """Night peaks from the latest monitor readings, for nights the nightly table has not reached.

    ``recent`` carries ``site_name``, ``time``, ``H2S`` and the astronomical frame
    (``astro_day_date``, ``day_night``, ``night_length_hours``, ``astro_day_end`` or
    ``hours_to_sunrise``); only nights that ended before ``issue`` are returned, with the
    same coverage measure the nightly summary uses.
    """
    r = recent[(recent["day_night"] == "night") & recent["H2S"].notna()].copy()
    if r.empty:
        return pd.DataFrame(columns=["site_name", "astro_day_date", "h2s_max", "h2s_coverage", "astro_day_complete"])
    r["astro_day_date"] = pd.to_datetime(r["astro_day_date"]).dt.tz_localize(None).dt.normalize()
    g = r.groupby(["site_name", "astro_day_date"])
    out = g.agg(h2s_max=("H2S", "max"), n=("H2S", "size"), night_length_hours=("night_length_hours", "first"),
                last_time=("time", "max")).reset_index()
    out["h2s_coverage"] = out["n"] / out["night_length_hours"]
    # a night is over by the next morning: anything dated before the issue's own night has ended
    out["astro_day_complete"] = out["astro_day_date"] < night_of(issue)
    return out[["site_name", "astro_day_date", "h2s_max", "h2s_coverage", "astro_day_complete"]]


def previous_peaks(nightly: pd.DataFrame, astro_day: pd.Timestamp, cfg: bt.BacktestConfig,
                   recent_peaks: pd.DataFrame | None = None) -> pd.DataFrame:
    """``prev1/3/7_peak_log`` for the night after the last usable one, per station,
    from the same filtered nightly table ``prepare_nightly`` uses, extended with nights
    from the latest readings that the table has not reached yet."""
    n = nightly.copy()
    n["astro_day_date"] = pd.to_datetime(n["astro_day_date"]).dt.tz_localize(None).dt.normalize()
    if recent_peaks is not None and not recent_peaks.empty:
        last = n.groupby("site_name")["astro_day_date"].max()
        extra = recent_peaks[recent_peaks["astro_day_date"] > recent_peaks["site_name"].map(last).fillna(pd.Timestamp.min)]
        n = pd.concat([n, extra], ignore_index=True)
    n = n[n["site_name"].isin(cfg.stations) & (n["astro_day_date"] < astro_day)]
    if "astro_day_complete" in n.columns:
        n = n[n["astro_day_complete"].astype(bool)]
    if "h2s_coverage" in n.columns:
        n = n[n["h2s_coverage"] >= cfg.min_night_coverage]
    n = n[n["h2s_max"].notna()].sort_values(["site_name", "astro_day_date"])
    rows = []
    for site, g in n.groupby("site_name", sort=False):
        peaks = g["h2s_max"].to_numpy()
        rows.append({
            "site_name": site,
            "prev1_peak_log": float(np.log1p(peaks[-1])),
            "prev3_peak_log": float(np.log1p(peaks[-3:].mean())),
            "prev7_peak_log": float(np.log1p(peaks[-7:].mean())),
            "last_night_scored": g["astro_day_date"].iloc[-1],
        })
    return pd.DataFrame(rows)


def extend_with_recent(hourly: pd.DataFrame, recent: pd.DataFrame | None) -> pd.DataFrame:
    """The hourly record with any newer monitor readings appended, for the issue-time history.

    The training record is rebuilt daily and can trail the monitors by a day or
    more; the ``hist_*`` features must describe the hours just before the issue.
    """
    if recent is None or recent.empty:
        return hourly
    parts = [hourly]
    for site, r in recent.groupby("site_name"):
        last = hourly.loc[hourly["site_name"] == site, "time"].max()
        new = r[r["time"] > last] if pd.notna(last) else r
        if not new.empty:
            parts.append(new[["site_name", "time", "H2S"]].assign(H2S=new["H2S"].clip(lower=0)))
    out = pd.concat(parts, ignore_index=True).sort_values(["site_name", "time"])
    flow = "Flow (m^3/s)--Border"
    if flow in out.columns:
        out[flow] = out.groupby("site_name")[flow].ffill()
    return out.reset_index(drop=True)


def history_at_issue(hourly: pd.DataFrame, issue: pd.Timestamp, cfg: bt.BacktestConfig) -> pd.DataFrame:
    """``hist_*`` features per station from the hourly record up to the issue time."""
    rows = []
    for site, s in hourly.groupby("site_name", sort=False):
        s = s[s["time"] <= issue].sort_values("time")
        if s.empty:
            continue
        st = bt.last_known_state(s, issue, cfg)
        rows.append({"site_name": site, **{k: st[k] for k in bt.NIGHT_HISTORY_FEATURES},
                     "last_h2s_time": s["time"].iloc[-1]})
    return pd.DataFrame(rows)


def tonight_features(forecast: pd.DataFrame, nightly: pd.DataFrame, hourly: pd.DataFrame,
                     issue: pd.Timestamp, cfg: bt.BacktestConfig, recent: pd.DataFrame | None = None) -> pd.DataFrame:
    """One row per station with every ``cfg.features_night`` column for the night ``issue`` covers.

    ``recent`` (optional) holds the latest monitor readings with the astronomical frame
    attached; it fills the gap between the end of the training record and the issue.
    """
    day = night_of(issue)
    met = forecast_night_features(forecast, day, cfg.stations)
    if met.empty:
        return met
    out = met.assign(astro_day_date=day, issue_time=issue)
    out["m_sin"] = np.sin(2 * np.pi * day.month / 12)
    out["m_cos"] = np.cos(2 * np.pi * day.month / 12)
    out["wd_sin"] = np.sin(np.radians(out["wind_resultant_direction"]))
    out["wd_cos"] = np.cos(np.radians(out["wind_resultant_direction"]))
    peaks = recent_night_peaks(recent, issue) if recent is not None and not recent.empty else None
    out = out.merge(previous_peaks(nightly, day, cfg, peaks), on="site_name", how="left")
    out = out.merge(history_at_issue(extend_with_recent(hourly, recent), issue, cfg), on="site_name", how="left")
    return out


def forecast(models, features: pd.DataFrame, cfg: bt.BacktestConfig) -> pd.DataFrame:
    """Tonight's probability per station, with a warning flag at each review cutoff.

    A station missing any feature (for example, no forecast weather for it) gets
    no probability rather than a guess.
    """
    out = features.copy()
    cols = list(cfg.features_night)
    ok = out[cols].notna().all(axis=1)
    out["probability"] = np.nan
    if ok.any():
        out.loc[ok, "probability"] = bt._predict(models, out[ok], cols)
    for c in REVIEW_CUTOFFS:
        out[f"warn_{int(round(c * 100))}"] = np.where(out["probability"].notna(), out["probability"] >= c, None)
    out["threshold_ppb"] = cfg.threshold
    out["missing_features"] = [", ".join(c for c in cols if pd.isna(r[c])) for _, r in out.iterrows()]
    return out
