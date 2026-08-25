#!/usr/bin/env python3
"""Measure H2S model skill the way the forecast actually runs it.

Why
---
`train_models_auto.py` reports R² ~0.36 / AUC ~0.93, but those are measured with
*true* H2S lag features. At forecast time the lags do not exist:
`forecast_features.engineer_station_features()` synthesises them as an
exponential decay from the last observation --

    h2s_lag_1h      = last_H2S      * exp(-h/12)
    h2s_rolling_24h = last_24h_mean * exp(-h/36)

-- so by 24 hours out they are 14% of the seed value, and by 36 hours 5%. Those
are the model's highest-importance features (~0.20-0.23 each). The model is
therefore trained on one distribution and served another, and the headline
metrics are nowcast numbers, not forecast numbers.

This script reconstructs the operational features exactly and scores skill by
lead time, against three references:

* **nowcast** -- true lags, i.e. the number currently reported;
* **persistence** -- predict the last observed value and hold it;
* **climatology** -- predict the training mean.

Optimistic by construction
--------------------------
Every non-lag feature here is the *observed* weather, tide and flow at the target
hour. A real forecast has forecast weather, with its own error. So the curve below
is an upper bound on operational skill, not an estimate of it.

Usage
-----
    python evaluate_forecast_horizon.py --data reframed.parquet --output horizon.json
"""

from __future__ import annotations

import argparse
import json
import warnings

import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, r2_score, roc_auc_score

import train_models_auto as tma

warnings.filterwarnings("ignore")

MAX_HORIZON = 48
ISSUE_EVERY = 24  # hours between simulated forecast issues
SEEDS = (42, 7)
N_FOLDS = 3
INITIAL_TRAIN_FRACTION = 0.5

#: A model trained only on what exists at forecast time. Its skill does not vary
#: with lead time, so it is the natural alternative to serving decayed surrogates.
NOLAG_FEATURES = [
    f for f in tma.FEATURES if not f.startswith(("h2s_lag", "h2s_rolling"))
]

LAG_COLS = [
    "h2s_lag_1h", "h2s_lag_3h", "h2s_lag_6h",
    "h2s_rolling_6h", "h2s_rolling_24h",
    "flow_lag_6h", "flow_rolling_24h",
]


def operational_lags(last: dict, n: int) -> dict[str, np.ndarray]:
    """Rebuild the lag features exactly as forecast_features does.

    Mirrors engineer_station_features(); kept as a copy rather than an import so
    the experiment states its own assumption explicitly and does not drift if the
    production helper is refactored.
    """
    h = np.arange(n)
    decay_fast = np.exp(-h / 12)
    decay_slow = np.exp(-h / 36)
    lh = last["h2s"]
    return {
        "h2s_lag_1h": np.concatenate([[lh], lh * decay_fast[:-1]])[:n],
        "h2s_lag_3h": np.concatenate([[lh] * min(3, n), (lh * decay_fast)[: max(n - 3, 0)]])[:n],
        "h2s_lag_6h": np.concatenate([[lh] * min(6, n), (lh * decay_fast)[: max(n - 6, 0)]])[:n],
        "h2s_rolling_6h": last["h2s_6h"] * decay_fast,
        "h2s_rolling_24h": last["h2s_24h"] * decay_slow,
        "flow_lag_6h": np.full(n, last["flow"]),
        "flow_rolling_24h": np.full(n, last["flow_24h"]),
    }


def last_known(hist: pd.DataFrame) -> dict:
    """The state get_last_known_state() would extract at the issue time."""
    return {
        "h2s": float(hist["H2S"].iloc[-1]),
        "h2s_6h": float(hist["H2S"].tail(6).mean()),
        "h2s_24h": float(hist["H2S"].tail(24).mean()),
        "flow": float(hist["Flow (m^3/s)--Border"].iloc[-1]),
        "flow_24h": float(hist["Flow (m^3/s)--Border"].tail(24).mean()),
    }


def folds(n_rows: int, n_folds: int, initial_fraction: float):
    start = int(n_rows * initial_fraction)
    block = (n_rows - start) // n_folds
    for i in range(n_folds):
        s = start + i * block
        e = s + block if i < n_folds - 1 else n_rows
        yield s, e


def run(df: pd.DataFrame) -> pd.DataFrame:
    feats = list(tma.FEATURES)
    rows = []

    for site in tma.STATIONS:
        sdf = df[df["site_name"] == site].sort_values("time").reset_index(drop=True)
        if sdf.empty:
            continue
        print(f"\n{site}: {len(sdf):,} rows")

        for fold, (test_start, test_end) in enumerate(
            folds(len(sdf), N_FOLDS, INITIAL_TRAIN_FRACTION)
        ):
            tr = sdf.iloc[:test_start]
            te = sdf.iloc[test_start:test_end].reset_index(drop=True)
            if len(te) < MAX_HORIZON + 2:
                continue
            print(f"  fold {fold}: train {len(tr):,}, test {len(te):,}")

            for seed in SEEDS:
                tma.RANDOM_STATE = seed
                reg = tma.get_rf_regressor()
                reg.fit(tr[feats].values, tr["H2S"].values)

                clf = None
                ytr5 = tr["exceed_5"].values
                if len(np.unique(ytr5)) > 1:
                    pos = max(float((ytr5 == 0).sum()) / max((ytr5 == 1).sum(), 1), 1.0)
                    clf = tma.get_rf_classifier(scale_pos=pos)
                    clf.fit(tr[feats].values, ytr5)

                reg_nolag = tma.get_rf_regressor()
                reg_nolag.fit(tr[NOLAG_FEATURES].values, tr["H2S"].values)
                clf_nolag = None
                if clf is not None:
                    clf_nolag = tma.get_rf_classifier(scale_pos=pos)
                    clf_nolag.fit(tr[NOLAG_FEATURES].values, ytr5)

                train_mean = float(tr["H2S"].mean())

                for issue in range(0, len(te) - MAX_HORIZON, ISSUE_EVERY):
                    hist = pd.concat([tr, te.iloc[: issue + 1]])
                    state = last_known(hist)

                    window = te.iloc[issue + 1 : issue + 1 + MAX_HORIZON].copy()
                    if window.empty:
                        continue
                    # Only score genuinely contiguous hours.
                    elapsed = (
                        window["time"] - te.iloc[issue]["time"]
                    ).dt.total_seconds() / 3600.0
                    keep = (elapsed - np.arange(1, len(window) + 1)).abs() < 0.01
                    window, elapsed = window[keep], elapsed[keep]
                    if window.empty:
                        continue

                    truth = window["H2S"].values
                    nowcast = reg.predict(window[feats].values)

                    op = window.copy()
                    for col, vals in operational_lags(state, len(op)).items():
                        op[col] = vals
                    forecast = reg.predict(op[feats].values)

                    nolag = reg_nolag.predict(window[NOLAG_FEATURES].values)

                    p_now = p_op = p_nolag = None
                    if clf is not None:
                        p_now = clf.predict_proba(window[feats].values)[:, 1]
                        p_op = clf.predict_proba(op[feats].values)[:, 1]
                        p_nolag = clf_nolag.predict_proba(window[NOLAG_FEATURES].values)[:, 1]

                    for i, lead in enumerate(elapsed.values):
                        rows.append(
                            {
                                "site": site, "fold": fold, "seed": seed,
                                "lead_hours": int(round(lead)),
                                "truth": float(truth[i]),
                                "exceed_5": int(truth[i] > 5),
                                "pred_forecast": float(forecast[i]),
                                "pred_nowcast": float(nowcast[i]),
                                "pred_nolag": float(nolag[i]),
                                "pred_persistence": state["h2s"],
                                "pred_climatology": train_mean,
                                "proba_forecast": None if p_op is None else float(p_op[i]),
                                "proba_nowcast": None if p_now is None else float(p_now[i]),
                                "proba_nolag": None if p_nolag is None else float(p_nolag[i]),
                            }
                        )
    return pd.DataFrame(rows)


def summarise(res: pd.DataFrame) -> pd.DataFrame:
    buckets = [(1, 6), (7, 12), (13, 24), (25, 36), (37, 48)]
    out = []
    for lo, hi in buckets:
        sub = res[(res["lead_hours"] >= lo) & (res["lead_hours"] <= hi)]
        if sub.empty:
            continue
        row = {"lead": f"{lo}-{hi}h", "n": len(sub)}
        for label, col in (
            ("forecast", "pred_forecast"),
            ("nowcast", "pred_nowcast"),
            ("nolag", "pred_nolag"),
            ("persistence", "pred_persistence"),
            ("climatology", "pred_climatology"),
        ):
            row[f"R2_{label}"] = round(float(r2_score(sub["truth"], sub[col])), 3)
            row[f"MAE_{label}"] = round(float(mean_absolute_error(sub["truth"], sub[col])), 2)
        if sub["proba_forecast"].notna().any() and sub["exceed_5"].nunique() > 1:
            row["AUC_forecast"] = round(
                float(roc_auc_score(sub["exceed_5"], sub["proba_forecast"])), 3
            )
            row["AUC_nowcast"] = round(
                float(roc_auc_score(sub["exceed_5"], sub["proba_nowcast"])), 3
            )
            row["AUC_nolag"] = round(
                float(roc_auc_score(sub["exceed_5"], sub["proba_nolag"])), 3
            )
        row["bias_forecast"] = round(float((sub["pred_forecast"] - sub["truth"]).mean()), 2)
        row["bias_nolag"] = round(float((sub["pred_nolag"] - sub["truth"]).mean()), 2)
        out.append(row)
    return pd.DataFrame(out)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True)
    ap.add_argument("--output", default="forecast_horizon.json")
    args = ap.parse_args()

    df = tma.prepare_data(args.data)
    res = run(df)
    summary = summarise(res)

    print("\n" + "=" * 100)
    print("SKILL BY LEAD TIME  (forecast = operational decayed lags; nowcast = true lags)")
    print("=" * 100)
    cols = [c for c in summary.columns if c.startswith(("lead", "n", "R2_", "AUC_", "bias"))]
    print(summary[cols].to_string(index=False))

    with open(args.output, "w") as fh:
        json.dump({"summary": summary.to_dict("records")}, fh, indent=2)
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()
