#!/usr/bin/env python3
"""Evaluate whether the astronomical-day features improve H2S model skill.

Answers the question deferred in docs/tj_data_basis.md: do `night_fraction` and
`solar_elevation_deg` earn a place in FEATURES, and can they retire the
`hour_sin`/`hour_cos`/`month_sin`/`month_cos` cyclicals?

Method
------
`train_models_auto.py` uses a single chronological 80/20 split. That is fine for
shipping a model but too fragile for comparing features: it yields one number
with no uncertainty, and its test block is one particular season in data whose
exceedances concentrate in ISO weeks 6-15. This uses instead:

* **walk-forward (rolling-origin) CV** -- an expanding training window with
  several contiguous test blocks, so every arm is scored on multiple periods and
  no fold ever trains on data that follows its test block;
* **paired comparison** -- every arm sees identical folds and seeds, so the
  per-fold difference isolates the feature change from fold difficulty;
* **multiple seeds** -- to separate a real effect from RandomForest/XGBoost
  fitting variance.

A note on `night_fraction`
--------------------------
It is null by construction during the day (it is a position *within a night*).
`prepare_data()` ends with `dropna(subset=FEATURES)`, so adding the raw column
would silently delete every daytime row and make the arms incomparable. It enters
the model as `night_phase`: the fraction at night, and -1 during the day, which
gives a tree a clean split point rather than a missing value.

Usage
-----
    python evaluate_astro_features.py --data reframed.parquet --output results.json
"""

from __future__ import annotations

import argparse
import json
import warnings

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    roc_auc_score,
)

import train_models_auto as tma

warnings.filterwarnings("ignore")

BASELINE = list(tma.FEATURES)
CYCLICALS = ["hour_sin", "hour_cos", "month_sin", "month_cos"]

#: The two features the deferred question names, plus the day/night-safe phase.
ASTRO_ADD = ["night_phase", "solar_elevation_deg"]
#: Season without the calendar-month step change.
ASTRO_SEASON = ["doy_sin", "doy_cos"]

#: H2S history. Available when training, but not at forecast time -- see
#: add_h2s_lag_features(), documented as training-only. Arms D/E drop them to
#: approximate the forecast regime, where temporal encodings have more work to do
#: because the model cannot lean on H2S autocorrelation.
H2S_HISTORY = [f for f in BASELINE if f.startswith(("h2s_lag", "h2s_rolling"))]
NOLAG = [f for f in BASELINE if f not in H2S_HISTORY]

ARMS = {
    "A_baseline": BASELINE,
    "B_plus_astro": BASELINE + ASTRO_ADD,
    "C_replace_cyclicals": (
        [f for f in BASELINE if f not in CYCLICALS] + ASTRO_ADD + ASTRO_SEASON
    ),
    "D_nolag_baseline": NOLAG,
    "E_nolag_plus_astro": NOLAG + ASTRO_ADD,
}

#: Each arm is compared against its own baseline, so the no-lag arms are compared
#: with each other rather than against a model that can see H2S history.
BASELINE_FOR = {
    "B_plus_astro": "A_baseline",
    "C_replace_cyclicals": "A_baseline",
    "E_nolag_plus_astro": "D_nolag_baseline",
}

N_FOLDS = 4
SEEDS = (42, 7, 2024)
INITIAL_TRAIN_FRACTION = 0.5


def add_astro_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Derive the model-facing astronomical columns."""
    out = df.copy()
    if "night_fraction" not in out.columns:
        raise KeyError(
            "night_fraction missing - point --data at modeldata_h2s_nofill_astronomical_day"
        )
    # -1 during the day: a real value a tree can split on, not a missing one.
    out["night_phase"] = out["night_fraction"].fillna(-1.0)
    return out


def walk_forward_folds(n_rows: int, n_folds: int, initial_fraction: float):
    """Expanding-window folds: (train_end, test_start, test_end) per fold."""
    start = int(n_rows * initial_fraction)
    block = (n_rows - start) // n_folds
    if block < 50:
        raise ValueError(f"test blocks too small ({block} rows) for {n_folds} folds")
    for i in range(n_folds):
        test_start = start + i * block
        test_end = test_start + block if i < n_folds - 1 else n_rows
        yield test_start, test_start, test_end


def score_regression(model, X_te, y_te) -> dict:
    pred = model.predict(X_te)
    return {
        "MAE": float(mean_absolute_error(y_te, pred)),
        "RMSE": float(np.sqrt(mean_squared_error(y_te, pred))),
        "R2": float(r2_score(y_te, pred)),
    }


def score_classification(model, X_te, y_te) -> dict | None:
    if len(np.unique(y_te)) < 2:
        return None  # a fold with one class cannot be scored
    proba = model.predict_proba(X_te)[:, 1]
    return {
        "AUC": float(roc_auc_score(y_te, proba)),
        "PR_AUC": float(average_precision_score(y_te, proba)),
    }


def run(df: pd.DataFrame) -> list[dict]:
    """Fit every (station, arm, fold, seed, model, task) combination."""
    records = []
    for site in tma.STATIONS:
        sdf = df[df["site_name"] == site].sort_values("time").reset_index(drop=True)
        if sdf.empty:
            print(f"  {site}: no rows, skipped")
            continue
        print(f"\n{site}: {len(sdf):,} rows")

        for fold, (train_end, test_start, test_end) in enumerate(
            walk_forward_folds(len(sdf), N_FOLDS, INITIAL_TRAIN_FRACTION)
        ):
            tr = sdf.iloc[:train_end]
            te = sdf.iloc[test_start:test_end]
            print(
                f"  fold {fold}: train {len(tr):,} "
                f"({tr['time'].min():%Y-%m-%d}..{tr['time'].max():%Y-%m-%d}) "
                f"test {len(te):,} ({te['time'].min():%Y-%m-%d}..{te['time'].max():%Y-%m-%d})"
            )

            for arm, feats in ARMS.items():
                Xtr, Xte = tr[feats].values, te[feats].values
                for seed in SEEDS:
                    tma.RANDOM_STATE = seed  # model constructors read this at call time

                    for name, make in (
                        ("RF", tma.get_rf_regressor),
                        ("XGB", tma.get_xgb_regressor),
                    ):
                        if name == "XGB" and not tma.HAS_XGB:
                            continue
                        m = make()
                        m.fit(Xtr, tr["H2S"].values)
                        records.append(
                            dict(
                                site=site, fold=fold, arm=arm, seed=seed,
                                model=name, task="regression",
                                **score_regression(m, Xte, te["H2S"].values),
                            )
                        )

                    for name, make in (
                        ("RF", tma.get_rf_classifier),
                        ("XGB", tma.get_xgb_classifier),
                    ):
                        if name == "XGB" and not tma.HAS_XGB:
                            continue
                        ytr, yte = tr["exceed_5"].values, te["exceed_5"].values
                        if len(np.unique(ytr)) < 2:
                            continue
                        pos = max(float((ytr == 0).sum()) / max((ytr == 1).sum(), 1), 1.0)
                        m = make(scale_pos=pos)
                        m.fit(Xtr, ytr)
                        s = score_classification(m, Xte, yte)
                        if s:
                            records.append(
                                dict(
                                    site=site, fold=fold, arm=arm, seed=seed,
                                    model=name, task="exceed_5", **s,
                                )
                            )
    return records


def summarise(records: list[dict]) -> pd.DataFrame:
    """Paired deltas of each arm against the baseline, over identical folds/seeds."""
    df = pd.DataFrame(records)
    keys = ["site", "fold", "seed", "model", "task"]
    metrics = ["MAE", "RMSE", "R2", "AUC", "PR_AUC"]

    rows = []
    for arm, baseline_arm in BASELINE_FOR.items():
        base = df[df["arm"] == baseline_arm].set_index(keys)
        cur = df[df["arm"] == arm].set_index(keys)
        if base.empty or cur.empty:
            continue
        common = base.index.intersection(cur.index)
        for metric in metrics:
            b = base.loc[common, metric].dropna()
            c = cur.loc[common, metric].dropna()
            idx = b.index.intersection(c.index)
            if len(idx) == 0:
                continue
            delta = c.loc[idx] - b.loc[idx]
            # Lower is better for error metrics, higher for the rest.
            better = (delta < 0) if metric in ("MAE", "RMSE") else (delta > 0)
            rows.append(
                {
                    "arm": arm,
                    "vs": baseline_arm,
                    "metric": metric,
                    "n": len(idx),
                    "baseline_mean": round(float(b.loc[idx].mean()), 4),
                    "arm_mean": round(float(c.loc[idx].mean()), 4),
                    "mean_delta": round(float(delta.mean()), 4),
                    "std_delta": round(float(delta.std()), 4),
                    "pct_folds_better": round(100 * float(better.mean()), 1),
                }
            )
    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--data", required=True, help="modeldata_h2s_nofill_astronomical_day.parquet")
    ap.add_argument("--output", default="astro_feature_eval.json")
    args = ap.parse_args()

    df = tma.prepare_data(args.data)
    df = add_astro_columns(df)
    print(f"\nArms: " + ", ".join(f"{k} ({len(v)} features)" for k, v in ARMS.items()))
    print(f"Folds: {N_FOLDS}, seeds: {SEEDS}")

    records = run(df)
    summary = summarise(records)

    print("\n" + "=" * 78)
    print("PAIRED DELTAS vs BASELINE  (negative is better for MAE/RMSE)")
    print("=" * 78)
    for task in ("regression", "exceed_5"):
        sub = pd.DataFrame(records)
        sub = sub[sub["task"] == task]
        if sub.empty:
            continue
        print(f"\n--- {task} ---")
        print(summarise(sub.to_dict("records")).to_string(index=False))

    with open(args.output, "w") as fh:
        json.dump(
            {"arms": {k: v for k, v in ARMS.items()}, "records": records,
             "summary": summary.to_dict("records")},
            fh,
            indent=2,
        )
    print(f"\nWrote {args.output}")


if __name__ == "__main__":
    main()
