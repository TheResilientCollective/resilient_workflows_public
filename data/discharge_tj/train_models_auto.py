#!/usr/bin/env python3
"""
H2S Model Training Pipeline — Auto-Select Best Algorithm
Trains both XGBoost and Random Forest for each station × task,
evaluates on time-series holdout, selects the winner per combination.
Optionally ensembles when performance is within a tight margin.

Usage:
    python train_models_auto.py --data modeldata_h2s_nofill.parquet --output ./models

Produces:
    - best_reg_{STATION}.pkl        (regression model)
    - best_clf5_{STATION}.pkl       (P(>5 ppb) classifier)
    - best_clf10_{STATION}.pkl      (P(>10 ppb) classifier)
    - training_report.json          (full metrics, algorithm choices, feature importance)
"""

import argparse
import json
import os
import pickle
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
from sklearn.metrics import (
    mean_absolute_error, mean_squared_error, r2_score,
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, brier_score_loss,
)

try:
    from xgboost import XGBRegressor, XGBClassifier
    HAS_XGB = True
except ImportError:
    HAS_XGB = False
    print("Warning: xgboost not installed. Using Random Forest only.")


# ============================================================
# CONFIGURATION
# ============================================================

STATIONS = {
    'SAN YSIDRO':   {'key': 'SAN_YSIDRO'},
    'NESTOR - BES': {'key': 'NESTOR__BES'},
    'IB CIVIC CTR': {'key': 'IB_CIVIC_CTR'},
}

FEATURES = [
    'temperature_2m', 'wind_speed_10m', 'wind_direction_sin', 'wind_direction_cos',
    'wind_gusts_10m', 'precipitation', 'relative_humidity_2m', 'surface_pressure',
    'cloud_cover', 'dewpoint_2m',
    'wind_speed_10m_avg_2h', 'wind_speed_10m_avg_3h', 'wind_speed_10m_avg_4h',
    'wind_gusts_10m_max_2h', 'wind_gusts_10m_max_3h', 'wind_gusts_10m_max_4h',
    'tide_height', 'tidal_state_encoded',
    'hour_sin', 'hour_cos', 'month_sin', 'month_cos',
    'is_night', 'source_regime',
    'flow_log', 'flow_low', 'flow_high',
    'wind_temp_interaction', 'humidity_temp_interaction',
    'stable_atm',
    'h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h',
    'h2s_rolling_6h', 'h2s_rolling_24h',
    'flow_lag_6h', 'flow_rolling_24h',
]

# Margin within which we ensemble instead of picking a winner
ENSEMBLE_AUC_MARGIN = 0.01   # for classifiers
ENSEMBLE_R2_MARGIN = 0.02    # for regression

TRAIN_FRACTION = 0.8
RANDOM_STATE = 42


# ============================================================
# ENSEMBLE WRAPPER
# ============================================================

class EnsembleRegressor:
    """Simple averaging ensemble of two regressors."""
    def __init__(self, model_a, model_b, weight_a=0.5):
        self.model_a = model_a
        self.model_b = model_b
        self.weight_a = weight_a
        self.weight_b = 1.0 - weight_a

    def predict(self, X):
        pa = self.model_a.predict(X)
        pb = self.model_b.predict(X)
        return self.weight_a * pa + self.weight_b * pb


class EnsembleClassifier:
    """Simple probability-averaging ensemble of two classifiers."""
    def __init__(self, model_a, model_b, weight_a=0.5):
        self.model_a = model_a
        self.model_b = model_b
        self.weight_a = weight_a
        self.weight_b = 1.0 - weight_a

    def predict_proba(self, X):
        pa = self.model_a.predict_proba(X)
        pb = self.model_b.predict_proba(X)
        return self.weight_a * pa + self.weight_b * pb

    def predict(self, X):
        proba = self.predict_proba(X)
        return (proba[:, 1] > 0.5).astype(int)


# ============================================================
# DATA PREPARATION
# ============================================================

def prepare_data(path):
    """Load, clean, and engineer features."""
    print("Loading data...")
    df = pd.read_parquet(path)
    df['time'] = pd.to_datetime(df['time'], utc=True)
    df = df[(df['h2s_measured'] == True) & (df['H2S'] <= 500)].copy()
    df = df.sort_values(['site_name', 'time']).reset_index(drop=True)
    df['H2S'] = df['H2S'].clip(lower=0)

    print("Engineering features...")
    df['hour'] = df['time'].dt.hour
    df['month'] = df['time'].dt.month
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['month_sin'] = np.sin(2 * np.pi * df['month'] / 12)
    df['month_cos'] = np.cos(2 * np.pi * df['month'] / 12)
    df['is_night'] = (df['day_night'] == 'night').astype(int)

    def source_regime(row):
        if row['day_night'] != 'night':
            return 0
        wd = row['wind_direction_10m']
        if 22.5 <= wd < 135: return 1
        elif wd >= 247.5 or wd < 22.5: return 2
        elif 135 <= wd < 247.5: return 3
        return 0

    df['source_regime'] = df.apply(source_regime, axis=1)
    df['flow_log'] = np.log1p(df['Flow (m^3/s)--Border'])
    df['flow_low'] = (df['Flow (m^3/s)--Border'] < 1).astype(int)
    df['flow_high'] = (df['Flow (m^3/s)--Border'] > 5).astype(int)
    df['stable_atm'] = ((df['wind_speed_10m'] < 5) & (df['is_night'] == 1)).astype(int)

    # Gusts (fill if missing)
    if 'wind_gusts_10m' not in df.columns:
        df['wind_gusts_10m'] = df['wind_speed_10m'] * 1.8
    if 'wind_gusts_10m_max_2h' not in df.columns:
        for site in df['site_name'].unique():
            m = df['site_name'] == site
            df.loc[m, 'wind_gusts_10m_max_2h'] = df.loc[m, 'wind_gusts_10m'].rolling(2, min_periods=1).max()
            df.loc[m, 'wind_gusts_10m_max_3h'] = df.loc[m, 'wind_gusts_10m'].rolling(3, min_periods=1).max()
            df.loc[m, 'wind_gusts_10m_max_4h'] = df.loc[m, 'wind_gusts_10m'].rolling(4, min_periods=1).max()

    # Lag features
    for site in df['site_name'].unique():
        m = df['site_name'] == site
        s = df.loc[m].copy()
        s['h2s_lag_1h'] = s['H2S'].shift(1)
        s['h2s_lag_3h'] = s['H2S'].shift(3)
        s['h2s_lag_6h'] = s['H2S'].shift(6)
        s['h2s_rolling_6h'] = s['H2S'].rolling(6, min_periods=1).mean()
        s['h2s_rolling_24h'] = s['H2S'].rolling(24, min_periods=1).mean()
        s['flow_lag_6h'] = s['Flow (m^3/s)--Border'].shift(6)
        s['flow_rolling_24h'] = s['Flow (m^3/s)--Border'].rolling(24, min_periods=1).mean()
        for col in ['h2s_lag_1h', 'h2s_lag_3h', 'h2s_lag_6h', 'h2s_rolling_6h',
                     'h2s_rolling_24h', 'flow_lag_6h', 'flow_rolling_24h']:
            df.loc[m, col] = s[col].values

    df['exceed_5'] = (df['H2S'] > 5).astype(int)
    df['exceed_10'] = (df['H2S'] > 10).astype(int)
    df = df.dropna(subset=FEATURES).reset_index(drop=True)

    print(f"  Clean records: {len(df):,}")
    return df


# ============================================================
# MODEL DEFINITIONS
# ============================================================

def get_rf_regressor():
    return RandomForestRegressor(
        n_estimators=500, max_depth=20, min_samples_leaf=5,
        max_features='sqrt', n_jobs=-1, random_state=RANDOM_STATE
    )

def get_rf_classifier(scale_pos=1.0):
    return RandomForestClassifier(
        n_estimators=500, max_depth=20, min_samples_leaf=5,
        max_features='sqrt', class_weight='balanced',
        n_jobs=-1, random_state=RANDOM_STATE
    )

def get_xgb_regressor():
    return XGBRegressor(
        n_estimators=500, max_depth=6, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, reg_alpha=0.1, reg_lambda=1.0,
        random_state=RANDOM_STATE, n_jobs=-1
    )

def get_xgb_classifier(scale_pos=1.0):
    return XGBClassifier(
        n_estimators=500, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.8,
        min_child_weight=5, scale_pos_weight=scale_pos,
        reg_alpha=0.1, reg_lambda=1.0,
        random_state=RANDOM_STATE, n_jobs=-1, eval_metric='logloss'
    )


# ============================================================
# EVALUATION
# ============================================================

def eval_regressor(model, X_test, y_test):
    """Evaluate a regression model. Returns dict of metrics and predictions."""
    y_pred = np.clip(model.predict(X_test), 0, None)
    return {
        'MAE': mean_absolute_error(y_test, y_pred),
        'RMSE': np.sqrt(mean_squared_error(y_test, y_pred)),
        'R2': r2_score(y_test, y_pred),
        'predictions': y_pred,
    }


def eval_classifier(model, X_test, y_test, threshold=0.3):
    """Evaluate a classifier. Returns dict of metrics and probabilities."""
    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = (y_prob > threshold).astype(int)
    auc = roc_auc_score(y_test, y_prob) if y_test.sum() > 0 and y_test.sum() < len(y_test) else 0.5
    return {
        'AUC': auc,
        'Brier': brier_score_loss(y_test, y_prob),
        'F1': f1_score(y_test, y_pred, zero_division=0),
        'Precision': precision_score(y_test, y_pred, zero_division=0),
        'Recall': recall_score(y_test, y_pred, zero_division=0),
        'Accuracy': accuracy_score(y_test, y_pred),
        'probabilities': y_prob,
    }


def get_feature_importance(model, feature_names, top_n=10):
    """Extract top feature importances."""
    imp = model.feature_importances_
    idx = np.argsort(imp)[::-1][:top_n]
    return {feature_names[i]: round(float(imp[i]), 4) for i in idx}


# ============================================================
# SELECTION LOGIC
# ============================================================

def select_best_regressor(metrics_rf, metrics_xgb, model_rf, model_xgb):
    """
    Pick the best regression model, or ensemble if close.
    Primary metric: R². Secondary: MAE.
    """
    r2_rf = metrics_rf['R2']
    r2_xgb = metrics_xgb['R2']
    diff = abs(r2_rf - r2_xgb)

    if diff < ENSEMBLE_R2_MARGIN:
        # Weight by relative R² (higher R² gets more weight)
        total = max(r2_rf + r2_xgb, 0.01)
        w_rf = max(r2_rf, 0) / total
        w_xgb = max(r2_xgb, 0) / total
        model = EnsembleRegressor(model_rf, model_xgb, weight_a=w_rf)
        choice = 'Ensemble'
        reason = f'R² within {ENSEMBLE_R2_MARGIN} (RF={r2_rf:.3f}, XGB={r2_xgb:.3f}), weights RF={w_rf:.2f}/XGB={w_xgb:.2f}'
    elif r2_rf > r2_xgb:
        model = model_rf
        choice = 'RandomForest'
        reason = f'RF R²={r2_rf:.3f} > XGB R²={r2_xgb:.3f}'
    else:
        model = model_xgb
        choice = 'XGBoost'
        reason = f'XGB R²={r2_xgb:.3f} > RF R²={r2_rf:.3f}'

    return model, choice, reason


def select_best_classifier(metrics_rf, metrics_xgb, model_rf, model_xgb):
    """
    Pick the best classifier, or ensemble if close.
    Primary metric: AUC. Secondary: F1.
    """
    auc_rf = metrics_rf['AUC']
    auc_xgb = metrics_xgb['AUC']
    diff = abs(auc_rf - auc_xgb)

    if diff < ENSEMBLE_AUC_MARGIN:
        # Weight by AUC
        total = auc_rf + auc_xgb
        w_rf = auc_rf / total
        w_xgb = auc_xgb / total
        model = EnsembleClassifier(model_rf, model_xgb, weight_a=w_rf)
        choice = 'Ensemble'
        reason = f'AUC within {ENSEMBLE_AUC_MARGIN} (RF={auc_rf:.3f}, XGB={auc_xgb:.3f}), weights RF={w_rf:.2f}/XGB={w_xgb:.2f}'
    elif auc_rf > auc_xgb:
        model = model_rf
        choice = 'RandomForest'
        reason = f'RF AUC={auc_rf:.3f} > XGB AUC={auc_xgb:.3f}'
    else:
        model = model_xgb
        choice = 'XGBoost'
        reason = f'XGB AUC={auc_xgb:.3f} > RF AUC={auc_rf:.3f}'

    return model, choice, reason


# ============================================================
# MAIN TRAINING LOOP
# ============================================================

def train_all(df, output_dir):
    """Train all models for all stations with auto-selection."""

    report = {
        'trained_at': datetime.now(timezone.utc).isoformat(),
        'total_records': len(df),
        'features': FEATURES,
        'train_fraction': TRAIN_FRACTION,
        'ensemble_auc_margin': ENSEMBLE_AUC_MARGIN,
        'ensemble_r2_margin': ENSEMBLE_R2_MARGIN,
        'algorithms_available': ['RandomForest'] + (['XGBoost'] if HAS_XGB else []),
        'stations': {},
    }

    summary_rows = []

    for site, site_info in STATIONS.items():
        skey = site_info['key']
        print(f"\n{'='*70}")
        print(f"  {site}  ({skey})")
        print(f"{'='*70}")

        sdf = df[df['site_name'] == site].copy().sort_values('time').reset_index(drop=True)
        X = sdf[FEATURES].values
        y_cont = sdf['H2S'].values
        y_5 = sdf['exceed_5'].values
        y_10 = sdf['exceed_10'].values

        split = int(len(sdf) * TRAIN_FRACTION)
        Xtr, Xte = X[:split], X[split:]
        ytr_c, yte_c = y_cont[:split], y_cont[split:]
        ytr_5, yte_5 = y_5[:split], y_5[split:]
        ytr_10, yte_10 = y_10[:split], y_10[split:]

        print(f"  Records: {len(sdf):,} (train: {split:,}, test: {len(sdf)-split:,})")
        print(f"  Exceedance: >5={y_5.mean()*100:.1f}%, >10={y_10.mean()*100:.1f}%")

        station_report = {
            'n_records': len(sdf), 'n_train': split, 'n_test': len(sdf) - split,
            'exceedance_5pct': round(y_5.mean()*100, 1),
            'exceedance_10pct': round(y_10.mean()*100, 1),
            'tasks': {}
        }

        # ---- REGRESSION ----
        print(f"\n  --- Regression ---")
        t0 = time.time()

        rf_reg = get_rf_regressor()
        rf_reg.fit(Xtr, ytr_c)
        m_rf = eval_regressor(rf_reg, Xte, yte_c)
        rf_time = time.time() - t0
        print(f"    RF:  MAE={m_rf['MAE']:.2f}, RMSE={m_rf['RMSE']:.2f}, R²={m_rf['R2']:.3f}  ({rf_time:.1f}s)")

        if HAS_XGB:
            t0 = time.time()
            xgb_reg = get_xgb_regressor()
            xgb_reg.fit(Xtr, ytr_c, eval_set=[(Xte, yte_c)], verbose=False)
            m_xgb = eval_regressor(xgb_reg, Xte, yte_c)
            xgb_time = time.time() - t0
            print(f"    XGB: MAE={m_xgb['MAE']:.2f}, RMSE={m_xgb['RMSE']:.2f}, R²={m_xgb['R2']:.3f}  ({xgb_time:.1f}s)")

            best_model, choice, reason = select_best_regressor(m_rf, m_xgb, rf_reg, xgb_reg)
        else:
            m_xgb = None
            best_model, choice, reason = rf_reg, 'RandomForest', 'XGBoost not available'

        print(f"    >> SELECTED: {choice} — {reason}")

        pickle.dump(best_model, open(os.path.join(output_dir, f'best_reg_{skey}.pkl'), 'wb'))

        task_report = {
            'selected': choice, 'reason': reason,
            'RF': {k: round(v, 4) for k, v in m_rf.items() if k != 'predictions'},
            'RF_importance': get_feature_importance(rf_reg, FEATURES),
        }
        if m_xgb:
            task_report['XGB'] = {k: round(v, 4) for k, v in m_xgb.items() if k != 'predictions'}
            task_report['XGB_importance'] = get_feature_importance(xgb_reg, FEATURES)
        station_report['tasks']['regression'] = task_report
        summary_rows.append({'station': site, 'task': 'Regression', 'metric': 'R²',
                             'RF': m_rf['R2'], 'XGB': m_xgb['R2'] if m_xgb else None, 'selected': choice})

        # ---- CLASSIFIER >5 ppb ----
        for threshold, label, ytr, yte in [(5, 'clf_5ppb', ytr_5, yte_5), (10, 'clf_10ppb', ytr_10, yte_10)]:
            print(f"\n  --- Classifier >{threshold} ppb ---")
            pos_rate = ytr.mean()
            scale_pos = (1 - pos_rate) / max(pos_rate, 0.01)

            t0 = time.time()
            rf_clf = get_rf_classifier(scale_pos)
            rf_clf.fit(Xtr, ytr)
            m_rf_c = eval_classifier(rf_clf, Xte, yte)
            rf_time = time.time() - t0
            print(f"    RF:  AUC={m_rf_c['AUC']:.3f}, F1={m_rf_c['F1']:.3f}, Rec={m_rf_c['Recall']:.3f}  ({rf_time:.1f}s)")

            if HAS_XGB:
                t0 = time.time()
                xgb_clf = get_xgb_classifier(scale_pos)
                xgb_clf.fit(Xtr, ytr, eval_set=[(Xte, yte)], verbose=False)
                m_xgb_c = eval_classifier(xgb_clf, Xte, yte)
                xgb_time = time.time() - t0
                print(f"    XGB: AUC={m_xgb_c['AUC']:.3f}, F1={m_xgb_c['F1']:.3f}, Rec={m_xgb_c['Recall']:.3f}  ({xgb_time:.1f}s)")

                best_clf, choice, reason = select_best_classifier(m_rf_c, m_xgb_c, rf_clf, xgb_clf)
            else:
                m_xgb_c = None
                best_clf, choice, reason = rf_clf, 'RandomForest', 'XGBoost not available'

            print(f"    >> SELECTED: {choice} — {reason}")

            pickle.dump(best_clf, open(os.path.join(output_dir, f'best_{label}_{skey}.pkl'), 'wb'))

            task_report = {
                'selected': choice, 'reason': reason,
                'RF': {k: round(v, 4) for k, v in m_rf_c.items() if k != 'probabilities'},
                'RF_importance': get_feature_importance(rf_clf, FEATURES),
            }
            if m_xgb_c:
                task_report['XGB'] = {k: round(v, 4) for k, v in m_xgb_c.items() if k != 'probabilities'}
                task_report['XGB_importance'] = get_feature_importance(xgb_clf, FEATURES)
            station_report['tasks'][label] = task_report
            summary_rows.append({'station': site, 'task': f'>{threshold}ppb', 'metric': 'AUC',
                                 'RF': m_rf_c['AUC'], 'XGB': m_xgb_c['AUC'] if m_xgb_c else None, 'selected': choice})

        report['stations'][site] = station_report

    # ============================================================
    # PRINT SUMMARY TABLE
    # ============================================================
    print(f"\n{'='*70}")
    print("MODEL SELECTION SUMMARY")
    print(f"{'='*70}")
    print(f"\n{'Station':<18} {'Task':<12} {'Metric':<8} {'RF':>8} {'XGB':>8} {'Selected':>12}")
    print("-" * 68)
    for row in summary_rows:
        xgb_val = f"{row['XGB']:.3f}" if row['XGB'] is not None else "N/A"
        print(f"{row['station']:<18} {row['task']:<12} {row['metric']:<8} {row['RF']:>8.3f} {xgb_val:>8} {row['selected']:>12}")

    # Count selections
    selections = [r['selected'] for r in summary_rows]
    print(f"\nTotal selections: RF={selections.count('RandomForest')}, "
          f"XGB={selections.count('XGBoost')}, "
          f"Ensemble={selections.count('Ensemble')}")

    # Save report
    report_path = os.path.join(output_dir, 'training_report.json')
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\nTraining report saved to {report_path}")

    return report


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description='H2S Model Training — Auto-Select Best Algorithm')
    parser.add_argument('--data', required=True, help='Path to modeldata_h2s_nofill.parquet')
    parser.add_argument('--output', default='./models', help='Output directory for model files')
    parser.add_argument('--train-fraction', type=float, default=0.8, help='Fraction for training (rest is test)')
    parser.add_argument('--ensemble-margin', type=float, default=0.01, help='AUC margin for ensembling')
    args = parser.parse_args()

    global TRAIN_FRACTION, ENSEMBLE_AUC_MARGIN, ENSEMBLE_R2_MARGIN
    TRAIN_FRACTION = args.train_fraction
    ENSEMBLE_AUC_MARGIN = args.ensemble_margin
    ENSEMBLE_R2_MARGIN = args.ensemble_margin * 2  # wider for R² since it's noisier

    os.makedirs(args.output, exist_ok=True)

    df = prepare_data(args.data)
    report = train_all(df, args.output)

    print(f"\nModel files saved to {args.output}/")
    print("Done.")


if __name__ == '__main__':
    main()
