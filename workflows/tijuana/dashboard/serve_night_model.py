"""Issue tonight's night-target forecast for the dashboard, until the pipeline asset is deployed.

Usage: python3 serve_night_model.py <out_dir> [--threshold 30]

Runs the same serving code as the ``h2sforecast/h2s_night_forecast_30ppb`` asset
(``tijuana.utils.h2s_night_forecast``), reading its inputs from the public bucket,
and writes ``<out_dir>/night_forecast_source.json`` in the asset's JSON format.
It writes nothing to the bucket; build_data.py prefers the asset's own output
when the pipeline has published one for tonight. Needs pandas, pyarrow,
scikit-learn and astral.
"""
import argparse
import importlib.util
import io
import json
import os
import sys
import types
import urllib.request
from pathlib import Path

import pandas as pd

PUBLIC = 'https://oss.resilientservice.mooo.com/resilentpublic/'
UTILS = Path(__file__).resolve().parents[1] / 'src' / 'tijuana' / 'utils'
INPUTS = {
    'hourly': 'latest/tijuana/forecast_data/modeldata_h2s_nofill.parquet',
    'nightly': 'latest/tijuana/forecast_data/astronomical_day/h2s_nightly_summary.parquet',
    'forecast': 'latest/tijuana/forecast_data/astronomical_day/modeldata_forecast_15min_astronomical_day.parquet',
}
RECENT = ('latest/tijuana/sd_apcd_air/h2s/apcd_h2s_latest.parquet', 'tijuana/sd_apcd_air/output/hs2_lastday.csv')


def load_utils(utils_dir):
    """Import the pipeline's utils as a package without importing the Dagster definitions."""
    pkg = '_tijuana_utils'
    mod = types.ModuleType(pkg)
    mod.__path__ = [str(utils_dir)]
    sys.modules[pkg] = mod

    def load(name):
        spec = importlib.util.spec_from_file_location(f'{pkg}.{name}', Path(utils_dir) / f'{name}.py')
        m = importlib.util.module_from_spec(spec)
        sys.modules[f'{pkg}.{name}'] = m
        spec.loader.exec_module(m)
        return m

    return load('h2s_backtest'), load('h2s_night_forecast'), load('astro_calendar')


def fetch(key):
    with urllib.request.urlopen(PUBLIC + key) as f:
        return f.read()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('out_dir')
    ap.add_argument('--threshold', type=float, default=30.0)
    ap.add_argument('--utils', default=str(UTILS), help='folder holding h2s_backtest.py and h2s_night_forecast.py')
    a = ap.parse_args()
    bt, nf, astro = load_utils(a.utils)
    cfg = bt.BacktestConfig(threshold=a.threshold)

    hourly = bt.prepare_hourly(pd.read_parquet(io.BytesIO(fetch(INPUTS['hourly']))), cfg)
    nightly = pd.read_parquet(io.BytesIO(fetch(INPUTS['nightly'])))
    rows = bt.prepare_nightly(nightly, hourly, cfg)
    models = nf.train(rows, cfg)

    frames = []
    for key in RECENT:
        try:
            data = fetch(key)
            frames.append(pd.read_parquet(io.BytesIO(data)) if key.endswith('.parquet') else pd.read_csv(io.BytesIO(data)))
        except Exception as e:
            print('could not read', key, e)
    recent = pd.concat([pd.DataFrame({
        'site_name': f['SiteName'],
        'time': pd.to_datetime(f['Date with time'], utc=True).dt.tz_convert('America/Los_Angeles'),
        'H2S': pd.to_numeric(f['Result'], errors='coerce')}) for f in frames], ignore_index=True)
    recent = recent[recent['site_name'].isin(cfg.stations)].dropna().drop_duplicates(['site_name', 'time'], keep='last')
    recent = astro.attach_astro_frame_to_events(recent.sort_values(['site_name', 'time']).reset_index(drop=True))

    issue = pd.Timestamp.now(tz='America/Los_Angeles').floor('min')
    feats = nf.tonight_features(pd.read_parquet(io.BytesIO(fetch(INPUTS['forecast']))), nightly, hourly, issue, cfg, recent=recent)
    if feats.empty:
        raise SystemExit(f'no forecast rows for the night of {nf.night_of(issue).date()}')
    out = nf.forecast(models, feats, cfg)
    out['astro_day_date'] = pd.to_datetime(out['astro_day_date']).dt.strftime('%Y-%m-%d')
    for c in ('issue_time', 'night_start', 'last_h2s_time', 'last_night_scored'):
        if c in out.columns:
            out[c] = out[c].astype(str)
    payload = {
        'generated_at': pd.Timestamp.now(tz='UTC').isoformat(),
        'threshold_ppb': cfg.threshold,
        'night_of': out['astro_day_date'].iloc[0],
        'cutoffs': list(nf.REVIEW_CUTOFFS),
        'model': {'variant': 'night', 'trained_on_nights': len(rows), 'event_nights': int(rows['y_night'].sum()),
                  'last_training_night': str(rows['astro_day_date'].max().date()), 'n_estimators': cfg.n_estimators},
        'issued_by': 'dashboard job (pipeline asset not yet deployed)',
        'stations': json.loads(out.to_json(orient='records')),
    }
    os.makedirs(a.out_dir, exist_ok=True)
    path = os.path.join(a.out_dir, 'night_forecast_source.json')
    json.dump(payload, open(path, 'w'), indent=2)
    print(out[['site_name', 'astro_day_date', 'probability']].to_string(index=False))
    print('wrote', path)


if __name__ == '__main__':
    main()
