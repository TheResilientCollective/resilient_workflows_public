"""Serve the night-target H2S model: tonight's chance of a night peak above the threshold.

Trains the night-target model (``h2s_backtest`` variant ``night``) on the whole
production record each run, builds tonight's features from the forecast
inference data on the astronomical day plus the latest monitor readings, and
publishes one probability per monitor. The logic lives in
``tijuana.utils.h2s_night_forecast``; this module is only I/O.

Outputs under ``tijuana/forecast_data/night_forecast/``:

- ``h2s_night_forecast_{t}ppb.json`` and ``.csv``: the current issue, one row per monitor;
- ``h2s_night_forecast_{t}ppb_history.csv``: every night's forecast as last issued
  before that night began, kept so the forecasts can be verified against what
  the monitors recorded.
"""

import io
import json
from typing import Optional

import pandas as pd
from dagster import (
    AssetKey,
    AutomationCondition,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
    get_dagster_logger,
)

from resilient_core.utils import store_assets

from ..utils import astro_calendar
from ..utils import h2s_backtest as bt
from ..utils import h2s_night_forecast as nf

OUTPUT_PATH = "tijuana/forecast_data/output/night_forecast/"
LATEST = "tijuana/forecast_data/night_forecast"
SOURCE_KEY = "latest/tijuana/forecast_data/modeldata_h2s_nofill.parquet"
NIGHTLY_KEY = "latest/tijuana/forecast_data/astronomical_day/h2s_nightly_summary.parquet"
FORECAST_KEY = "latest/tijuana/forecast_data/astronomical_day/modeldata_forecast_15min_astronomical_day.parquet"
#: the monitors' readings for the current year, and the last two days (the freshest)
RECENT_KEYS = ("latest/tijuana/sd_apcd_air/h2s/apcd_h2s_latest.parquet", "tijuana/sd_apcd_air/output/hs2_lastday.csv")
#: what the history keeps per night
HISTORY_COLUMNS = ["astro_day_date", "site_name", "issue_time", "night_start", "probability", "warn_30", "warn_40",
                   "warn_50", "threshold_ppb", "model_trained_on_nights", "missing_features"]


class NightForecastConfig(Config):
    """Run-time knobs. Defaults match the backtest's night-target model."""

    threshold: Optional[float] = None
    issue_hour: int = 15
    n_estimators: int = 300
    #: read the inputs from another bucket (for a development run against production data)
    source_bucket: Optional[str] = None


def _read(s3_resource, bucket: str, key: str) -> bytes:
    obj = s3_resource.getClient().get_object(bucket, key)
    try:
        return obj.read()
    finally:
        obj.close()
        obj.release_conn()


def _parquet(s3_resource, bucket, key):
    return pd.read_parquet(io.BytesIO(_read(s3_resource, bucket, key)))


def _put_text(s3_resource, text: str, name: str, content_type: str):
    """Write to the dated output path and the stable latest path; return the latest key."""
    latest_key = f"{store_assets.get_latest_basepath()}/{LATEST}/{name}"
    for key in (f"{OUTPUT_PATH}{name}", latest_key):
        store_assets.text_to_s3(text, key, s3_resource, contenttype=content_type)
    return latest_key


def recent_readings(frames, stations) -> pd.DataFrame:
    """The monitors' latest hourly readings, framed on the astronomical day."""
    parts = []
    for raw in frames:
        if raw is None or raw.empty:
            continue
        parts.append(pd.DataFrame({
            "site_name": raw["SiteName"],
            "time": pd.to_datetime(raw["Date with time"], utc=True).dt.tz_convert("America/Los_Angeles"),
            "H2S": pd.to_numeric(raw["Result"], errors="coerce"),
        }))
    if not parts:
        return pd.DataFrame()
    r = pd.concat(parts, ignore_index=True)
    r = r[r["site_name"].isin(stations)].dropna().drop_duplicates(["site_name", "time"], keep="last")
    return astro_calendar.attach_astro_frame_to_events(r.sort_values(["site_name", "time"]).reset_index(drop=True))


def update_history(history: pd.DataFrame, issued: pd.DataFrame) -> pd.DataFrame:
    """Add this issue to the per-night history. A night's row is replaced only by an issue
    made before that night began, so the history holds each night's forecast as it stood
    when the night started, never one that saw the night's own readings."""
    new = issued[pd.to_datetime(issued["issue_time"], utc=True) < pd.to_datetime(issued["night_start"], utc=True)]
    new = new[[c for c in HISTORY_COLUMNS if c in new.columns]].copy()
    for frame in (history, new):
        if not frame.empty:
            frame["astro_day_date"] = pd.to_datetime(frame["astro_day_date"]).dt.strftime("%Y-%m-%d")
    out = pd.concat([history, new], ignore_index=True) if not history.empty else new
    return out.drop_duplicates(["astro_day_date", "site_name"], keep="last").sort_values(["astro_day_date", "site_name"])


def _night_forecast_asset(default_threshold: float, variant: str = "night", suffix: str = ""):
    """One serving asset per classifier threshold and served variant; all share the logic below."""
    t = f"{default_threshold:g}"
    variant_note = {
        "night": "from the night-target model the backtest recommends: trained each run on every "
                 "station-night of the record, served with tonight's forecast meteorology, flow, "
                 "SBIWTP and tide, the previous nights' peaks and the H2S history at the issue. ",
        "binary": "from the binary night model: a below / at-or-above classifier trained each run on "
                  "every station-night of the record and served with tonight's forecast meteorology, "
                  "flow, SBIWTP and tide plus exceedance indicators only (did the last night exceed, how "
                  "many of the last seven did, nights since the last, whether the 24 h and 72 h before "
                  "issue reached the threshold). No H2S concentration enters this model. ",
    }[variant]

    @asset(
        group_name="tijuana",
        key_prefix="h2sforecast",
        name=f"h2s_night_forecast_{t}ppb{suffix}",
        required_resource_keys={"s3"},
        deps=[
            AssetKey(["h2sforecast", "modeldata_h2s_nofill"]),
            AssetKey(["h2sforecast", "h2s_nightly_summary"]),
            AssetKey(["h2sforecast", "modeldata_forecast_15min_astronomical_day"]),
        ],
        metadata={
            "source": "h2sforecast/modeldata_forecast_15min_astronomical_day",
            "description": (
                f"Tonight's chance that each monitor's night peak (sunset to sunrise) passes {t} ppb, "
                + variant_note +
                "Flags warnings at cutoffs 0.3, 0.4 and 0.5 and keeps a per-night history for verification."
            ),
            "variableMeasured": ["H2S", "Exceedance Probability"],
        },
        # re-issues whenever the forecast inference data are rebuilt
        automation_condition=AutomationCondition.eager(),
    )
    def _run(context, config: NightForecastConfig) -> MaterializeResult:
        return _serve(context, config, default_threshold, variant, suffix)

    return _run


def _serve(context, config: NightForecastConfig, default_threshold: float, variant: str = "night", suffix: str = "") -> MaterializeResult:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    bucket = config.source_bucket or s3_resource.S3_BUCKET
    cfg = bt.BacktestConfig(
        threshold=config.threshold if config.threshold is not None else default_threshold,
        issue_hour=config.issue_hour, n_estimators=config.n_estimators,
    )
    name = f"h2s_night_forecast_{cfg.threshold:g}ppb{suffix}"

    hourly = bt.prepare_hourly(_parquet(s3_resource, bucket, SOURCE_KEY), cfg)
    nightly_raw = _parquet(s3_resource, bucket, NIGHTLY_KEY)
    rows = bt.prepare_nightly(nightly_raw, hourly, cfg)
    logger.info(f"Training on {len(rows):,} station-nights, {int(rows['y_night'].sum()):,} above {cfg.threshold:g} ppb")
    models = nf.train(rows, cfg, variant)

    frames = []
    for key in RECENT_KEYS:
        try:
            data = _read(s3_resource, bucket, key)
            frames.append(pd.read_parquet(io.BytesIO(data)) if key.endswith(".parquet") else pd.read_csv(io.BytesIO(data)))
        except Exception as e:  # a missing recent file only makes the history less fresh
            logger.warning(f"Could not read {key}: {e}")
    recent = recent_readings(frames, cfg.stations)

    issue = pd.Timestamp.now(tz="America/Los_Angeles").floor("min")
    feats = nf.tonight_features(_parquet(s3_resource, bucket, FORECAST_KEY), nightly_raw, hourly, issue, cfg, recent=recent)
    if feats.empty:
        raise ValueError(f"No forecast rows for the night of {nf.night_of(issue).date()}; is the forecast data current?")
    out = nf.forecast(models, feats, cfg, variant)
    out["model_trained_on_nights"] = len(rows)
    out["astro_day_date"] = pd.to_datetime(out["astro_day_date"]).dt.strftime("%Y-%m-%d")
    for c in ("issue_time", "night_start", "last_h2s_time", "last_night_scored"):
        if c in out.columns:
            out[c] = out[c].astype(str)
    logger.info(out[["site_name", "astro_day_date", "probability"]].to_string(index=False))

    payload = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "threshold_ppb": cfg.threshold,
        "night_of": out["astro_day_date"].iloc[0],
        "cutoffs": list(nf.REVIEW_CUTOFFS),
        "model": {"variant": variant, "trained_on_nights": len(rows), "event_nights": int(rows["y_night"].sum()),
                  "last_training_night": str(rows["astro_day_date"].max().date()), "n_estimators": cfg.n_estimators},
        "note": "Meteorology, flow, SBIWTP and tide for tonight are forecasts; the backtest scored the model on observed values.",
        "stations": json.loads(out.to_json(orient="records")),
    }
    json_key = _put_text(s3_resource, json.dumps(payload, indent=2), f"{name}.json", "application/json")
    _put_text(s3_resource, out.to_csv(index=False), f"{name}.csv", "text/csv")

    history_name = f"{name}_history.csv"
    try:
        history = pd.read_csv(io.BytesIO(_read(s3_resource, s3_resource.S3_BUCKET, f"{store_assets.get_latest_basepath()}/{LATEST}/{history_name}")))
    except Exception:
        history = pd.DataFrame(columns=HISTORY_COLUMNS)
    history = update_history(history, out)
    _put_text(s3_resource, history.to_csv(index=False), history_name, "text/csv")
    logger.info(f"Published {json_key}; history holds {len(history)} station-nights")

    top = out.loc[out["probability"].idxmax()] if out["probability"].notna().any() else None
    return MaterializeResult(metadata={
        "forecast": MetadataValue.path(f"s3://{s3_resource.S3_BUCKET}/{json_key}"),
        "night_of": payload["night_of"],
        "max_probability": MetadataValue.float(float(top["probability"])) if top is not None else "n/a",
        "max_station": str(top["site_name"]) if top is not None else "n/a",
        "trained_on_nights": len(rows),
        "history_nights": len(history),
        "source_bucket": bucket,
    })


#: 30 ppb is the County's alert level; 70 ppb the ATSDR acute MRL tier, a NESTOR product for now.
h2s_night_forecast_30ppb = _night_forecast_asset(30.0)
h2s_night_forecast_70ppb = _night_forecast_asset(70.0)
#: Binary classifier (< 30 / >= 30 ppb) served with no H2S concentration in any feature.
h2s_night_forecast_30ppb_binary = _night_forecast_asset(30.0, variant="binary", suffix="_binary")
