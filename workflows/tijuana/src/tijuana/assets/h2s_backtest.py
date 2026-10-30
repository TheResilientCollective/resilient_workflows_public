"""Backtest of the H2S exceedance classifier, published as an evaluation page.

Reads the production training record (``h2sforecast/modeldata_h2s_nofill``),
re-issues the classifier walk-forward exactly as the forecast is served, and
writes three things to S3: the per-hour predictions (csv + parquet), the
metrics (json), and a self-contained HTML evaluation page. The logic lives in
``tijuana.utils.h2s_backtest``; this module is only I/O.
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

from ..utils import h2s_backtest as bt

OUTPUT_PATH = "tijuana/forecast_data/output/backtest/"
LATEST = "tijuana/forecast_data/backtest"
SOURCE_KEY = "latest/tijuana/forecast_data/modeldata_h2s_nofill.parquet"
NIGHTLY_KEY = "latest/tijuana/forecast_data/astronomical_day/h2s_nightly_summary.parquet"


class BacktestRunConfig(Config):
    """Run-time knobs. Defaults reproduce the published evaluation."""

    #: exceedance threshold in ppb; None means the asset's own default (30 or 70)
    threshold: Optional[float] = None
    issue_hour: int = 15
    horizon_hours: int = 18
    n_folds: int = 4
    initial_train_fraction: float = 0.5
    n_estimators: int = 300
    cutoff: float = 0.5
    #: read the training record from another bucket (for a development run
    #: against production data while writing to the development bucket)
    source_bucket: Optional[str] = None


def _load_parquet(s3_resource, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_resource.getClient().get_object(bucket, key)
    try:
        return pd.read_parquet(io.BytesIO(obj.read()))
    finally:
        obj.close()
        obj.release_conn()


def _put_text(s3_resource, text: str, name: str, content_type: str):
    """Write to the dated output path and the stable latest path; return the latest key."""
    latest_key = f"{store_assets.get_latest_basepath()}/{LATEST}/{name}"
    for key in (f"{OUTPUT_PATH}{name}", latest_key):
        store_assets.text_to_s3(text, key, s3_resource, contenttype=content_type)
    return latest_key


def _backtest_asset(default_threshold: float, cron: str):
    """One backtest asset per classifier threshold; all share the logic below."""
    t = f"{default_threshold:g}"

    @asset(
        group_name="tijuana",
        key_prefix="h2sforecast",
        name=f"h2s_backtest_{t}ppb",
        required_resource_keys={"s3"},
        deps=[AssetKey(["h2sforecast", "modeldata_h2s_nofill"]), AssetKey(["h2sforecast", "h2s_nightly_summary"])],
        metadata={
            "source": "h2sforecast/modeldata_h2s_nofill",
            "description": (
                f"Walk-forward backtest of the {t} ppb H2S exceedance classifier, issued "
                "every afternoon as the forecast is served (H2S history decayed from the "
                "last observation) and verified per station-night, alongside served-time, "
                "night-target, persistence and climatology comparison lines. Publishes "
                "per-hour predictions, a metrics JSON and a self-contained HTML evaluation "
                "page. Weather, tide and flow inputs are observed values, so the skill is "
                "an upper bound on operational skill."
            ),
            "variableMeasured": ["H2S", "Exceedance Probability", "Forecast Skill"],
        },
        # weekly; the backtest re-fits a model per fold and station and is not cheap
        automation_condition=AutomationCondition.on_cron(cron),
    )
    def _run(context, config: BacktestRunConfig) -> MaterializeResult:
        return _run_backtest(context, config, default_threshold)

    return _run


def _run_backtest(context, config: BacktestRunConfig, default_threshold: float) -> MaterializeResult:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    bucket = config.source_bucket or s3_resource.S3_BUCKET
    cfg = bt.BacktestConfig(
        threshold=config.threshold if config.threshold is not None else default_threshold,
        issue_hour=config.issue_hour, horizon_hours=config.horizon_hours,
        n_folds=config.n_folds, initial_train_fraction=config.initial_train_fraction,
        n_estimators=config.n_estimators, cutoff=config.cutoff,
    )
    name = f"h2s_backtest_{cfg.threshold:g}ppb"

    raw = _load_parquet(s3_resource, bucket, SOURCE_KEY)
    logger.info(f"Loaded {len(raw):,} rows of {SOURCE_KEY} from bucket {bucket}")
    hourly = bt.prepare_hourly(raw, cfg)
    logger.info(f"{len(hourly):,} measured hours with complete features; {int(hourly['y'].sum()):,} above {cfg.threshold:g} ppb")
    nightly = bt.prepare_nightly(_load_parquet(s3_resource, bucket, NIGHTLY_KEY), hourly, cfg)
    logger.info(f"{len(nightly):,} station-nights for the night-target model; {int(nightly['y_night'].sum()):,} above {cfg.threshold:g} ppb")

    preds = bt.simulate(hourly, cfg, nightly=nightly)
    metrics = bt.score(preds, cfg)
    generated = pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%d %H:%M UTC")
    page = bt.render_html(metrics, preds, cfg, generated_at=generated)

    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=f"{name}_predictions", description=meta["description"], source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )
    store_assets.store_dataframe_to_s3(
        preds, OUTPUT_PATH, f"{name}_predictions", s3_resource,
        latestdatasetpath=LATEST, enable_latest_path=True, formats=["csv", "parquet"], metadata=metadata,
    )
    metrics_key = _put_text(s3_resource, json.dumps(metrics, indent=2, default=str), f"{name}_metrics.json", "application/json")
    page_key = _put_text(s3_resource, page, f"{name}.html", "text/html")
    logger.info(f"Published {page_key} and {metrics_key} to bucket {s3_resource.S3_BUCKET}")

    dep = metrics["nightly"]["deployed"]
    cut = next(c for c in metrics["cutoffs"]["deployed"] if c["cutoff"] == cfg.cutoff)
    return MaterializeResult(
        metadata={
            "page": MetadataValue.path(f"s3://{s3_resource.S3_BUCKET}/{page_key}"),
            "metrics": MetadataValue.path(f"s3://{s3_resource.S3_BUCKET}/{metrics_key}"),
            "nights_scored": metrics["record"]["nights_scored"],
            "event_nights": dep.get("positives", 0),
            "nightly_auc_deployed": MetadataValue.float(dep["auc"]) if dep.get("auc") is not None else "n/a",
            "pod_at_cutoff": MetadataValue.float(cut["pod"]) if cut["pod"] is not None else "n/a",
            "far_at_cutoff": MetadataValue.float(cut["far"]) if cut["far"] is not None else "n/a",
            "source_bucket": bucket,
        }
    )


#: The 30 ppb classifier is the County's alert level; 70 ppb is the ATSDR acute
#: MRL tier (see docs/tj_prediction_redo/README.md). Staggered so the two
#: weekly runs do not compete for the same worker.
h2s_backtest_30ppb = _backtest_asset(30.0, "0 14 * * 1")
h2s_backtest_70ppb = _backtest_asset(70.0, "0 16 * * 1")
