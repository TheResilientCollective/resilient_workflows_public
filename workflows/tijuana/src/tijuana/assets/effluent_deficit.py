"""SBIWTP effluent deficit — the odour-risk signal that drives channel colouring.

The public map (see `docs/mapinterface_plan.md` in the `tijuana-dispersion`
repo) colours the river reaches by how much sewage is bypassing treatment rather
than by how much water is moving down the channel. The two tell opposite
stories: high gauge discharge can mean a healthy storm flush, whereas a *low*
plant throughput means untreated sewage is reaching the river, and that is what
precedes an odour episode.

The relationship is established in the project's own findings — SBIWTP flow is
inversely correlated with H2S at Nestor (r = -0.47 at a 1-day lag), and the
compound of low plant throughput with warm temperatures produces the extreme
events. See `docs/project_context.md`, finding 1.

The deficit definition here is deliberately identical to the `sbiwtp_deficit`
feature that `hysplit_forecasting.add_sbiwtp_features` feeds to the forecast
models: a one-day lag, a 30-day rolling baseline, and a floor at zero so that
*surplus* throughput reads as "no deficit" rather than as negative risk. Keeping
one definition means the public map and the models cannot tell different stories
about the same day.

What this module does *not* do is decide where the colour goes. The channel
geometry — the two reaches running from the plant to the Saturn crossings — is
not yet resolvable from any dataset available here: the basin hydrography has
the channels but not the plant outfall, and OpenStreetMap's Saturn Boulevard
stops about 1.7 km north of the northernmost channel crossing. Until those
endpoints are pinned, this asset publishes the signal and the map applies it to
whatever geometry it is given.
"""

from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd

from dagster import (
    asset,
    AssetIn,
    AssetKey,
    get_dagster_logger,
    AutomationCondition,
)

from resilient_core.utils import store_assets

s3_output_path = 'tijuana/effluent_flow/output'
s3_latest_path = 'tijuana/effluent_flow'

# Matches `add_sbiwtp_features` in hysplit_forecasting.py. Changing either
# without the other will make the map and the forecast models disagree.
BASELINE_WINDOW_DAYS = 30
LAG_DAYS = 1

# Deficit in MGD at which the channel colouring saturates.
#
# Measured over the full published record (2,423 days, 2020-01-01 to
# 2026-08-19): median 0.06, p90 4.14, p95 5.89, p99 8.77, maximum 14.51 MGD
# against a mean baseline of 25.3 MGD. Saturating at p99 puts the worst ~1% of
# days at full colour and spreads the rest across the usable range; a rounder
# 15.0 would never be reached by any day on record, so every day would render
# pale and the map would under-state the risk it exists to communicate.
DEFICIT_SATURATION_MGD = 9.0


def daily_series(df: pd.DataFrame) -> pd.Series:
    """Reduce a raw IBWC effluent export to a daily mean series in MGD.

    The portal's export carries a timestamp column stamped `UTC-08:00`
    year-round — it is a fixed offset, not a local clock, so it is parsed as
    such rather than localised to Pacific time (which would shift half the year
    by an hour and smear values across the date boundary).
    """
    if df.empty:
        raise ValueError("effluent flow frame is empty; refusing to derive a deficit from nothing")

    ts_col = next((c for c in df.columns if c.lower().startswith('timestamp')), None)
    val_col = next((c for c in df.columns if c.lower().startswith('value')), None)
    if ts_col is None or val_col is None:
        raise ValueError(
            f"could not find timestamp/value columns in effluent frame; got {list(df.columns)}"
        )

    times = pd.to_datetime(df[ts_col], errors='coerce')
    if times.dt.tz is None:
        times = times.dt.tz_localize('Etc/GMT+8')

    values = pd.to_numeric(df[val_col], errors='coerce')
    series = pd.Series(values.to_numpy(), index=times).dropna()
    if series.empty:
        raise ValueError("no parseable effluent flow values found")

    return series.sort_index().resample('D').mean().rename('flow_mgd')


def effluent_deficit(daily_mgd: pd.Series) -> pd.DataFrame:
    """Daily flow, its rolling baseline, and the shortfall against it.

    Identical in definition to the `sbiwtp_deficit` model feature:

        flow      = daily mean MGD, lagged one day
        baseline  = 30-day rolling mean of that lagged series
        deficit   = max(0, baseline - flow)          [MGD]
        anomaly   = (flow - baseline) / baseline     [dimensionless]

    The lag is not cosmetic: the H2S response to a treatment shortfall shows up
    about a day later, so the deficit that matters for today's odour is
    yesterday's.
    """
    if daily_mgd.empty:
        raise ValueError("daily effluent series is empty")

    lagged = daily_mgd.shift(LAG_DAYS)
    baseline = daily_mgd.rolling(BASELINE_WINDOW_DAYS, min_periods=1).mean().shift(LAG_DAYS)

    frame = pd.DataFrame({
        'flow_mgd': lagged,
        'baseline_mgd': baseline,
        'deficit_mgd': (baseline - lagged).clip(lower=0),
        # Guard the ratio rather than letting a zero-flow day produce an inf.
        'anomaly': (lagged - baseline) / baseline.replace(0, np.nan),
    })
    frame.index.name = 'date'
    return frame


def deficit_index(deficit_mgd: pd.Series, saturation: float = DEFICIT_SATURATION_MGD) -> pd.Series:
    """Scale the deficit to 0-1 for colouring.

    A public map needs a bounded number, but the bound has to come from the data
    rather than from a round figure that happens to look tidy. `saturation`
    defaults to the 99th percentile of the observed record (see
    DEFICIT_SATURATION_MGD) — the asset logs the percentiles on every run so the
    constant can be revisited as the record grows.

    Values above saturation clamp to 1: past that point the channel is already
    drawn at maximum and further precision would not change what a resident
    does.
    """
    if saturation <= 0:
        raise ValueError(f"saturation must be positive, got {saturation}")
    return (deficit_mgd / saturation).clip(lower=0, upper=1)


@asset(
    group_name="tijuana",
    key_prefix="ibwc",
    name="effluent_deficit",
    required_resource_keys={"s3"},
    ins={"effluent_flow_current_year": AssetIn(key=AssetKey(["ibwc", "effluent_flow_current_year"]))},
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": "IBWC AQWebportal — SBIWTP effluent flow",
        "description": (
            "Daily SBIWTP effluent flow against its 30-day baseline, with the "
            "treatment deficit (MGD) and a 0-1 index for map colouring. Low plant "
            "throughput means more untreated sewage reaching the river."
        ),
        "variableMeasured": ["flow_mgd", "baseline_mgd", "deficit_mgd", "anomaly", "deficit_index"],
    },
)
def effluent_deficit_asset(context, effluent_flow_current_year: pd.DataFrame):
    """Publish the treatment-deficit series and a small current-value JSON."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    daily = daily_series(effluent_flow_current_year)
    logger.info(f"Daily effluent series: {len(daily)} days, {daily.index.min()} to {daily.index.max()}")

    # The upstream asset carries the current calendar year only, so for the first
    # weeks of January the 30-day baseline is computed from fewer than 30 days
    # and the deficit is correspondingly soft. It is still the honest number
    # available; flag it rather than hiding it.
    if len(daily) < BASELINE_WINDOW_DAYS:
        logger.warning(
            f"Only {len(daily)} days available, fewer than the {BASELINE_WINDOW_DAYS}-day "
            "baseline window; early-year deficits are computed on a partial baseline"
        )

    frame = effluent_deficit(daily)
    frame['deficit_index'] = deficit_index(frame['deficit_mgd'])

    observed = frame['deficit_mgd'].dropna()
    if observed.empty:
        raise Exception("no deficit values could be computed from the effluent series")

    # Logged every run so DEFICIT_SATURATION_MGD stays evidence-based.
    pct = {f"p{q}": round(float(observed.quantile(q / 100)), 2) for q in (50, 75, 90, 95, 99)}
    logger.info(f"Deficit distribution (MGD): {pct}, max {observed.max():.2f}")
    saturating = int((observed >= DEFICIT_SATURATION_MGD).sum())
    logger.info(
        f"{saturating} of {len(observed)} days ({saturating / len(observed):.1%}) "
        f"reach the {DEFICIT_SATURATION_MGD} MGD colour saturation"
    )

    published = frame.reset_index()
    published['date'] = published['date'].dt.strftime('%Y-%m-%d')

    store_assets.store_dataframe_to_s3(
        published,
        f"{s3_output_path}/effluent_deficit/",
        "effluent_deficit",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=f"{s3_latest_path}/deficit",
        formats=["csv", "parquet"],
    )

    latest = frame.dropna(subset=['deficit_mgd']).iloc[-1]
    current = {
        "date": frame.dropna(subset=['deficit_mgd']).index[-1].strftime('%Y-%m-%d'),
        "flow_mgd": _clean(latest['flow_mgd']),
        "baseline_mgd": _clean(latest['baseline_mgd']),
        "deficit_mgd": _clean(latest['deficit_mgd']),
        "anomaly": _clean(latest['anomaly']),
        "deficit_index": _clean(latest['deficit_index']),
        "saturation_mgd": DEFICIT_SATURATION_MGD,
        "baseline_window_days": BASELINE_WINDOW_DAYS,
        "lag_days": LAG_DAYS,
        "generated_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "note": (
            "Deficit is how far below its own 30-day baseline the plant is running, "
            "lagged one day. Higher means more sewage bypassing treatment. This is "
            "plant throughput, not river discharge."
        ),
    }

    current_metadata = metadata.copy()
    current_metadata.name = "effluent_deficit_current"
    current_metadata.description = "Latest SBIWTP treatment deficit against its 30-day baseline"
    store_assets.text_to_s3(
        json.dumps(current),
        f"{store_assets.get_latest_basepath()}/{s3_latest_path}/deficit/effluent_deficit_current.json",
        s3_resource,
        contenttype="application/json",
        metadata=current_metadata,
    )

    context.add_output_metadata({
        "days": len(frame),
        "latest_date": current["date"],
        "latest_flow_mgd": current["flow_mgd"],
        "latest_deficit_mgd": current["deficit_mgd"],
        "latest_deficit_index": current["deficit_index"],
        "days_at_saturation": saturating,
        **pct,
    })
    return published


def _clean(value) -> float | None:
    """NaN is not valid JSON; publish null instead of a literal the parser rejects."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    return round(float(value), 3)
