"""SBIWTP plant balance — influent against effluent, both against capacity, and
the river at the border against the plant's outflow.

Three daily measures, all in million US gallons per day (MGD):

1. **Net flow** — influent minus effluent. The plant reports what enters and
   what it discharges; the difference is what it retained, lost or diverted
   that day. A persistently large gap, or a sudden one, is a plant-operations
   signal that neither series shows on its own.

2. **Capacity exceedance** — whether influent or effluent ran above the plant's
   rated treatment capacity. The rating is 35 MGD today and is expected to
   change; see `PLANT_CAPACITY_MGD` and `CAPACITY_CHANGES` for how a new
   rating is introduced without re-flagging history.

3. **Border flow against effluent** — the Tijuana River at the international
   boundary gauge (IBWC 11013300) compared with the plant's outflow. The gauge
   reports m³/s hourly; it is converted to MGD and compared with the day's
   effluent both as a daily mean and hour by hour. This is a candidate feature
   for the H2S forecast: the project's findings tie odour episodes to sewage
   bypassing treatment, and river flow exceeding what the plant discharges is
   one way that bypass could show up in the record. `add_plant_balance_features`
   carries the same quantities into the model training data so the question
   can be tested there.

Nothing here decides whether these belong in the forecast. The model feature
list (`utils/forecast_features.MODEL_FEATURES`) is unchanged; the columns are
published as candidates for evaluation, following the convention used for the
astronomical features.
"""

from datetime import datetime, timezone
import json

import numpy as np
import pandas as pd

from dagster import (
    asset,
    AssetIn,
    AssetKey,
    AutomationCondition,
    Field,
    get_dagster_logger,
)

from resilient_core.utils import store_assets
from .effluent_deficit import daily_series

s3_output_path = 'tijuana/plant_balance/output'
s3_latest_path = 'tijuana/plant_balance'

# 1 m³/s expressed in million US gallons per day: 264.172052 gal/m³ × 86,400 s/day.
CMS_TO_MGD = 22.824465

# Rated treatment capacity of SBIWTP, in MGD, as of the time of writing.
#
# Only the present rating is asserted. Days before the first entry in
# CAPACITY_CHANGES are judged against PLANT_CAPACITY_MGD, so historical flags
# describe today's plant, not the plant of that day. When the rating changes,
# append (effective date, new MGD) to CAPACITY_CHANGES rather than editing the
# constant, so the flags already published for earlier days keep their meaning.
PLANT_CAPACITY_MGD = 35.0
CAPACITY_CHANGES: list[tuple[str, float]] = []

# Matches effluent_deficit / add_sbiwtp_features: daily plant quantities enter
# the model data lagged one day, because today's odour follows yesterday's plant.
LAG_DAYS = 1

# An influent reading this low while the plant discharges more than
# SUSPECT_EFFLUENT_FLOOR_MGD is a meter or reporting outage, not the plant: the
# 2026 record has 56 consecutive days of 0.00 MGD influent (4 May - 1 July)
# against ~34 MGD effluent. Such days are flagged `influent_suspect` and their
# influent is treated as missing rather than scored as a -34 MGD net flow.
SUSPECT_INFLUENT_MGD = 1.0
SUSPECT_EFFLUENT_FLOOR_MGD = 5.0

BORDER_FLOW_COL = 'Average (m^3/s)'
BORDER_TIME_COL = 'Start of Interval (UTC-08:00)'


def capacity_series(
    index: pd.DatetimeIndex,
    base: float = PLANT_CAPACITY_MGD,
    changes: list[tuple[str, float]] | None = None,
) -> pd.Series:
    """The rated capacity in force on each day of `index`.

    `base` applies until the first dated change; each change applies from its
    effective date until the next. Changes need not be given in order.
    """
    if base <= 0:
        raise ValueError(f"capacity must be positive, got {base}")
    changes = CAPACITY_CHANGES if changes is None else changes

    capacity = pd.Series(float(base), index=index, name='capacity_mgd')
    for effective, mgd in sorted(changes, key=lambda c: pd.Timestamp(c[0])):
        if mgd <= 0:
            raise ValueError(f"capacity change on {effective} must be positive, got {mgd}")
        when = pd.Timestamp(effective)
        if index.tz is not None and when.tz is None:
            when = when.tz_localize(index.tz)
        capacity[index >= when] = float(mgd)
    return capacity


def border_hourly_mgd(boundary_df: pd.DataFrame) -> pd.Series:
    """Reduce the IBWC border gauge export to an hourly series in MGD.

    The export carries 'Start of Interval (UTC-08:00)' (as the index or a
    column) and 'Average (m^3/s)'. The stamp is a fixed UTC-8 offset, so it is
    localised as such — the same treatment the effluent series gets — rather
    than to Pacific time, which would shift half the year by an hour.
    """
    if boundary_df is None or boundary_df.empty:
        raise ValueError("border flow frame is empty")

    df = boundary_df
    if BORDER_TIME_COL not in df.columns:
        if df.index.name == BORDER_TIME_COL:
            df = df.reset_index()
        else:
            raise ValueError(
                f"could not find '{BORDER_TIME_COL}' in border flow frame; got {list(df.columns)}"
            )
    if BORDER_FLOW_COL not in df.columns:
        raise ValueError(
            f"could not find '{BORDER_FLOW_COL}' in border flow frame; got {list(df.columns)}"
        )

    times = pd.to_datetime(df[BORDER_TIME_COL], errors='coerce')
    if times.dt.tz is None:
        times = times.dt.tz_localize('Etc/GMT+8')
    values = pd.to_numeric(df[BORDER_FLOW_COL], errors='coerce') * CMS_TO_MGD
    series = pd.Series(values.to_numpy(), index=times).dropna()
    if series.empty:
        raise ValueError("no parseable border flow values found")
    series = series[~series.index.duplicated(keep='last')].sort_index()
    return series.rename('border_flow_mgd')


def plant_balance(
    influent_mgd: pd.Series,
    effluent_mgd: pd.Series,
    border_mgd: pd.Series | None = None,
    capacity: float = PLANT_CAPACITY_MGD,
    capacity_changes: list[tuple[str, float]] | None = None,
) -> pd.DataFrame:
    """Daily plant balance.

        influent_reported_mgd        = influent exactly as the portal reported it
        influent_suspect             = influent ~0 while effluent ran; influent_mgd is NaN there
        net_mgd                      = influent - effluent
        net_fraction                 = net_mgd / influent
        capacity_mgd                 = rated capacity in force that day
        influent_over_capacity       = influent > capacity
        effluent_over_capacity       = effluent > capacity
        over_capacity                = either of the above
        influent_excess_mgd          = max(0, influent - capacity)
        effluent_excess_mgd          = max(0, effluent - capacity)
        capacity_utilisation         = influent / capacity
        border_flow_mgd              = daily mean of the hourly border gauge
        border_minus_effluent_mgd    = border_flow_mgd - effluent
        border_effluent_ratio        = border_flow_mgd / effluent
        border_over_effluent         = border_flow_mgd > effluent
        border_over_effluent_hours   = hours that day the gauge exceeded the day's effluent
        border_hours_reported        = hours that day with a gauge reading
        border_over_effluent_fraction= border_over_effluent_hours / border_hours_reported

    `influent_mgd` and `effluent_mgd` are daily series; `border_mgd` is hourly.
    Days present in one series and not another are kept with NaN, so a gap in
    one source does not hide the others. Flags are nullable booleans and stay
    missing where the inputs are.
    """
    if influent_mgd.empty and effluent_mgd.empty:
        raise ValueError("both plant flow series are empty; nothing to balance")

    influent = _daily(influent_mgd, 'influent_mgd')
    effluent = _daily(effluent_mgd, 'effluent_mgd')
    frame = pd.concat([influent, effluent], axis=1)

    if border_mgd is not None and not border_mgd.empty:
        border = border_mgd.copy()
        if border.index.tz is None:
            border.index = border.index.tz_localize(frame.index.tz)
        else:
            border.index = border.index.tz_convert(frame.index.tz)
        border_daily = border.resample('D').agg(['mean', 'count'])
        border_daily.columns = ['border_flow_mgd', 'border_hours_reported']
        frame = frame.join(border_daily, how='outer')
    else:
        frame['border_flow_mgd'] = np.nan
        frame['border_hours_reported'] = np.nan
        border = None

    frame = frame.sort_index()
    frame.index.name = 'date'

    suspect = (frame['influent_mgd'] < SUSPECT_INFLUENT_MGD) & (frame['effluent_mgd'] > SUSPECT_EFFLUENT_FLOOR_MGD)
    frame['influent_reported_mgd'] = frame['influent_mgd']
    frame['influent_suspect'] = _flag(suspect, frame['influent_mgd'].notna() & frame['effluent_mgd'].notna())
    frame.loc[suspect, 'influent_mgd'] = np.nan

    frame['net_mgd'] = frame['influent_mgd'] - frame['effluent_mgd']
    frame['net_fraction'] = frame['net_mgd'] / frame['influent_mgd'].replace(0, np.nan)

    cap = capacity_series(frame.index, base=capacity, changes=capacity_changes)
    frame['capacity_mgd'] = cap
    frame['influent_over_capacity'] = _flag(frame['influent_mgd'] > cap, frame['influent_mgd'].notna())
    frame['effluent_over_capacity'] = _flag(frame['effluent_mgd'] > cap, frame['effluent_mgd'].notna())
    frame['over_capacity'] = frame['influent_over_capacity'] | frame['effluent_over_capacity']
    frame['influent_excess_mgd'] = (frame['influent_mgd'] - cap).clip(lower=0)
    frame['effluent_excess_mgd'] = (frame['effluent_mgd'] - cap).clip(lower=0)
    frame['capacity_utilisation'] = frame['influent_mgd'] / cap

    frame['border_minus_effluent_mgd'] = frame['border_flow_mgd'] - frame['effluent_mgd']
    frame['border_effluent_ratio'] = frame['border_flow_mgd'] / frame['effluent_mgd'].replace(0, np.nan)
    known = frame['border_flow_mgd'].notna() & frame['effluent_mgd'].notna()
    frame['border_over_effluent'] = _flag(frame['border_flow_mgd'] > frame['effluent_mgd'], known)

    if border is not None:
        day_effluent = frame['effluent_mgd'].reindex(border.index.normalize()).to_numpy()
        over = pd.Series((border.to_numpy() > day_effluent) & ~np.isnan(day_effluent), index=border.index)
        hours = over.resample('D').sum()
        frame['border_over_effluent_hours'] = hours.reindex(frame.index)
        frame.loc[~known, 'border_over_effluent_hours'] = np.nan
    else:
        frame['border_over_effluent_hours'] = np.nan
    frame['border_over_effluent_fraction'] = (
        frame['border_over_effluent_hours'] / frame['border_hours_reported'].replace(0, np.nan)
    )

    return frame[[
        'influent_mgd', 'effluent_mgd', 'net_mgd', 'net_fraction',
        'influent_reported_mgd', 'influent_suspect',
        'capacity_mgd', 'influent_over_capacity', 'effluent_over_capacity', 'over_capacity',
        'influent_excess_mgd', 'effluent_excess_mgd', 'capacity_utilisation',
        'border_flow_mgd', 'border_minus_effluent_mgd', 'border_effluent_ratio',
        'border_over_effluent', 'border_over_effluent_hours', 'border_hours_reported',
        'border_over_effluent_fraction',
    ]]


def _daily(series: pd.Series, name: str) -> pd.Series:
    """A daily series on a UTC-8 day boundary, whatever it arrived as."""
    s = series.copy()
    if s.empty:
        return pd.Series(dtype=float, name=name, index=pd.DatetimeIndex([], tz='Etc/GMT+8'))
    if s.index.tz is None:
        s.index = s.index.tz_localize('Etc/GMT+8')
    else:
        s.index = s.index.tz_convert('Etc/GMT+8')
    return s.resample('D').mean().rename(name)


def _flag(condition: pd.Series, known: pd.Series) -> pd.Series:
    """A nullable boolean that is missing wherever the input was."""
    return condition.astype('boolean').mask(~known, pd.NA)


def add_plant_balance_features(
    df: pd.DataFrame,
    influent_daily: pd.Series,
    effluent_daily: pd.Series,
    logger,
    capacity: float = PLANT_CAPACITY_MGD,
) -> pd.DataFrame:
    """Carry the plant balance into the hourly model data as candidate features.

    `df` has a 'time' column (America/Los_Angeles) and, when the border gauge
    was merged, 'Flow (m^3/s)--Border'. Daily plant quantities are lagged one
    day and mapped by date, exactly as `add_sbiwtp_features` does for the
    effluent; the border comparison uses the same hour's gauge reading against
    the lagged effluent, which is the plant's most recent known outflow at
    forecast time.

    Adds:
      sbiwtp_influent_mgd        — daily influent, lagged 1 day
      sbiwtp_net_mgd             — influent - effluent, lagged 1 day
      sbiwtp_capacity_mgd        — rated capacity on the lagged day
      sbiwtp_over_capacity       — 1.0 when influent or effluent exceeded capacity, lagged 1 day
      border_flow_mgd            — the hour's border gauge reading in MGD
      border_minus_effluent_mgd  — border_flow_mgd - lagged daily effluent
      border_over_effluent       — 1.0 when border_flow_mgd exceeds the lagged effluent

    Columns are NaN where an input is missing; nothing here is in MODEL_FEATURES.
    """
    daily_cols = ['sbiwtp_influent_mgd', 'sbiwtp_net_mgd', 'sbiwtp_capacity_mgd', 'sbiwtp_over_capacity']
    border_cols = ['border_flow_mgd', 'border_minus_effluent_mgd', 'border_over_effluent']
    df = df.copy()

    if influent_daily.empty or effluent_daily.empty:
        logger.warning("Influent or effluent series is empty — plant balance daily features left NaN")
        for col in daily_cols:
            df[col] = np.nan
    else:
        balance = plant_balance(influent_daily, effluent_daily, border_mgd=None, capacity=capacity)
        # Shift the index, not the values: the newest plant day must land on the
        # following day's rows even when that day is not yet in the plant record.
        lagged = balance.copy()
        lagged.index = lagged.index + pd.Timedelta(days=LAG_DAYS)
        times = df['time']
        if times.dt.tz is None:
            times = times.dt.tz_localize('America/Los_Angeles')
        dates = times.dt.tz_convert('America/Los_Angeles').dt.normalize()
        keyed = lagged.copy()
        keyed.index = keyed.index.tz_convert('America/Los_Angeles').normalize()
        keyed = keyed[~keyed.index.duplicated(keep='last')]
        df['sbiwtp_influent_mgd'] = dates.map(keyed['influent_mgd']).to_numpy()
        df['sbiwtp_net_mgd'] = dates.map(keyed['net_mgd']).to_numpy()
        df['sbiwtp_capacity_mgd'] = dates.map(keyed['capacity_mgd']).to_numpy()
        df['sbiwtp_over_capacity'] = dates.map(keyed['over_capacity'].astype('float')).to_numpy()
        logger.info(
            f"Plant balance daily features: {df['sbiwtp_influent_mgd'].notna().sum()} rows with influent, "
            f"{int(np.nansum(df['sbiwtp_over_capacity']))} rows on an over-capacity day"
        )

    flow_col = 'Flow (m^3/s)--Border'
    if flow_col in df.columns and 'sbiwtp_flow_mgd' in df.columns:
        df['border_flow_mgd'] = pd.to_numeric(df[flow_col], errors='coerce') * CMS_TO_MGD
        df['border_minus_effluent_mgd'] = df['border_flow_mgd'] - df['sbiwtp_flow_mgd']
        df['border_over_effluent'] = (df['border_minus_effluent_mgd'] > 0).astype(float)
        df.loc[df['border_minus_effluent_mgd'].isna(), 'border_over_effluent'] = np.nan
        logger.info(
            f"Border-vs-effluent features: {df['border_over_effluent'].notna().sum()} rows compared, "
            f"{int(np.nansum(df['border_over_effluent']))} with border flow above effluent"
        )
    else:
        logger.warning(
            f"'{flow_col}' or 'sbiwtp_flow_mgd' missing — border-vs-effluent features left NaN"
        )
        for col in border_cols:
            df[col] = np.nan

    return df


PLANT_BALANCE_FEATURE_COLUMNS = [
    'sbiwtp_influent_mgd', 'sbiwtp_net_mgd', 'sbiwtp_capacity_mgd', 'sbiwtp_over_capacity',
    'border_flow_mgd', 'border_minus_effluent_mgd', 'border_over_effluent',
]


@asset(
    group_name="tijuana",
    key_prefix="ibwc",
    name="plant_balance",
    required_resource_keys={"s3"},
    ins={
        "influent_flow_current_year": AssetIn(key=AssetKey(["ibwc", "influent_flow_current_year"])),
        "effluent_flow_current_year": AssetIn(key=AssetKey(["ibwc", "effluent_flow_current_year"])),
        "boundary_cms": AssetIn(key=AssetKey(["streamflow", "boundary_cms"])),
    },
    config_schema={
        "capacity_mgd": Field(
            float,
            default_value=PLANT_CAPACITY_MGD,
            description="Rated treatment capacity of SBIWTP in MGD; overrides PLANT_CAPACITY_MGD for this run.",
        ),
    },
    automation_condition=AutomationCondition.eager(),
    metadata={
        "source": "IBWC AQWebportal — SBIWTP influent and effluent flow; Tijuana River at International Boundary (11013300)",
        "description": (
            "Daily SBIWTP plant balance: influent minus effluent, both against the "
            "plant's rated capacity, and the Tijuana River flow at the border against "
            "the plant's effluent. All flows in MGD."
        ),
        "variableMeasured": [
            "influent_mgd", "effluent_mgd", "net_mgd", "capacity_mgd", "over_capacity",
            "border_flow_mgd", "border_minus_effluent_mgd", "border_over_effluent",
            "border_over_effluent_hours",
        ],
    },
)
def plant_balance_asset(
    context,
    influent_flow_current_year: pd.DataFrame,
    effluent_flow_current_year: pd.DataFrame,
    boundary_cms: pd.DataFrame,
):
    """Publish the daily plant balance and a small current-value JSON."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    metadata = store_assets.objectMetadata(
        name=str(context.asset_key.path[-1]),
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    capacity = context.op_config["capacity_mgd"]

    influent = daily_series(influent_flow_current_year)
    effluent = daily_series(effluent_flow_current_year)
    logger.info(f"Influent: {len(influent)} days to {influent.index.max()}; effluent: {len(effluent)} days to {effluent.index.max()}")

    try:
        border = border_hourly_mgd(boundary_cms)
        logger.info(f"Border gauge: {len(border)} hourly readings to {border.index.max()}")
    except ValueError as e:
        logger.warning(f"Border gauge unavailable, publishing plant balance without it: {e}")
        border = None

    frame = plant_balance(influent, effluent, border_mgd=border, capacity=capacity)

    observed = frame.dropna(subset=['influent_mgd', 'effluent_mgd'])
    if observed.empty:
        raise Exception("no day has both influent and effluent; cannot publish a plant balance")

    suspect_days = int(frame['influent_suspect'].fillna(False).sum())
    if suspect_days:
        logger.warning(
            f"{suspect_days} days report near-zero influent while the plant discharged; "
            "flagged influent_suspect and excluded from the balance"
        )
    over_cap = int(observed['over_capacity'].fillna(False).sum())
    compared = frame.dropna(subset=['border_over_effluent'])
    border_over = int(compared['border_over_effluent'].sum()) if not compared.empty else 0
    logger.info(
        f"{len(observed)} days with both flows; net flow median {observed['net_mgd'].median():.2f} MGD; "
        f"{over_cap} days over {capacity} MGD capacity; "
        f"border flow above effluent on {border_over} of {len(compared)} compared days"
    )

    published = frame.reset_index()
    published['date'] = published['date'].dt.strftime('%Y-%m-%d')
    store_assets.store_dataframe_to_s3(
        published,
        f"{s3_output_path}/plant_balance/",
        "plant_balance",
        s3_resource,
        metadata=metadata,
        enable_latest_path=True,
        latestdatasetpath=f"{s3_latest_path}/daily",
        formats=["csv", "parquet"],
    )

    latest_day = observed.index[-1]
    latest = frame.loc[latest_day]
    current = {
        "date": latest_day.strftime('%Y-%m-%d'),
        **{col: _clean(latest[col]) for col in frame.columns},
        "capacity_source": "run config" if capacity != PLANT_CAPACITY_MGD else "PLANT_CAPACITY_MGD",
        "lag_days": 0,
        "generated_at": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        "note": (
            "net_mgd is influent minus effluent at SBIWTP for the day shown. Capacity flags "
            "compare each flow with the plant's rated capacity in force that day. Border "
            "figures compare the Tijuana River at the international boundary (converted from "
            "m3/s) with the day's effluent. No lag is applied here; the model features are "
            "lagged one day."
        ),
    }
    current_metadata = metadata.copy()
    current_metadata.name = "plant_balance_current"
    current_metadata.description = "Latest SBIWTP plant balance: net flow, capacity flags and border-vs-effluent comparison"
    store_assets.text_to_s3(
        json.dumps(current),
        f"{store_assets.get_latest_basepath()}/{s3_latest_path}/daily/plant_balance_current.json",
        s3_resource,
        contenttype="application/json",
        metadata=current_metadata,
    )

    context.add_output_metadata({
        "days": len(frame),
        "days_with_both_flows": len(observed),
        "latest_date": current["date"],
        "latest_influent_mgd": current["influent_mgd"],
        "latest_effluent_mgd": current["effluent_mgd"],
        "latest_net_mgd": current["net_mgd"],
        "capacity_mgd": capacity,
        "days_influent_suspect": suspect_days,
        "days_over_capacity": over_cap,
        "days_border_over_effluent": border_over,
        "days_border_compared": len(compared),
    })
    return published


def _clean(value):
    """NaN and pandas NA are not valid JSON; publish null instead."""
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (float, np.floating)) and np.isnan(value):
        return None
    if isinstance(value, (int, np.integer)):
        return int(value)
    return round(float(value), 3)
