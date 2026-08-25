"""Night-centred analysis assets built on the astronomical-day frame.

Published alongside the existing clock-based `h2s_peaks` and
`h2s_exceedance_periods` assets rather than replacing them, so no current
consumer changes. See `docs/tj_data_basis.md` for the before/after comparison.
"""

from datetime import datetime

from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetIn,
    AssetKey,
    AutomationCondition,
    asset,
    asset_check,
    get_dagster_logger,
)

from resilient_core.utils import store_assets

from ..utils import night_analysis
from .astronomical_day import DATA_FORMATS, LATEST, OUTPUT_PATH

REFRAMED_TRAINING = AssetKey(["h2sforecast", "modeldata_h2s_nofill_astronomical_day"])


def _metadata_for(context, dataset: str):
    meta = context.assets_def.metadata_by_key[context.asset_key]
    return store_assets.objectMetadata(
        name=dataset,
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )


def _store(context, df, dataset: str):
    store_assets.store_dataframe_to_s3(
        df,
        OUTPUT_PATH,
        dataset,
        context.resources.s3,
        latestdatasetpath=LATEST,
        enable_latest_path=True,
        formats=DATA_FORMATS,
        metadata=_metadata_for(context, dataset),
    )


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_peaks_astronomical_day",
    required_resource_keys={"s3"},
    ins={"reframed": AssetIn(key=REFRAMED_TRAINING)},
    metadata={
        "source": "h2sforecast/modeldata_h2s_nofill_astronomical_day",
        "description": (
            "H2S threshold exceedance counts per site, astronomical day and day/night "
            "segment. The astronomical-frame counterpart of h2s_peaks, which splits "
            "day and night at a hardcoded 6 AM / 6 PM clock boundary. Two deliberate "
            "differences: the split here is the true sunset/sunrise boundary, and "
            "counts cover measured observations only rather than including gap-filled "
            "values. Published alongside h2s_peaks, which is unchanged."
        ),
        "variableMeasured": ["H2S", "Exceedance Counts", "Astronomical Day"],
    },
    automation_condition=AutomationCondition.eager(),
)
def h2s_peaks_astronomical_day(context, reframed):
    """Exceedance counts on the true sun boundary."""
    logger = get_dagster_logger()
    segments = night_analysis.exceedances_by_segment(reframed)
    if segments.empty:
        logger.warning("No valid H2S measurements; nothing to summarise")
        return segments

    segments["date_processed"] = datetime.now().isoformat()
    night = segments[segments["period"] == "night"]
    day = segments[segments["period"] == "day"]
    logger.info(
        f"{len(segments)} segments; exceedance-hours >5ppb: "
        f"night {int(night['count_exceeds_5'].sum())}, day {int(day['count_exceeds_5'].sum())}"
    )

    _store(context, segments, "h2s_peaks_astronomical_day")
    context.add_output_metadata(
        {
            "segments": len(segments),
            "astro_days": int(segments["astro_day_date"].nunique()),
            "night_exceedance_hours_5ppb": int(night["count_exceeds_5"].sum()),
            "day_exceedance_hours_5ppb": int(day["count_exceeds_5"].sum()),
            "night_exceedance_hours_30ppb": int(night["count_exceeds_30"].sum()),
            "day_exceedance_hours_30ppb": int(day["count_exceeds_30"].sum()),
        }
    )
    return segments


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_nightly_summary",
    required_resource_keys={"s3"},
    ins={"reframed": AssetIn(key=REFRAMED_TRAINING)},
    metadata={
        "source": "h2sforecast/modeldata_h2s_nofill_astronomical_day",
        "description": (
            "One row per astronomical night per site: peak H2S and when it occurred "
            "(as a fraction of the night as well as hours after sunset), hours above "
            "the 5 and 30 ppb thresholds, vector-mean overnight wind, and the night's "
            "flow, effluent, tide and meteorological conditions. Because a night is a "
            "single unit in the astronomical frame, these per-event statistics no "
            "longer have to be stitched across a midnight boundary."
        ),
        "variableMeasured": [
            "H2S",
            "Exceedance Hours",
            "Peak Timing",
            "Wind Direction",
            "Wind Speed",
            "Streamflow",
            "SBIWTP Effluent Flow",
            "Tide Height",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def h2s_nightly_summary(context, reframed):
    """Per-night, per-site summary -- the headline analysis product."""
    logger = get_dagster_logger()
    summary = night_analysis.summarize_nights(reframed)
    if summary.empty:
        logger.warning("No night rows; nothing to summarise")
        return summary

    with_h2s = summary[summary["h2s_observations"] > 0]
    logger.info(
        f"{len(summary)} night-site rows across {summary['astro_day_date'].nunique()} nights; "
        f"{len(with_h2s)} with H2S data"
    )

    _store(context, summary, "h2s_nightly_summary")
    context.add_output_metadata(
        {
            "night_site_rows": len(summary),
            "nights": int(summary["astro_day_date"].nunique()),
            "sites": int(summary["site_name"].nunique()),
            "nights_with_h2s": len(with_h2s),
            "nights_above_5ppb": int((summary["hours_above_5"] > 0).sum()),
            "nights_above_30ppb": int((summary["hours_above_30"] > 0).sum()),
            "max_h2s": float(summary["h2s_max"].max()) if not with_h2s.empty else 0.0,
            "first_night": str(summary["astro_day_date"].min()),
            "last_night": str(summary["astro_day_date"].max()),
        }
    )
    return summary


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_exceedance_periods_astronomical_day",
    required_resource_keys={"s3"},
    ins={
        "reframed": AssetIn(key=REFRAMED_TRAINING),
        "h2s_peaks_astronomical_day": AssetIn(
            key=AssetKey(["h2sforecast", "h2s_peaks_astronomical_day"])
        ),
    },
    metadata={
        "source": "h2sforecast/modeldata_h2s_nofill_astronomical_day and h2s_peaks_astronomical_day",
        "description": (
            "Full hourly environmental data for every astronomical day/night segment "
            "that saw at least one H2S exceedance, published separately for the 5 ppb "
            "and 30 ppb thresholds. The astronomical-frame counterpart of "
            "h2s_exceedance_periods, which windows on a 6 AM / 6 PM clock split."
        ),
        "variableMeasured": [
            "H2S Exceedances",
            "Astronomical Day",
            "Wind Direction",
            "Wind Speed",
            "Streamflow",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def h2s_exceedance_periods_astronomical_day(context, reframed, h2s_peaks_astronomical_day):
    """Hourly data for exceedance segments, one dataset per threshold."""
    logger = get_dagster_logger()
    results = {}
    counts = {}
    for threshold in night_analysis.THRESHOLDS:
        filtered = night_analysis.filter_exceedance_segments(
            reframed, h2s_peaks_astronomical_day, threshold
        )
        key = f"h2s_exceeds_{threshold}"
        results[key] = filtered
        counts[f"rows_{threshold}ppb"] = len(filtered)
        if filtered.empty:
            logger.info(f"No segments exceeded {threshold} ppb")
            continue
        counts[f"segments_{threshold}ppb"] = int(
            filtered.groupby(["site_name", "astro_day_date", "period"]).ngroups
        )
        logger.info(
            f"{threshold} ppb: {len(filtered)} hourly rows across "
            f"{counts[f'segments_{threshold}ppb']} segments"
        )
        _store(context, filtered, f"h2s_exceedance_model_data_{threshold}ppb_astronomical_day")

    context.add_output_metadata(counts)
    return results


@asset_check(
    asset=AssetKey(["h2sforecast", "h2s_nightly_summary"]),
    description="Nightly summary invariants: one row per night per site, peak timing inside the night.",
)
def h2s_nightly_summary_check(
    context: AssetCheckExecutionContext, h2s_nightly_summary
) -> AssetCheckResult:
    df = h2s_nightly_summary
    failures = []
    if df.empty:
        failures.append("summary is empty")
    else:
        if df.duplicated(["astro_day_date", "site_name"]).any():
            failures.append("duplicate night/site rows")
        nf = df["peak_night_fraction"].dropna()
        if len(nf) and (nf.min() < 0 or nf.max() > 1):
            failures.append(f"peak_night_fraction out of [0, 1]: [{nf.min():.3f}, {nf.max():.3f}]")
        with_peak = df[df["h2s_observations"] > 0]
        if with_peak["peak_time"].isna().any():
            failures.append("rows with H2S observations but no peak_time")
        for thr in night_analysis.THRESHOLDS:
            bad = df[f"hours_above_{thr}"] > df["h2s_observations"]
            if bad.any():
                failures.append(f"hours_above_{thr} exceeds the observation count on {int(bad.sum())} rows")
        steadiness = df["wind_steadiness"].dropna()
        if len(steadiness) and (steadiness.min() < 0 or steadiness.max() > 1.000001):
            failures.append("wind_steadiness outside [0, 1]")

    return AssetCheckResult(
        passed=not failures,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "rows": len(df),
            "nights": int(df["astro_day_date"].nunique()) if not df.empty else 0,
            "failures": "; ".join(failures) if failures else "none",
        },
    )
