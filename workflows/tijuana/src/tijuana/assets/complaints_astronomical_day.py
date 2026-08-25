"""Environmental complaints on the astronomical-day frame, linked to H2S nights.

Complaints are irregular events rather than a regular grid, so they use
``astro_calendar.attach_astro_frame_to_events()`` -- the frame is computed
directly from solar geometry per timestamp rather than joined to a 15-minute
calendar.

Because an astronomical day runs sunset(D) -> sunset(D+1), an overnight odour
event and the complaints it prompts the following morning fall in the *same*
unit. A calendar day splits them across two dates, which is what made linking
complaints to H2S events awkward.
"""

import pandas as pd
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

from ..utils import astro_calendar
from .astronomical_day import DATA_FORMATS, LATEST, OUTPUT_PATH

COMPLAINTS_OUTPUT = "tijuana/sd_complaints/output/astronomical_day/"
COMPLAINTS_LATEST = "tijuana/sd_complaints/astronomical_day"

#: Complaints carry geometry, so geojson travels with the astronomical frame.
COMPLAINT_FORMATS = ["csv", "geojson", "parquet"]


@asset(
    group_name="tijuana",
    key_prefix="complaints",
    name="sd_complaints_astronomical_day",
    required_resource_keys={"s3"},
    ins={"sd_complaints": AssetIn(key=AssetKey(["complaints", "sd_complaints"]))},
    metadata={
        "source": "complaints/sd_complaints placed on the astronomical calendar",
        "description": (
            "San Diego APCD environmental complaints placed on the sunset-to-sunset "
            "astronomical day, with night phase, week and day-of-year indices, "
            "twilight times and solar position. Because the frame runs sunset to "
            "sunset, an overnight odour event and the complaints it prompts the next "
            "morning share one astro_day_date. Rows whose source record carries no "
            "time of day (time_of_day_known = false) are framed at local midnight and "
            "should be excluded from sub-daily analysis."
        ),
        "variableMeasured": [
            "Complaints",
            "Nature of Complaint",
            "Astronomical Day",
            "Night of Year",
            "Night Fraction",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def sd_complaints_astronomical_day(context, sd_complaints):
    """Attach the astronomical frame to complaint events."""
    logger = get_dagster_logger()
    meta = context.assets_def.metadata_by_key[context.asset_key]

    # Keep the GeoDataFrame: store_assets routes it through geodataframe_to_s3,
    # which drops geometry for the parquet rendering and keeps it for geojson.
    df = sd_complaints.copy()
    if "datetime" not in df.columns:
        raise KeyError("sd_complaints has no 'datetime' column")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True).dt.tz_convert(
        astro_calendar.TZ
    )

    before = len(df)
    framed = astro_calendar.attach_astro_frame_to_events(df, time_col="datetime")
    if len(framed) != before:
        raise ValueError(f"framing changed row count: {before} -> {len(framed)}")

    known = (
        int(framed["time_of_day_known"].sum()) if "time_of_day_known" in framed.columns else 0
    )
    night = framed["day_night"] == "night"
    logger.info(
        f"Framed {len(framed)} complaints onto {framed['astro_day_date'].nunique()} "
        f"astronomical days; {known} have a real time of day; {int(night.sum())} at night"
    )

    metadata = store_assets.objectMetadata(
        name="sd_complaints_astronomical_day",
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )
    # geopandas converts datetime64 columns to ISO strings when writing geojson but
    # leaves object-dtype python dates alone, and json.dumps cannot encode those.
    # Cast for the stored copy only: the returned frame keeps real dates so
    # h2s_nightly_summary_with_complaints can still join on astro_day_date.
    to_store = framed.copy()
    to_store["astro_day_date"] = to_store["astro_day_date"].astype("string")

    store_assets.store_dataframe_to_s3(
        to_store,
        COMPLAINTS_OUTPUT,
        "sd_complaints_astronomical_day",
        context.resources.s3,
        latestdatasetpath=COMPLAINTS_LATEST,
        enable_latest_path=True,
        formats=COMPLAINT_FORMATS,
        metadata=metadata,
    )
    context.add_output_metadata(
        {
            "complaints": len(framed),
            "with_time_of_day": known,
            "date_only": len(framed) - known,
            "astro_days": int(framed["astro_day_date"].nunique()),
            "night_complaints": int(night.sum()),
            "day_complaints": int((~night).sum()),
            "first_astro_day": str(framed["astro_day_date"].min()),
            "last_astro_day": str(framed["astro_day_date"].max()),
        }
    )
    return framed


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="h2s_nightly_summary_with_complaints",
    required_resource_keys={"s3"},
    ins={
        "h2s_nightly_summary": AssetIn(key=AssetKey(["h2sforecast", "h2s_nightly_summary"])),
        "sd_complaints_astronomical_day": AssetIn(
            key=AssetKey(["complaints", "sd_complaints_astronomical_day"])
        ),
    },
    metadata={
        "source": "h2sforecast/h2s_nightly_summary and complaints/sd_complaints_astronomical_day",
        "description": (
            "Per-night, per-site H2S summary with the count of environmental "
            "complaints falling in the same astronomical day. Because the frame runs "
            "sunset to sunset, an overnight event and the next morning's complaints "
            "are attributed to the same night with no guesswork about which calendar "
            "date to use. Complaint counts are night-wide, not per-site: they are "
            "attached to every site row for that night, because a complaint's location "
            "is not matched to a monitoring station. Complaints without a real time of "
            "day are excluded from the attribution."
        ),
        "variableMeasured": [
            "H2S",
            "Exceedance Hours",
            "Complaints",
            "Astronomical Day",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def h2s_nightly_summary_with_complaints(
    context, h2s_nightly_summary, sd_complaints_astronomical_day
):
    """Attach per-night complaint counts to the nightly H2S summary."""
    logger = get_dagster_logger()
    meta = context.assets_def.metadata_by_key[context.asset_key]

    complaints = sd_complaints_astronomical_day
    if "time_of_day_known" in complaints.columns:
        # Date-only records sit at local midnight, which is not a real observation
        # time; counting them would push every one into the preceding night.
        usable = complaints[complaints["time_of_day_known"].fillna(False).astype(bool)]
    else:
        usable = complaints
    dropped = len(complaints) - len(usable)
    if dropped:
        logger.info(f"Excluded {dropped} date-only complaints from night attribution")

    counts = (
        usable.groupby("astro_day_date")
        .agg(
            complaints_total=("astro_day_date", "size"),
            complaints_at_night=("is_night", "sum"),
        )
        .reset_index()
    )

    before = len(h2s_nightly_summary)
    merged = h2s_nightly_summary.merge(counts, on="astro_day_date", how="left")
    if len(merged) != before:
        raise ValueError(f"merge changed row count: {before} -> {len(merged)}")
    for col in ("complaints_total", "complaints_at_night"):
        merged[col] = merged[col].fillna(0).astype(int)

    with_any = int((merged["complaints_total"] > 0).sum())
    logger.info(
        f"{len(merged)} night-site rows; {with_any} have at least one complaint "
        f"in the same astronomical day"
    )

    metadata = store_assets.objectMetadata(
        name="h2s_nightly_summary_with_complaints",
        description=meta["description"],
        source_url=meta.get("source"),
        variableMeasured=meta.get("variableMeasured"),
    )
    store_assets.store_dataframe_to_s3(
        merged,
        OUTPUT_PATH,
        "h2s_nightly_summary_with_complaints",
        context.resources.s3,
        latestdatasetpath=LATEST,
        enable_latest_path=True,
        formats=DATA_FORMATS,
        metadata=metadata,
    )
    context.add_output_metadata(
        {
            "night_site_rows": len(merged),
            "rows_with_complaints": with_any,
            "complaints_attributed": int(
                merged.groupby("astro_day_date")["complaints_total"].first().sum()
            ),
            "complaints_excluded_date_only": dropped,
        }
    )
    return merged


@asset_check(
    asset=AssetKey(["complaints", "sd_complaints_astronomical_day"]),
    description="Framed complaints keep their row count and carry a usable astronomical frame.",
)
def sd_complaints_astronomical_day_check(
    context: AssetCheckExecutionContext, sd_complaints_astronomical_day
) -> AssetCheckResult:
    df = sd_complaints_astronomical_day
    failures = []
    if df.empty:
        failures.append("no complaints")
    else:
        dated = df[df["datetime"].notna()]
        if dated["astro_day_date"].isna().any():
            failures.append("rows with a timestamp but no astro_day_date")
        outside = (dated["datetime"] < dated["astro_day_start"]) | (
            dated["datetime"] >= dated["astro_day_end"]
        )
        if outside.any():
            failures.append(f"{int(outside.sum())} rows outside their own astro day")
        nf = df["night_fraction"].dropna()
        if len(nf) and (nf.min() < 0 or nf.max() > 1):
            failures.append("night_fraction out of [0, 1]")
        if "time_of_day_known" in df.columns and not df["time_of_day_known"].any():
            failures.append("no complaint has a real time of day - check date_and_time_received")

    known = int(df["time_of_day_known"].sum()) if "time_of_day_known" in df.columns else 0
    return AssetCheckResult(
        passed=not failures,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "complaints": len(df),
            "with_time_of_day": known,
            "failures": "; ".join(failures) if failures else "none",
        },
    )
