"""Astronomical-day calendar asset.

Publishes the reusable sunset-to-sunset calendar that the ``_astronomical_day``
variants of the H2S modeling datasets join against. See
``tijuana.utils.astro_calendar`` for the frame definition and
``docs/tj_data_basis.md`` for the design rationale.
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

from ..utils import astro_calendar

OUTPUT_PATH = "tijuana/forecast_data/output/astronomical_day/"
LATEST = "tijuana/forecast_data/astronomical_day"

#: The calendar is a machine-side join table, never read by hand, and its eleven
#: tz-aware timestamp columns make CSV several times larger than parquet.
FORMATS = ["parquet"]

#: Years beyond the current one to generate, so forecast timestamps always match.
LOOKAHEAD_YEARS = 1

#: The reframed data assets keep the existing multi-format output.
DATA_FORMATS = ["csv", "parquet"]



@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="astronomical_calendar",
    required_resource_keys={"s3"},
    metadata={
        "source": "Computed from astral 3.2 solar geometry for San Diego (32.7157, -117.1611)",
        "description": (
            "Sunset-to-sunset astronomical day calendar for the Tijuana River H2S "
            "modeling datasets. Reframes time from a midnight-anchored calendar day "
            "to an astronomical day so that a night -- when H2S events overwhelmingly "
            "occur -- is contained whole in one unit instead of being split across two "
            "calendar dates. Published at 15-minute and hourly grain; the hourly frame "
            "is the exact on-the-hour subset of the 15-minute one."
        ),
        "variableMeasured": [
            "Astronomical Day",
            "Night of Year",
            "Week of Year",
            "Sunrise",
            "Sunset",
            "Twilight",
            "Night Fraction",
            "Solar Elevation",
            "Solar Azimuth",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def astronomical_calendar(context):
    """Generate and publish the astronomical calendar at 15-minute and hourly grain.

    Returns the 15-minute frame. Downstream assets that need the hourly grain call
    ``astro_calendar.to_hourly()`` on it rather than reading a second dataset, which
    is what guarantees the two grains can never disagree.
    """
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    variableMeasured = meta.get("variableMeasured")

    s3_resource = context.resources.s3
    logger = get_dagster_logger()

    start_year = astro_calendar.START_YEAR
    end_year = datetime.now().year + LOOKAHEAD_YEARS
    logger.info(f"Building astronomical calendar for {start_year}-{end_year}")

    calendar = astro_calendar.build_astro_calendar(start_year, end_year, freq="15min")

    failures = astro_calendar.validate_astro_calendar(calendar)
    if failures:
        raise ValueError("astronomical calendar failed validation: " + "; ".join(failures))

    hourly = astro_calendar.to_hourly(calendar)
    logger.info(f"Built {len(calendar)} 15-minute rows, {len(hourly)} hourly rows")

    for grain, df in (("15min", calendar), ("hourly", hourly)):
        dataset = f"astronomical_calendar_{grain}"
        grain_metadata = store_assets.objectMetadata(
            name=dataset,
            description=f"{description} This is the {grain} grain.",
            source_url=source_url,
            variableMeasured=variableMeasured,
        )
        store_assets.store_dataframe_to_s3(
            df,
            OUTPUT_PATH,
            dataset,
            s3_resource,
            latestdatasetpath=LATEST,
            enable_latest_path=True,
            formats=FORMATS,
            metadata=grain_metadata,
        )

    complete = calendar.loc[calendar["astro_day_complete"], "astro_day_date"]
    context.add_output_metadata(
        {
            "start_year": start_year,
            "end_year": end_year,
            "rows_15min": len(calendar),
            "rows_hourly": len(hourly),
            "astro_days_total": int(calendar["astro_day_date"].nunique()),
            # The two grid-edge days are truncated by the range bounds and flagged
            # incomplete so per-night aggregation can drop them.
            "astro_days_complete": int(complete.nunique()),
            "first_astro_day": str(calendar["astro_day_date"].min()),
            "last_astro_day": str(calendar["astro_day_date"].max()),
            "validation": "passed",
        }
    )
    return calendar


def _store_reframed(context, df, dataset, description, source_url, variableMeasured):
    """Publish a reframed dataset and record the frame's coverage as run metadata."""
    metadata = store_assets.objectMetadata(
        name=dataset,
        description=description,
        source_url=source_url,
        variableMeasured=variableMeasured,
    )
    store_assets.store_dataframe_to_s3(
        df,
        OUTPUT_PATH,
        dataset,
        context.resources.s3,
        latestdatasetpath=LATEST,
        enable_latest_path=True,
        formats=DATA_FORMATS,
        metadata=metadata,
    )
    nights = df.loc[df["is_night"] == 1, "astro_day_date"]
    context.add_output_metadata(
        {
            "rows": len(df),
            "sites": int(df["site_name"].nunique()) if "site_name" in df.columns else 0,
            "astro_days": int(df["astro_day_date"].nunique()),
            "nights_with_data": int(nights.nunique()),
            "first_astro_day": str(df["astro_day_date"].min()),
            "last_astro_day": str(df["astro_day_date"].max()),
            "complete_astro_days": int(df.loc[df["astro_day_complete"], "astro_day_date"].nunique()),
            "night_rows": int((df["is_night"] == 1).sum()),
            "day_rows": int((df["is_night"] == 0).sum()),
        }
    )


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_h2s_nofill_astronomical_day",
    required_resource_keys={"s3"},
    ins={
        "modeldata_h2s_nofill": AssetIn(key=AssetKey(["h2sforecast", "modeldata_h2s_nofill"])),
        "astronomical_calendar": AssetIn(key=AssetKey(["h2sforecast", "astronomical_calendar"])),
    },
    metadata={
        "source": "h2sforecast/modeldata_h2s_nofill joined to h2sforecast/astronomical_calendar",
        "description": (
            "Hourly H2S training data reframed onto the sunset-to-sunset astronomical "
            "day, so each night is a single unit rather than being split at midnight. "
            "Adds year / week / day-of-year / night-of-year indices, night phase, "
            "twilight times and solar position. Purely additive: every original "
            "column is unchanged."
        ),
        "variableMeasured": [
            "H2S",
            "Wind Direction",
            "Wind Speed",
            "Streamflow",
            "Astronomical Day",
            "Night of Year",
            "Night Fraction",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def modeldata_h2s_nofill_astronomical_day(context, modeldata_h2s_nofill, astronomical_calendar):
    """Reframe the hourly training dataset onto the astronomical day."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    logger = get_dagster_logger()

    hourly = astro_calendar.to_hourly(astronomical_calendar)
    before = len(modeldata_h2s_nofill)
    reframed = astro_calendar.attach_astro_frame(modeldata_h2s_nofill, hourly)
    logger.info(
        f"Reframed {before} hourly rows onto "
        f"{reframed['astro_day_date'].nunique()} astronomical days"
    )

    _store_reframed(
        context,
        reframed,
        "modeldata_h2s_nofill_astronomical_day",
        meta["description"],
        meta.get("source"),
        meta.get("variableMeasured"),
    )
    return reframed


@asset(
    group_name="tijuana",
    key_prefix="h2sforecast",
    name="modeldata_forecast_15min_astronomical_day",
    required_resource_keys={"s3"},
    ins={
        "modeldata_forecast_15min": AssetIn(
            key=AssetKey(["h2sforecast", "modeldata_forecast_15min"])
        ),
        "astronomical_calendar": AssetIn(key=AssetKey(["h2sforecast", "astronomical_calendar"])),
    },
    metadata={
        "source": "h2sforecast/modeldata_forecast_15min joined to h2sforecast/astronomical_calendar",
        "description": (
            "15-minute H2S forecast inference data reframed onto the sunset-to-sunset "
            "astronomical day, matching the training dataset's frame so night-relative "
            "features mean the same thing at train and serve time. Purely additive: "
            "every original column is unchanged. day_night comes from the shared "
            "astronomical calendar, the same source as the training data."
        ),
        "variableMeasured": [
            "Wind Direction",
            "Wind Speed",
            "Tide Height",
            "Streamflow",
            "Astronomical Day",
            "Night of Year",
            "Night Fraction",
        ],
    },
    automation_condition=AutomationCondition.eager(),
)
def modeldata_forecast_15min_astronomical_day(
    context, modeldata_forecast_15min, astronomical_calendar
):
    """Reframe the 15-minute forecast dataset onto the astronomical day."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    logger = get_dagster_logger()

    before = len(modeldata_forecast_15min)
    # Strict: modeldata_forecast_15min now takes day_night from the same shared
    # calendar, so any disagreement at all is a regression, not a difference of
    # solar model. This is what would fail if a second source were reintroduced.
    reframed = astro_calendar.attach_astro_frame(modeldata_forecast_15min, astronomical_calendar)
    logger.info(
        f"Reframed {before} 15-minute rows onto "
        f"{reframed['astro_day_date'].nunique()} astronomical days"
    )

    _store_reframed(
        context,
        reframed,
        "modeldata_forecast_15min_astronomical_day",
        meta["description"],
        meta.get("source"),
        meta.get("variableMeasured"),
    )
    return reframed


def _frame_check_result(df, label: str) -> AssetCheckResult:
    """Run the reframed-dataset invariants and report them as an asset check."""
    failures = astro_calendar.validate_reframed(df)
    summary = astro_calendar.reframed_frame_summary(df)
    return AssetCheckResult(
        passed=not failures,
        severity=AssetCheckSeverity.ERROR,
        metadata={**summary, "failures": "; ".join(failures) if failures else "none", "dataset": label},
    )


@asset_check(
    asset=AssetKey(["h2sforecast", "astronomical_calendar"]),
    description="Calendar invariants: no nulls, no split astro days, rows inside their own day bounds.",
)
def astronomical_calendar_check(
    context: AssetCheckExecutionContext, astronomical_calendar
) -> AssetCheckResult:
    failures = astro_calendar.validate_astro_calendar(astronomical_calendar)
    complete = astronomical_calendar.loc[
        astronomical_calendar["astro_day_complete"], "astro_day_date"
    ]
    return AssetCheckResult(
        passed=not failures,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "rows": len(astronomical_calendar),
            "astro_days": int(astronomical_calendar["astro_day_date"].nunique()),
            "complete_astro_days": int(complete.nunique()),
            "failures": "; ".join(failures) if failures else "none",
        },
    )


@asset_check(
    asset=AssetKey(["h2sforecast", "modeldata_h2s_nofill_astronomical_day"]),
    description="Reframed training data invariants, including that no night is split across two astro days.",
)
def modeldata_h2s_nofill_astronomical_day_check(
    context: AssetCheckExecutionContext, modeldata_h2s_nofill_astronomical_day
) -> AssetCheckResult:
    return _frame_check_result(
        modeldata_h2s_nofill_astronomical_day, "modeldata_h2s_nofill_astronomical_day"
    )


@asset_check(
    asset=AssetKey(["h2sforecast", "modeldata_forecast_15min_astronomical_day"]),
    description="Reframed forecast data invariants, including that no night is split across two astro days.",
)
def modeldata_forecast_15min_astronomical_day_check(
    context: AssetCheckExecutionContext, modeldata_forecast_15min_astronomical_day
) -> AssetCheckResult:
    return _frame_check_result(
        modeldata_forecast_15min_astronomical_day, "modeldata_forecast_15min_astronomical_day"
    )
