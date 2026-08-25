"""Astronomical-day calendar for the Tijuana River H2S modeling datasets.

Reframes time from a midnight-anchored calendar day to a **sunset-to-sunset
astronomical day**, so that a night -- when H2S events overwhelmingly occur --
is always contained whole inside exactly one unit instead of being split across
two calendar dates.

An astronomical day ``D`` runs from sunset on calendar date ``D`` to sunset on
``D + 1``. It has two segments:

    sunset(D) ............ sunrise(D+1) ............ sunset(D+1)
    |<---- night segment ---->|<---- day segment ---->|
    |<-------------- astro day D -------------------->|

Design notes
------------
* The timestamp grid is built in **UTC** and converted to
  ``America/Los_Angeles``. All elapsed-time columns are derived from the UTC
  values, so the 23-hour and 25-hour local days created by DST transitions
  never produce a gap, a repeat, or a silently wrong duration. Never subtract
  two tz-aware local timestamps directly.
* A single day-of-year template cannot be reused across years: sun times drift
  annually, leap years shift day-of-year alignment, and DST transition dates
  move. The calendar is therefore generated per year over the full range.
* This module is deliberately free of Dagster and S3 imports so it can be
  unit-tested directly.
"""

from __future__ import annotations

import datetime as dt
from typing import Iterable

import numpy as np
import pandas as pd
from astral import Depression, LocationInfo
from astral.sun import azimuth, dawn, dusk, elevation, sun

# Single source of truth for the observer. Previously inlined in
# hysplit_forecasting.add_day_night().
SAN_DIEGO = LocationInfo(
    name="San Diego",
    region="USA",
    timezone="America/Los_Angeles",
    latitude=32.7157,
    longitude=-117.1611,
)

TZ = SAN_DIEGO.timezone

#: Earliest year the calendar is generated for. Set to cover the APCD H2S record.
START_YEAR = 2015

#: Mean weeks per year, used as the period for the ISO-week cyclic encoding so
#: that week 53 -> week 1 is continuous.
WEEKS_PER_YEAR = 52.1775

#: Columns that carry the astronomical frame onto a dataset.
FRAME_COLUMNS = [
    "astro_day_start",
    "astro_day_end",
    "astro_day_date",
    "astro_day_complete",
    "astro_year",
    "astro_day_of_year",
    "astro_week_of_year",
    "astro_iso_year",
    "night_of_year",
    "hours_into_astro_day",
    "sunrise",
    "sunset",
    "solar_noon",
    "dawn_civil",
    "dusk_civil",
    "dawn_nautical",
    "dusk_nautical",
    "dawn_astronomical",
    "dusk_astronomical",
    "day_night",
    "is_night",
    "hours_since_sunset",
    "hours_to_sunrise",
    "night_length_hours",
    "day_length_hours",
    "night_fraction",
    "solar_elevation_deg",
    "solar_azimuth_deg",
    "doy_sin",
    "doy_cos",
    "week_sin",
    "week_cos",
    "night_fraction_sin",
    "night_fraction_cos",
    "is_dst",
    "utc_offset_hours",
    "dst_transition",
]


def _daily_sun_table(start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
    """Sunrise/sunset/noon and the three twilight pairs, one row per calendar date.

    Indexed by ``date``. All timestamps are tz-aware ``America/Los_Angeles``.
    """
    dates = pd.date_range(start_date, end_date, freq="D").date
    rows = []
    for date in dates:
        s = sun(SAN_DIEGO.observer, date=date, tzinfo=TZ)
        rows.append(
            {
                "date": date,
                "sunrise": s["sunrise"],
                "sunset": s["sunset"],
                "solar_noon": s["noon"],
                "dawn_civil": s["dawn"],
                "dusk_civil": s["dusk"],
                "dawn_nautical": dawn(SAN_DIEGO.observer, date, Depression.NAUTICAL, TZ),
                "dusk_nautical": dusk(SAN_DIEGO.observer, date, Depression.NAUTICAL, TZ),
                "dawn_astronomical": dawn(SAN_DIEGO.observer, date, Depression.ASTRONOMICAL, TZ),
                "dusk_astronomical": dusk(SAN_DIEGO.observer, date, Depression.ASTRONOMICAL, TZ),
            }
        )
    table = pd.DataFrame(rows).set_index("date")
    for col in table.columns:
        table[col] = pd.to_datetime(table[col], utc=True).dt.tz_convert(TZ)
    return table


def _hours_between(later: pd.Series, earlier: pd.Series) -> pd.Series:
    """Elapsed hours, computed in UTC so DST transitions cannot distort it."""
    return (later.dt.tz_convert("UTC") - earlier.dt.tz_convert("UTC")).dt.total_seconds() / 3600.0


def _solar_position(times: pd.DatetimeIndex) -> tuple[np.ndarray, np.ndarray]:
    """Solar elevation and azimuth in degrees for each timestamp."""
    obs = SAN_DIEGO.observer
    elev = np.fromiter((elevation(obs, t) for t in times), dtype=float, count=len(times))
    azim = np.fromiter((azimuth(obs, t) for t in times), dtype=float, count=len(times))
    return elev, azim


def _dst_transition_labels(local_dates: pd.Series, offsets: pd.Series) -> pd.Series:
    """Label every row of a 23-hour or 25-hour local day.

    A local date whose UTC offset changes between its first and last row is a
    DST transition day: offset increasing (-8 -> -7) is spring forward,
    decreasing is fall back.
    """
    frame = pd.DataFrame({"date": local_dates, "offset": offsets})
    first = frame.groupby("date")["offset"].first()
    last = frame.groupby("date")["offset"].last()
    label = pd.Series(pd.NA, index=first.index, dtype="object")
    label[last > first] = "spring_forward"
    label[last < first] = "fall_back"
    return local_dates.map(label)


def build_astro_calendar(
    start_year: int = START_YEAR,
    end_year: int | None = None,
    freq: str = "15min",
    with_solar_position: bool = True,
) -> pd.DataFrame:
    """Build the astronomical calendar over ``[start_year, end_year]`` inclusive.

    Parameters
    ----------
    start_year, end_year:
        Inclusive year bounds in local time. ``end_year`` defaults to
        ``start_year``.
    freq:
        Pandas offset alias for the grid. ``"15min"`` is the canonical grain;
        the hourly frame is the exact subset where ``minute == 0``, which
        guarantees the two grains can never disagree.
    with_solar_position:
        Compute ``solar_elevation_deg`` / ``solar_azimuth_deg``. These are
        per-timestamp astral calls and dominate runtime; tests that do not need
        them can turn them off.

    Returns
    -------
    DataFrame with a ``time`` column (tz-aware ``America/Los_Angeles``) plus
    every column in :data:`FRAME_COLUMNS`.
    """
    if end_year is None:
        end_year = start_year
    if end_year < start_year:
        raise ValueError(f"end_year {end_year} precedes start_year {start_year}")

    # Grid bounds: local midnight on Jan 1 through the last step before local
    # midnight on Jan 1 of the following year. Built in UTC so the DST-affected
    # local days come out with their true 23 or 25 hours.
    local_start = pd.Timestamp(f"{start_year}-01-01 00:00", tz=TZ)
    local_end = pd.Timestamp(f"{end_year + 1}-01-01 00:00", tz=TZ)
    grid = pd.date_range(
        local_start.tz_convert("UTC"),
        local_end.tz_convert("UTC"),
        freq=freq,
        inclusive="left",
        tz="UTC",
    ).tz_convert(TZ)

    if end_year is None:
        end_year = start_year
    if end_year < start_year:
        raise ValueError(f"end_year {end_year} precedes start_year {start_year}")

    # Grid bounds: local midnight on Jan 1 through the last step before local
    # midnight on Jan 1 of the following year. Built in UTC so the DST-affected
    # local days come out with their true 23 or 25 hours.
    local_start = pd.Timestamp(f"{start_year}-01-01 00:00", tz=TZ)
    local_end = pd.Timestamp(f"{end_year + 1}-01-01 00:00", tz=TZ)
    grid = pd.date_range(
        local_start.tz_convert("UTC"),
        local_end.tz_convert("UTC"),
        freq=freq,
        inclusive="left",
        tz="UTC",
    ).tz_convert(TZ)
    return _frame_for_index(
        grid, with_solar_position=with_solar_position, grid_bounds=(local_start, local_end)
    )


def _frame_for_index(
    times,
    with_solar_position: bool = True,
    grid_bounds: tuple | None = None,
) -> pd.DataFrame:
    """Compute the astronomical-day frame for any set of tz-aware timestamps.

    Shared by :func:`build_astro_calendar`, which passes a regular grid, and
    :func:`frame_for_timestamps`, which passes irregular event times. Keeping one
    implementation is what stops a grid-derived frame and an event-derived frame
    disagreeing about the same instant.

    ``grid_bounds`` is the ``(start, end)`` of a generated range, used only to
    flag the two astro days truncated at its edges. Event data passes ``None``.
    """
    df = pd.DataFrame({"time": pd.DatetimeIndex(times)})
    local_date = df["time"].dt.date

    # One day of padding on each side: rows before the first sunset of the range
    # belong to the astro day opened on the preceding date, and the last astro
    # day closes on the sunset of the following date.
    sun_table = _daily_sun_table(
        min(local_date) - dt.timedelta(days=1),
        max(local_date) + dt.timedelta(days=1),
    )

    def lookup(column: str, dates: Iterable[dt.date]) -> pd.Series:
        return pd.Series(
            pd.Index(dates).map(sun_table[column]), index=df.index, dtype=f"datetime64[ns, {TZ}]"
        )

    # --- Sun times for the row's own calendar date -------------------------
    df["sunrise"] = lookup("sunrise", local_date)
    df["sunset"] = lookup("sunset", local_date)
    df["solar_noon"] = lookup("solar_noon", local_date)
    for col in (
        "dawn_civil",
        "dusk_civil",
        "dawn_nautical",
        "dusk_nautical",
        "dawn_astronomical",
        "dusk_astronomical",
    ):
        df[col] = lookup(col, local_date)

    # --- day/night -----------------------------------------------------------
    # Semantics are identical to the original add_day_night(): a row is 'day'
    # when sunrise <= t < sunset for its own calendar date. Preserved exactly so
    # existing is_night / source_regime features and trained models are unaffected.
    is_day = (df["time"] >= df["sunrise"]) & (df["time"] < df["sunset"])
    df["day_night"] = np.where(is_day, "day", "night")
    df["is_night"] = (~is_day).astype(int)

    # --- astronomical day assignment ----------------------------------------
    # The cycle opened by the most recent sunset at or before t.
    after_sunset = df["time"] >= df["sunset"]
    astro_date = pd.Series(
        np.where(after_sunset, local_date, local_date - dt.timedelta(days=1)), index=df.index
    )
    df["astro_day_date"] = astro_date
    df["astro_day_start"] = lookup("sunset", astro_date)
    df["astro_day_end"] = lookup("sunset", astro_date + dt.timedelta(days=1))
    next_sunrise = lookup("sunrise", astro_date + dt.timedelta(days=1))

    # The astro days at the two grid edges are truncated by the range bounds.
    # Flagged explicitly so per-night aggregation can exclude partial nights
    # instead of silently under-counting them.
    if grid_bounds is None:
        # Event data does not tile a range, so "complete" has no coverage meaning.
        df["astro_day_complete"] = True
    else:
        grid_start, grid_end = grid_bounds
        df["astro_day_complete"] = (df["astro_day_start"] >= grid_start) & (
            df["astro_day_end"] <= grid_end
        )

    astro_ts = pd.to_datetime(astro_date)
    iso = astro_ts.dt.isocalendar()
    df["astro_year"] = astro_ts.dt.year
    df["astro_day_of_year"] = astro_ts.dt.dayofyear
    df["astro_week_of_year"] = iso["week"].astype(int)
    df["astro_iso_year"] = iso["year"].astype(int)
    # Sequential night index within the astro year. Equal to astro_day_of_year
    # by construction, named explicitly because it is the analysis unit.
    df["night_of_year"] = df["astro_day_of_year"]

    # --- elapsed-time columns (all UTC-derived) ------------------------------
    df["hours_into_astro_day"] = _hours_between(df["time"], df["astro_day_start"])
    # Identical to hours_into_astro_day under the sunset-to-sunset boundary;
    # kept under its physical name, and would diverge if the boundary changed.
    df["hours_since_sunset"] = df["hours_into_astro_day"]
    # Signed: positive during the night segment (time remaining until sunrise),
    # negative during the day segment (time elapsed since that sunrise).
    df["hours_to_sunrise"] = _hours_between(next_sunrise, df["time"])

    df["night_length_hours"] = _hours_between(next_sunrise, df["astro_day_start"])
    df["day_length_hours"] = _hours_between(df["astro_day_end"], next_sunrise)

    in_night_segment = df["time"] < next_sunrise
    df["night_fraction"] = np.where(
        in_night_segment, df["hours_into_astro_day"] / df["night_length_hours"], np.nan
    )

    # --- solar position ------------------------------------------------------
    if with_solar_position:
        elev, azim = _solar_position(df["time"])
        df["solar_elevation_deg"] = elev
        df["solar_azimuth_deg"] = azim
    else:
        df["solar_elevation_deg"] = np.nan
        df["solar_azimuth_deg"] = np.nan

    # --- cyclic encodings ----------------------------------------------------
    days_in_year = np.where(astro_ts.dt.is_leap_year, 366.0, 365.0)
    df["doy_sin"] = np.sin(2 * np.pi * df["astro_day_of_year"] / days_in_year)
    df["doy_cos"] = np.cos(2 * np.pi * df["astro_day_of_year"] / days_in_year)
    df["week_sin"] = np.sin(2 * np.pi * df["astro_week_of_year"] / WEEKS_PER_YEAR)
    df["week_cos"] = np.cos(2 * np.pi * df["astro_week_of_year"] / WEEKS_PER_YEAR)
    # Half-cycle: night_fraction is a bounded 0->1 phase, not a wrapping angle.
    # sin peaks at mid-night; cos runs monotonically +1 at sunset to -1 at sunrise.
    df["night_fraction_sin"] = np.sin(np.pi * df["night_fraction"])
    df["night_fraction_cos"] = np.cos(np.pi * df["night_fraction"])

    # --- DST bookkeeping -----------------------------------------------------
    offsets = df["time"].apply(lambda t: t.utcoffset().total_seconds() / 3600.0)
    df["utc_offset_hours"] = offsets
    df["is_dst"] = offsets == -7.0
    df["dst_transition"] = _dst_transition_labels(pd.Series(local_date, index=df.index), offsets)

    return df[["time"] + FRAME_COLUMNS].reset_index(drop=True)


def frame_for_timestamps(times, with_solar_position: bool = True) -> pd.DataFrame:
    """The astronomical-day frame for irregular event timestamps.

    Complaint records, spill reports and similar event data are not on a regular
    grid, so they cannot use the exact-join path in :func:`attach_astro_frame`.
    This computes the frame directly from solar geometry for each timestamp, which
    is exact rather than snapping to the nearest grid row.

    ``astro_day_complete`` is always True here: it describes coverage of a
    generated range, which has no meaning for sparse events.
    """
    idx = pd.DatetimeIndex(pd.to_datetime(times))
    if idx.tz is None:
        raise TypeError("times must be tz-aware")
    return _frame_for_index(idx.tz_convert(TZ), with_solar_position=with_solar_position)


def attach_astro_frame_to_events(
    df: pd.DataFrame, time_col: str = "time", with_solar_position: bool = True
) -> pd.DataFrame:
    """Attach the astronomical-day frame to an event table, in row order.

    The counterpart of :func:`attach_astro_frame` for irregular timestamps. Row
    count and order are preserved; rows with a null timestamp get a null frame
    rather than being dropped.
    """
    if time_col not in df.columns:
        raise KeyError(f"{time_col!r} not in dataframe columns")
    times = pd.to_datetime(df[time_col])
    if not isinstance(times.dtype, pd.DatetimeTZDtype):
        raise TypeError(f"{time_col!r} must be tz-aware; got {times.dtype}")

    out = df.copy()
    usable = times.notna()
    if not usable.any():
        for col in FRAME_COLUMNS:
            out[col] = pd.NA
        return out

    frame = frame_for_timestamps(
        times[usable], with_solar_position=with_solar_position
    ).drop(columns=["time"])
    frame.index = times[usable].index
    for col in FRAME_COLUMNS:
        out[col] = frame[col] if col in frame.columns else pd.NA
    return out



def label_day_night(times: pd.Series) -> pd.Series:
    """Label each tz-aware timestamp ``'day'`` or ``'night'``.

    A timestamp is ``'day'`` when ``sunrise <= t < sunset`` for its own calendar
    date. This is the shared implementation behind
    ``hysplit_forecasting.add_day_night()`` and the calendar's own ``day_night``
    column, so the two can never drift apart. Output is identical to the original
    per-asset implementation; see ``test_day_night_matches_original_implementation``.
    """
    times = pd.to_datetime(times)
    if not isinstance(times.dtype, pd.DatetimeTZDtype):
        raise TypeError(f"times must be tz-aware; got {times.dtype}")
    local = times.dt.tz_convert(TZ)
    dates = local.dt.date
    sun_table = _daily_sun_table(min(dates), max(dates))
    sunrise = pd.Series(
        pd.Index(dates).map(sun_table["sunrise"]), index=times.index, dtype=f"datetime64[ns, {TZ}]"
    )
    sunset = pd.Series(
        pd.Index(dates).map(sun_table["sunset"]), index=times.index, dtype=f"datetime64[ns, {TZ}]"
    )
    is_day = (local >= sunrise) & (local < sunset)
    return pd.Series(np.where(is_day, "day", "night"), index=times.index)


def to_hourly(calendar: pd.DataFrame) -> pd.DataFrame:
    """The hourly frame: the exact subset of the 15-minute calendar on the hour."""
    return calendar[calendar["time"].dt.minute == 0].reset_index(drop=True)


def day_night_mismatches(
    df: pd.DataFrame, calendar: pd.DataFrame, time_col: str = "time"
) -> pd.DataFrame:
    """Rows whose existing ``day_night`` disagrees with the calendar.

    Returns the mismatched rows with a ``minutes_from_boundary`` column giving the
    distance to the nearer of that day's sunrise and sunset. Disagreement close to
    a boundary is expected when the dataset derived its labels from a different
    solar model -- ``modeldata_forecast_15min`` uses OpenMeteo's ``is_day`` flag
    rather than astral -- whereas disagreement far from a boundary indicates a
    real bug. Empty frame when the two agree everywhere.
    """
    if "day_night" not in df.columns:
        return pd.DataFrame(columns=[time_col, "minutes_from_boundary"])

    right = calendar.rename(columns={"time": time_col})[
        [time_col, "day_night", "sunrise", "sunset"]
    ].drop_duplicates(time_col)
    left = df[[time_col, "day_night"]].copy()
    left[time_col] = left[time_col].dt.tz_convert(right[time_col].dt.tz)

    check = left.merge(right, on=time_col, how="inner", suffixes=("_existing", "_calendar"))
    check = check[check["day_night_existing"] != check["day_night_calendar"]].copy()
    if check.empty:
        return check.assign(minutes_from_boundary=pd.Series(dtype=float))

    to_sunrise = (check[time_col] - check["sunrise"]).abs()
    to_sunset = (check[time_col] - check["sunset"]).abs()
    check["minutes_from_boundary"] = (
        pd.concat([to_sunrise, to_sunset], axis=1).min(axis=1).dt.total_seconds() / 60.0
    )
    return check


def attach_astro_frame(
    df: pd.DataFrame,
    calendar: pd.DataFrame,
    time_col: str = "time",
    drop_conflicts: bool = True,
    day_night_tolerance_minutes: float = 0.0,
) -> pd.DataFrame:
    """Left-join the astronomical frame onto ``df`` on an exact tz-aware timestamp.

    An exact merge is used rather than ``merge_asof``: both ``modeldata_h2s_nofill``
    (hourly) and ``modeldata_forecast_15min`` sit on clean tz-aware grids, so
    exactness is achievable and makes coverage gaps loud instead of silently
    snapping to a neighbour.

    Raises if the row count changes or any row fails to match -- either means the
    calendar range or grain is wrong.

    When ``df`` already carries ``day_night`` / ``is_night``, the existing columns
    win and the calendar's versions are asserted to agree. That check is the
    regression test proving the calendar reproduces the original add_day_night().

    ``day_night_tolerance_minutes`` permits disagreement within that many minutes
    of sunrise or sunset, for datasets that derived their labels from a different
    solar model. Disagreement further from a boundary always raises. The default
    of 0 is strict.
    """
    if time_col not in df.columns:
        raise KeyError(f"{time_col!r} not in dataframe columns")
    if not isinstance(df[time_col].dtype, pd.DatetimeTZDtype):
        raise TypeError(f"{time_col!r} must be tz-aware; got {df[time_col].dtype}")

    left = df.copy()
    right = calendar.rename(columns={"time": time_col})
    # Align to the calendar's timezone so the merge keys compare equal.
    left[time_col] = left[time_col].dt.tz_convert(right[time_col].dt.tz)

    conflicts = [c for c in right.columns if c != time_col and c in left.columns]
    if conflicts and drop_conflicts:
        _assert_day_night_agrees(left, calendar, time_col, day_night_tolerance_minutes)
        right = right.drop(columns=conflicts)

    before = len(left)
    merged = left.merge(right, on=time_col, how="left", validate="many_to_one")
    if len(merged) != before:
        raise ValueError(f"join changed row count: {before} -> {len(merged)}")

    unmatched = merged["astro_day_date"].isna().sum()
    if unmatched:
        missing = merged.loc[merged["astro_day_date"].isna(), time_col]
        raise ValueError(
            f"{unmatched} rows had no calendar match "
            f"(e.g. {missing.iloc[0]} .. {missing.iloc[-1]}); "
            "extend the calendar range or regenerate at a finer grain"
        )
    return merged


def _assert_day_night_agrees(
    left: pd.DataFrame, calendar: pd.DataFrame, time_col: str, tolerance_minutes: float
) -> None:
    """Verify the calendar's day/night labels match the ones already on the data."""
    mismatches = day_night_mismatches(left, calendar, time_col)
    if mismatches.empty:
        return
    beyond = mismatches[mismatches["minutes_from_boundary"] > tolerance_minutes]
    if not beyond.empty:
        worst = beyond["minutes_from_boundary"].max()
        sample = beyond[time_col].head(3).tolist()
        raise ValueError(
            f"calendar day_night disagrees with existing column on {len(beyond)} rows "
            f"more than {tolerance_minutes} min from sunrise/sunset "
            f"(worst {worst:.1f} min; e.g. {sample})"
        )


def validate_astro_calendar(calendar: pd.DataFrame) -> list[str]:
    """Return a list of failed invariant checks; empty means the calendar is sound."""
    failures: list[str] = []

    required = set(["time"] + FRAME_COLUMNS)
    if missing := required - set(calendar.columns):
        failures.append(f"missing columns: {sorted(missing)}")
        return failures

    never_null = [
        "astro_day_date",
        "astro_day_complete",
        "astro_day_of_year",
        "astro_week_of_year",
        "hours_into_astro_day",
        "astro_day_start",
        "astro_day_end",
        "day_night",
    ]
    for col in never_null:
        if (n := int(calendar[col].isna().sum())) > 0:
            failures.append(f"{col} has {n} nulls")

    if calendar["time"].duplicated().any():
        failures.append("duplicate timestamps in the grid")

    if not calendar["time"].is_monotonic_increasing:
        failures.append("timestamps are not monotonically increasing")

    # An astro day must be one continuous run of rows, never split.
    starts = calendar["astro_day_date"].ne(calendar["astro_day_date"].shift())
    if starts.sum() != calendar["astro_day_date"].nunique():
        failures.append("at least one astro_day_date appears in more than one run")

    # Every row must fall inside its own astro day.
    outside = (calendar["time"] < calendar["astro_day_start"]) | (
        calendar["time"] >= calendar["astro_day_end"]
    )
    if (n := int(outside.sum())) > 0:
        failures.append(f"{n} rows fall outside their own astro day bounds")

    hours = calendar["hours_into_astro_day"]
    if (n := int((hours < 0).sum())) > 0:
        failures.append(f"{n} rows have negative hours_into_astro_day")

    nf = calendar["night_fraction"].dropna()
    if len(nf) and (nf.min() < 0 or nf.max() > 1):
        failures.append(f"night_fraction out of [0, 1]: [{nf.min()}, {nf.max()}]")

    # night_fraction must be defined exactly on the night segment.
    night_rows = calendar["day_night"] == "night"
    if (n := int((calendar["night_fraction"].notna() & ~night_rows).sum())) > 0:
        failures.append(f"{n} day rows have a non-null night_fraction")

    return failures


def validate_reframed(
    df: pd.DataFrame, time_col: str = "time", site_col: str = "site_name"
) -> list[str]:
    """Return a list of failed invariant checks on a reframed dataset; empty means sound.

    Complements :func:`validate_astro_calendar`, which checks the calendar itself.
    These checks run against data that has been through :func:`attach_astro_frame`
    and are what Phase 4's asset checks surface.
    """
    failures: list[str] = []

    required = [
        "astro_day_date",
        "astro_day_of_year",
        "astro_week_of_year",
        "hours_into_astro_day",
        "astro_day_start",
        "astro_day_end",
        "night_length_hours",
        "day_night",
        "is_night",
        "night_fraction",
    ]
    if missing := [c for c in required if c not in df.columns]:
        return [f"missing frame columns: {missing}"]
    if df.empty:
        return ["dataset is empty"]

    for col in ("astro_day_date", "astro_day_of_year", "astro_week_of_year", "hours_into_astro_day"):
        if (n := int(df[col].isna().sum())) > 0:
            failures.append(f"{col} has {n} nulls")

    outside = (df[time_col] < df["astro_day_start"]) | (df[time_col] >= df["astro_day_end"])
    if (n := int(outside.sum())) > 0:
        failures.append(f"{n} rows fall outside their own astro day bounds")

    hours = df["hours_into_astro_day"]
    if (n := int(((hours < 0) | (hours >= 25)).sum())) > 0:
        failures.append(f"{n} rows have hours_into_astro_day outside [0, 25)")

    if (df["is_night"] != (df["day_night"] == "night").astype(int)).any():
        failures.append("is_night disagrees with day_night")

    night = df["day_night"] == "night"
    if (n := int((df["night_fraction"].notna() & ~night).sum())) > 0:
        failures.append(f"{n} day rows have a non-null night_fraction")
    if (n := int((df["night_fraction"].isna() & night).sum())) > 0:
        failures.append(f"{n} night rows have a null night_fraction")
    nf = df["night_fraction"].dropna()
    if len(nf) and (nf.min() < 0 or nf.max() > 1):
        failures.append(f"night_fraction out of [0, 1]: [{nf.min():.3f}, {nf.max():.3f}]")

    # The whole point of the reframing: a night's rows must all share one
    # astro_day_date, and so must span no more than that night's length.
    group_cols = ["astro_day_date"] + ([site_col] if site_col in df.columns else [])
    nights = df[night]
    if not nights.empty:
        spans = nights.groupby(group_cols, dropna=False).agg(
            first=(time_col, "min"), last=(time_col, "max"), night_len=("night_length_hours", "max")
        )
        span_hours = _hours_between(spans["last"], spans["first"])
        # One grid step of slack: the last sample can sit just short of sunrise.
        over = span_hours > spans["night_len"] + 1.0
        if (n := int(over.sum())) > 0:
            failures.append(f"{n} night groups span longer than the night itself")

    return failures


def reframed_frame_summary(
    df: pd.DataFrame, time_col: str = "time", site_col: str = "site_name"
) -> dict:
    """Coverage figures for a reframed dataset, for asset-check metadata."""
    night = df["day_night"] == "night"
    group_cols = ["astro_day_date"] + ([site_col] if site_col in df.columns else [])
    nights = df[night]
    crossing = 0
    if not nights.empty:
        dates_per_night = nights.groupby(group_cols, dropna=False)[time_col].apply(
            lambda s: s.dt.date.nunique()
        )
        crossing = int((dates_per_night > 1).sum())
    return {
        "rows": len(df),
        "astro_days": int(df["astro_day_date"].nunique()),
        "nights_with_data": int(nights["astro_day_date"].nunique()) if not nights.empty else 0,
        "night_rows": int(night.sum()),
        "day_rows": int((~night).sum()),
        # Nights that span two calendar dates -- these are exactly the ones a
        # midnight-anchored frame would have split in two.
        "nights_crossing_midnight": crossing,
        "first_astro_day": str(df["astro_day_date"].min()),
        "last_astro_day": str(df["astro_day_date"].max()),
    }
