"""Night-centred analysis over the astronomical-day frame.

Everything here operates on data that has already been through
``astro_calendar.attach_astro_frame()``, so the ``day_night`` split is the true
sunset/sunrise boundary rather than a clock approximation, and a night is a
single ``astro_day_date`` rather than two calendar dates.

Pure functions, no Dagster or S3 imports, so they can be unit-tested directly.
See ``docs/tj_data_basis.md`` for the design rationale.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .h2s_exceedance import THRESHOLDS, aggregate_exceedances

__all__ = [
    "THRESHOLDS",
    "FLOW_COL",
    "exceedances_by_segment",
    "summarize_nights",
    "filter_exceedance_segments",
]

FLOW_COL = "Flow (m^3/s)--Border"


def _resultant_wind(speed: pd.Series, direction_deg: pd.Series) -> dict:
    """Vector-mean wind from scalar speed and meteorological direction.

    Direction is the compass bearing the wind blows *from*, so the vector
    components are negated. Averaging the vector rather than the bearing is what
    stops 350 deg and 10 deg averaging to 180.

    Returns resultant speed, resultant bearing, and steadiness -- the ratio of
    vector-mean speed to scalar-mean speed, 1.0 for perfectly constant direction
    and near 0 when the wind boxes the compass over the night.
    """
    valid = speed.notna() & direction_deg.notna()
    if not valid.any():
        return {
            "wind_speed_mean": np.nan,
            "wind_resultant_speed": np.nan,
            "wind_resultant_direction": np.nan,
            "wind_steadiness": np.nan,
        }
    spd = speed[valid].astype(float)
    rad = np.deg2rad(direction_deg[valid].astype(float))
    u = (-spd * np.sin(rad)).mean()
    v = (-spd * np.cos(rad)).mean()
    resultant = float(np.hypot(u, v))
    scalar_mean = float(spd.mean())
    bearing = float((np.degrees(np.arctan2(-u, -v))) % 360)
    return {
        "wind_speed_mean": scalar_mean,
        "wind_resultant_speed": resultant,
        "wind_resultant_direction": bearing,
        "wind_steadiness": resultant / scalar_mean if scalar_mean > 0 else np.nan,
    }


def _safe_mean(df: pd.DataFrame, col: str) -> float:
    return float(df[col].mean()) if col in df.columns and df[col].notna().any() else np.nan


def _safe_max(df: pd.DataFrame, col: str) -> float:
    return float(df[col].max()) if col in df.columns and df[col].notna().any() else np.nan


def exceedances_by_segment(
    df: pd.DataFrame, thresholds: tuple[int, ...] = THRESHOLDS
) -> pd.DataFrame:
    """Exceedance counts per site, astronomical day, and day/night segment.

    The astronomical-frame counterpart of the ``h2s_peaks`` asset, differing from
    it in one deliberate respect: the day/night split is the true sunset/sunrise
    boundary rather than ``6 <= hour < 18``. Both assets share
    ``aggregate_exceedances``, so they count gap-filled values identically.
    """
    segments = aggregate_exceedances(
        df, group_keys=["site_name", "astro_day_date", "day_night"], thresholds=thresholds
    )
    if segments.empty:
        return segments
    out = segments.rename(columns={"day_night": "period"})

    # Carry the frame's own descriptors so the table is usable without a re-join.
    frame_cols = [
        "astro_day_date",
        "astro_year",
        "astro_day_of_year",
        "astro_week_of_year",
        "night_of_year",
        "night_length_hours",
        "astro_day_complete",
    ]
    have = [c for c in frame_cols if c in df.columns]
    descriptors = df[have].drop_duplicates("astro_day_date")
    out = out.merge(descriptors, on="astro_day_date", how="left")

    for thr in thresholds:
        out[f"count_exceeds_{thr}"] = out[f"count_exceeds_{thr}"].astype(int)
    return out.sort_values(["site_name", "astro_day_date", "period"]).reset_index(drop=True)


def summarize_nights(
    df: pd.DataFrame, thresholds: tuple[int, ...] = THRESHOLDS
) -> pd.DataFrame:
    """One row per astronomical night per site.

    The headline analysis product of the reframing: because a night is a single
    unit, per-event statistics such as peak H2S and hours above threshold no
    longer need to be stitched back together across a midnight boundary.

    Timing of the peak is reported as ``peak_night_fraction`` (0 at sunset, 1 at
    sunrise) as well as raw hours, since night length at this latitude varies from
    roughly 9.7 to 14.0 hours across the year and raw hours are not comparable
    between seasons.
    """
    if df.empty:
        return pd.DataFrame()

    nights = df[df["day_night"] == "night"].copy()
    if nights.empty:
        return pd.DataFrame()

    rows = []
    for (astro_day, site), grp in nights.groupby(["astro_day_date", "site_name"], dropna=False):
        grp = grp.sort_values("time")
        measured = grp[grp["H2S"].notna()]
        night_len = float(grp["night_length_hours"].iloc[0])

        row = {
            "astro_day_date": astro_day,
            "site_name": site,
            "night_start": grp["astro_day_start"].iloc[0],
            "night_length_hours": night_len,
            "astro_day_complete": bool(grp["astro_day_complete"].iloc[0]),
            "observations": len(grp),
            "h2s_observations": len(measured),
            # Fraction of the night's hours that carry a real H2S measurement.
            "h2s_coverage": len(measured) / night_len if night_len > 0 else np.nan,
        }
        for col in ("astro_year", "astro_day_of_year", "astro_week_of_year", "night_of_year"):
            if col in grp.columns:
                row[col] = grp[col].iloc[0]

        if measured.empty:
            row.update(
                {
                    "h2s_max": np.nan,
                    "h2s_mean": np.nan,
                    "h2s_p95": np.nan,
                    "peak_time": pd.NaT,
                    "peak_night_fraction": np.nan,
                    "peak_hours_after_sunset": np.nan,
                }
            )
            for thr in thresholds:
                row[f"hours_above_{thr}"] = 0
        else:
            peak = measured.loc[measured["H2S"].idxmax()]
            row.update(
                {
                    "h2s_max": float(measured["H2S"].max()),
                    "h2s_mean": float(measured["H2S"].mean()),
                    "h2s_p95": float(measured["H2S"].quantile(0.95)),
                    "peak_time": peak["time"],
                    "peak_night_fraction": float(peak["night_fraction"]),
                    "peak_hours_after_sunset": float(peak["hours_into_astro_day"]),
                }
            )
            for thr in thresholds:
                row[f"hours_above_{thr}"] = int((measured["H2S"] > thr).sum())

        if "wind_speed_10m" in grp.columns and "wind_direction_10m" in grp.columns:
            row.update(_resultant_wind(grp["wind_speed_10m"], grp["wind_direction_10m"]))
        row["wind_gust_max"] = _safe_max(grp, "wind_gusts_10m")

        row["flow_mean"] = _safe_mean(grp, FLOW_COL)
        row["flow_max"] = _safe_max(grp, FLOW_COL)
        row["sbiwtp_flow_mgd_mean"] = _safe_mean(grp, "sbiwtp_flow_mgd")
        row["tide_height_mean"] = _safe_mean(grp, "tide_height")
        row["tide_height_max"] = _safe_max(grp, "tide_height")
        row["temperature_2m_mean"] = _safe_mean(grp, "temperature_2m")
        row["relative_humidity_2m_mean"] = _safe_mean(grp, "relative_humidity_2m")
        row["surface_pressure_mean"] = _safe_mean(grp, "surface_pressure")
        if "stable_atm" in grp.columns:
            row["stable_atm_hours"] = int(grp["stable_atm"].sum())

        rows.append(row)

    return pd.DataFrame(rows).sort_values(["astro_day_date", "site_name"]).reset_index(drop=True)


def filter_exceedance_segments(
    df: pd.DataFrame, segments: pd.DataFrame, threshold: int
) -> pd.DataFrame:
    """Full hourly rows for every segment that saw at least one exceedance.

    The astronomical-frame counterpart of ``h2s_exceedance_periods_filter``, using
    a join rather than the original row-by-row loop.
    """
    count_col = f"count_exceeds_{threshold}"
    if df.empty or segments.empty or count_col not in segments.columns:
        return pd.DataFrame()

    hits = segments.loc[segments[count_col] > 0, ["site_name", "astro_day_date", "period"]]
    if hits.empty:
        return pd.DataFrame()

    keyed = df.rename(columns={"day_night": "period"})
    out = keyed.merge(hits, on=["site_name", "astro_day_date", "period"], how="inner")
    out["exceedance_threshold"] = f"{threshold}_ppb"
    return out.sort_values(["site_name", "time"]).reset_index(drop=True)
