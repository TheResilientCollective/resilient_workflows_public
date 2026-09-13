"""Shared data access for the Tijuana River H2S prediction review notebooks.

Every notebook in this directory imports from here so that all of them read
the same published datasets, with the same cleaning, from the same bucket.

Data comes from the production S3 bucket (``resilentpublic`` by default) via the
same ``latest/`` paths the Dagster assets publish to. Files are cached under
``.cache/`` next to this module so a notebook can be re-run offline; delete the
cache to refresh.

Environment: ``S3_ADDRESS``, ``S3_ACCESS_KEY``, ``S3_SECRET_KEY`` (as in
``workflows/.env``). ``TJ_REVIEW_BUCKET`` overrides the bucket.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd

BUCKET = os.environ.get("TJ_REVIEW_BUCKET", "resilentpublic")
CACHE_DIR = Path(__file__).resolve().parent / ".cache"
TZ = "America/Los_Angeles"

#: Fixed station order and colours; every chart uses these so a station keeps
#: its colour no matter which subset is shown.
STATIONS = ["SAN YSIDRO", "NESTOR - BES", "IB CIVIC CTR"]
STATION_COLORS = {"SAN YSIDRO": "#2a78d6", "NESTOR - BES": "#eb6834", "IB CIVIC CTR": "#1baf7a"}

#: Thresholds the review is framed around. 5 and 10 ppb are what the deployed
#: classifiers predict today; 30 ppb is the APCD ask; 100 ppb is the question.
THRESHOLDS = [5, 10, 30, 50, 100, 200]

#: Bounding box used to call an odour complaint "Tijuana River Valley".
TRV_BOX = dict(lon_min=-117.14, lon_max=-117.02, lat_min=32.52, lat_max=32.61)

MGD_TO_CMS = 0.043813

KEYS = {
    "hourly": "latest/tijuana/forecast_data/modeldata_h2s_nofill.parquet",
    "hourly_astro": "latest/tijuana/forecast_data/astronomical_day/modeldata_h2s_nofill_astronomical_day.parquet",
    "nightly": "latest/tijuana/forecast_data/astronomical_day/h2s_nightly_summary_with_complaints.parquet",
    "complaints": "latest/tijuana/sd_complaints/complaints.parquet",
    "border": "latest/tijuana/streamflow/boundary_cms/boundary_cms_{year}.parquet",
    "canal": "latest/tijuana/streamflow/canal_cms/canal_cms_{year}.parquet",
    "effluent": "latest/tijuana/effluent_flow/yearly/effluent_flow_{year}.parquet",
    "products": "latest/tijuana/forecast_data/products_latest.parquet",
    "skill_report": "latest/tijuana/forecast_data/forecast_skill_report.json",
}


# ---------------------------------------------------------------------------
# S3 access with a local cache
# ---------------------------------------------------------------------------

def _s3_client():
    import boto3
    from botocore.config import Config

    address = os.environ["S3_ADDRESS"]
    endpoint = address if address.startswith("http") else f"https://{address}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4"),
    )


def fetch(key: str, bucket: str = BUCKET, refresh: bool = False) -> Path:
    """Return a local path for an S3 object, downloading it on first use."""
    dst = CACHE_DIR / bucket / key
    if refresh or not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        _s3_client().download_file(bucket, key, str(dst))
    return dst


def read_parquet(key: str, **kw) -> pd.DataFrame:
    return pd.read_parquet(fetch(key, **kw))


# ---------------------------------------------------------------------------
# Datasets
# ---------------------------------------------------------------------------

def hourly(measured_only: bool = True) -> pd.DataFrame:
    """Hourly station observations with the engineered model features.

    Source: ``h2sforecast/modeldata_h2s_nofill`` — H2S is null wherever it was not
    measured, so no gap-filled value can be mistaken for an observation.
    """
    df = read_parquet(KEYS["hourly"])
    df = df.drop(columns=[c for c in df.columns if c.startswith("__index")], errors="ignore")
    df["time"] = pd.to_datetime(df["time"]).dt.tz_convert(TZ)
    if measured_only:
        df = df[df["h2s_measured"] & df["H2S"].notna()].copy()
    df["H2S"] = df["H2S"].clip(lower=0)
    df["year"] = df["time"].dt.year
    df["month"] = df["time"].dt.month
    df["hour"] = df["time"].dt.hour
    df["date"] = df["time"].dt.date
    df = df.rename(columns={"Flow (m^3/s)--Border": "border_cms"})
    df["site_name"] = pd.Categorical(df["site_name"], categories=STATIONS, ordered=True)
    return df.sort_values(["site_name", "time"]).reset_index(drop=True)


def nightly(min_coverage: float = 0.5) -> pd.DataFrame:
    """One row per station per astronomical night (sunset to sunrise).

    Source: ``h2sforecast/h2s_nightly_summary_with_complaints``. Nights truncated
    by the record edges or with less than ``min_coverage`` of their hours
    measured are dropped so peak statistics are not biased low.
    """
    df = read_parquet(KEYS["nightly"])
    df = df[df["astro_day_complete"] & (df["h2s_coverage"] >= min_coverage)].copy()
    df["night_start"] = pd.to_datetime(df["night_start"]).dt.tz_convert(TZ)
    df["astro_day_date"] = pd.to_datetime(df["astro_day_date"]).dt.date
    df["month"] = df["night_start"].dt.month
    df["site_name"] = pd.Categorical(df["site_name"], categories=STATIONS, ordered=True)
    df = df.sort_values(["site_name", "night_start"]).reset_index(drop=True)
    df["prev_h2s_max"] = df.groupby("site_name", observed=True)["h2s_max"].shift(1)
    df["sbiwtp_cms_mean"] = df["sbiwtp_flow_mgd_mean"] * MGD_TO_CMS
    return df


def complaints() -> pd.DataFrame:
    """SDAPCD public complaints with a real time of day, tagged for this review.

    ``is_odor`` — nature_of_complaint mentions odour.
    ``in_trv``  — geocoded inside :data:`TRV_BOX`.
    ``is_caspian`` — the ``Caspian Way and N McCoy Trail`` cluster, the single
    "source adjacent location" APCD assigns to most Tijuana River odour reports.
    """
    df = read_parquet(KEYS["complaints"])
    df["dt"] = pd.to_datetime(df["date_and_time_received"], unit="ms", utc=True).dt.tz_convert(TZ)
    df["hour_floor"] = df["dt"].dt.floor("h")
    df["year"] = df["dt"].dt.year
    df["hour"] = df["dt"].dt.hour
    df["is_odor"] = df["nature_of_complaint"].fillna("").str.contains("odor", case=False)
    df["is_caspian"] = df["cross_street___intersection"].fillna("").str.contains("caspian", case=False)
    b = TRV_BOX
    df["in_trv"] = (
        df["x_coordinate"].between(b["lon_min"], b["lon_max"])
        & df["y_coordinate"].between(b["lat_min"], b["lat_max"])
    )
    return df


def _yearly(key_template: str, years, columns):
    frames = []
    for y in years:
        try:
            f = read_parquet(key_template.format(year=y)).iloc[:, : len(columns)]
        except Exception:  # a year that has not been published yet
            continue
        f.columns = columns
        frames.append(f)
    df = pd.concat(frames, ignore_index=True)
    # IBWC exports are stamped in fixed UTC-8; convert to local wall clock.
    df["time"] = pd.to_datetime(df["time"]).dt.tz_localize("Etc/GMT+8").dt.tz_convert(TZ)
    return df.sort_values("time").reset_index(drop=True)


def border_flow(years=range(2020, 2027)) -> pd.DataFrame:
    """IBWC gauge 11013300, Tijuana River at the international boundary, hourly m³/s."""
    return _yearly(KEYS["border"], years, ["time", "border_cms"])


def canal_flow(years=range(2024, 2027)) -> pd.DataFrame:
    """IBWC 11-TIJUANA-CANAL, hourly m³/s."""
    return _yearly(KEYS["canal"], years, ["time", "canal_cms"])


def effluent(years=range(2020, 2027)) -> pd.DataFrame:
    """SBIWTP plant effluent, daily million US gallons per day."""
    df = _yearly(KEYS["effluent"], years, ["time", "sbiwtp_mgd"])
    df["date"] = df["time"].dt.date
    df["sbiwtp_cms"] = df["sbiwtp_mgd"] * MGD_TO_CMS
    return df


def daily_drivers() -> pd.DataFrame:
    """Daily border flow, SBIWTP effluent and mean temperature on one calendar."""
    b = border_flow()
    b["date"] = b["time"].dt.date
    daily = b.groupby("date")["border_cms"].agg(["mean", "max"]).rename(columns={"mean": "border_cms", "max": "border_cms_max"})
    e = effluent().groupby("date")["sbiwtp_mgd"].mean()
    out = daily.join(e, how="outer")
    out["sbiwtp_cms"] = out["sbiwtp_mgd"] * MGD_TO_CMS
    out.index = pd.to_datetime(out.index)
    out["month"] = out.index.month
    out["year"] = out.index.year
    return out


# ---------------------------------------------------------------------------
# Small helpers used by more than one notebook
# ---------------------------------------------------------------------------

def runs_above(df: pd.DataFrame, threshold: float) -> pd.DataFrame:
    """Contiguous runs of hourly H2S above ``threshold``, per station.

    A run breaks on any hour at or below the threshold *or* on a gap in the
    hourly record, so a run length is always a count of consecutive measured
    exceedance hours.
    """
    rows = []
    for site, g in df.groupby("site_name", observed=True):
        g = g.sort_values("time")
        above = (g["H2S"] > threshold).to_numpy()
        gap = g["time"].diff().dt.total_seconds().div(3600).fillna(1).to_numpy() != 1
        new_run = np.r_[True, (~above[:-1]) | gap[1:]]
        run_id = np.cumsum(new_run & above)
        r = g[above].groupby(run_id[above]).agg(
            start=("time", "first"), end=("time", "last"), hours=("H2S", "size"), peak=("H2S", "max")
        )
        r["site_name"] = site
        rows.append(r)
    out = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(columns=["start", "end", "hours", "peak", "site_name"])
    out["site_name"] = pd.Categorical(out["site_name"], categories=STATIONS, ordered=True)
    return out


def spearman(df: pd.DataFrame, target: str, cols, by: str = "site_name") -> pd.DataFrame:
    """Long table of Spearman correlations between ``target`` and each column, by group."""
    rows = []
    for key, g in df.groupby(by, observed=True):
        for c in cols:
            pair = g[[target, c]].dropna()
            rho = pair.corr(method="spearman").iloc[0, 1] if len(pair) > 10 else np.nan
            rows.append({by: key, "driver": c, "rho": rho, "n": len(pair)})
    return pd.DataFrame(rows)
