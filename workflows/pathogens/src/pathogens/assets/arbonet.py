"""CDC ArboNET case-count assets: West Nile virus and dengue, county + state.

VectorSurv (see :mod:`vectorsurv`) is a vector-activity surveillance feed
(trap counts, invasive-species detections, arbovirus positivity in mosquito
pools/birds/sentinels) with no human case counts, scoped to California. It
does not answer "how many people got sick" anywhere in the US. ArboNET is
CDC's national arboviral surveillance system and is the source for that:
human case counts by county and state.

ArboNET has no public bulk-download API. These assets pull the CSV files
that back the public CDC dashboards (stable ``wcms/vizdata`` URLs, verified
against the live site) rather than scraping the dashboards themselves:

* ``arbonet_wnv_county_yearly`` — West Nile virus human + non-human activity
  by county, every year 1999-present, plus the companion 1999-2025
  cumulative county incidence/population table.
* ``arbonet_wnv_state_cumulative`` — West Nile virus cumulative case totals
  by state (1999-2025, CDC's own precomputed cumulative table) plus the
  current season's state counts. This is the table that replaces
  hand-typed "ArboNET" numbers in downstream consumers.
* ``arbonet_wnv_current`` — West Nile virus current-season county activity,
  refreshed by CDC roughly biweekly June-December and overwritten in place;
  archived here with a dated snapshot so the within-season time series is
  not lost.
* ``arbonet_dengue_current`` — current-season dengue case counts by county
  and jurisdiction (travel-associated vs. locally acquired), plus the
  national weekly epi curve. Small county counts are published as
  suppressed bins (e.g. "1 to 4"); both the bin label and a parsed
  numeric range/midpoint are kept.

Notes shared across these assets:

* County FIPS codes are kept as CDC publishes them, as zero-padded
  5-character strings. Connecticut switched from counties to planning
  regions starting with 2023 data; both FIPS schemes appear in the
  historic county file (``fips_scheme`` marks which). State-level rollups
  are unaffected.
* The most recent year/season in each file is provisional (CDC finalizes
  prior-year WNV data the following spring); rows are flagged accordingly.
* Hantavirus is not an arboviral disease and has no ArboNET feed — NNDSS
  (see :mod:`cdc_nnds`) remains its only live source.
"""

import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetIn,
    AssetKey,
    Output,
    RunRequest,
    asset,
    define_asset_job,
    get_dagster_logger,
    schedule,
)

from resilient_core.utils import store_assets

ARBONET_WNV_BASE = "https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV"
ARBONET_DEN_BASE = "https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/DEN"

S3_RAW_PATH = "pathogens/vectorborne/raw/arbonet"
S3_OUTPUT_PATH = "pathogens/vectorborne/output"
ARBONET_LATEST_PATH = "pathogens/vectorborne/arbonet"

REQUEST_TIMEOUT = 60
REQUEST_HEADERS = {"User-Agent": "resilient-workflows/arbonet (+resilientservice.org)"}

# Standard 2-digit state/territory FIPS -> USPS abbreviation, covering every
# prefix that appears in CDC's ArboNET county files (50 states + DC + PR).
STATE_FIPS_TO_ABBR = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA", "08": "CO",
    "09": "CT", "10": "DE", "11": "DC", "12": "FL", "13": "GA", "15": "HI",
    "16": "ID", "17": "IL", "18": "IN", "19": "IA", "20": "KS", "21": "KY",
    "22": "LA", "23": "ME", "24": "MD", "25": "MA", "26": "MI", "27": "MN",
    "28": "MS", "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
    "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC", "46": "SD",
    "47": "TN", "48": "TX", "49": "UT", "50": "VT", "51": "VA", "53": "WA",
    "54": "WV", "55": "WI", "56": "WY", "60": "AS", "66": "GU", "69": "MP",
    "72": "PR", "78": "VI",
}

# Connecticut planning-region FIPS (used for 2023+ data) vs. the legacy
# county FIPS (used through 2022). Both appear in the historic county file.
CT_PLANNING_REGION_PREFIXES = tuple(f"091{n}0" for n in range(1, 10))


# ---------------------------------------------------------------------------
# Fetch helper
# ---------------------------------------------------------------------------

def _get_csv(url: str, logger=None, retries: int = 3) -> str:
    for attempt in range(retries):
        try:
            response = requests.get(
                url, timeout=REQUEST_TIMEOUT, headers=REQUEST_HEADERS
            )
            response.raise_for_status()
            return response.text
        except Exception as e:  # noqa: BLE001 - retry any network error
            if attempt == retries - 1:
                raise
            if logger:
                logger.warning(f"retry {attempt + 1} for {url}: {e}")
            time.sleep(2 ** attempt)
    return ""


# ---------------------------------------------------------------------------
# Pure transforms (unit-testable without Dagster/S3)
# ---------------------------------------------------------------------------

def _county_fips(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.zfill(5)


def _state_abbr_from_fips(county_fips: pd.Series) -> pd.Series:
    return county_fips.str[:2].map(STATE_FIPS_TO_ABBR)


def _fips_scheme(county_fips: pd.Series) -> pd.Series:
    is_planning_region = county_fips.str.startswith(CT_PLANNING_REGION_PREFIXES)
    return pd.Series("county", index=county_fips.index).where(
        ~is_planning_region, "planning_region"
    )


def parse_binned_count(label) -> tuple:
    """Parse an ArboNET suppressed-count label into ``(min, max)``.

    Handles exact integers (``"6"`` -> ``(6, 6)``), closed bins
    (``"1 to 4"`` -> ``(1, 4)``) and open-ended bins (``"50+"`` ->
    ``(50, None)``). Returns ``(None, None)`` for anything unrecognized
    (empty labels, or a bin format CDC has not published before).
    """
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return (None, None)
    text = str(label).strip()
    if not text:
        return (None, None)
    if text.isdigit():
        value = int(text)
        return (value, value)
    range_match = re.match(r"^(\d+)\s*to\s*(\d+)$", text, re.IGNORECASE)
    if range_match:
        return (int(range_match.group(1)), int(range_match.group(2)))
    open_match = re.match(r"^(\d+)\s*\+$", text)
    if open_match:
        return (int(open_match.group(1)), None)
    return (None, None)


def wnv_county_yearly_to_df(yearly_csv_text: str, incidence_csv_text: str) -> pd.DataFrame:
    """West Nile virus human + non-human activity by county, 1999-present.

    ``yearly_csv_text`` is ``wnv_hist_hum_nonhum_yearly.csv`` (one row per
    county-year with reported/neuroinvasive/blood-donor counts and an
    activity category). ``incidence_csv_text`` is
    ``wnv_hum_historic_County Inc.csv``, CDC's precomputed 1999-2025
    cumulative neuroinvasive incidence per county (with the population base
    used for that calculation); its two columns are joined on
    ``county_fips`` to give each county-year row the CDC-published
    denominator, rather than sourcing population separately.
    """
    yearly = pd.read_csv(pd.io.common.StringIO(yearly_csv_text), dtype=str)
    yearly.columns = [c.strip() for c in yearly.columns]

    df = pd.DataFrame(index=yearly.index)
    df["year"] = pd.to_numeric(yearly["Year"], errors="coerce").astype("Int64")
    df["county_fips"] = _county_fips(yearly["County"])
    df["state_abbr"] = _state_abbr_from_fips(df["county_fips"])
    df["fips_scheme"] = _fips_scheme(df["county_fips"])
    df["activity"] = yearly["Activity"]
    df["human_cases"] = pd.to_numeric(yearly["Reported human cases"], errors="coerce")
    df["neuroinvasive_cases"] = pd.to_numeric(
        yearly["Neuroinvasive disease cases"], errors="coerce"
    )
    df["blood_donor_cases"] = pd.to_numeric(
        yearly["Identified by Blood Donor Screening"], errors="coerce"
    )
    df["notes"] = yearly.get("Notes")
    df["provisional"] = df["year"] == df["year"].max()

    if incidence_csv_text:
        incidence = pd.read_csv(pd.io.common.StringIO(incidence_csv_text), dtype=str)
        incidence.columns = [c.strip() for c in incidence.columns]
        incidence = incidence[incidence["Type"] == "Neuroinvasive disease cases"]
        inc = pd.DataFrame(
            {
                "county_fips": _county_fips(incidence["County"]),
                "cumulative_period": incidence["Year"],
                "cumulative_population": pd.to_numeric(
                    incidence["Population"], errors="coerce"
                ),
                "cumulative_neuroinvasive_incidence_per_100k": pd.to_numeric(
                    incidence["Incidence"], errors="coerce"
                ),
            }
        ).drop_duplicates("county_fips")
        df = df.merge(inc, on="county_fips", how="left")

    return df.sort_values(["county_fips", "year"]).reset_index(drop=True)


def wnv_state_cumulative_to_df(
    state_hist_csv_text: str, state_current_csv_text: str, as_of: str
) -> pd.DataFrame:
    """State-level West Nile virus cumulative burden (1999-2025) + current season.

    ``state_hist_csv_text`` is ``wnv_hum_historic - Historic Sta.csv``: CDC's
    own precomputed cumulative case totals by state for 1999-2025, split
    into "All disease cases" and "Neuroinvasive disease cases". This is the
    table intended to replace hand-typed ArboNET cumulative dictionaries in
    downstream consumers. ``state_current_csv_text`` is
    ``wnv_hum_current_CountbyState.csv``, the in-progress current season's
    state counts (updated ~biweekly June-December, provisional).
    """
    hist = pd.read_csv(pd.io.common.StringIO(state_hist_csv_text), dtype=str)
    hist.columns = [c.strip() for c in hist.columns]
    hist["Reported Cases"] = pd.to_numeric(hist["Reported Cases"], errors="coerce")
    wide = hist.pivot_table(
        index="State", columns="Type", values="Reported Cases", aggfunc="first"
    ).reset_index()
    wide = wide.rename(
        columns={
            "State": "state_abbr",
            "All disease cases": "total_human_cases",
            "Neuroinvasive disease cases": "total_neuroinvasive_cases",
        }
    )
    for col in ("total_human_cases", "total_neuroinvasive_cases"):
        if col not in wide.columns:
            wide[col] = pd.NA
    wide["cumulative_year_range"] = hist["Year"].iloc[0] if len(hist) else None

    current_year = datetime.now(timezone.utc).year
    if state_current_csv_text:
        current = pd.read_csv(pd.io.common.StringIO(state_current_csv_text), dtype=str)
        current.columns = [c.strip() for c in current.columns]
        current = pd.DataFrame(
            {
                "state_abbr": current["State"],
                "current_season_reported_cases": pd.to_numeric(
                    current["Reported Cases"], errors="coerce"
                ),
            }
        )
        wide = wide.merge(current, on="state_abbr", how="outer")
    else:
        wide["current_season_reported_cases"] = pd.NA

    wide["current_season_year"] = current_year
    wide["includes_current_season"] = wide["current_season_reported_cases"].notna()
    wide["as_of"] = as_of
    return wide.sort_values("state_abbr").reset_index(drop=True)


def wnv_current_to_df(current_csv_text: str, as_of: str) -> pd.DataFrame:
    """West Nile virus current-season activity by county (provisional)."""
    current = pd.read_csv(pd.io.common.StringIO(current_csv_text), dtype=str)
    current.columns = [c.strip() for c in current.columns]

    df = pd.DataFrame(index=current.index)
    df["county_fips"] = _county_fips(current["County"])
    df["state_abbr"] = _state_abbr_from_fips(df["county_fips"])
    df["fips_scheme"] = _fips_scheme(df["county_fips"])
    df["activity"] = current["Activity"]
    df["total_human_cases"] = pd.to_numeric(
        current["Total human disease cases"], errors="coerce"
    )
    df["neuroinvasive_cases"] = pd.to_numeric(
        current["Neuroinvasive disease cases"], errors="coerce"
    )
    df["presumptive_viremic_blood_donors"] = pd.to_numeric(
        current["Presumptive viremic blood donors"], errors="coerce"
    )
    df["season_year"] = datetime.now(timezone.utc).year
    df["provisional"] = True
    df["as_of"] = as_of
    return df.sort_values("county_fips").reset_index(drop=True)


_TRAVEL_STATUS_MAP = {
    "all": "all",
    "travel associated": "travel_associated",
    "locally acquired": "locally_acquired",
}


def _normalize_travel_status(series: pd.Series) -> pd.Series:
    return series.astype("string").str.strip().str.lower().map(_TRAVEL_STATUS_MAP)


def _dengue_binned_geo_to_df(csv_text: str, geo_col: str, geo_out_col: str, as_of: str) -> pd.DataFrame:
    parsed = pd.read_csv(pd.io.common.StringIO(csv_text), dtype=str)
    parsed.columns = [c.strip() for c in parsed.columns]

    df = pd.DataFrame(index=parsed.index)
    df["year"] = pd.to_numeric(parsed["Year"], errors="coerce").astype("Int64")
    df["travel_status"] = _normalize_travel_status(parsed["Travel status"])
    if geo_out_col == "county_fips":
        df[geo_out_col] = _county_fips(parsed[geo_col])
        df["state_abbr"] = _state_abbr_from_fips(df[geo_out_col])
    else:
        df[geo_out_col] = parsed[geo_col].astype("string").str.strip()
    df["count_label"] = parsed["Count"]
    bounds = df["count_label"].map(parse_binned_count)
    df["count_min"] = bounds.map(lambda t: t[0]).astype("Int64")
    df["count_max"] = bounds.map(lambda t: t[1]).astype("Int64")
    df["count_midpoint"] = df.apply(
        lambda r: r["count_min"]
        if pd.isna(r["count_max"]) or pd.isna(r["count_min"])
        else (r["count_min"] + r["count_max"]) / 2,
        axis=1,
    )
    df["notes"] = parsed.get("Notes")
    df["season_year"] = datetime.now(timezone.utc).year
    df["provisional"] = True
    df["as_of"] = as_of
    return df.sort_values([geo_out_col, "travel_status"]).reset_index(drop=True)


def dengue_county_current_to_df(county_csv_text: str, as_of: str) -> pd.DataFrame:
    """Current-season dengue cases by county. Small counts are suppressed bins."""
    return _dengue_binned_geo_to_df(county_csv_text, "County", "county_fips", as_of)


def dengue_jurisdiction_current_to_df(jurisdiction_csv_text: str, as_of: str) -> pd.DataFrame:
    """Current-season dengue cases by state/territory jurisdiction."""
    return _dengue_binned_geo_to_df(
        jurisdiction_csv_text, "Jurisdiction", "state_abbr", as_of
    )


def dengue_epi_curve_to_df(epi_csv_text: str, as_of: str) -> pd.DataFrame:
    """National weekly reported dengue cases by travel status, current season."""
    parsed = pd.read_csv(pd.io.common.StringIO(epi_csv_text), dtype=str)
    parsed.columns = [c.strip() for c in parsed.columns]
    df = pd.DataFrame(
        {
            "year": pd.to_numeric(parsed["Year"], errors="coerce").astype("Int64"),
            "travel_status": _normalize_travel_status(parsed["Travel status"]),
            "week": pd.to_numeric(parsed["Week"], errors="coerce").astype("Int64"),
            "reported_cases": pd.to_numeric(parsed["Reported cases"], errors="coerce"),
        }
    )
    df["as_of"] = as_of
    return df.sort_values(["travel_status", "week"]).reset_index(drop=True)


def _metadata(context, name, description=None):
    """schema.org Dataset metadata via store_assets.objectMetadata."""
    meta = context.assets_def.metadata_by_key[context.asset_key]
    return store_assets.objectMetadata(
        name=name,
        description=description or meta.get("description"),
        source_url=meta.get("source"),
    )


def _archive_raw(context, s3_resource, text: str, name: str, as_of_date: str, description: str):
    """Store a raw snapshot at its stable path plus a dated archive copy.

    CDC overwrites the current-season CSVs in place roughly biweekly, so the
    dated archive is what preserves the within-season time series that the
    live file itself discards.
    """
    store_assets.raw_to_s3(
        text.encode(),
        f"{S3_RAW_PATH}/{name}.csv",
        s3_resource,
        contenttype="text/csv",
        metadata=_metadata(context, f"arbonet_{name}_raw", description=description),
    )
    store_assets.raw_to_s3(
        text.encode(),
        f"{S3_RAW_PATH}/current_archive/{name}_{as_of_date}.csv",
        s3_resource,
        contenttype="text/csv",
    )


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------

WNV_COUNTY_YEARLY_KEY = AssetKey(["arbonet", "arbonet_wnv_county_yearly"])


@asset(
    group_name="vectorborne",
    key_prefix="arbonet",
    name="arbonet_wnv_county_yearly",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{ARBONET_WNV_BASE}/wnv_hist_hum_nonhum_yearly.csv",
        "description": (
            "CDC ArboNET West Nile virus human and non-human activity by "
            "county, every year 1999-present. Reported human cases, "
            "neuroinvasive disease cases and presumptive viremic blood "
            "donor detections per county-year, joined with CDC's "
            "precomputed 1999-2025 cumulative neuroinvasive incidence and "
            "population base per county."
        ),
    },
    check_specs=[
        AssetCheckSpec(
            name="arbonet_wnv_county_schema",
            asset=WNV_COUNTY_YEARLY_KEY,
            description="Required source columns are present (schema-drift guard for an undocumented URL).",
        ),
        AssetCheckSpec(
            name="arbonet_wnv_county_values",
            asset=WNV_COUNTY_YEARLY_KEY,
            description="Counts are non-negative, FIPS are well-formed, neuroinvasive <= human cases.",
        ),
    ],
)
def arbonet_wnv_county_yearly(context):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    as_of = datetime.now(timezone.utc).date().isoformat()

    yearly_url = f"{ARBONET_WNV_BASE}/wnv_hist_hum_nonhum_yearly.csv"
    incidence_url = f"{ARBONET_WNV_BASE}/wnv_hum_historic_County%20Inc.csv"
    yearly_text = _get_csv(yearly_url, logger)
    incidence_text = _get_csv(incidence_url, logger)

    required_cols = {
        "Year", "County", "Activity", "Reported human cases",
        "Neuroinvasive disease cases", "Identified by Blood Donor Screening",
    }
    header = pd.read_csv(pd.io.common.StringIO(yearly_text), nrows=0).columns
    header = {c.strip() for c in header}
    missing = sorted(required_cols - header)
    yield AssetCheckResult(
        check_name="arbonet_wnv_county_schema",
        passed=not missing,
        severity=AssetCheckSeverity.ERROR,
        description=(
            "All required columns present."
            if not missing
            else f"CDC ArboNET WNV county yearly file is missing columns: {missing}. "
            "The dashboard-backing URL may have changed shape."
        ),
        metadata={"missing_columns": missing},
    )

    store_assets.raw_to_s3(
        yearly_text.encode(),
        f"{S3_RAW_PATH}/wnv_hist_hum_nonhum_yearly.csv",
        s3_resource,
        contenttype="text/csv",
        metadata=_metadata(
            context,
            "arbonet_wnv_hist_hum_nonhum_yearly_raw",
            description="Raw CDC ArboNET WNV county-year activity CSV as fetched.",
        ),
    )
    store_assets.raw_to_s3(
        incidence_text.encode(),
        f"{S3_RAW_PATH}/wnv_county_incidence.csv",
        s3_resource,
        contenttype="text/csv",
        metadata=_metadata(
            context,
            "arbonet_wnv_county_incidence_raw",
            description="Raw CDC ArboNET WNV 1999-2025 cumulative county incidence CSV as fetched.",
        ),
    )

    df = wnv_county_yearly_to_df(yearly_text, incidence_text)
    logger.info(
        f"{len(df)} WNV county-year rows, {df['county_fips'].nunique()} counties, "
        f"{int(df['year'].min())}-{int(df['year'].max())}"
    )

    negative = (
        (df["human_cases"].fillna(0) < 0)
        | (df["neuroinvasive_cases"].fillna(0) < 0)
        | (df["blood_donor_cases"].fillna(0) < 0)
    )
    bad_fips = ~df["county_fips"].str.match(r"^\d{5}$") | df["state_abbr"].isna()
    exceeds = df["neuroinvasive_cases"].fillna(0) > df["human_cases"].fillna(0)
    problems = int(negative.sum()) or int(bad_fips.sum())
    yield AssetCheckResult(
        check_name="arbonet_wnv_county_values",
        passed=not problems,
        severity=AssetCheckSeverity.WARN if problems else AssetCheckSeverity.WARN,
        description=(
            f"{len(df)} rows: {int(negative.sum())} negative counts, "
            f"{int(bad_fips.sum())} malformed FIPS, "
            f"{int(exceeds.sum())} rows where neuroinvasive > human cases."
        ),
        metadata={
            "negative_count_rows": int(negative.sum()),
            "malformed_fips_rows": int(bad_fips.sum()),
            "neuroinvasive_exceeds_human_rows": int(exceeds.sum()),
        },
    )

    metadata = _metadata(context, "arbonet_wnv_county_yearly")
    store_assets.store_dataframe_to_s3(
        df,
        f"{S3_OUTPUT_PATH}/arbonet_wnv_county_yearly",
        "arbonet_wnv_county_yearly",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    yield Output(
        df,
        metadata={
            "rows": len(df),
            "counties": int(df["county_fips"].nunique()),
            "years": f"{int(df['year'].min())}-{int(df['year'].max())}",
            "as_of": as_of,
        },
    )


WNV_STATE_CUMULATIVE_KEY = AssetKey(["arbonet", "arbonet_wnv_state_cumulative"])


@asset(
    group_name="vectorborne",
    key_prefix="arbonet",
    name="arbonet_wnv_state_cumulative",
    required_resource_keys={"s3"},
    ins={
        "arbonet_wnv_county_yearly": AssetIn(
            key=AssetKey(["arbonet", "arbonet_wnv_county_yearly"])
        )
    },
    metadata={
        "source": f"{ARBONET_WNV_BASE}/wnv_hum_historic - Historic Sta.csv",
        "description": (
            "CDC ArboNET West Nile virus cumulative case totals by state: "
            "CDC's own precomputed 1999-2025 cumulative (all disease cases "
            "and neuroinvasive disease cases) plus the current season's "
            "state counts (provisional, updated ~biweekly Jun-Dec). This is "
            "real reported-case data intended to replace any hand-typed "
            "'ArboNET' cumulative tables downstream."
        ),
    },
    check_specs=[
        AssetCheckSpec(
            name="arbonet_wnv_state_totals",
            asset=WNV_STATE_CUMULATIVE_KEY,
            description="County-yearly rollup approximately agrees with the state cumulative table.",
        ),
    ],
)
def arbonet_wnv_state_cumulative(context, arbonet_wnv_county_yearly):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    as_of = datetime.now(timezone.utc).date().isoformat()

    hist_url = f"{ARBONET_WNV_BASE}/wnv_hum_historic%20-%20Historic%20Sta.csv"
    current_url = f"{ARBONET_WNV_BASE}/wnv_hum_current_CountbyState.csv"
    hist_text = _get_csv(hist_url, logger)
    current_text = _get_csv(current_url, logger)

    store_assets.raw_to_s3(
        hist_text.encode(),
        f"{S3_RAW_PATH}/wnv_state_cumulative_1999_2025.csv",
        s3_resource,
        contenttype="text/csv",
        metadata=_metadata(
            context,
            "arbonet_wnv_state_cumulative_raw",
            description="Raw CDC ArboNET WNV 1999-2025 state cumulative CSV as fetched.",
        ),
    )
    _archive_raw(
        context,
        s3_resource,
        current_text,
        "wnv_current_state",
        as_of,
        "Raw CDC ArboNET WNV current-season state count CSV as fetched.",
    )

    df = wnv_state_cumulative_to_df(hist_text, current_text, as_of)
    logger.info(f"{len(df)} states/territories, cumulative_year_range={df['cumulative_year_range'].iloc[0]}")

    # Sanity check: summing the county-yearly rollup (excluding the
    # provisional in-progress year) should be in the same ballpark as CDC's
    # own state cumulative table. Large disagreement means a partial fetch
    # or a schema change upstream.
    county_final = arbonet_wnv_county_yearly[~arbonet_wnv_county_yearly["provisional"]]
    rollup = (
        county_final.groupby("state_abbr")["neuroinvasive_cases"]
        .sum(min_count=1)
        .rename("rollup_neuroinvasive")
    )
    comparison = df.set_index("state_abbr")[["total_neuroinvasive_cases"]].join(rollup)
    comparison = comparison.dropna()
    comparison["diff_pct"] = (
        (comparison["total_neuroinvasive_cases"] - comparison["rollup_neuroinvasive"]).abs()
        / comparison["total_neuroinvasive_cases"].replace(0, pd.NA)
    )
    large_diff = comparison[comparison["diff_pct"].fillna(0) > 0.15]
    yield AssetCheckResult(
        check_name="arbonet_wnv_state_totals",
        passed=len(large_diff) == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            f"County rollup agrees with the state cumulative table within 15% for "
            f"{len(comparison) - len(large_diff)}/{len(comparison)} states."
            if len(large_diff) == 0
            else f"{len(large_diff)} states diverge >15% between county rollup and "
            f"CDC's state cumulative table: {sorted(large_diff.index)}."
        ),
        metadata={"states_compared": len(comparison), "states_diverging": len(large_diff)},
    )

    metadata = _metadata(context, "arbonet_wnv_state_cumulative")
    store_assets.store_dataframe_to_s3(
        df,
        f"{S3_OUTPUT_PATH}/arbonet_wnv_state_cumulative",
        "arbonet_wnv_state_cumulative",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    yield Output(
        df,
        metadata={
            "states": len(df),
            "total_neuroinvasive_cases_1999_2025": int(df["total_neuroinvasive_cases"].fillna(0).sum()),
            "as_of": as_of,
        },
    )


@asset(
    group_name="vectorborne",
    key_prefix="arbonet",
    name="arbonet_wnv_current",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{ARBONET_WNV_BASE}/wnv_current_hum_nonhum.csv",
        "description": (
            "CDC ArboNET West Nile virus current-season activity by county "
            "(provisional; CDC updates this file in place roughly biweekly "
            "June-December). All rows are provisional."
        ),
    },
)
def arbonet_wnv_current(context):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    as_of = datetime.now(timezone.utc).date().isoformat()

    current_text = _get_csv(f"{ARBONET_WNV_BASE}/wnv_current_hum_nonhum.csv", logger)
    _archive_raw(
        context,
        s3_resource,
        current_text,
        "wnv_current_hum_nonhum",
        as_of,
        "Raw CDC ArboNET WNV current-season county activity CSV as fetched.",
    )

    df = wnv_current_to_df(current_text, as_of)
    logger.info(f"{len(df)} current-season county rows as of {as_of}")

    metadata = _metadata(context, "arbonet_wnv_current")
    store_assets.store_dataframe_to_s3(
        df,
        f"{S3_OUTPUT_PATH}/arbonet_wnv_current",
        "arbonet_wnv_current",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    return Output(
        df,
        metadata={
            "rows": len(df),
            "counties": int(df["county_fips"].nunique()) if len(df) else 0,
            "as_of": as_of,
        },
    )


DENGUE_CURRENT_KEY = AssetKey(["arbonet", "arbonet_dengue_current"])


@asset(
    group_name="vectorborne",
    key_prefix="arbonet",
    name="arbonet_dengue_current",
    required_resource_keys={"s3"},
    metadata={
        "source": f"{ARBONET_DEN_BASE}/Cases_by_County_Current.csv",
        "description": (
            "CDC ArboNET current-season dengue cases by county and "
            "jurisdiction, split by travel-associated vs. locally acquired, "
            "plus the national weekly epi curve. County counts under 5 are "
            "published as suppressed bins (e.g. '1 to 4'); count_label "
            "keeps the original string, count_min/count_max/count_midpoint "
            "give the parsed numeric range. Jurisdiction counts are exact. "
            "All rows are provisional (current season)."
        ),
    },
    check_specs=[
        AssetCheckSpec(
            name="arbonet_dengue_bins",
            asset=DENGUE_CURRENT_KEY,
            description="Every count_label parses to a numeric bin and every travel_status is known.",
        ),
    ],
)
def arbonet_dengue_current(context):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    as_of = datetime.now(timezone.utc).date().isoformat()

    county_text = _get_csv(f"{ARBONET_DEN_BASE}/Cases_by_County_Current.csv", logger)
    jurisdiction_text = _get_csv(
        f"{ARBONET_DEN_BASE}/Cases_by_Jurisdiction_Current.csv", logger
    )
    epi_text = _get_csv(f"{ARBONET_DEN_BASE}/Epi_Curve_Current.csv", logger)

    for name, text, desc in (
        ("dengue_cases_by_county_current", county_text, "county"),
        ("dengue_cases_by_jurisdiction_current", jurisdiction_text, "jurisdiction"),
        ("dengue_epi_curve_current", epi_text, "national weekly epi curve"),
    ):
        _archive_raw(
            context, s3_resource, text, name, as_of,
            f"Raw CDC ArboNET current-season dengue {desc} CSV as fetched.",
        )

    county_df = dengue_county_current_to_df(county_text, as_of)
    jurisdiction_df = dengue_jurisdiction_current_to_df(jurisdiction_text, as_of)
    epi_df = dengue_epi_curve_to_df(epi_text, as_of)
    logger.info(
        f"dengue current season: {len(county_df)} county rows, "
        f"{len(jurisdiction_df)} jurisdiction rows, {len(epi_df)} epi-curve rows"
    )

    unparsed = county_df[
        county_df["count_min"].isna() & county_df["count_label"].notna()
    ]
    known_status = set(_TRAVEL_STATUS_MAP.values())
    bad_status = county_df[~county_df["travel_status"].isin(known_status)]
    problems = len(unparsed) + len(bad_status)
    yield AssetCheckResult(
        check_name="arbonet_dengue_bins",
        passed=problems == 0,
        severity=AssetCheckSeverity.WARN,
        description=(
            "All county count labels parsed and all travel_status values known."
            if problems == 0
            else f"{len(unparsed)} unparsed count labels "
            f"({sorted(unparsed['count_label'].unique())[:10]}), "
            f"{len(bad_status)} rows with an unrecognized travel status."
        ),
        metadata={"unparsed_labels": len(unparsed), "unknown_travel_status_rows": len(bad_status)},
    )

    store_assets.store_dataframe_to_s3(
        county_df,
        f"{S3_OUTPUT_PATH}/arbonet_dengue_current",
        "arbonet_dengue_current",
        s3_resource,
        metadata=_metadata(context, "arbonet_dengue_current"),
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    store_assets.store_dataframe_to_s3(
        jurisdiction_df,
        f"{S3_OUTPUT_PATH}/arbonet_dengue_current",
        "arbonet_dengue_jurisdiction_current",
        s3_resource,
        metadata=_metadata(
            context,
            "arbonet_dengue_jurisdiction_current",
            description=(
                "CDC ArboNET current-season dengue cases by state/territory "
                "jurisdiction, split by travel-associated vs. locally "
                "acquired. Exact counts (not suppressed)."
            ),
        ),
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    store_assets.store_dataframe_to_s3(
        epi_df,
        f"{S3_OUTPUT_PATH}/arbonet_dengue_current",
        "arbonet_dengue_epi_curve",
        s3_resource,
        metadata=_metadata(
            context,
            "arbonet_dengue_epi_curve",
            description=(
                "CDC ArboNET current-season national weekly reported dengue "
                "cases by travel status."
            ),
        ),
        latestdatasetpath=ARBONET_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    return Output(
        county_df,
        metadata={
            "county_rows": len(county_df),
            "jurisdiction_rows": len(jurisdiction_df),
            "epi_curve_rows": len(epi_df),
            "as_of": as_of,
        },
    )


# ---------------------------------------------------------------------------
# Job + weekly schedule
# ---------------------------------------------------------------------------

arbonet_weekly_job = define_asset_job(
    "arbonet_weekly_job",
    selection=[
        AssetKey(["arbonet", "arbonet_wnv_county_yearly"]),
        AssetKey(["arbonet", "arbonet_wnv_state_cumulative"]),
        AssetKey(["arbonet", "arbonet_wnv_current"]),
        AssetKey(["arbonet", "arbonet_dengue_current"]),
    ],
)


@schedule(
    job=arbonet_weekly_job,
    cron_schedule="0 7 * * 1",
    name="arbonet_weekly_schedule",
    execution_timezone="America/Los_Angeles",
)
def arbonet_weekly_schedule(context):
    """CDC refreshes current-season files ~biweekly; weekly comfortably tracks it."""
    return RunRequest(run_key=f"arbonet_{datetime.now().date().isoformat()}")
