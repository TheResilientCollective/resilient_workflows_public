"""Unified human-case dataset for dengue, West Nile virus and hantavirus.

Merges the two case-count sources collected elsewhere in this package:

* NNDSS weekly (:mod:`cdc_nnds`) — state-level, weekly, 2022-present,
  corrected-count QA already applied.
* ArboNET (:mod:`arbonet`) — county-level (West Nile, dengue) and state
  cumulative (West Nile), annual/seasonal cadence.

These describe the same underlying cases at different grains, so this table
does **not** collapse them into one row per (disease, place, time) — that
would either lose the county detail ArboNET uniquely provides or double
count a case that appears in both a state-week and a county-year bucket.
Instead every row keeps its native ``grain`` and ``source``, and the table
is documented as a lookup, not a sum: never ``SUM(cases)`` across the whole
table — always filter to one ``(disease, grain)`` pair first, e.g. "NNDSS
state-week rows for west_nile" or "ArboNET county-year rows for west_nile".

VectorSurv (mosquito/vector activity, not human cases) is intentionally
**not** included here — it lives at a different unit of observation
entirely and stays in its own ``pathogens/vectorborne/output/vectorsurv_*``
tables, cross-referenced by disease name only.

Hantavirus has no ArboNET presence (it is not an arboviral disease); its
only rows here come from NNDSS.
"""

import io

import pandas as pd
from datetime import datetime

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

from .arbonet import ARBONET_LATEST_PATH, S3_OUTPUT_PATH as ARBONET_S3_OUTPUT_PATH
from .cdc_nnds import CDC_SOURCE_URL, s3_diseases_path

VECTORBORNE_OUTPUT_PATH = "pathogens/vectorborne/output"
VECTORBORNE_LATEST_PATH = "pathogens/vectorborne"

UNIFIED_COLUMNS = [
    "disease", "source", "grain", "geo_level", "geo_id", "period_start",
    "period_end", "cases", "cases_min", "cases_max", "case_type",
    "travel_status", "provisional", "as_of", "source_url",
]

# NNDSS Jurisdiction values (no spaces, CamelCase) -> USPS state/territory
# abbreviation. NewYorkCity is folded into NY (NNDSS reports it separately
# from "NewYork" for historical reasons; both are New York State cases).
NNDSS_JURISDICTION_TO_ABBR = {
    "Alabama": "AL", "Alaska": "AK", "AmericanSamoa": "AS", "Arizona": "AZ",
    "Arkansas": "AR", "California": "CA", "Colorado": "CO",
    "CommonwealthOfNorthernMarianaIslands": "MP", "Connecticut": "CT",
    "Delaware": "DE", "DistrictOfColumbia": "DC", "Florida": "FL",
    "Georgia": "GA", "Guam": "GU", "Hawaii": "HI", "Idaho": "ID",
    "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
    "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN",
    "Mississippi": "MS", "Missouri": "MO", "Montana": "MT", "Nebraska": "NE",
    "Nevada": "NV", "NewHampshire": "NH", "NewJersey": "NJ",
    "NewMexico": "NM", "NewYork": "NY", "NewYorkCity": "NY",
    "NorthCarolina": "NC", "NorthDakota": "ND",
    "NorthernMarianaIslands": "MP", "Ohio": "OH", "Oklahoma": "OK",
    "Oregon": "OR", "Pennsylvania": "PA", "PuertoRico": "PR",
    "RhodeIsland": "RI", "SouthCarolina": "SC", "SouthDakota": "SD",
    "Tennessee": "TN", "Texas": "TX", "U.S.VirginIslands": "VI",
    "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
    "WestVirginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
}


def _read_parquet(s3_resource, key: str) -> pd.DataFrame:
    raw = s3_resource.getFile(key)
    return pd.read_parquet(io.BytesIO(raw))


def _read_nndss_disease_basic(s3_resource, slug: str, variant: str = "bylabel_basic") -> pd.DataFrame:
    """Read one NNDSS per-disease output written by ``nndss_disease_subsets``."""
    key = f"{s3_diseases_path}/{slug}/{slug}_{variant}.parquet"
    try:
        return _read_parquet(s3_resource, key)
    except Exception as e:  # noqa: BLE001 - surfaced via the completeness check, not a hard failure
        get_dagster_logger().warning(f"could not read NNDSS disease output {key}: {e}")
        return pd.DataFrame()


def _case_type_from_label(disease_label, unmapped: set) -> str:
    """The part of an NNDSS disease_label after the disease name, snake-cased."""
    if pd.isna(disease_label):
        unmapped.add(str(disease_label))
        return "unknown"
    text = str(disease_label)
    suffix = text.split(",", 1)[1].strip() if "," in text else text
    slug = "_".join(suffix.lower().split())
    return slug or "unknown"


def _nndss_bylabel_to_unified(df: pd.DataFrame, disease: str, source_url: str, unmapped: set) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    out = pd.DataFrame(index=df.index)
    out["disease"] = disease
    out["source"] = "nndss"
    out["grain"] = "state_week"
    out["geo_level"] = "state"
    out["geo_id"] = df["Jurisdiction"].map(NNDSS_JURISDICTION_TO_ABBR)
    out["period_start"] = df["date_week_start"]
    out["period_end"] = df["date_week_end"]
    out["cases"] = pd.to_numeric(df["Cases"], errors="coerce")
    out["cases_min"] = out["cases"]
    out["cases_max"] = out["cases"]
    out["case_type"] = df["disease_label"].map(lambda v: _case_type_from_label(v, unmapped))
    out["travel_status"] = pd.NA
    out["provisional"] = df["Week_Type"].astype("string").str.lower().str.contains(
        "preliminary", na=False
    )
    out["as_of"] = pd.NA
    out["source_url"] = source_url
    return out[UNIFIED_COLUMNS]


def _wnv_county_yearly_to_unified(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    frames = []
    for case_type, column in (
        ("all", "human_cases"),
        ("neuroinvasive", "neuroinvasive_cases"),
        ("blood_donor", "blood_donor_cases"),
    ):
        rows = df[df[column].notna()]
        if rows.empty:
            continue
        period_start = rows["year"].astype("string") + "-01-01"
        period_end = rows["year"].astype("string") + "-12-31"
        frames.append(
            pd.DataFrame(
                {
                    "disease": "west_nile",
                    "source": "arbonet",
                    "grain": "county_year",
                    "geo_level": "county",
                    "geo_id": rows["county_fips"],
                    "period_start": period_start,
                    "period_end": period_end,
                    "cases": rows[column],
                    "cases_min": rows[column],
                    "cases_max": rows[column],
                    "case_type": case_type,
                    "travel_status": pd.NA,
                    "provisional": rows["provisional"],
                    "as_of": pd.NA,
                    "source_url": source_url,
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    return pd.concat(frames, ignore_index=True)[UNIFIED_COLUMNS]


def _wnv_state_cumulative_to_unified(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    frames = []
    for case_type, column in (
        ("all", "total_human_cases"),
        ("neuroinvasive", "total_neuroinvasive_cases"),
    ):
        rows = df[df[column].notna()]
        if rows.empty:
            continue
        year_range = rows["cumulative_year_range"].astype("string").str.split("-")
        frames.append(
            pd.DataFrame(
                {
                    "disease": "west_nile",
                    "source": "arbonet",
                    "grain": "state_cumulative",
                    "geo_level": "state",
                    "geo_id": rows["state_abbr"],
                    "period_start": year_range.map(lambda p: f"{p[0]}-01-01" if isinstance(p, list) and len(p) == 2 else pd.NA),
                    "period_end": year_range.map(lambda p: f"{p[1]}-12-31" if isinstance(p, list) and len(p) == 2 else pd.NA),
                    "cases": rows[column],
                    "cases_min": rows[column],
                    "cases_max": rows[column],
                    "case_type": case_type,
                    "travel_status": pd.NA,
                    "provisional": False,
                    "as_of": rows["as_of"],
                    "source_url": source_url,
                }
            )
        )
    current = df[df["current_season_reported_cases"].notna()]
    if not current.empty:
        frames.append(
            pd.DataFrame(
                {
                    "disease": "west_nile",
                    "source": "arbonet",
                    "grain": "state_season_current",
                    "geo_level": "state",
                    "geo_id": current["state_abbr"],
                    "period_start": current["current_season_year"].astype("string") + "-01-01",
                    "period_end": current["current_season_year"].astype("string") + "-12-31",
                    "cases": current["current_season_reported_cases"],
                    "cases_min": current["current_season_reported_cases"],
                    "cases_max": current["current_season_reported_cases"],
                    "case_type": "all",
                    "travel_status": pd.NA,
                    "provisional": True,
                    "as_of": current["as_of"],
                    "source_url": source_url,
                }
            )
        )
    if not frames:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    return pd.concat(frames, ignore_index=True)[UNIFIED_COLUMNS]


def _dengue_county_current_to_unified(df: pd.DataFrame, source_url: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=UNIFIED_COLUMNS)
    period = df["season_year"].astype("string")
    return pd.DataFrame(
        {
            "disease": "dengue",
            "source": "arbonet",
            "grain": "county_season",
            "geo_level": "county",
            "geo_id": df["county_fips"],
            "period_start": period + "-01-01",
            "period_end": period + "-12-31",
            "cases": df["count_midpoint"],
            "cases_min": df["count_min"],
            "cases_max": df["count_max"],
            "case_type": "confirmed",
            "travel_status": df["travel_status"],
            "provisional": df["provisional"],
            "as_of": df["as_of"],
            "source_url": source_url,
        }
    )[UNIFIED_COLUMNS]


UNIFIED_ASSET_KEY = AssetKey(["vectorborne", "vectorborne_cases_unified"])

UNIFIED_DESCRIPTION = (
    "Unified human-case dataset for dengue, West Nile virus and hantavirus, "
    "merging CDC NNDSS weekly state-level surveillance (source=nndss, "
    "grain=state_week) with CDC ArboNET county/state case data "
    "(source=arbonet, grain in county_year/county_season/state_cumulative/"
    "state_season_current). Each row keeps its native grain — this is NOT "
    "one row per (disease, place, time): the same cases can appear at "
    "multiple grains (e.g. a West Nile case is in both a state-week NNDSS "
    "row and a county-year ArboNET row). Never SUM(cases) across the whole "
    "table; filter to one (disease, grain) pair first. dengue cases_min/"
    "cases_max/cases (midpoint) reflect ArboNET's suppressed county count "
    "bins for small values; NNDSS and ArboNET state/cumulative counts are "
    "exact (cases_min=cases_max=cases). VectorSurv mosquito/vector-activity "
    "data is a separate covariate layer, not included here. Hantavirus has "
    "no ArboNET presence and appears only as source=nndss."
)


@asset(
    group_name="vectorborne",
    key_prefix="vectorborne",
    name="vectorborne_cases_unified",
    required_resource_keys={"s3"},
    deps=[AssetKey(["cdc", "nndss_disease_subsets"])],
    ins={
        "arbonet_wnv_county_yearly": AssetIn(
            key=AssetKey(["arbonet", "arbonet_wnv_county_yearly"])
        ),
        "arbonet_wnv_state_cumulative": AssetIn(
            key=AssetKey(["arbonet", "arbonet_wnv_state_cumulative"])
        ),
        "arbonet_dengue_current": AssetIn(
            key=AssetKey(["arbonet", "arbonet_dengue_current"])
        ),
    },
    metadata={"description": UNIFIED_DESCRIPTION, "source": CDC_SOURCE_URL},
    check_specs=[
        AssetCheckSpec(
            name="vectorborne_unified_completeness",
            asset=UNIFIED_ASSET_KEY,
            description="All three diseases have rows from every source that should cover them.",
        ),
    ],
)
def vectorborne_cases_unified(
    context,
    arbonet_wnv_county_yearly,
    arbonet_wnv_state_cumulative,
    arbonet_dengue_current,
):
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    unmapped_labels = set()

    dengue_nndss = _read_nndss_disease_basic(s3_resource, "dengue_virus_infections")
    arboviral_nndss = _read_nndss_disease_basic(s3_resource, "arboviral_diseases")
    wnv_nndss = arboviral_nndss[
        arboviral_nndss.get("disease_label") == "Arboviral diseases, West Nile virus disease"
    ] if not arboviral_nndss.empty else arboviral_nndss
    hanta_non_hps = _read_nndss_disease_basic(s3_resource, "hantavirus_infection")
    hanta_hps = _read_nndss_disease_basic(s3_resource, "hantavirus_pulmonary_syndrome")

    frames = [
        _nndss_bylabel_to_unified(dengue_nndss, "dengue", CDC_SOURCE_URL, unmapped_labels),
        _nndss_bylabel_to_unified(wnv_nndss, "west_nile", CDC_SOURCE_URL, unmapped_labels),
        _nndss_bylabel_to_unified(hanta_non_hps, "hantavirus", CDC_SOURCE_URL, unmapped_labels),
        _nndss_bylabel_to_unified(hanta_hps, "hantavirus", CDC_SOURCE_URL, unmapped_labels),
        _wnv_county_yearly_to_unified(
            arbonet_wnv_county_yearly,
            f"{ARBONET_S3_OUTPUT_PATH}/arbonet_wnv_county_yearly",
        ),
        _wnv_state_cumulative_to_unified(
            arbonet_wnv_state_cumulative,
            f"{ARBONET_S3_OUTPUT_PATH}/arbonet_wnv_state_cumulative",
        ),
        _dengue_county_current_to_unified(
            arbonet_dengue_current,
            f"{ARBONET_S3_OUTPUT_PATH}/arbonet_dengue_current",
        ),
    ]
    unified = pd.concat(frames, ignore_index=True)
    if unified.empty:
        raise ValueError("vectorborne_cases_unified produced no rows from any source")

    counts = unified.groupby(["disease", "source", "grain"]).size().to_dict()
    logger.info(f"unified vector-borne cases: {len(unified)} rows, by (disease,source,grain): {counts}")

    expected = {
        ("dengue", "nndss"),
        ("dengue", "arbonet"),
        ("west_nile", "nndss"),
        ("west_nile", "arbonet"),
        ("hantavirus", "nndss"),
    }
    present = set(zip(unified["disease"], unified["source"]))
    missing = sorted(expected - present)
    yield AssetCheckResult(
        check_name="vectorborne_unified_completeness",
        passed=not missing,
        severity=AssetCheckSeverity.ERROR,
        description=(
            f"All expected (disease, source) pairs present across {len(unified)} rows."
            if not missing
            else f"Missing (disease, source) pairs: {missing}. An upstream fetch likely "
            "returned no rows."
        ),
        metadata={
            "missing_disease_sources": [f"{d}/{s}" for d, s in missing],
            "row_counts_by_disease_source_grain": {
                f"{d}|{s}|{g}": int(n) for (d, s, g), n in counts.items()
            },
        },
    )

    metadata = store_assets.objectMetadata(
        name="vectorborne_cases_unified",
        description=UNIFIED_DESCRIPTION,
        source_url=CDC_SOURCE_URL,
    )
    store_assets.store_dataframe_to_s3(
        unified,
        f"{VECTORBORNE_OUTPUT_PATH}/vectorborne_cases_unified",
        "vectorborne_cases_unified",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=VECTORBORNE_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "parquet"],
    )
    yield Output(
        unified,
        metadata={
            "rows": len(unified),
            "diseases": sorted(unified["disease"].unique().tolist()),
            "unmapped_nndss_labels": sorted(unmapped_labels),
        },
    )


vectorborne_unified_job = define_asset_job(
    "vectorborne_unified_job",
    selection=[UNIFIED_ASSET_KEY],
)


# nndss_all (@weekly = Sunday) and arbonet_weekly_schedule (Monday 7am PT)
# both need to have completed; 9am PT Monday gives both a two-hour margin.
@schedule(
    job=vectorborne_unified_job,
    cron_schedule="0 9 * * 1",
    name="vectorborne_unified_schedule",
    execution_timezone="America/Los_Angeles",
)
def vectorborne_unified_schedule(context):
    return RunRequest(run_key=f"vectorborne_unified_{datetime.now().date().isoformat()}")
