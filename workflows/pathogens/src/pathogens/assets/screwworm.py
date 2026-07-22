"""New World Screwworm (NWS) surveillance assets.

First in a planned set of assets that collect and reformat New World Screwworm
data published by USDA APHIS.

Source: the "CSV" button on
https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status
which points at a Drupal-hosted file that APHIS refreshes daily (after ~5:30pm
Eastern). The file sits behind Akamai bot protection, so the request must send a
full browser-like header set or the connection is reset.
"""

import io
import pickle
from datetime import datetime

import geopandas as gpd
import pandas as pd
import pytz
from dagster import (
    asset,
    asset_check,
    schedule,
    define_asset_job,
    AssetKey,
    AssetCheckResult,
    AssetCheckSeverity,
    Config,
    RunRequest,
    get_dagster_logger,
)
import requests

from resilient_core.utils import store_assets

# S3 layout (overridable via env, matching the other pathogen modules)
import os

SCREWWORM_S3_PATH = os.environ.get("SCREWWORM_PATH", "pathogens/screwworm/")
SCREWWORM_RAW_PATH = os.environ.get("SCREWWORM_RAW_PATH", f"{SCREWWORM_S3_PATH}raw/")
SCREWWORM_OUTPUT_PATH = os.environ.get("SCREWWORM_OUTPUT_PATH", f"{SCREWWORM_S3_PATH}output/")
SCREWWORM_LATEST_PATH = os.environ.get("SCREWWORM_LATEST_PATH", "screwworm")

NWS_STATUS_PAGE = (
    "https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status"
)
NWS_CSV_URL = "https://www.aphis.usda.gov/sites/default/files/nws-weekly-status.csv"

# APHIS serves this file through Akamai; a partial header set gets the stream
# reset (HTTP/2 INTERNAL_ERROR). Sending a complete Chrome header set lets it
# through.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": NWS_STATUS_PAGE,
}

# Source header -> tidy snake_case name. Footnote markers are preserved in the
# values (e.g. "**" in the dispersal column) so the reformatted data stays
# faithful to the published table.
COLUMN_RENAMES = {
    "Date (mm/dd/yy)": "report_date",
    "State": "state",
    "Species": "species",
    "Age": "age",
    "Active/Inactive*": "status",
    "USDA Sterile Insect Dispersal": "sterile_insect_dispersal",
    "Approximate Miles From US": "approx_miles_from_us",
}


@asset(
    group_name="pathogens",
    key_prefix="screwworm",
    name="nws_aphis_mx_weekly",
    required_resource_keys={"s3"},
)
def nws_weekly_status(context) -> pd.DataFrame:
    """Download and reformat the USDA APHIS New World Screwworm weekly status CSV.

    Stores the raw CSV verbatim, plus a cleaned/reformatted dataset (CSV + JSON)
    to both the asset path and the ``latest`` path.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    response = requests.get(NWS_CSV_URL, headers=BROWSER_HEADERS, timeout=60)
    response.raise_for_status()
    logger.info(f"Downloaded NWS status CSV: {len(response.content)} bytes")

    retrieved_at = datetime.now(pytz.timezone("America/New_York")).isoformat()

    # Preserve the raw download verbatim (BOM and all).
    store_assets.raw_to_s3(
        response.content,
        f"{SCREWWORM_RAW_PATH}nws-weekly-status.csv",
        s3_resource,
        contenttype="text/csv",
    )

    # utf-8-sig strips the leading BOM on the first column name.
    df = pd.read_csv(io.BytesIO(response.content), encoding="utf-8-sig", dtype=str)
    df = df.rename(columns=COLUMN_RENAMES)

    missing = set(COLUMN_RENAMES.values()) - set(df.columns)
    if missing:
        raise ValueError(
            f"NWS CSV is missing expected columns {sorted(missing)}; "
            f"source layout changed. Got columns: {list(df.columns)}"
        )

    # Two-digit years: pandas maps 00-68 -> 2000-2068, which covers this dataset.
    df["report_date"] = pd.to_datetime(
        df["report_date"], format="%m/%d/%y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    bad_dates = df["report_date"].isna().sum()
    if bad_dates:
        logger.warning(f"{bad_dates} rows had unparseable report_date values")

    df["approx_miles_from_us"] = pd.to_numeric(
        df["approx_miles_from_us"], errors="coerce"
    )

    df = df.sort_values("report_date", ascending=False, na_position="last").reset_index(drop=True)
    df["retrieved_at"] = retrieved_at

    logger.info(f"Reformatted NWS status: {len(df)} rows, {df['report_date'].max()} latest report date")

    metadata = store_assets.objectMetadata(
        name="nws_weekly_status",
        description=(
            "USDA APHIS New World Screwworm weekly status: confirmed cases in "
            "Mexico by report date, state, species, age, active/inactive status, "
            "USDA sterile insect dispersal, and approximate miles from the US "
            "border. Reformatted from the daily APHIS CSV."
        ),
        source_url=NWS_STATUS_PAGE,
    )

    store_assets.store_dataframe_to_s3(
        df,
        SCREWWORM_OUTPUT_PATH,
        "nws_weekly_status",
        s3_resource,
        metadata=metadata,
        latestdatasetpath=SCREWWORM_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "json"],
    )

    return df


nws_weekly_status_job = define_asset_job(
    "nws_aphis_mx_job",
    selection=[AssetKey(["screwworm", "nws_aphis_mx_weekly"])],
)


# APHIS refreshes the source CSV daily after ~5:30pm Eastern; run at 6:00pm
# Eastern to give the publisher a buffer to finish.
@schedule(
    job=nws_weekly_status_job,
    cron_schedule="0 18 * * *",
    name="nws_aphis_mx_weekly_schedule",
    execution_timezone="America/New_York",
)
def nws_weekly_status_schedule(context):
    return RunRequest()


# ---------------------------------------------------------------------------
# USDA APHIS NWS Public Reporting Tableau dashboard
#
# The us-confirmed-cases-new-world page embeds a Tableau Server viz that holds
# the *US* (Texas) surveillance data — a per-case line list, a county map, and
# summary/timeline sheets. This is distinct data from the weekly-status CSV
# above (that CSV covers Mexican cases only).
#
# The dashboard is behind Akamai and its VizQL config is populated by JavaScript,
# so a plain HTTP scrape sees an empty config. We drive a headless browser
# (Playwright) once to clear the Akamai challenge and read the rendered config,
# then hand the warmed cookies + config to TableauScraper to pull each
# worksheet's data.
# ---------------------------------------------------------------------------

NWS_DASHBOARD_VIEW = (
    "https://publicdashboards.dl.usda.gov/t/MRP_PUB/views/"
    "NewWorldScrewwormPublicReporting_17805168329840/SummaryDashboard"
)

# Worksheet -> {source field (without the TableauScraper "-alias" suffix): tidy name}.
# Only listed fields are kept in the cleaned output; everything else (raw geometry,
# spacer measures, internal sqlproxy columns) is dropped but preserved in the raw dump.
US_CASES_FIELDS = {
    "Confirmed Date": "confirmed_date",
    "State": "state",
    "County": "county",
    "Case Type": "case_type",
    "Animal Type": "animal_type",
    "Species": "species",
    "Animal ID": "animal_id",
    "Status": "status",
}
COUNTY_SUMMARY_FIELDS = {
    "State Name": "state",
    "State Abbreviation": "state_abbr",
    "County Name": "county",
    "Latitude (generated)": "latitude",
    "Longitude (generated)": "longitude",
    "AGG(Total Cases)": "total_cases",
    "AGG(Number of Domestic Cases)": "domestic_cases",
    "AGG(Number of Feral & Wildlife Cases)": "wildlife_feral_cases",
    "AGG(Number of Fly Trap Detections)": "fly_trap_detections",
    "AGG(Active/Inactive)": "status",
    "AGG(Latest Animal Confirmed Date)": "latest_animal_confirmed_date",
    "AGG(Latest Fly Confirmed Date)": "latest_fly_confirmed_date",
}


def _fetch_dashboard_worksheets(logger) -> dict:
    """Return {worksheet_name: DataFrame} for the NWS Tableau dashboard.

    Uses Playwright to pass Akamai and read the JS-populated VizQL config, then
    TableauScraper to bootstrap the session and parse each worksheet.
    """
    import json
    import re
    from urllib.parse import urlparse

    from playwright.sync_api import sync_playwright
    from tableauscraper import TableauScraper, api

    ua = BROWSER_HEADERS["User-Agent"]
    config = None
    cookies = None
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        try:
            ctx = browser.new_context(user_agent=ua)
            page = ctx.new_page()
            page.goto(
                f"{NWS_DASHBOARD_VIEW}?:embed=y&:showVizHome=no",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            # The viz bootstraps asynchronously; wait for the config textarea to fill.
            page.wait_for_function(
                "() => { const el = document.querySelector('#tsConfigContainer');"
                " return el && (el.value || el.textContent || '').length > 100; }",
                timeout=45000,
            )
            config = page.eval_on_selector(
                "#tsConfigContainer", "el => el.value || el.textContent"
            )
            cookies = ctx.cookies()
        finally:
            browser.close()

    if not config:
        raise RuntimeError("Could not read Tableau VizQL config from the dashboard page")

    ts = TableauScraper()
    api.setSession(ts)
    ts.session.headers.update({"User-Agent": ua})
    for c in cookies:
        ts.session.cookies.set(
            c["name"], c["value"], domain=c["domain"].lstrip("."), path=c.get("path", "/")
        )
    ts.tableauData = json.loads(config)
    uri = urlparse(NWS_DASHBOARD_VIEW)
    ts.host = f"{uri.scheme}://{uri.netloc}"

    raw = api.getTableauData(ts)
    match = re.search(r"\d+;({.*})\d+;({.*})", raw, re.MULTILINE)
    if not match:
        raise RuntimeError("Unexpected Tableau bootstrap payload; could not parse data")
    ts.info = json.loads(match.group(1))
    ts.data = json.loads(match.group(2))

    worksheets = {ws.name: ws.data for ws in ts.getWorkbook().worksheets}
    logger.info(
        "Fetched NWS dashboard worksheets: "
        + ", ".join(f"{n}({len(d)})" for n, d in worksheets.items())
    )
    return worksheets


def _clean_worksheet(df: pd.DataFrame, fields: dict) -> pd.DataFrame:
    """Keep the TableauScraper ``-alias`` columns for the requested fields and
    rename them to tidy names, preserving the ``fields`` ordering."""
    renamed = {}
    for source, tidy in fields.items():
        col = f"{source}-alias"
        if col in df.columns:
            renamed[tidy] = df[col].reset_index(drop=True)
    return pd.DataFrame(renamed)


@asset(
    group_name="pathogens",
    key_prefix="screwworm",
    name="nws_aphis_us",
    required_resource_keys={"s3"},
)
def nws_dashboard(context) -> dict:
    """Scrape the USDA APHIS NWS Public Reporting Tableau dashboard (US data).

    Writes a raw JSON dump of every non-empty worksheet plus two cleaned
    datasets: the per-case US line list (``nws_us_cases``) and the county-level
    summary with coordinates (``nws_us_county_summary``).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    worksheets = _fetch_dashboard_worksheets(logger)
    retrieved_at = datetime.now(pytz.timezone("America/New_York")).isoformat()

    # Raw provenance: dump each non-empty worksheet verbatim.
    for name, wdf in worksheets.items():
        if wdf.empty:
            continue
        safe = name.replace("/", "_").replace(" ", "_").replace("(", "").replace(")", "").lower()
        store_assets.dataframe_to_s3(
            wdf, f"{SCREWWORM_RAW_PATH}dashboard/{safe}", s3_resource, formats=["json"]
        )

    def _store(df, identifier: str, description: str, formats=("csv", "json")) -> int:
        df = df.copy()
        df["retrieved_at"] = retrieved_at
        metadata = store_assets.objectMetadata(
            name=identifier, description=description, source_url=NWS_DASHBOARD_VIEW
        )
        store_assets.store_dataframe_to_s3(
            df, SCREWWORM_OUTPUT_PATH, identifier, s3_resource,
            metadata=metadata, latestdatasetpath=SCREWWORM_LATEST_PATH,
            enable_latest_path=True, formats=list(formats),
        )
        return len(df)

    counts = {}

    # US per-case line list (purpose-built "ExportToCSV" worksheet).
    cases = _clean_worksheet(worksheets.get("ExportToCSV", pd.DataFrame()), US_CASES_FIELDS)
    if not cases.empty:
        cases["confirmed_date"] = pd.to_datetime(
            cases["confirmed_date"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
        cases = cases.sort_values("confirmed_date", ascending=False, na_position="last").reset_index(drop=True)
        counts["nws_us_cases"] = _store(
            cases, "nws_us_cases",
            "USDA APHIS New World Screwworm confirmed US cases (line list): "
            "confirmed date, state, county, case type, animal type, species, "
            "animal ID, and active/inactive status. Scraped from the APHIS NWS "
            "Public Reporting Tableau dashboard.",
        )

    # County-level summary with coordinates ("State Map" worksheet).
    county = _clean_worksheet(worksheets.get("State Map", pd.DataFrame()), COUNTY_SUMMARY_FIELDS)
    if not county.empty:
        for numeric in [
            "latitude", "longitude", "total_cases", "domestic_cases",
            "wildlife_feral_cases", "fly_trap_detections",
        ]:
            if numeric in county.columns:
                county[numeric] = pd.to_numeric(county[numeric], errors="coerce")
        county = county.sort_values("total_cases", ascending=False, na_position="last").reset_index(drop=True)
        # Emit as a point layer (one point per county centroid, EPSG:4326).
        county = county.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
        county_gdf = gpd.GeoDataFrame(
            county,
            geometry=gpd.points_from_xy(county["longitude"], county["latitude"]),
            crs="EPSG:4326",
        )
        counts["nws_us_county_summary"] = _store(
            county_gdf, "nws_us_county_summary",
            "USDA APHIS New World Screwworm US county-level summary as points "
            "(one per county centroid): total cases, domestic vs feral/wildlife "
            "case counts, fly trap detections, active/inactive status, and latest "
            "confirmed dates. Scraped from the APHIS NWS Public Reporting Tableau dashboard.",
            formats=("geojson", "csv"),
        )

    logger.info(f"NWS dashboard stored datasets: {counts}")
    return {"retrieved_at": retrieved_at, "row_counts": counts}


nws_dashboard_job = define_asset_job(
    "nws_aphis_us_job",
    selection=[AssetKey(["screwworm", "nws_aphis_us"])],
)


# Run at 6:15pm Eastern, staggered after the weekly-status CSV pull.
@schedule(
    job=nws_dashboard_job,
    cron_schedule="15 18 * * *",
    name="nws_aphis_us_schedule",
    execution_timezone="America/New_York",
)
def nws_dashboard_schedule(context):
    return RunRequest()


# ---------------------------------------------------------------------------
# OMSA / WOAH regional Power BI dashboard (Mexico + Central America)
#
# "Reportes de Focos de Gusano Barrenador del Ganado en México y Centroamérica,
# OMSA" — a Power BI "publish to web" report backed by a single tabular dataset
# (GBG_OMSA). Unlike the APHIS sources above (US and Mexico-only), this covers
# the whole outbreak region: Belize, Costa Rica, El Salvador, USA, Guatemala,
# Honduras, Mexico, Nicaragua, and Panama.
#
# The report is one flat table at the grain of a *focus* (outbreak): country,
# province, locality, coordinates, start date, and susceptible/case counts per
# species. We query it directly through the Power BI public querydata API — no
# browser is needed. The report's resource key is encoded in the share URL; the
# dataset/report/model identifiers come from the report's initial network calls
# (visible in the browser Network tab as reports/conceptualschema and
# reports/querydata) and are stable until the report is republished.
# ---------------------------------------------------------------------------

NWS_OMSA_VIEW = (
    "https://app.powerbi.com/view?r=eyJrIjoiYWJmODE4MTUtNjAwYS00NjA0LTllY2Ut"
    "MzhmYzE2NDFmM2EzIiwidCI6ImM1OWRjNTZhLTkzZWMtNGIwNy1iNzFkLTQzYzg0NDkyNTcxOCIsImMiOjR9"
)
# Cluster 4 -> south-central-us (the "c" field in the decoded share token).
NWS_OMSA_QUERY_URL = (
    "https://wabi-south-central-us-api.analysis.windows.net/public/reports/querydata?synchronous=true"
)
NWS_OMSA_RESOURCE_KEY = "abf81815-600a-4604-9ece-38fc1641f3a3"
NWS_OMSA_DATASET_ID = "0cb38669-8335-43e1-a315-23a942ebf2db"
NWS_OMSA_REPORT_ID = "ec900b4c-8a6d-4301-9e75-2fb5573ba43a"
NWS_OMSA_MODEL_ID = 8810354
NWS_OMSA_ENTITY = "GBG_OMSA"

# Source column -> tidy snake_case name, in output order. Dimension columns
# first, then per-species susceptible/case counts, then the totals. "Susc"
# columns are the count of susceptible animals; "Casos"/"cases" are confirmed
# cases. Species: canine, equine, swine (porcino), bovine, ovine (sheep),
# poultry (aves de corral), caprine (goat), feline, buffalo, wild birds,
# terrestrial wildlife, and domestic rabbit.
OMSA_DIM_COLS = {
    "País": "country",
    "ABREV": "country_abbr",
    "ID": "focus_id",
    "Provincia": "province",
    "Localidad": "locality",
    "Latitud": "latitude",
    "Longitud": "longitude",
    "Fecha Inicio": "start_date",
}
OMSA_MEASURE_COLS = {
    "Susc_Canino": "susceptible_canine",
    "Casos_Canin": "cases_canine",
    "Susc_equin": "susceptible_equine",
    "CasosEquin": "cases_equine",
    "SuscepPorci": "susceptible_swine",
    "CasosPorcin": "cases_swine",
    "SuscBovino": "susceptible_bovine",
    "CasosBovin": "cases_bovine",
    "SuscOvino": "susceptible_ovine",
    "CasosOvino": "cases_ovine",
    "SuscAvesCorr": "susceptible_poultry",
    "CasosAvesCorr": "cases_poultry",
    "SuscCapri": "susceptible_caprine",
    "CasosCapri": "cases_caprine",
    "Susc_feli": "susceptible_feline",
    "Casos Felinos": "cases_feline",
    "Susc_bufal": "susceptible_buffalo",
    "CasosBufalos": "cases_buffalo",
    "Susc_AvesSilvestres": "susceptible_wild_birds",
    "CasosAvesSilvestres": "cases_wild_birds",
    "Susc_Silvestres_Terrestres": "susceptible_terrestrial_wildlife",
    "CasosSilvestres Terrestres": "cases_terrestrial_wildlife",
    "Sus_Conejos_Doméstico": "susceptible_domestic_rabbit",
    "Casos_Conejos_Doméstico": "cases_domestic_rabbit",
    "Tot_Suscep": "total_susceptible",
    "Total_casos": "total_cases",
    "focos_conteo": "focus_count",
}
OMSA_COLUMN_RENAMES = {**OMSA_DIM_COLS, **OMSA_MEASURE_COLS}


def _omsa_querydata_body() -> dict:
    """Build the Power BI querydata request that dumps the whole GBG_OMSA table.

    One SemanticQueryDataShapeCommand selecting every dimension column and every
    per-species measure, grouped so each focus (outbreak) comes back as one row.
    """
    src = {"Source": "g"}
    selects = []
    for col in OMSA_DIM_COLS:
        selects.append(
            {"Column": {"Expression": {"SourceRef": src}, "Property": col}, "Name": f"g.{col}"}
        )
    for col in OMSA_MEASURE_COLS:
        selects.append(
            {
                "Aggregation": {
                    "Expression": {"Column": {"Expression": {"SourceRef": src}, "Property": col}},
                    "Function": 0,  # Sum
                },
                "Name": f"Sum(g.{col})",
            }
        )
    command = {
        "SemanticQueryDataShapeCommand": {
            "Query": {
                "Version": 2,
                "From": [{"Name": "g", "Entity": NWS_OMSA_ENTITY, "Type": 0}],
                "Select": selects,
            },
            "Binding": {
                "Primary": {"Groupings": [{"Projections": list(range(len(selects)))}]},
                # DataVolume 4 + a wide window returns the full table in one page
                # (well above the ~4.7k focus rows this report holds).
                "DataReduction": {"DataVolume": 4, "Primary": {"Window": {"Count": 30000}}},
                "Version": 1,
            },
            "ExecutionMetricsKind": 1,
        }
    }
    return {
        "version": "1.0.0",
        "queries": [
            {
                "Query": {"Commands": [command]},
                "QueryId": "",
                "ApplicationContext": {
                    "DatasetId": NWS_OMSA_DATASET_ID,
                    "Sources": [{"ReportId": NWS_OMSA_REPORT_ID}],
                },
            }
        ],
        "cancelQueries": [],
        "modelId": NWS_OMSA_MODEL_ID,
    }


def _parse_powerbi_dsr(payload: dict) -> pd.DataFrame:
    """Decode a Power BI querydata DSR response into a DataFrame.

    The DSR packs each row against the previous one: a value dictionary
    (``ValueDicts``) holds repeated strings, the ``R`` bitmask marks columns
    copied from the row above, the ``Ø`` bitmask marks nulls, and ``C`` lists
    only the remaining literal values left to right. Column descriptors (``S``)
    arrive once, on the first row.
    """
    ds = payload["results"][0]["result"]["data"]["dsr"]["DS"][0]
    dicts = ds.get("ValueDicts", {})
    rows_raw = ds["PH"][0]["DM0"]
    if not rows_raw:
        return pd.DataFrame(columns=list(OMSA_COLUMN_RENAMES.values()))

    descriptors = rows_raw[0]["S"]
    ncols = len(descriptors)
    dict_names = [d.get("DN") for d in descriptors]  # None unless dictionary-encoded

    prev = [None] * ncols
    rows = []
    for r in rows_raw:
        repeat_mask = r.get("R", 0)
        null_mask = r.get("Ø", 0)
        literals = r.get("C", [])
        li = 0
        row = [None] * ncols
        for i in range(ncols):
            if (null_mask >> i) & 1:
                row[i] = None
            elif (repeat_mask >> i) & 1:
                row[i] = prev[i]
            else:
                row[i] = literals[li]
                li += 1
        prev = row
        # Dictionary-encoded columns store an integer index into ValueDicts on
        # first use, but the literal string once the value is new — only indices
        # need resolving.
        resolved = []
        for i, value in enumerate(row):
            dn = dict_names[i]
            if dn is not None and dn in dicts and isinstance(value, int) and not isinstance(value, bool):
                resolved.append(dicts[dn][value])
            else:
                resolved.append(value)
        rows.append(resolved)

    return pd.DataFrame(rows, columns=list(OMSA_COLUMN_RENAMES.values()))


def _fetch_omsa_focos(logger) -> tuple[bytes, pd.DataFrame]:
    """Query the OMSA Power BI report and return (raw response bytes, tidy df)."""
    headers = {
        "X-PowerBI-ResourceKey": NWS_OMSA_RESOURCE_KEY,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
    }
    response = requests.post(
        NWS_OMSA_QUERY_URL, headers=headers, json=_omsa_querydata_body(), timeout=90
    )
    response.raise_for_status()
    df = _parse_powerbi_dsr(response.json())
    logger.info(f"Fetched OMSA GBG focus table: {len(df)} rows")
    return response.content, df


@asset(
    group_name="pathogens",
    key_prefix="screwworm",
    name="nws_omsa_centroamerica",
    required_resource_keys={"s3"},
)
def nws_omsa(context) -> dict:
    """Scrape the OMSA regional New World Screwworm Power BI dashboard.

    Pulls the underlying GBG_OMSA focus (outbreak) line list for Mexico and
    Central America and writes: the raw querydata response, a cleaned line list
    (``nws_omsa_focos``, CSV + JSON), and a point layer of the same focus
    records (``nws_omsa_focos_points``, EPSG:4326 GeoJSON + CSV).
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    raw_bytes, df = _fetch_omsa_focos(logger)
    retrieved_at = datetime.now(pytz.timezone("America/New_York")).isoformat()

    # Raw provenance: the querydata response verbatim.
    store_assets.raw_to_s3(
        raw_bytes,
        f"{SCREWWORM_RAW_PATH}omsa/gbg_omsa_querydata.json",
        s3_resource,
        contenttype="application/json",
    )

    # Types: dates from epoch-ms, coordinates/counts numeric.
    df["start_date"] = pd.to_datetime(
        df["start_date"], unit="ms", errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    numeric_cols = ["latitude", "longitude", *OMSA_MEASURE_COLS.values()]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    # Count columns are whole animals; keep them as nullable integers.
    for col in OMSA_MEASURE_COLS.values():
        df[col] = df[col].astype("Int64")

    df = df.sort_values("start_date", ascending=False, na_position="last").reset_index(drop=True)
    df["retrieved_at"] = retrieved_at
    logger.info(
        f"OMSA focos: {len(df)} rows, {int(df['total_cases'].sum())} total cases, "
        f"{df['country'].nunique()} countries, latest start_date {df['start_date'].max()}"
    )

    # Cleaned line list (CSV + JSON).
    cases_metadata = store_assets.objectMetadata(
        name="nws_omsa_focos",
        description=(
            "OMSA/WOAH New World Screwworm outbreak (focus) line list for Mexico "
            "and Central America: country, province, locality, coordinates, start "
            "date, and susceptible/confirmed-case counts per species (canine, "
            "equine, swine, bovine, ovine, poultry, caprine, feline, buffalo, wild "
            "birds, terrestrial wildlife, domestic rabbit) plus totals. Scraped "
            "from the OMSA regional Power BI dashboard."
        ),
        source_url=NWS_OMSA_VIEW,
    )
    store_assets.store_dataframe_to_s3(
        df,
        SCREWWORM_OUTPUT_PATH,
        "nws_omsa_focos",
        s3_resource,
        metadata=cases_metadata,
        latestdatasetpath=SCREWWORM_LATEST_PATH,
        enable_latest_path=True,
        formats=["csv", "json"],
    )

    # Point layer: one point per focus with valid coordinates (EPSG:4326).
    geo = df.dropna(subset=["latitude", "longitude"]).reset_index(drop=True)
    geo = geo[(geo["latitude"] != 0) | (geo["longitude"] != 0)]
    focos_gdf = gpd.GeoDataFrame(
        geo,
        geometry=gpd.points_from_xy(geo["longitude"], geo["latitude"]),
        crs="EPSG:4326",
    )
    points_metadata = store_assets.objectMetadata(
        name="nws_omsa_focos_points",
        description=(
            "OMSA/WOAH New World Screwworm outbreaks (foci) for Mexico and Central "
            "America as EPSG:4326 points, one per outbreak: country, province, "
            "locality, start date, and per-species susceptible/case counts. Scraped "
            "from the OMSA regional Power BI dashboard."
        ),
        source_url=NWS_OMSA_VIEW,
    )
    store_assets.store_dataframe_to_s3(
        focos_gdf,
        SCREWWORM_OUTPUT_PATH,
        "nws_omsa_focos_points",
        s3_resource,
        metadata=points_metadata,
        latestdatasetpath=SCREWWORM_LATEST_PATH,
        enable_latest_path=True,
        formats=["geojson", "csv"],
    )

    counts = {"nws_omsa_focos": len(df), "nws_omsa_focos_points": len(focos_gdf)}
    logger.info(f"OMSA dashboard stored datasets: {counts}")
    return {"retrieved_at": retrieved_at, "row_counts": counts}


# ---------------------------------------------------------------------------
# Schema-drift guard for the OMSA Power BI report
#
# The nws_omsa asset queries GBG_OMSA columns by their source names (the keys of
# OMSA_COLUMN_RENAMES). If OMSA renames or drops a property, the querydata call
# silently returns that column as null and the data quietly degrades. To catch
# that, we snapshot the authoritative GBG_OMSA property list from the report's
# conceptualschema endpoint and compare it to a pickled baseline — mirroring the
# WAHIS Excel column check.
# ---------------------------------------------------------------------------

NWS_OMSA_SCHEMA_URL = (
    "https://wabi-south-central-us-api.analysis.windows.net/public/reports/conceptualschema"
)
NWS_OMSA_COLUMNS_CURRENT_KEY = f"{SCREWWORM_RAW_PATH}omsa/reference/gbg_omsa_columns.current.pkl"
NWS_OMSA_COLUMNS_BASELINE_KEY = f"{SCREWWORM_RAW_PATH}omsa/reference/gbg_omsa_columns.baseline.pkl"


def _fetch_omsa_schema_columns(logger) -> list:
    """Return the GBG_OMSA property names from the report's conceptualschema."""
    headers = {
        "X-PowerBI-ResourceKey": NWS_OMSA_RESOURCE_KEY,
        "Content-Type": "application/json;charset=UTF-8",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Origin": "https://app.powerbi.com",
        "Referer": "https://app.powerbi.com/",
    }
    body = {"modelIds": [NWS_OMSA_MODEL_ID], "userPreferredLocale": "en-US"}
    response = requests.post(NWS_OMSA_SCHEMA_URL, headers=headers, json=body, timeout=60)
    response.raise_for_status()
    schema = response.json()
    for s in schema.get("schemas", []):
        for entity in s.get("schema", {}).get("Entities", []):
            if entity.get("Name") == NWS_OMSA_ENTITY:
                columns = [p["Name"] for p in entity.get("Properties", [])]
                logger.info(f"Fetched {NWS_OMSA_ENTITY} schema: {len(columns)} properties")
                return columns
    raise RuntimeError(
        f"Entity {NWS_OMSA_ENTITY} not found in conceptualschema response; report layout changed."
    )


def _pickle_columns_to_s3(columns, key, s3_resource):
    """Pickle a list of column names to a fixed S3 key."""
    s3_resource.putFile(
        data=pickle.dumps(list(columns)),
        path=key,
        content_type="application/octet-stream",
    )


def _load_pickled_columns(key, s3_resource):
    """Load a pickled list of column names, or None if the key is absent."""
    try:
        data = s3_resource.getFile(path=key)
    except Exception:
        return None
    return list(pickle.loads(data))


class NwsOmsaColumnsConfig(Config):
    # Set true to re-bless the current schema as the baseline (accept a schema
    # change). Left false, the baseline is only written once, on first run.
    update_baseline: bool = False


@asset(
    group_name="pathogens",
    key_prefix="screwworm",
    name="nws_omsa_columns",
    required_resource_keys={"s3"},
)
def nws_omsa_columns(context, config: NwsOmsaColumnsConfig) -> dict:
    """Snapshot the OMSA GBG_OMSA schema column names for drift detection.

    Pickles the current property names to a fixed S3 key on every run, and writes
    the baseline the first time (or whenever ``update_baseline`` is set, to
    accept a deliberate schema change). The companion asset check compares the
    current snapshot against the baseline.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    columns = _fetch_omsa_schema_columns(logger)
    _pickle_columns_to_s3(columns, NWS_OMSA_COLUMNS_CURRENT_KEY, s3_resource)

    baseline = _load_pickled_columns(NWS_OMSA_COLUMNS_BASELINE_KEY, s3_resource)
    baseline_written = False
    if baseline is None or config.update_baseline:
        _pickle_columns_to_s3(columns, NWS_OMSA_COLUMNS_BASELINE_KEY, s3_resource)
        baseline_written = True
        logger.info(f"Wrote GBG_OMSA column baseline ({len(columns)} columns)")

    return {
        "columns": columns,
        "column_count": len(columns),
        "baseline_written": baseline_written,
        "current_key": NWS_OMSA_COLUMNS_CURRENT_KEY,
        "baseline_key": NWS_OMSA_COLUMNS_BASELINE_KEY,
    }


@asset_check(
    asset=AssetKey(["screwworm", "nws_omsa_columns"]),
    name="nws_omsa_columns_unchanged",
    required_resource_keys={"s3", "slack"},
)
def check_nws_omsa_columns(context) -> AssetCheckResult:
    """Fail (and Slack) if the GBG_OMSA schema columns drift from the baseline.

    Compares the current schema snapshot to the blessed baseline written by
    ``nws_omsa_columns``. Also flags any column the nws_omsa query depends on
    (the keys of OMSA_COLUMN_RENAMES) that is missing from the live schema, since
    that silently breaks the data pull.
    """
    logger = get_dagster_logger()
    s3_resource = context.resources.s3

    current = _load_pickled_columns(NWS_OMSA_COLUMNS_CURRENT_KEY, s3_resource)
    baseline = _load_pickled_columns(NWS_OMSA_COLUMNS_BASELINE_KEY, s3_resource)

    if current is None:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description="No current schema snapshot found; materialize nws_omsa_columns first.",
        )

    # Columns the data asset queries that are absent from the live schema — this
    # breaks nws_omsa regardless of the baseline.
    missing_queried = [c for c in OMSA_COLUMN_RENAMES if c not in current]

    if baseline is None:
        return AssetCheckResult(
            passed=not missing_queried,
            severity=AssetCheckSeverity.ERROR if missing_queried else AssetCheckSeverity.WARN,
            description=(
                "No schema baseline established yet; materialize nws_omsa_columns to set one."
                if not missing_queried
                else f"Queried columns missing from live schema: {missing_queried}"
            ),
            metadata={"current_count": len(current), "missing_queried_columns": missing_queried},
        )

    # Schema property order isn't meaningful here, so compare as sets.
    added = [c for c in current if c not in baseline]
    removed = [c for c in baseline if c not in current]
    changed = bool(added or removed)

    metadata = {
        "baseline_count": len(baseline),
        "current_count": len(current),
        "added_columns": added,
        "removed_columns": removed,
        "missing_queried_columns": missing_queried,
    }

    if not changed and not missing_queried:
        return AssetCheckResult(
            passed=True,
            description=f"GBG_OMSA schema unchanged ({len(current)} columns).",
            metadata=metadata,
        )

    lines = ["⚠️ OMSA GBG_OMSA schema columns changed vs baseline:"]
    if added:
        lines.append(f"• Added ({len(added)}): {added}")
    if removed:
        lines.append(f"• Removed ({len(removed)}): {removed}")
    if missing_queried:
        lines.append(f"• Queried columns now MISSING (breaks nws_omsa): {missing_queried}")
    lines.append(
        "If this change is expected, re-run nws_omsa_columns with config "
        "update_baseline=true to accept the new schema."
    )
    message = "\n".join(lines)

    try:
        channel = os.environ.get("SLACK_CHANNEL_FAILURES", "workflows-failures")
        context.resources.slack.get_client().chat_postMessage(channel=channel, text=message)
    except Exception as e:
        logger.error(f"Failed to send Slack OMSA schema-drift alert: {e}")

    # A missing queried column is a hard break; a pure add/remove elsewhere is a warning.
    return AssetCheckResult(
        passed=False,
        severity=AssetCheckSeverity.ERROR if missing_queried else AssetCheckSeverity.WARN,
        description=message,
        metadata=metadata,
    )


nws_omsa_job = define_asset_job(
    "nws_omsa_centroamerica_job",
    selection=[
        AssetKey(["screwworm", "nws_omsa_centroamerica"]),
        AssetKey(["screwworm", "nws_omsa_columns"]),
    ],
)


# OMSA refreshes on its own cadence; run at 6:30pm Eastern, staggered after the
# two APHIS pulls.
@schedule(
    job=nws_omsa_job,
    cron_schedule="30 18 * * *",
    name="nws_omsa_centroamerica_schedule",
    execution_timezone="America/New_York",
)
def nws_omsa_schedule(context):
    return RunRequest()
