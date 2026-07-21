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
from datetime import datetime

import geopandas as gpd
import pandas as pd
import pytz
from dagster import (
    asset,
    schedule,
    define_asset_job,
    AssetKey,
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
