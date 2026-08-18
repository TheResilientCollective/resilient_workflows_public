"""
CDPH Mpox Cases Data Scraper
Extracts the "Cases by Episode Date" chart data from the California Mpox dashboard.

Requirements:
    pip install playwright pandas
    playwright install chromium
"""

import json
import pandas as pd
from playwright.sync_api import sync_playwright
from dagster import asset, AssetIn, AutomationCondition, schedule, RunRequest, define_asset_job, AssetKey, get_dagster_logger
from datetime import datetime, date
from epiweeks import Week
from resilient_core.utils import store_assets
from resilient_core.utils.resilient_epi_schemas import (
    ResilientEpiProcessor,
)

DASHBOARD_URL = "https://skylab4.cdph.ca.gov/mpx_dash/"

s3_output_path = 'pathogens/ca/state/'
s3_latest_path = 'pathogens/mpox/california'

def scrape_mpox_data(shinyapp_url: str = DASHBOARD_URL) -> pd.DataFrame:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Loading dashboard...")
        page.goto(shinyapp_url, wait_until="networkidle", timeout=60_000)

        print("Waiting for chart to render...")
        # Wait until Plotly has actually populated the chart's _fullData
        page.wait_for_function(
            """
            () => {
                const div = document.getElementById('cases_plot');
                return div && div._fullData && div._fullData.length > 0;
            }
            """,
            timeout=30_000,
        )

        print("Extracting chart data...")
        # Pull x/y arrays from the Plotly figure that Shiny rendered
        data = page.evaluate("""
            () => {
                const div = document.getElementById('cases_plot');
                if (!div || !div._fullData) return null;
                return div._fullData.map(trace => ({
                    name: trace.name,
                    x: trace.x,
                    y: trace.y
                }));
            }
        """)

        browser.close()

    if not data:
        raise RuntimeError("Could not find chart data — the page may not have loaded correctly.")

    # Build a tidy DataFrame from the two traces
    traces = {trace["name"]: dict(zip(trace["x"], trace["y"])) for trace in data}

    all_dates = sorted(set().union(*[t["x"] for t in data]))

    rows = []
    for date in all_dates:
        rows.append({
            "date": date,
            "cases_per_day": traces.get("Cases per day", {}).get(date),
            "seven_day_avg": traces.get("7-Day Average Cases", {}).get(date),
        })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # df.to_csv(output_csv, index=False)
    #print(f"Saved {len(df)} rows to '{output_csv}'")
    #print(df.head(10).to_string(index=False))

    return df

@asset(group_name="pathogens", key_prefix="mpox",
       name="mpox_california_daily",
required_resource_keys={"s3"},
       metadata={
           "source": "https://api.tidesandcurrents.noaa.gov/api/prod/"
       ,"description":'''
           CDPH Mpox Cases Data Scraper
           Extracted the "Cases by Episode Date" chart data from the California Mpox dashboard. 
           '''
#,"variableMeasured":variableMeasured
       },
       automation_condition=AutomationCondition.eager())
def get_mpox_data(context) -> pd.DataFrame:
    s3_resource = context.resources.s3
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")
    metadata = store_assets.objectMetadata(name=str(context.asset_key.path[-1]), description=description, source_url=source_url)
    df = scrape_mpox_data()

    store_assets.store_dataframe_to_s3(df, f"{s3_output_path}raw", 'mpox_california_daily', s3_resource, metadata=metadata, formats=['csv', 'parquet'])

    return df



@asset(group_name="pathogens", key_prefix="mpox",
       name="mpox_california_weekly",
       ins={"mpox_california_daily": AssetIn(key=AssetKey(["mpox", "mpox_california_daily"]))},
       required_resource_keys={"s3"},
       metadata={
           "source": DASHBOARD_URL,
           "description": "California Mpox cases aggregated to CDC epiweeks (Sunday start) from daily CDPH data."
       },
       automation_condition=AutomationCondition.eager())
def mpox_california_weekly(context, mpox_california_daily: pd.DataFrame) -> pd.DataFrame:
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    meta = context.assets_def.metadata_by_key[context.asset_key]
    description = meta["description"]
    source_url = meta.get("source")

    daily_df = mpox_california_daily.copy()
    daily_df["date"] = pd.to_datetime(daily_df["date"])

    # Snap each date to its CDC epiweek start (Sunday)
    daily_df["epiweek_start"] = daily_df["date"].apply(
        lambda d: pd.Timestamp(Week.fromdate(d, system="cdc").startdate())
    )

    # Aggregate to weekly
    weekly_df = daily_df.groupby("epiweek_start").agg(
        cases_per_week=("cases_per_day", "sum"),
    ).reset_index()
    weekly_df = weekly_df.sort_values("epiweek_start").reset_index(drop=True)
    logger.info(f"Aggregated {len(daily_df)} daily rows -> {len(weekly_df)} CDC epiweeks")

    # Save raw weekly aggregation
    raw_metadata = store_assets.objectMetadata(name="mpox_california_weekly", description=description, source_url=source_url)
    store_assets.store_dataframe_to_s3(weekly_df, f"{s3_output_path}raw", "mpox_california_weekly", s3_resource,
                                       metadata=raw_metadata, formats=["csv", "parquet"])

    # Basic epidemiology schema output
    basic_data = weekly_df[["epiweek_start", "cases_per_week"]].copy()
    basic_data = basic_data.rename(columns={"epiweek_start": "Date", "cases_per_week": "Count"})

    epi_processor = ResilientEpiProcessor()
    validated_basic = epi_processor.process_basic_epidemiology_data(
        basic_data, jurisdiction="California", validate=True
    )

    if not validated_basic.empty:
        validated_basic["source_jurisdiction"] = "California"
        validated_basic["data_source"] = "CDPH"
        validated_basic["disease"] = "Mpox"

        metadata_basic = store_assets.objectMetadata(
            name="mpox_california_weekly_basic_epidemiology",
            description="California Mpox weekly data in basic epidemiology schema format",
            source_url=source_url
        )
        store_assets.store_dataframe_to_s3(
            validated_basic, f"{s3_output_path}output/validated_epi_schema", "mpox_california_weekly_basic", s3_resource,
            metadata=metadata_basic, formats=["csv", "json"],
            enable_latest_path=True, latestdatasetpath=s3_latest_path
        )
        logger.info(f"Stored basic_epi weekly: {len(validated_basic)} rows")

    return validated_basic

mpox_california_job = define_asset_job(
    "mpox_california_data",
    selection=[AssetKey(["mpox", "mpox_california_daily"]), AssetKey(["mpox", "mpox_california_weekly"])]
)

@schedule(job=mpox_california_job, cron_schedule="@weekly", name="mpox_california_weekly_schedule")
def mpox_california_weekly_schedule(context):
    return RunRequest()

if __name__ == "__main__":
    scrape_mpox_data()
