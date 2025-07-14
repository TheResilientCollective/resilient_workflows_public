import requests
import json
import pandas as pd
from dagster import asset, AutomationCondition, schedule, RunRequest, define_asset_job, AssetKey
import geopandas as gpd
from datetime import datetime, date
import requests
from ..utils import store_assets

s3_output_path = 'pathogens/ca/counties/'

@asset(group_name="pathogens", key_prefix="mpox",
       name="mpox_la_weekly",
required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())

def mpox_la_powerbi(context):
    s3_resource = context.resources.s3
    name = 'mpox_la_weekly'
    description = '''
       Weekly MPOX data from Los Angeles County. Scraped from PowerBI website 
       '''
    source_url = 'https://app.powerbigov.us/view?r=eyJrIjoiNGI5MmEyMmQtYzVjMS00OTgwLWFhMDgtM2YyMTkxMjJlNmFhIiwidCI6IjA3NTk3MjQ4LWVhMzgtNDUxYi04YWJlLWE2MzhlZGRiYWM4MSJ9'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    base_url = "https://wabi-us-gov-iowa-api.analysis.usgovcloudapi.net/public/reports/querydata?synchronous=true"
    resource_key= "4b92a22d-c5c1-4980-aa08-3f219122e6aa"
    headers = {
        "Content-Type":"application/json, text/plain"
               ,"x-powerbi-resourcekey":resource_key}

    payload = {"version":"1.0.0","queries":[{"Query":{"Commands":[{"SemanticQueryDataShapeCommand":{"Query":{"Version":2,"From":[{"Name":"_","Entity":"_measures","Type":0},{"Name":"r","Entity":"RollingAvgs","Type":0},{"Name":"d","Entity":"Dates","Type":0},{"Name":"_1","Entity":"_titles","Type":0},{"Name":"p","Entity":"Param - MWD","Type":0}],"Select":[{"Measure":{"Expression":{"SourceRef":{"Source":"_"}},"Property":"n"},"Name":"_measures.n","NativeReferenceName":"Case Count"},{"Measure":{"Expression":{"SourceRef":{"Source":"_"}},"Property":"epi_curve_note"},"Name":"_measures.epi_curve_note","NativeReferenceName":"Note"},{"Measure":{"Expression":{"SourceRef":{"Source":"r"}},"Property":"rollingavg_table"},"Name":"RollingAvgs.rollingavg_table","NativeReferenceName":"7-Day Average"},{"Column":{"Expression":{"SourceRef":{"Source":"d"}},"Property":"week_start_dt"},"Name":"Dates.week_start_dt","NativeReferenceName":"Episode Week"},{"Measure":{"Expression":{"SourceRef":{"Source":"_"}},"Property":"epi_curve_text_color"},"Name":"_measures.epi_curve_text_color"},{"Measure":{"Expression":{"SourceRef":{"Source":"_"}},"Property":"epi_curve_text_avg_color"},"Name":"_measures.epi_curve_text_avg_color"},{"Measure":{"Expression":{"SourceRef":{"Source":"_1"}},"Property":"epi_curve_title"},"Name":"_titles.epi_curve_title"}],"Where":[{"Condition":{"In":{"Expressions":[{"Column":{"Expression":{"SourceRef":{"Source":"p"}},"Property":"Fields"}}],"Values":[[{"Literal":{"Value":"'''Dates''[week_start_dt]'"}}]]}}}]},"Binding":{"Primary":{"Groupings":[{"Projections":[0,1,2,3,4,5],"Subtotal":1}]},"Projections":[6],"DataReduction":{"DataVolume":3,"Primary":{"Window":{"Count":500}}},"SuppressedJoinPredicates":[4,5],"Version":1},"ExecutionMetricsKind":1}}]},"CacheKey":"{\"Commands\":[{\"SemanticQueryDataShapeCommand\":{\"Query\":{\"Version\":2,\"From\":[{\"Name\":\"_\",\"Entity\":\"_measures\",\"Type\":0},{\"Name\":\"r\",\"Entity\":\"RollingAvgs\",\"Type\":0},{\"Name\":\"d\",\"Entity\":\"Dates\",\"Type\":0},{\"Name\":\"_1\",\"Entity\":\"_titles\",\"Type\":0},{\"Name\":\"p\",\"Entity\":\"Param - MWD\",\"Type\":0}],\"Select\":[{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"_\"}},\"Property\":\"n\"},\"Name\":\"_measures.n\",\"NativeReferenceName\":\"Case Count\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"_\"}},\"Property\":\"epi_curve_note\"},\"Name\":\"_measures.epi_curve_note\",\"NativeReferenceName\":\"Note\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"r\"}},\"Property\":\"rollingavg_table\"},\"Name\":\"RollingAvgs.rollingavg_table\",\"NativeReferenceName\":\"7-Day Average\"},{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"d\"}},\"Property\":\"week_start_dt\"},\"Name\":\"Dates.week_start_dt\",\"NativeReferenceName\":\"Episode Week\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"_\"}},\"Property\":\"epi_curve_text_color\"},\"Name\":\"_measures.epi_curve_text_color\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"_\"}},\"Property\":\"epi_curve_text_avg_color\"},\"Name\":\"_measures.epi_curve_text_avg_color\"},{\"Measure\":{\"Expression\":{\"SourceRef\":{\"Source\":\"_1\"}},\"Property\":\"epi_curve_title\"},\"Name\":\"_titles.epi_curve_title\"}],\"Where\":[{\"Condition\":{\"In\":{\"Expressions\":[{\"Column\":{\"Expression\":{\"SourceRef\":{\"Source\":\"p\"}},\"Property\":\"Fields\"}}],\"Values\":[[{\"Literal\":{\"Value\":\"'''Dates''[week_start_dt]'\"}}]]}}}]},\"Binding\":{\"Primary\":{\"Groupings\":[{\"Projections\":[0,1,2,3,4,5],\"Subtotal\":1}]},\"Projections\":[6],\"DataReduction\":{\"DataVolume\":3,\"Primary\":{\"Window\":{\"Count\":500}}},\"SuppressedJoinPredicates\":[4,5],\"Version\":1},\"ExecutionMetricsKind\":1}}]}","QueryId":"","ApplicationContext":{"DatasetId":"ef8eaa57-8505-48f9-98db-97efa77f32b3","Sources":[{"ReportId":"b2460244-f012-444e-9c95-81d48968da32","VisualId":"11bd57ce0058a8dc517c"}]}}],"cancelQueries":[],"modelId":797078}

    response = requests.post(
         base_url,
        headers=headers,
        json=payload
    )
    data = response.json()
    rows = (
        data["results"][0]["result"]["data"]["dsr"]["DS"][0]["PH"][1]["DM1"]
    )

    # Extract records
    records = []
    for row in rows:
        cols = row["C"]
        # Some rows might have only a date or a note at the end
        if len(cols) >= 2:
            ts = cols[0]
            count = cols[1]
            # Convert the ms timestamp to date
            date = datetime.utcfromtimestamp(ts / 1000).date()
            records.append({"week_start_date": date, "cases": count})
        elif len(cols) == 1 and isinstance(cols[0], int):
            # Only date, count is missing
            date = datetime.utcfromtimestamp(cols[0] / 1000).date()
            records.append({"week_start_date": date, "cases": None})

    # Save to CSV
    df = pd.DataFrame(records)

    filename = f"{s3_output_path}output/mpox_la_weekly"
    store_assets.dataframe_to_s3(df, filename, s3_resource, metadata=metadata, formats=['csv','json'])

@asset(group_name="pathogens", key_prefix="mpox",
       name="mpox_sf_weekly",
required_resource_keys={"s3"})
def mpox_sf_dataportal(context):
    '''https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/explore/query/SELECT%0A%20%20%60episode_date%60%2C%0A%20%20%60new_cases%60%2C%0A%20%20%60cumulative_cases%60%2C%0A%20%20%60max_episode_date%60%2C%0A%20%20%60data_as_of%60%2C%0A%20%20%60data_updated_at%60%2C%0A%20%20%60data_loaded_at%60/page/filter'''
    '''https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/about_data'''
    s3_resource = context.resources.s3
    name = 'mpox_sf_weekly'
    description = '''
          Weekly MPOX data from San Francisco County. Downloaded from Data Portal. 
          '''
    source_url = 'https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/about_data'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    base_url = "https://data.sfgov.org/resource/vi7r-brsi.csv"
    mpox_df=pd.read_csv(base_url)

    filename = f"{s3_output_path}output/mpox_sf_weekly"
    store_assets.dataframe_to_s3(mpox_df, filename, s3_resource, metadata=metadata, formats=['csv','json'])

mpox_counties_job = define_asset_job(
    "mpox_counties_weekly",
    selection=[AssetKey(["mpox", "mpox_la_weekly"]), AssetKey(["mpox", "mpox_sf_weekly"])]
)

@schedule(job=mpox_counties_job, cron_schedule="@weekly", name="mpox_counties_weekly_schedule")
def mpox_counties_weekly_schedule(context):
    return RunRequest()
