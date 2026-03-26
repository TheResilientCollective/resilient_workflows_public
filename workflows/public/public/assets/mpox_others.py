import requests
import json
import pandas as pd
from dagster import asset, AutomationCondition, schedule, RunRequest, define_asset_job, AssetKey, get_dagster_logger
import geopandas as gpd
from datetime import datetime, date
import requests
from epiweeks import Week
from ..utils import store_assets
from ..utils.resilient_epi_schemas import (
    BasicEpidemiologySchema,
    StatisticalExtensionSchema,
    ResilientEpiProcessor,
    EpidemiologyValidationError,
    transform_to_basic_epidemiology,
    create_statistical_extension_record
)

s3_output_path = 'pathogens/ca/counties/'
s3_latest_path = 'pathogens/mpox/california'

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
    filename = f"{s3_output_path}raw/mpox_la_weekly.json"
    store_assets.text_to_s3(json.dumps(data,indent=2), filename, s3_resource, metadata=metadata, contenttype='application/json')

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

    df = pd.DataFrame(records)
    df['isfill'] = df['cases'].apply(lambda x: 'false' if pd.notna(x) else 'true')
    df['cases']=df['cases'].fillna(0)

    # Snap week_start_date to CDC epiweek Sunday start
    df['week_start_date'] = pd.to_datetime(df['week_start_date']).apply(
        lambda d: Week.fromdate(d, system='cdc').startdate()
    )

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🏙️ Processing {len(df)} Los Angeles County Mpox records with resilient epi schemas")

    # Store original format
    filename = f"{s3_output_path}output/mpox_la_weekly"
    store_assets.dataframe_to_s3(df, filename, s3_resource, metadata=metadata, formats=['csv','json'])
    logger.info(f"📊 Stored original LA Mpox data: {len(df)} rows")

    # Process through resilient epi schemas
    try:
        # Filter out records with missing case data and prepare for basic epidemiology schema
        valid_records = df.dropna(subset=['cases']).copy()

        if not valid_records.empty:
            # Prepare data for basic epidemiology schema transformation
            basic_data = valid_records[['week_start_date', 'cases']].copy()
            basic_data['week_start_date'] = pd.to_datetime(basic_data['week_start_date'])
            basic_data = basic_data.rename(columns={
                'week_start_date': 'Date',
                'cases': 'Count'
            })

            # Transform to basic epidemiology schema
            validated_basic = epi_processor.process_basic_epidemiology_data(
                basic_data,
                jurisdiction='LosAngelesCounty',
                validate=True
            )

            if not validated_basic.empty:
                logger.info(f"✅ Created {len(validated_basic)} validated basic epidemiology records for LA County")

                # Add additional metadata
                validated_basic['source_jurisdiction'] = 'Los Angeles County'
                validated_basic['data_source'] = 'PowerBI'
                validated_basic['disease'] = 'Mpox'

                # Store validated basic epidemiology data
                filename_basic = f"{s3_output_path}output/validated_epi_schema"
                metadata_basic = store_assets.objectMetadata(
                    name="mpox_la_weekly_basic_epidemiology",
                    description="Los Angeles County Mpox weekly data in basic epidemiology schema format",
                    source_url=source_url
                )

                store_assets.store_dataframe_to_s3(validated_basic, filename_basic,"mpox_la_weekly_basic", s3_resource,
                                           metadata=metadata_basic, formats=['csv', 'json'],
                                             enable_latest_path=True, latestdatasetpath=s3_latest_path)
                logger.info(f"📋 Stored validated basic epidemiology data for LA County: {len(validated_basic)} rows")

                # Create statistical extension records
                statistical_records = []
                for _, row in valid_records.iterrows():
                    date_str = pd.to_datetime(row['week_start_date']).strftime('%Y-%m-%d')

                    stat_record = create_statistical_extension_record(
                        jurisdiction='LosAngelesCounty',
                        date=date_str,
                        disease='Mpox',
                        metric='cases',
                        observation_type='actual',
                        count=int(row['cases']) if pd.notna(row['cases']) else 0
                    )

                    if not stat_record.empty:
                        stat_record['source_jurisdiction'] = 'Los Angeles County'
                        stat_record['data_source'] = 'PowerBI'
                        statistical_records.append(stat_record)

                if statistical_records:
                    combined_statistical = pd.concat(statistical_records, ignore_index=True)
                    logger.info(f"✅ Created {len(combined_statistical)} statistical extension records for LA County")

                    filename_statistical = f"{s3_output_path}output/validated_epi_schema/mpox_la_weekly_statistical"
                    metadata_statistical = store_assets.objectMetadata(
                        name="mpox_la_weekly_statistical_extension",
                        description="Los Angeles County Mpox weekly data in statistical extension schema format",
                        source_url=source_url
                    )

                    store_assets.dataframe_to_s3(combined_statistical, filename_statistical, s3_resource,
                                               metadata=metadata_statistical, formats=['csv', 'json'])
                    logger.info(f"📊 Stored statistical extension data for LA County: {len(combined_statistical)} rows")
            else:
                logger.warning(f"⚠️  No valid epidemiology data created for LA County")
        else:
            logger.warning(f"⚠️  No valid case data found in LA County records")

    except EpidemiologyValidationError as ve:
        logger.error(f"❌ Validation error for LA County Mpox data: {ve}")
    except Exception as e:
        logger.error(f"❌ Error processing LA County Mpox data with resilient epi schemas: {e}")

@asset(group_name="pathogens", key_prefix="mpox",
       name="mpox_sf_weekly",
required_resource_keys={"s3"},
       automation_condition=AutomationCondition.eager())
def mpox_sf_weekly(context):
    '''https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/explore/query/SELECT%0A%20%20%60episode_date%60%2C%0A%20%20%60new_cases%60%2C%0A%20%20%60cumulative_cases%60%2C%0A%20%20%60max_episode_date%60%2C%0A%20%20%60data_as_of%60%2C%0A%20%20%60data_updated_at%60%2C%0A%20%20%60data_loaded_at%60/page/filter'''
    '''https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/about_data'''
    s3_resource = context.resources.s3
    name = 'mpox_sf_weekly'
    description = '''
          Weekly MPOX data from San Francisco County. Downloaded from Data Portal. 
          '''
    source_url = 'https://data.sfgov.org/Health-and-Social-Services/Mpox-Cases-Over-Time/vi7r-brsi/about_data'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=source_url)

    count_url = "https://data.sfgov.org/resource/vi7r-brsi.geojson?$select=count(cumulative_cases)"
    count_response = requests.get(count_url)
    if count_response.status_code == 200:
        count_json = count_response.json()
        count = int(count_json["features"][0]["properties"][f"count_cumulative_cases"])
        get_dagster_logger().info(f"count {count} for url :{count_url} ")
    else:
        get_dagster_logger().info(f"access failed: {count_response.status_code} {count_response.text}")
        raise Exception(f"access failed: {count_response.status_code} {count_response.text}")
    limit = 1000
    offset = 0
    mpox_df = None
    for i in range(0, count, limit):
        mpox_url = f"https://data.sfgov.org/resource/vi7r-brsi.csv?$offset={i}&$limit={limit}"
        try:
            this_df = pd.read_csv(mpox_url)
            if mpox_df is None:
                mpox_df = this_df
            else:
                mpox_df = pd.concat([mpox_df, this_df], ignore_index=True)
        except Exception as e:
            print(e)
            get_dagster_logger().error(f"{i}: access failed:{mpox_url} {e} ")

    logger = get_dagster_logger()
    epi_processor = ResilientEpiProcessor()

    logger.info(f"🌉 Processing {len(mpox_df)} San Francisco County Mpox records with resilient epi schemas")

    # Store original format
    filename = f"{s3_output_path}raw/mpox_sf_weekly"
    store_assets.dataframe_to_s3(mpox_df, filename, s3_resource, metadata=metadata, formats=['csv','json'])
    logger.info(f"📊 Stored original SF Mpox data: {len(mpox_df)} rows")

    # Process through resilient epi schemas
    try:
        # Filter out records with missing case data and prepare for basic epidemiology schema
        valid_records = mpox_df.dropna(subset=['new_cases']).copy()

        if not valid_records.empty:
            # Aggregate daily episode_date to CDC epiweeks (Sunday start)
            valid_records['episode_date'] = pd.to_datetime(valid_records['episode_date'])
            valid_records['epiweek_start'] = valid_records['episode_date'].apply(
                lambda d: Week.fromdate(d, system='cdc').startdate()
            )
            agg = {'new_cases': 'sum'}
            if 'cumulative_cases' in valid_records.columns:
                agg['cumulative_cases'] = 'max'
            weekly = valid_records.groupby('epiweek_start').agg(agg).reset_index()
            logger.info(f"Aggregated {len(valid_records)} daily SF records → {len(weekly)} CDC epiweeks")

            basic_data = weekly[['epiweek_start', 'new_cases']].copy()
            basic_data = basic_data.rename(columns={
                'epiweek_start': 'Date',
                'new_cases': 'Count'
            })

            # Transform to basic epidemiology schema
            validated_basic = epi_processor.process_basic_epidemiology_data(
                basic_data,
                jurisdiction='SanFranciscoCounty',
                validate=True
            )

            if not validated_basic.empty:
                logger.info(f"✅ Created {len(validated_basic)} validated basic epidemiology records for SF County")

                # Add additional metadata
                validated_basic['source_jurisdiction'] = 'San Francisco County'
                validated_basic['data_source'] = 'DataPortal'
                validated_basic['disease'] = 'Mpox'

                # Store validated basic epidemiology data
                filename_basic = f"{s3_output_path}output/validated_epi_schema"
                metadata_basic = store_assets.objectMetadata(
                    name="mpox_sf_weekly_basic_epidemiology",
                    description="San Francisco County Mpox weekly data in basic epidemiology schema format",
                    source_url=source_url
                )

                store_assets.store_dataframe_to_s3(validated_basic, filename_basic,'mpox_sf_weekly_basic',s3_resource,
                                           metadata=metadata_basic, formats=['csv', 'json'],
                                             enable_latest_path=True, latestdatasetpath=s3_latest_path
                                             )
                logger.info(f"📋 Stored validated basic epidemiology data for SF County: {len(validated_basic)} rows")

                # Create statistical extension records for both new cases and cumulative cases
                statistical_records = []
                for _, row in weekly.iterrows():
                    date_str = pd.Timestamp(row['epiweek_start']).strftime('%Y-%m-%d')

                    # New cases record
                    if pd.notna(row['new_cases']):
                        new_cases_record = create_statistical_extension_record(
                            jurisdiction='SanFranciscoCounty',
                            date=date_str,
                            disease='Mpox',
                            metric='cases',
                            observation_type='actual',
                            count=int(row['new_cases'])
                        )

                        if not new_cases_record.empty:
                            new_cases_record['source_jurisdiction'] = 'San Francisco County'
                            new_cases_record['data_source'] = 'DataPortal'
                            new_cases_record['metric_type'] = 'new_cases'
                            statistical_records.append(new_cases_record)

                    # Cumulative cases record
                    if pd.notna(row['cumulative_cases']):
                        cumulative_record = create_statistical_extension_record(
                            jurisdiction='SanFranciscoCounty',
                            date=date_str,
                            disease='Mpox',
                            metric='cases',
                            observation_type='partial-data estimate',
                            count=int(row['cumulative_cases'])
                        )

                        if not cumulative_record.empty:
                            cumulative_record['source_jurisdiction'] = 'San Francisco County'
                            cumulative_record['data_source'] = 'DataPortal'
                            cumulative_record['metric_type'] = 'cumulative_cases'
                            statistical_records.append(cumulative_record)

                if statistical_records:
                    combined_statistical = pd.concat(statistical_records, ignore_index=True)
                    logger.info(f"✅ Created {len(combined_statistical)} statistical extension records for SF County")

                    filename_statistical = f"{s3_output_path}output/validated_epi_schema/mpox_sf_weekly_statistical"
                    metadata_statistical = store_assets.objectMetadata(
                        name="mpox_sf_weekly_statistical_extension",
                        description="San Francisco County Mpox weekly data in statistical extension schema format",
                        source_url=source_url
                    )

                    store_assets.dataframe_to_s3(combined_statistical, filename_statistical, s3_resource,
                                               metadata=metadata_statistical, formats=['csv', 'json'])
                    logger.info(f"📊 Stored statistical extension data for SF County: {len(combined_statistical)} rows")
            else:
                logger.warning(f"⚠️  No valid epidemiology data created for SF County")
        else:
            logger.warning(f"⚠️  No valid case data found in SF County records")

    except EpidemiologyValidationError as ve:
        logger.error(f"❌ Validation error for SF County Mpox data: {ve}")
    except Exception as e:
        logger.error(f"❌ Error processing SF County Mpox data with resilient epi schemas: {e}")

mpox_counties_job = define_asset_job(
    "mpox_counties_weekly",
    selection=[AssetKey(["mpox", "mpox_la_weekly"]), AssetKey(["mpox", "mpox_sf_weekly"])]
)

@schedule(job=mpox_counties_job, cron_schedule="@weekly", name="mpox_counties_weekly_schedule")
def mpox_counties_weekly_schedule(context):
    return RunRequest()

