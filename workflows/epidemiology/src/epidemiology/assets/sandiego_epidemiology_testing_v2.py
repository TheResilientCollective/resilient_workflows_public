import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import requests
from dagster import (
    asset, get_dagster_logger, define_asset_job, AssetKey, sensor, RunRequest,
    SensorEvaluationContext, AssetIn, AutomationCondition,
)
from six import StringIO

from resilient_core.utils import store_assets
from resilient_core.utils.resilient_epi_schemas import ResilientEpiProcessor, EpidemiologyValidationError
from resilient_core.utils.store_assets import store_dataframe_to_s3

from ..utils.tableau_workbook import TableauWorkbookConfig, TableauWorkbookProcessor, convert_tableau_timestamps_to_datetime
from .sandiego_epidemiology_testing import dates3Path, fixTableNames

s3_output_path = 'pathogens/sandiego/sandiego_epidemiology/'
SLACK_CHANNEL = os.environ.get("SLACK_CHANNEL_UPDATES", "#test")

# Draft workbook, replaces CombinedHighlights. Data is one long sheet (Sheet1) of Disease x Metric x week.
config = TableauWorkbookConfig(
    url="https://public.tableau.com/workbooks/DraftCombinedUpdates.twb",
    workbook_name="sandiego_epidemiology_testing_v2",
    wb_api_url="https://public.tableau.com/profile/api/single_workbook/DraftCombinedUpdates",
)

# Tableau Disease -> disease name used in v1 outputs (kept so v1 and v2 stay comparable)
DISEASE_NAMES = {'COVID': 'COVID', 'Influenza': 'FLU', 'RSV': 'RSV'}


def to_epi_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Map the long Tableau sheet to StatisticalExtensionSchema rows.

    PercPos -> metric 'tests':  count=Tests_Visits (comma string, "." when absent), rate=Percent
    Hosp    -> 'hospitalizations': count=Count
    ED      -> 'ed_visits': count=Tests_Visits (total ED visits, same for every disease), rate=Percent, source values as-is
    PercPos -> 'test_positivity', observation_type 'calculated': count=Tests_Visits*Percent (positive tests),
        rate=Percent, only weeks where Tests_Visits is reported
    ED      -> 'ed_visits_cases', observation_type 'calculated': count=Tests_Visits*Percent (ED visits for the disease),
        rate=Percent, only weeks where Tests_Visits is reported
    Deaths  -> 'deaths': count=Count, only weeks where Count is reported (Percent is a constant placeholder, ignored)
    Not mapped: Total Hosp/Deaths/Level (season aggregates, WkStart is null).
    """
    df = df[df['WkStart'].notna() & df['Disease'].isin(DISEASE_NAMES)].copy()
    # v1 used 'Week Ending' (Saturday); WkStart is the Sunday, so +6 days keeps the date comparable
    df['date'] = (pd.to_datetime(df['WkStart']) + pd.Timedelta(days=6)).dt.strftime('%Y-%m-%d')
    df['disease'] = df['Disease'].map(DISEASE_NAMES)
    df['Jurisdiction'] = 'SanDiego' + df['disease']
    df['observation_type'] = 'actual'

    visits = pd.to_numeric(df['Tests_Visits'].astype(str).str.replace(',', ''), errors='coerce')

    def source_rows(tableau_metric, metric_name):
        d = df[df['Metric'] == tableau_metric]
        return d.assign(metric=metric_name, count=visits[d.index], rate=d['Percent'])

    hosp = df[df['Metric'] == 'Hosp'].assign(metric='hospitalizations', count=lambda d: d['Count'], rate=float('nan'))
    # Calculated positives: Tests_Visits * Percent, only where Tests_Visits is reported
    pos = source_rows('PercPos', 'test_positivity')
    pos = pos[pos['count'].notna()].assign(observation_type='calculated', count=lambda d: d['count'] * d['rate'])
    ed_cases = source_rows('ED', 'ed_visits_cases')
    ed_cases = ed_cases[ed_cases['count'].notna()].assign(observation_type='calculated', count=lambda d: d['count'] * d['rate'])
    deaths = df[(df['Metric'] == 'Deaths') & df['Count'].notna()].assign(metric='deaths', count=lambda d: d['Count'], rate=float('nan'))

    cols = ['Jurisdiction', 'date', 'disease', 'metric', 'observation_type', 'count', 'rate']
    out = pd.concat([source_rows('PercPos', 'tests')[cols], source_rows('ED', 'ed_visits')[cols], hosp[cols], pos[cols],
                     ed_cases[cols], deaths[cols]],
                    ignore_index=True)
    return out.drop_duplicates(['disease', 'metric', 'date']).sort_values(['disease', 'metric', 'date']).reset_index(drop=True)


@asset(group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_testing_workbook_download_v2",
    required_resource_keys={"s3"},
    description="Download San Diego epidemiology Testing (DraftCombinedUpdates) Tableau workbook and store in S3",
    automation_condition=AutomationCondition.eager(),
)
def sandiego_epidemiology_testing_workbook_download_v2(context) -> Dict[str, Any]:
    date_path = dates3Path()
    name = f'sandiego_epidemiology_testing_v2_workbook_download_{date_path}'
    description = f'San Diego Epidemiology Testing Data (v2) from Tableau website on {date_path}'
    metadata = store_assets.objectMetadata(name=name, description=description, source_url=config.url)
    s3_resource = context.resources.s3
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)

    workbook_content = processor.download_workbook(config.url)
    s3_key = f"{s3_output_path}raw/{config.workbook_name}/{date_path}/testing_workbook.twbx"
    store_assets.raw_to_s3(workbook_content, s3_key, s3_resource,
                           contenttype='application/octet-stream', metadata=metadata)
    logger.info(f"Stored workbook in S3: s3://{s3_resource.S3_BUCKET}/{s3_key} ({len(workbook_content)} bytes)")

    return {
        "s3_key": s3_key,
        "file_size": len(workbook_content),
        "url": config.url,
        "workbook_name": config.workbook_name,
    }


@asset(group_name="health",
    key_prefix="sandiego",
    name="sandiego_epidemiology_testing_hyper_extraction_v2",
    deps=[AssetKey(['sandiego', 'sandiego_epidemiology_testing_workbook_download_v2'])],
    required_resource_keys={"s3"},
    description="Extract Hyper files from testing v2 Tableau workbook and store in S3",
    ins={
        "sandiego_epidemiology_testing_workbook_download_v2": AssetIn(
            key=AssetKey(['sandiego', 'sandiego_epidemiology_testing_workbook_download_v2'])
        )
    },
    automation_condition=AutomationCondition.eager(),
)
def sandiego_epidemiology_testing_hyper_extraction_v2(
    context,
    sandiego_epidemiology_testing_workbook_download_v2: Dict[str, Any],
) -> Dict[str, Any]:
    date_path = dates3Path()
    logger = get_dagster_logger()
    processor = TableauWorkbookProcessor(logger)
    s3_resource = context.resources.s3
    workbook_name = sandiego_epidemiology_testing_workbook_download_v2["workbook_name"]
    s3_key = sandiego_epidemiology_testing_workbook_download_v2["s3_key"]

    all_dataframes = {}
    with tempfile.TemporaryDirectory() as temp_dir:
        workbook_content = s3_resource.getFile(s3_key)
        extract_dir = Path(temp_dir) / "extracted"
        extraction_info = processor.extract_workbook(workbook_content, extract_dir)

        for hyper_file_rel_path in extraction_info["hyper_files"]:
            hyper_file_path = extract_dir / hyper_file_rel_path
            if not hyper_file_path.exists():
                continue

            hyper_s3_key = f"{s3_output_path}raw/{workbook_name}/{date_path}/hyper/{hyper_file_path.name}"
            metadata = store_assets.objectMetadata(
                name=f'sandiego_epidemiology_testing_v2_workbook_data {hyper_file_path.name}',
                description=f'San Diego Epidemiology testing v2 data files from Tableau {workbook_name} {hyper_file_path.name} on {date_path}',
                source_url=config.url)
            with open(hyper_file_path, 'rb') as f:
                store_assets.raw_to_s3(f.read(), hyper_s3_key, s3_resource,
                                       contenttype='application/octet-stream', metadata=metadata)

            hyper_name = hyper_file_path.name.replace('.hyper', '')
            for table_name, df in processor.extract_hyper_data(hyper_file_path).items():
                if df.empty:
                    continue
                df['extraction_date'] = pd.Timestamp.now().isoformat()
                df['source'] = 'sandiego_epidemiology_tableau'
                df['workbook_name'] = workbook_name
                df['hyper_file'] = hyper_file_path.name
                table_name = fixTableNames(table_name, hyper_name)

                metadata = store_assets.objectMetadata(
                    name=f'sandiego_epidemiology_testing_v2_workbook_data from {hyper_file_path.name} for {table_name}',
                    description=f'San Diego Epidemiology testing v2 data from Tableau {workbook_name} {hyper_file_path.name} for {table_name} on {date_path}',
                    source_url=config.url)
                s3_path = f"{s3_output_path}output/{workbook_name}/{date_path}/{hyper_name}"
                store_assets.store_dataframe_to_s3(
                    df=df, s3_resource=s3_resource, path=s3_path,
                    dataset_identifier=table_name, metadata=metadata)
                all_dataframes[table_name] = {"rows": len(df), "columns": len(df.columns), "s3_path": s3_path}
                logger.info(f"Stored {table_name} from {hyper_file_path.name}")

    return {
        "workbook_name": workbook_name,
        "processed_datasets": all_dataframes,
        "total_datasets": len(all_dataframes),
        "processing_timestamp": pd.Timestamp.now().isoformat(),
        "date_path": date_path,
    }


@asset(
    group_name="health",
    key_prefix="sandiego",
    name="sd_testing_v2",
    deps=[AssetKey(['sandiego', 'sandiego_epidemiology_testing_hyper_extraction_v2'])],
    required_resource_keys={"s3"},
    description="Process SD testing v2 (DraftCombinedUpdates) into resilient statistical-extension epi schema for COVID, FLU, RSV",
    ins={
        "sandiego_epidemiology_testing_hyper_extraction_v2": AssetIn(
            key=AssetKey(['sandiego', 'sandiego_epidemiology_testing_hyper_extraction_v2'])
        )
    },
    automation_condition=AutomationCondition.eager(),
)
def sd_testing_v2(context, sandiego_epidemiology_testing_hyper_extraction_v2: Dict[str, Any]) -> Dict[str, Any]:
    """Read the Sheet1 CSV from S3, map to epi schema per disease/metric, validate and store."""
    logger = get_dagster_logger()
    s3_resource = context.resources.s3
    epi_processor = ResilientEpiProcessor()
    date_path = dates3Path()

    processed_datasets = sandiego_epidemiology_testing_hyper_extraction_v2.get("processed_datasets", {})
    # Sheet2 holds the level thresholds, only Sheet1 is the weekly data
    sheet1_key = next((k for k in processed_datasets if k.lower().startswith("sheet1")), None)
    if sheet1_key is None:
        logger.error(f"No Sheet1 dataset found in processed_datasets. Keys: {list(processed_datasets.keys())}")
        return {"status": "error", "message": "Sheet1 dataset not found"}

    csv_path = f'{processed_datasets[sheet1_key]["s3_path"]}/{sheet1_key}.csv'
    logger.info(f"Reading testing CSV from S3: {csv_path}")
    df = pd.read_csv(StringIO(s3_resource.getFile(csv_path).decode('utf-8')))
    logger.info(f"Loaded testing data: {len(df)} rows, columns: {list(df.columns)}")

    epi_df = to_epi_frame(df)
    results = {}
    for disease, disease_df in epi_df.groupby('disease'):
        try:
            # count/rate are non-nullable when present, so validate each (metric, has-count) slice on its own columns
            validated_df = pd.concat([
                epi_processor.process_statistical_extension_data(part.dropna(axis=1, how='all'), validate=True)
                for _, part in disease_df.groupby(['metric', disease_df['count'].notna()])
            ], ignore_index=True)
            metadata = store_assets.objectMetadata(
                name=f'sd_testing_v2_validated_{disease}_{date_path}',
                description=f'San Diego {disease} testing data (v2) in validated epi schema on {date_path}',
                source_url=config.url)
            store_dataframe_to_s3(
                df=validated_df,
                s3_resource=s3_resource,
                path=f"{s3_output_path}output/sandiego_epidemiology_testing_v2/{date_path}/",
                dataset_identifier=f"validated_epi_schema/{disease}_testing",
                metadata=metadata,
                latestdatasetpath="sandiego_epidemiology_testing_v2/",
                enable_latest_path=True,
            )
            logger.info(f"Stored validated epi schema for {disease}: {len(validated_df)} rows")
            results[disease] = {"rows": len(disease_df), "validated_rows": len(validated_df), "status": "success"}
        except EpidemiologyValidationError as ve:
            logger.error(f"Validation failed for {disease}: {ve}")
            results[disease] = {"rows": len(disease_df), "validated_rows": 0, "status": "validation_error", "error": str(ve)}
        except Exception as e:
            logger.error(f"Error processing {disease}: {e}")
            results[disease] = {"rows": len(disease_df), "validated_rows": 0, "status": "error", "error": str(e)}

    logger.info(f"sd_testing_v2 complete: {results}")
    return {"date_path": date_path, "diseases": results}


sandiego_epidemiology_testing_v2_job = define_asset_job(
    "sandiego_epidemiology_testing_v2",
    selection=[
        AssetKey(["sandiego", "sandiego_epidemiology_testing_workbook_download_v2"]),
        AssetKey(["sandiego", "sandiego_epidemiology_testing_hyper_extraction_v2"]),
        AssetKey(["sandiego", "sd_testing_v2"]),
    ],
)


@sensor(
    job=sandiego_epidemiology_testing_v2_job,
    minimum_interval_seconds=3600,
    required_resource_keys={"slack"},
)
def sandiego_epidemiology_testing_v2_sensor(context: SensorEvaluationContext):
    """Triggers the v2 job when the Tableau workbook's lastPublishDate changes."""
    logger = get_dagster_logger()
    slack = context.resources.slack
    try:
        response = requests.get(config.wb_api_url)
        response.raise_for_status()
        converted_data = convert_tableau_timestamps_to_datetime(response.json())

        if 'lastPublishDate' not in converted_data:
            logger.warning("lastPublishDate field not found in API response")
            return

        last_publish_date = converted_data['lastPublishDate']
        if (context.cursor or None) == str(last_publish_date):
            logger.info(f"No change in lastPublishDate: {last_publish_date}")
            return

        logger.info(f"sandiego_epidemiology_testing_v2 lastPublishDate changed from {context.cursor} to {last_publish_date}")
        try:
            slack.get_client().chat_postMessage(channel=SLACK_CHANNEL,
                                                text=f'sandiego_epidemiology_testing_v2 updated to {last_publish_date}')
        except Exception:
            logger.error('Slack post error for sandiego_epidemiology_testing_v2 updated')
        yield RunRequest(
            run_key=f"sandiego_epidemiology_testing_v2_{last_publish_date.strftime('%Y%m%d_%H%M%S')}",
            run_config={},
        )
        context.update_cursor(str(last_publish_date))
    except Exception as e:
        logger.error(f"Error checking Tableau API: {e}")
