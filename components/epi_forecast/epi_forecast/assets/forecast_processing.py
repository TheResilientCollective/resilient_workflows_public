"""
Forecast Processing Assets for the Epidemiology Forecast Component.

These assets process forecast data from S3 and publish to various outputs.
"""

import io
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    get_dagster_logger,
    AssetsDefinition,
)

from ..config import EpiForecastComponentConfig
from ..utils import store_assets


class ForecastProcessorConfig(Config):
    """Configuration for the forecast processor asset."""

    forecast_run_path: str


def create_forecast_processing_assets(
    config: EpiForecastComponentConfig,
) -> List[AssetsDefinition]:
    """
    Create forecast processing assets based on component configuration.

    Args:
        config: Component configuration

    Returns:
        List of Dagster AssetsDefinition
    """
    key_prefix = config.get_asset_key_prefix()

    @asset(
        group_name=config.asset_group_name,
        key_prefix=key_prefix,
        name="forecast_processor",
        required_resource_keys={"s3", "slack"},
    )
    def forecast_processor(
        context: AssetExecutionContext, asset_config: ForecastProcessorConfig
    ) -> Dict[str, Any]:
        """
        Process forecast CSV files from S3 and store processed data.

        This asset is triggered by the S3 monitor sensor when new forecast
        data is detected in the configured S3 path.
        """
        logger = get_dagster_logger()
        s3_resource = context.resources.s3
        slack_resource = context.resources.slack

        results = {
            "processed_files": [],
            "failed_files": [],
            "run_info": {},
        }

        try:
            run_path = asset_config.forecast_run_path
            if not run_path:
                logger.warning("No forecast_run_path provided")
                return results

            # Determine the path to check for files
            check_path = run_path
            if config.s3_monitor.subdirectory_pattern:
                check_path = f"{run_path}{config.s3_monitor.subdirectory_pattern}"

            logger.info(f"Processing files from: {check_path}")

            # Parse run information
            from ..sensors.s3_monitor import parse_run_id

            run_datetime, run_id = parse_run_id(run_path)
            results["run_info"] = {
                "run_path": run_path,
                "run_datetime": run_datetime.isoformat() if run_datetime else None,
                "run_id": run_id,
            }

            # List files in the path
            try:
                files = list(
                    s3_resource.listPath(
                        check_path, bucket=config.s3_monitor.monitor_bucket
                    )
                )
                csv_files = [f for f in files if f.object_name.endswith(".csv")]
                logger.info(f"Found {len(csv_files)} CSV files")

                if not csv_files:
                    logger.info("No CSV files found to process")
                    return results

            except Exception as e:
                logger.error(f"Error listing files in {check_path}: {e}")
                return results

            # Process each CSV file
            for csv_file in csv_files:
                object_name = Path(csv_file.object_name).name

                try:
                    # Read CSV from S3
                    csv_content = s3_resource.getFile(
                        csv_file.object_name, bucket=config.s3_monitor.monitor_bucket
                    )
                    df = pd.read_csv(io.StringIO(csv_content.decode("utf-8")))

                    if df.empty:
                        logger.warning(f"Empty dataframe for {csv_file.object_name}")
                        results["failed_files"].append(
                            {"file": csv_file.object_name, "error": "Empty dataframe"}
                        )
                        continue

                    logger.info(
                        f"Processing {csv_file.object_name} with {len(df)} rows"
                    )

                    # Store processed data to S3
                    output_path = (
                        f"{config.s3_output_base_path}/output/processed/{run_id}"
                    )
                    output_filename = object_name.replace(".csv", "")

                    metadata = store_assets.objectMetadata(
                        name=f"{config.jurisdiction}_{output_filename}",
                        description=f"Processed forecast data for {config.jurisdiction_display}",
                        source_url=f"s3://{config.s3_monitor.monitor_bucket}/{csv_file.object_name}",
                    )

                    store_assets.store_dataframe_to_s3(
                        df=df,
                        path=output_path,
                        dataset_identifier=output_filename,
                        s3_resource=s3_resource,
                        metadata=metadata,
                        formats=["csv", "json"],
                    )

                    results["processed_files"].append(
                        {
                            "file": csv_file.object_name,
                            "rows_processed": len(df),
                            "output_path": f"{output_path}/{output_filename}",
                        }
                    )

                except Exception as e:
                    logger.error(f"Error processing {csv_file.object_name}: {e}")
                    results["failed_files"].append(
                        {"file": csv_file.object_name, "error": str(e)}
                    )

            # Send Slack notification
            total_processed = len(results["processed_files"])
            total_failed = len(results["failed_files"])

            slack_message = f"""
Forecast Processing Complete ({config.jurisdiction})
Run: {run_id}
Processed: {total_processed} files
Failed: {total_failed} files
            """

            try:
                slack_resource.get_client().chat_postMessage(
                    channel=config.slack_channel, text=slack_message
                )
            except Exception as e:
                logger.warning(f"Failed to send Slack notification: {e}")

        except Exception as e:
            logger.error(f"Error in forecast_processor: {e}")
            results["error"] = str(e)

        return results

    @asset(
        group_name=config.asset_group_name,
        key_prefix=key_prefix,
        name="forecast_latest",
        required_resource_keys={"s3", "slack"},
    )
    def forecast_latest(
        context: AssetExecutionContext, asset_config: ForecastProcessorConfig
    ) -> Dict[str, Any]:
        """
        Copy forecast files to the 'latest' directory in S3.

        This asset makes the most recent forecast data available at a
        consistent S3 path for downstream consumers.
        """
        logger = get_dagster_logger()
        s3_resource = context.resources.s3
        slack_resource = context.resources.slack

        run_path = asset_config.forecast_run_path
        s3_latest_path = f"latest/{config.jurisdiction.lower()}_forecast"

        results = {"files": [], "latest_path": s3_latest_path}

        try:
            # Determine the path to check for files
            check_path = run_path
            if config.s3_monitor.subdirectory_pattern:
                check_path = f"{run_path}{config.s3_monitor.subdirectory_pattern}"

            files = list(
                s3_resource.listPath(
                    check_path, bucket=config.s3_monitor.monitor_bucket
                )
            )

            logger.info(f"Found {len(files)} files to copy to latest")

            s3_client = s3_resource.getClient()
            from minio.commonconfig import CopySource

            for f in files:
                try:
                    source_object = CopySource(
                        config.s3_monitor.monitor_bucket, f.object_name
                    )
                    object_name = Path(f.object_name).name
                    dest_path = f"{s3_latest_path}/{object_name}"

                    s3_client.copy_object(
                        config.public_bucket, dest_path, source_object
                    )
                    results["files"].append(dest_path)
                    logger.info(f"Copied {object_name} to {dest_path}")

                except Exception as e:
                    logger.error(f"Error copying {f.object_name}: {e}")

        except Exception as e:
            logger.error(f"Error in forecast_latest: {e}")
            results["error"] = str(e)

        return results

    return [forecast_processor, forecast_latest]
