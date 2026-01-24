"""
S3 Path Monitoring Sensor for the Epidemiology Forecast Component.

This sensor monitors an S3 path for new forecast data and triggers
downstream processing when new runs are detected.
"""

import re
from datetime import datetime
from typing import Optional, Callable, Iterator
from dataclasses import dataclass

from dagster import (
    sensor,
    SensorDefinition,
    SensorEvaluationContext,
    RunRequest,
    RunConfig,
    get_dagster_logger,
    JobDefinition,
)

from ..config import EpiForecastComponentConfig, S3MonitorConfig


@dataclass
class S3RunInfo:
    """Information about a detected S3 run."""

    run_path: str
    run_datetime: Optional[datetime]
    run_id: str
    files_found: int
    bucket: str


def parse_run_id(run_path: str) -> tuple[Optional[datetime], str]:
    """
    Parse run ID from path like '2025-09-07T00-07-25_run15'.

    Args:
        run_path: The run directory name/path

    Returns:
        Tuple of (datetime, run_id) where datetime may be None if parsing fails
    """
    run_name = run_path.rstrip("/").split("/")[-1]  # Get last component
    match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})_(.+)", run_name)
    if match:
        date_str = match.group(1)
        # Convert back to standard format for the time part
        date_str = date_str[:10] + "T" + date_str[11:].replace("-", ":")
        run_id = match.group(2)
        try:
            run_datetime = datetime.fromisoformat(date_str)
            return run_datetime, run_id
        except ValueError:
            get_dagster_logger().warning(f"Could not parse datetime from {date_str}")
            return None, run_id
    return None, run_name


def create_s3_monitor_sensor(
    config: EpiForecastComponentConfig,
    job: JobDefinition,
    run_config_builder: Optional[Callable[[S3RunInfo], RunConfig]] = None,
    sensor_name: Optional[str] = None,
) -> SensorDefinition:
    """
    Create an S3 monitoring sensor for the epidemiology forecast component.

    This sensor monitors an S3 path for new forecast runs and triggers
    the specified job when new data is detected.

    Args:
        config: Component configuration containing S3 monitor settings
        job: The Dagster job to trigger when new data is found
        run_config_builder: Optional function to build RunConfig from S3RunInfo.
                          If not provided, a default config with forecast_run_path is used.
        sensor_name: Optional custom sensor name (defaults to "{jurisdiction}_s3_monitor_sensor")

    Returns:
        A configured SensorDefinition
    """
    monitor_config = config.s3_monitor

    if sensor_name is None:
        sensor_name = f"{config.jurisdiction.lower()}_s3_monitor_sensor"

    @sensor(
        job=job,
        name=sensor_name,
        minimum_interval_seconds=monitor_config.minimum_interval_seconds,
        required_resource_keys={"s3", "slack"},
    )
    def s3_monitor_sensor(context: SensorEvaluationContext) -> Iterator[RunRequest]:
        """
        Sensor to watch for new forecast runs in configured S3 path.

        Triggers when new run directories matching the configured pattern are found.
        """
        logger = get_dagster_logger()
        s3_resource = context.resources.s3
        slack_resource = context.resources.slack

        try:
            # List directories in the forecast base path
            run_directories = list(
                s3_resource.listPath(
                    monitor_config.monitor_path, bucket=monitor_config.monitor_bucket
                )
            )

            if not run_directories:
                logger.info(
                    f"No run directories found in {monitor_config.monitor_path}"
                )
                return

            # Filter for run directories matching the expected pattern if specified
            valid_runs = run_directories
            if monitor_config.run_path_pattern:
                run_pattern = re.compile(monitor_config.run_path_pattern)
                valid_runs = [
                    d
                    for d in run_directories
                    if run_pattern.match(d.object_name.rstrip("/").split("/")[-1])
                ]

                if not valid_runs:
                    logger.info(
                        f"No valid run directories matching pattern found in {monitor_config.monitor_path}"
                    )
                    return

            # Sort by name (which corresponds to timestamp) and get the latest
            latest_run = sorted(valid_runs, key=lambda x: x.object_name)[-1]
            run_path = latest_run.object_name

            # Ensure run_path ends with /
            if not run_path.endswith("/"):
                run_path = f"{run_path}/"

            logger.info(f"Latest run found: {run_path}")

            # Check if this run has been processed before using cursor
            last_processed = context.cursor or ""

            if run_path == last_processed:
                logger.info(f"Run {run_path} already processed")
                return

            # Check if subdirectory with files exists
            check_path = run_path
            if monitor_config.subdirectory_pattern:
                check_path = f"{run_path}{monitor_config.subdirectory_pattern}"

            try:
                files_in_path = list(
                    s3_resource.listPath(
                        check_path, bucket=monitor_config.monitor_bucket
                    )
                )

                # Filter by file pattern
                if monitor_config.file_pattern == "*.csv":
                    matching_files = [
                        f for f in files_in_path if f.object_name.endswith(".csv")
                    ]
                else:
                    # Simple glob matching for other patterns
                    matching_files = files_in_path

                if not matching_files:
                    logger.info(f"No matching files found in {check_path}")
                    return

                files_found = len(matching_files)
                logger.info(f"Found {files_found} matching files in {check_path}")

            except Exception as e:
                logger.info(
                    f"Subdirectory not found or error accessing it: {check_path} - {e}"
                )
                return

            # Parse run info
            run_datetime, run_id = parse_run_id(run_path)

            run_info = S3RunInfo(
                run_path=run_path,
                run_datetime=run_datetime,
                run_id=run_id,
                files_found=files_found,
                bucket=monitor_config.monitor_bucket,
            )

            # Send Slack notification about new run
            try:
                slack_message = f"""
New Forecast Run Detected ({config.jurisdiction})
Run: {run_id}
Path: {run_path}
Files Found: {files_found}
Starting processing...
                """
                slack_resource.get_client().chat_postMessage(
                    channel=config.slack_channel, text=slack_message
                )
            except Exception as e:
                logger.warning(f"Failed to send Slack notification: {e}")

            # Update cursor
            context.update_cursor(run_path)

            # Build run config
            if run_config_builder:
                run_config = run_config_builder(run_info)
            else:
                # Default run config
                run_config = RunConfig(
                    ops={
                        f"{config.get_asset_key_prefix()}__forecast_processor": {
                            "config": {
                                "forecast_run_path": run_path,
                            }
                        }
                    }
                )

            yield RunRequest(
                run_key=f"forecast_run_{run_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                tags={
                    "forecast_run_path": run_path,
                    "forecast_run_id": run_id,
                    "jurisdiction": config.jurisdiction,
                },
                run_config=run_config,
            )

        except Exception as e:
            logger.error(f"Error in S3 monitor sensor: {e}")
            try:
                slack_resource.get_client().chat_postMessage(
                    channel=config.slack_channel,
                    text=f"Error in {config.jurisdiction} S3 monitor sensor: {e}",
                )
            except:
                pass

    return s3_monitor_sensor
