"""
Dagster Definitions Factory for the Epidemiology Forecast Component.

This module provides a factory function to create Dagster Definitions
for an epidemiology forecast pipeline based on configuration.
"""

import os
from typing import Optional, Dict, Any, Callable

from dagster import (
    Definitions,
    EnvVar,
    define_asset_job,
    AssetKey,
    RunConfig,
)

from .config import EpiForecastComponentConfig
from .resources import S3Resource
from .assets import create_forecast_processing_assets
from .sensors import create_s3_monitor_sensor, S3RunInfo


def build_default_resources(config: EpiForecastComponentConfig) -> Dict[str, Any]:
    """
    Build default resources for the component.

    Args:
        config: Component configuration

    Returns:
        Dictionary of resource name to resource instance
    """
    return {
        "s3": S3Resource(
            S3_BUCKET=EnvVar("S3_BUCKET"),
            S3_ADDRESS=EnvVar("S3_ADDRESS"),
            S3_PORT=EnvVar("S3_PORT"),
            S3_ACCESS_KEY=EnvVar("S3_ACCESS_KEY"),
            S3_SECRET_KEY=EnvVar("S3_SECRET_KEY"),
        ),
    }


def create_epi_forecast_definitions(
    config: EpiForecastComponentConfig,
    resources: Optional[Dict[str, Any]] = None,
    run_config_builder: Optional[Callable[[S3RunInfo], RunConfig]] = None,
    include_sensor: bool = True,
) -> Definitions:
    """
    Create Dagster Definitions for an epidemiology forecast component.

    This factory function creates all assets, jobs, and sensors needed for
    an epidemiology forecast pipeline based on the provided configuration.

    Args:
        config: Component configuration defining jurisdiction, S3 paths,
               diseases, and publishing options
        resources: Optional resource overrides. If not provided, default
                  resources will be created from environment variables.
        run_config_builder: Optional function to build RunConfig from S3RunInfo.
                          Allows customization of how detected runs trigger assets.
        include_sensor: Whether to include the S3 monitoring sensor (default: True)

    Returns:
        Dagster Definitions object with assets, jobs, sensors, and resources

    Example:
        ```python
        from epi_forecast import (
            create_epi_forecast_definitions,
            EpiForecastComponentConfig,
            S3MonitorConfig,
        )

        config = EpiForecastComponentConfig(
            jurisdiction="SanDiego",
            jurisdiction_display="San Diego County",
            s3_output_base_path="pathogens/sandiego/epidemiology",
            public_bucket="public-data",
            s3_monitor=S3MonitorConfig(
                monitor_path="api_run/",
                monitor_bucket="forecast-data",
            ),
        )

        defs = create_epi_forecast_definitions(config)
        ```
    """
    key_prefix = config.get_asset_key_prefix()

    # Build assets
    processing_assets = create_forecast_processing_assets(config)
    all_assets = processing_assets

    # Build job for the assets
    asset_keys = [
        AssetKey([key_prefix, "forecast_processor"]),
        AssetKey([key_prefix, "forecast_latest"]),
    ]

    forecast_job = define_asset_job(
        name=f"{key_prefix}_forecast_job",
        selection=asset_keys,
    )

    all_jobs = [forecast_job]

    # Build default run config builder if not provided
    if run_config_builder is None:

        def default_run_config_builder(run_info: S3RunInfo) -> RunConfig:
            return RunConfig(
                ops={
                    f"{key_prefix}__forecast_processor": {
                        "config": {
                            "forecast_run_path": run_info.run_path,
                        }
                    },
                    f"{key_prefix}__forecast_latest": {
                        "config": {
                            "forecast_run_path": run_info.run_path,
                        }
                    },
                }
            )

        run_config_builder = default_run_config_builder

    # Build sensors
    all_sensors = []
    if include_sensor:
        s3_sensor = create_s3_monitor_sensor(
            config=config,
            job=forecast_job,
            run_config_builder=run_config_builder,
        )
        all_sensors.append(s3_sensor)

    # Build resources
    if resources is None:
        resources = build_default_resources(config)

    return Definitions(
        assets=all_assets,
        jobs=all_jobs,
        sensors=all_sensors,
        resources=resources,
    )
