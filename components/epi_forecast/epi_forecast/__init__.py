"""
Epidemiology Forecast Component

A reusable Dagster component for epidemiology forecasting pipelines.

This component can be configured for multiple forecast runs with different
simulators, jurisdictions, and diseases.

Example usage:

    ```python
    from epi_forecast import (
        create_epi_forecast_definitions,
        EpiForecastComponentConfig,
        SimulatorConfig,
        S3MonitorConfig,
        DiseaseConfig,
    )

    # San Diego ILI Forecast
    ili_config = EpiForecastComponentConfig(
        name="sandiego_ili",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        s3_output_base_path="pathogens/sandiego/epidemiology",
        public_bucket="public-data",
        simulator=SimulatorConfig(
            simulator_id=1,
            output_bucket="resilientseasonal",
        ),
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket="resilientseasonal",
        ),
        diseases=[
            DiseaseConfig(name="COVID", display_name="COVID-19", input_csv_suffix="COVID"),
            DiseaseConfig(name="FLU", display_name="Influenza", input_csv_suffix="FLU"),
        ],
    )

    # San Diego MPOX (different simulator)
    mpox_config = EpiForecastComponentConfig(
        name="sandiego_mpox",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        s3_output_base_path="pathogens/sandiego/mpox",
        public_bucket="public-data",
        simulator=SimulatorConfig(
            simulator_id=2,
            output_bucket="resilientmpox",
        ),
        s3_monitor=S3MonitorConfig(
            monitor_path="mpox_runs/",
            monitor_bucket="resilientmpox",
        ),
        diseases=[
            DiseaseConfig(name="MPOX", display_name="Mpox", input_csv_suffix="MPOX"),
        ],
    )

    # Create definitions for each
    ili_defs = create_epi_forecast_definitions(ili_config)
    mpox_defs = create_epi_forecast_definitions(mpox_config)
    ```
"""

from .definitions import create_epi_forecast_definitions
from .config import (
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    DiseaseConfig,
    TemplateConfig,
    DataSourceConfig,
    PublishingConfig,
    create_sandiego_ili_config,
)

__all__ = [
    "create_epi_forecast_definitions",
    "EpiForecastComponentConfig",
    "SimulatorConfig",
    "S3MonitorConfig",
    "DiseaseConfig",
    "TemplateConfig",
    "DataSourceConfig",
    "PublishingConfig",
    "create_sandiego_ili_config",
]
