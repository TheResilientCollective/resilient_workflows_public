"""
Dagster Component for Epidemiology Forecast.

This module defines a Dagster Component that can be configured via YAML (defs.yaml)
to create multiple forecast pipeline instances with different simulators.

Usage with dg CLI:
    dg scaffold defs epi_forecast.EpiForecastComponent --name sandiego_ili

Example defs.yaml:
    type: epi_forecast.EpiForecastComponent
    attributes:
      name: sandiego_ili
      jurisdiction: SanDiego
      jurisdiction_display: San Diego County
      simulator:
        simulator_id: 1
        output_bucket: resilientseasonal
      s3_monitor:
        monitor_path: api_run/
        monitor_bucket: resilientseasonal
      diseases:
        - name: COVID
          display_name: COVID-19
          input_csv_suffix: COVID
        - name: FLU
          display_name: Influenza
          input_csv_suffix: FLU
"""

from typing import Optional, List, Dict, Any, Sequence
from pydantic import BaseModel, Field

try:
    from dagster import (
        Definitions,
        AssetsDefinition,
        SensorDefinition,
        JobDefinition,
    )
    from dagster._core.definitions.module_loaders.object_list import ModuleScopedDagsterDefs

    # Try importing the component system (Dagster 1.9+)
    try:
        from dagster.components import Component, ComponentLoadContext
        COMPONENTS_AVAILABLE = True
    except ImportError:
        # Fallback for older Dagster versions
        COMPONENTS_AVAILABLE = False
        Component = object
        ComponentLoadContext = None

except ImportError:
    COMPONENTS_AVAILABLE = False
    Component = object
    ComponentLoadContext = None

from .config import (
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    DiseaseConfig,
    TemplateConfig,
    DataSourceConfig,
    PublishingConfig,
)
from .definitions import create_epi_forecast_definitions


# ============================================================================
# Pydantic Models for YAML Schema (used by Dagster Component system)
# ============================================================================

class SimulatorYAMLConfig(BaseModel):
    """YAML schema for simulator configuration."""

    simulator_id: int = Field(description="Unique identifier of the simulator to run")
    server_url: str = Field(
        default="https://sims.resilientservice.mooo.com",
        description="Base URL for ResilientSIMS API"
    )
    api_path: str = Field(default="/api/v1", description="API path prefix")
    username_env_var: str = Field(
        default="RESILIENTSIMS_USERNAME",
        description="Environment variable name for simulator username"
    )
    password_env_var: str = Field(
        default="RESILIENTSIMS_PASSWORD",
        description="Environment variable name for simulator password"
    )
    output_bucket: str = Field(description="S3 bucket where simulator writes output")
    output_path_prefix: str = Field(
        default="api_run/",
        description="S3 path prefix where simulator writes run outputs"
    )
    check_interval_seconds: int = Field(default=30)
    max_wait_seconds: int = Field(default=3600)


class S3MonitorYAMLConfig(BaseModel):
    """YAML schema for S3 monitoring configuration."""

    monitor_path: str = Field(description="S3 path to monitor for new forecast data")
    monitor_bucket: str = Field(description="S3 bucket to monitor")
    file_pattern: str = Field(default="*.csv")
    subdirectory_pattern: Optional[str] = Field(default="ForAirTable/")
    minimum_interval_seconds: int = Field(default=600)
    run_path_pattern: Optional[str] = Field(
        default=r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_run\d+"
    )


class DiseaseYAMLConfig(BaseModel):
    """YAML schema for disease configuration."""

    name: str = Field(description="Disease identifier (e.g., 'COVID', 'FLU', 'RSV')")
    display_name: str = Field(description="Human-readable disease name")
    input_csv_suffix: str = Field(description="Suffix for input CSV files")
    report_delays_key: Optional[str] = Field(default=None)
    enabled: bool = Field(default=True)


class PublishingYAMLConfig(BaseModel):
    """YAML schema for publishing configuration."""

    github_repo_url: Optional[str] = Field(default=None)
    github_output_path: str = Field(default="forecast_rt")
    github_token_env_var: str = Field(default="FORECAST_GITHUB_RT_TOKEN")
    netlify_preview_hook: Optional[str] = Field(default=None)
    netlify_production_hook: Optional[str] = Field(default=None)
    llm_webhook_uuid: Optional[str] = Field(default=None)
    publish_to_latest: bool = Field(default=True)
    latest_path_prefix: str = Field(default="latest")


class EpiForecastYAMLConfig(BaseModel):
    """
    Root YAML schema for the Epidemiology Forecast Component.

    This schema defines the structure of the `attributes` section
    in the component's defs.yaml file.
    """

    # Required fields
    name: str = Field(description="Unique name for this forecast instance")
    jurisdiction: str = Field(description="Jurisdiction identifier")
    jurisdiction_display: str = Field(description="Human-readable jurisdiction name")
    s3_output_base_path: str = Field(description="Base path for S3 output")
    public_bucket: str = Field(description="Public S3 bucket for output data")

    # Nested configurations
    simulator: SimulatorYAMLConfig = Field(description="Simulator configuration")
    s3_monitor: S3MonitorYAMLConfig = Field(description="S3 monitoring configuration")

    # Optional configurations
    diseases: List[DiseaseYAMLConfig] = Field(default_factory=list)
    publishing: PublishingYAMLConfig = Field(default_factory=PublishingYAMLConfig)

    # Feature flags
    enable_data_extraction: bool = Field(default=True)
    enable_simulation: bool = Field(default=True)
    enable_processing: bool = Field(default=True)
    enable_llm_generation: bool = Field(default=False)
    enable_github_publishing: bool = Field(default=False)
    enable_netlify_deploy: bool = Field(default=False)

    # Asset configuration
    slack_channel: str = Field(default="#test")
    asset_group_name: str = Field(default="health")
    asset_key_prefix: Optional[str] = Field(default=None)


def yaml_config_to_component_config(yaml_config: EpiForecastYAMLConfig) -> EpiForecastComponentConfig:
    """Convert YAML config model to the internal component config."""
    return EpiForecastComponentConfig(
        name=yaml_config.name,
        jurisdiction=yaml_config.jurisdiction,
        jurisdiction_display=yaml_config.jurisdiction_display,
        s3_output_base_path=yaml_config.s3_output_base_path,
        public_bucket=yaml_config.public_bucket,
        simulator=SimulatorConfig(
            simulator_id=yaml_config.simulator.simulator_id,
            server_url=yaml_config.simulator.server_url,
            api_path=yaml_config.simulator.api_path,
            username_env_var=yaml_config.simulator.username_env_var,
            password_env_var=yaml_config.simulator.password_env_var,
            output_bucket=yaml_config.simulator.output_bucket,
            output_path_prefix=yaml_config.simulator.output_path_prefix,
            check_interval_seconds=yaml_config.simulator.check_interval_seconds,
            max_wait_seconds=yaml_config.simulator.max_wait_seconds,
        ),
        s3_monitor=S3MonitorConfig(
            monitor_path=yaml_config.s3_monitor.monitor_path,
            monitor_bucket=yaml_config.s3_monitor.monitor_bucket,
            file_pattern=yaml_config.s3_monitor.file_pattern,
            subdirectory_pattern=yaml_config.s3_monitor.subdirectory_pattern,
            minimum_interval_seconds=yaml_config.s3_monitor.minimum_interval_seconds,
            run_path_pattern=yaml_config.s3_monitor.run_path_pattern,
        ),
        diseases=[
            DiseaseConfig(
                name=d.name,
                display_name=d.display_name,
                input_csv_suffix=d.input_csv_suffix,
                report_delays_key=d.report_delays_key,
                enabled=d.enabled,
            )
            for d in yaml_config.diseases
        ],
        publishing=PublishingConfig(
            github_repo_url=yaml_config.publishing.github_repo_url,
            github_output_path=yaml_config.publishing.github_output_path,
            github_token_env_var=yaml_config.publishing.github_token_env_var,
            netlify_preview_hook=yaml_config.publishing.netlify_preview_hook,
            netlify_production_hook=yaml_config.publishing.netlify_production_hook,
            llm_webhook_uuid=yaml_config.publishing.llm_webhook_uuid,
            publish_to_latest=yaml_config.publishing.publish_to_latest,
            latest_path_prefix=yaml_config.publishing.latest_path_prefix,
        ),
        enable_data_extraction=yaml_config.enable_data_extraction,
        enable_simulation=yaml_config.enable_simulation,
        enable_processing=yaml_config.enable_processing,
        enable_llm_generation=yaml_config.enable_llm_generation,
        enable_github_publishing=yaml_config.enable_github_publishing,
        enable_netlify_deploy=yaml_config.enable_netlify_deploy,
        slack_channel=yaml_config.slack_channel,
        asset_group_name=yaml_config.asset_group_name,
        asset_key_prefix=yaml_config.asset_key_prefix,
    )


# ============================================================================
# Dagster Component Definition
# ============================================================================

if COMPONENTS_AVAILABLE:

    class EpiForecastComponent(Component):
        """
        Dagster Component for Epidemiology Forecasting.

        This component creates a complete forecast pipeline including:
        - S3 path monitoring sensor
        - Forecast processing assets
        - Publishing to GitHub/Netlify (optional)

        Configure via defs.yaml:

        ```yaml
        type: epi_forecast.EpiForecastComponent
        attributes:
          name: sandiego_ili
          jurisdiction: SanDiego
          jurisdiction_display: San Diego County
          s3_output_base_path: pathogens/sandiego/epidemiology
          public_bucket: public-data
          simulator:
            simulator_id: 1
            output_bucket: resilientseasonal
          s3_monitor:
            monitor_path: api_run/
            monitor_bucket: resilientseasonal
          diseases:
            - name: COVID
              display_name: COVID-19
              input_csv_suffix: COVID
            - name: FLU
              display_name: Influenza
              input_csv_suffix: FLU
            - name: RSV
              display_name: RSV
              input_csv_suffix: RSV
        ```
        """

        # The Pydantic model that defines the YAML schema
        params_schema = EpiForecastYAMLConfig

        def __init__(self, params: EpiForecastYAMLConfig):
            self.params = params
            self._config = yaml_config_to_component_config(params)

        def build_defs(self, context: ComponentLoadContext) -> Definitions:
            """Build Dagster Definitions from the component configuration."""
            return create_epi_forecast_definitions(self._config)

        @classmethod
        def get_description(cls) -> str:
            return "Epidemiology forecast pipeline with S3 monitoring and configurable simulators"

else:
    # Fallback for older Dagster versions without component system
    class EpiForecastComponent:
        """
        Fallback component class for Dagster versions without the component system.

        Use create_epi_forecast_definitions() directly instead.
        """

        def __init__(self, params: EpiForecastYAMLConfig):
            self.params = params
            self._config = yaml_config_to_component_config(params)

        def build_defs(self) -> "Definitions":
            """Build Dagster Definitions from the component configuration."""
            return create_epi_forecast_definitions(self._config)


# ============================================================================
# Helper function for programmatic component creation
# ============================================================================

def load_component_from_yaml(yaml_path: str) -> EpiForecastComponent:
    """
    Load an EpiForecastComponent from a YAML file.

    Args:
        yaml_path: Path to the defs.yaml file

    Returns:
        Configured EpiForecastComponent instance
    """
    import yaml

    with open(yaml_path, "r") as f:
        yaml_content = yaml.safe_load(f)

    attributes = yaml_content.get("attributes", {})
    params = EpiForecastYAMLConfig(**attributes)
    return EpiForecastComponent(params)
