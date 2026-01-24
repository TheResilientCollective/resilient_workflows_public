"""
Pydantic configuration models for the Epidemiology Forecast Component.

These models define the configurable aspects of the component, allowing
it to be reused for different jurisdictions, diseases, simulators, and forecast types.
"""

from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class SimulatorConfig(BaseModel):
    """Configuration for a ResilientSims simulator."""

    simulator_id: int = Field(
        description="Unique identifier of the simulator to run"
    )
    server_url: str = Field(
        default="https://sims.resilientservice.mooo.com",
        description="Base URL for ResilientSIMS API"
    )
    api_path: str = Field(
        default="/api/v1",
        description="API path prefix"
    )
    username_env_var: str = Field(
        default="RESILIENTSIMS_USERNAME",
        description="Environment variable name for simulator username"
    )
    password_env_var: str = Field(
        default="RESILIENTSIMS_PASSWORD",
        description="Environment variable name for simulator password"
    )
    output_bucket: str = Field(
        description="S3 bucket where simulator writes output"
    )
    output_path_prefix: str = Field(
        default="api_run/",
        description="S3 path prefix where simulator writes run outputs"
    )
    check_interval_seconds: int = Field(
        default=30,
        description="Interval between status checks when monitoring runs"
    )
    max_wait_seconds: int = Field(
        default=3600,
        description="Maximum time to wait for simulator completion"
    )


class S3MonitorConfig(BaseModel):
    """Configuration for S3 path monitoring sensor."""

    monitor_path: str = Field(
        description="S3 path to monitor for new forecast data (e.g., 'api_run/')"
    )
    monitor_bucket: str = Field(
        description="S3 bucket to monitor"
    )
    file_pattern: str = Field(
        default="*.csv",
        description="File pattern to match (e.g., '*.csv', 'ForAirTable/*.csv')"
    )
    subdirectory_pattern: Optional[str] = Field(
        default="ForAirTable/",
        description="Subdirectory within run folders to check for files"
    )
    minimum_interval_seconds: int = Field(
        default=600,
        description="Minimum interval between sensor checks in seconds"
    )
    run_path_pattern: Optional[str] = Field(
        default=r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}_run\d+",
        description="Regex pattern for valid run directory names"
    )


class DiseaseConfig(BaseModel):
    """Configuration for a specific disease in the forecast."""

    name: str = Field(description="Disease identifier (e.g., 'COVID', 'FLU', 'RSV')")
    display_name: str = Field(description="Human-readable disease name")
    input_csv_suffix: str = Field(
        description="Suffix for input CSV files for this disease"
    )
    report_delays_key: Optional[str] = Field(
        default=None,
        description="S3 key for report delays RDS file"
    )
    enabled: bool = Field(
        default=True,
        description="Whether this disease is enabled for forecasting"
    )


class TemplateConfig(BaseModel):
    """Configuration for Jinja2 templates used in forecast generation."""

    config_template: str = Field(
        default="forecast_config.yaml",
        description="Jinja2 template filename for simulator configuration"
    )
    run_template: str = Field(
        default="forecast_run.yaml",
        description="Jinja2 template filename for simulator run parameters"
    )
    template_search_paths: List[str] = Field(
        default_factory=lambda: [
            "templates",
            "public/templates",
            "public/public/templates",
            "workflows/public/public/templates",
        ],
        description="Search paths for Jinja2 templates"
    )
    extra_template_vars: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional variables to pass to templates"
    )


class DataSourceConfig(BaseModel):
    """Configuration for data source (input data for forecasts)."""

    source_type: str = Field(
        default="s3",
        description="Type of data source: 's3', 'tableau', 'api'"
    )
    # S3-specific
    s3_input_bucket: Optional[str] = Field(
        default=None,
        description="S3 bucket for input data"
    )
    s3_input_path: Optional[str] = Field(
        default=None,
        description="S3 path for input data"
    )
    # Tableau-specific
    tableau_url: Optional[str] = Field(
        default=None,
        description="Tableau workbook URL"
    )
    tableau_api_url: Optional[str] = Field(
        default=None,
        description="Tableau API endpoint URL"
    )
    workbook_name: Optional[str] = Field(
        default=None,
        description="Tableau workbook name"
    )


class PublishingConfig(BaseModel):
    """Configuration for publishing forecast outputs (no Airtable)."""

    # GitHub
    github_repo_url: Optional[str] = Field(
        default=None,
        description="GitHub repository URL for Rt data publishing"
    )
    github_output_path: str = Field(
        default="forecast_rt",
        description="Path within the GitHub repo for output files"
    )
    github_token_env_var: str = Field(
        default="FORECAST_GITHUB_RT_TOKEN",
        description="Environment variable name for GitHub token"
    )

    # Netlify
    netlify_preview_hook: Optional[str] = Field(
        default=None,
        description="Netlify preview deploy webhook URL"
    )
    netlify_production_hook: Optional[str] = Field(
        default=None,
        description="Netlify production deploy webhook URL"
    )
    netlify_preview_url: Optional[str] = Field(
        default=None,
        description="Netlify preview URL"
    )
    netlify_production_url: Optional[str] = Field(
        default=None,
        description="Netlify production URL"
    )

    # LLM
    llm_webhook_uuid: Optional[str] = Field(
        default=None,
        description="UUID for ResilientLLM webhook"
    )

    # Latest path publishing
    publish_to_latest: bool = Field(
        default=True,
        description="Whether to copy outputs to a 'latest' path"
    )
    latest_path_prefix: str = Field(
        default="latest",
        description="Prefix for 'latest' paths in S3"
    )


class EpiForecastComponentConfig(BaseModel):
    """
    Root configuration for the Epidemiology Forecast Component.

    This configuration defines all aspects of the forecast pipeline,
    making it reusable across different jurisdictions, simulators, and diseases.

    Example usage for multiple simulators:

    ```python
    # San Diego ILI Forecast
    sandiego_ili_config = EpiForecastComponentConfig(
        name="sandiego_ili",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
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
            DiseaseConfig(name="RSV", display_name="RSV", input_csv_suffix="RSV"),
        ],
    )

    # San Diego MPOX Forecast (different simulator)
    sandiego_mpox_config = EpiForecastComponentConfig(
        name="sandiego_mpox",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        simulator=SimulatorConfig(
            simulator_id=2,  # Different simulator
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
    ```
    """

    # Component identity
    name: str = Field(
        description="Unique name for this forecast component instance (e.g., 'sandiego_ili', 'la_covid')"
    )

    # Jurisdiction
    jurisdiction: str = Field(
        description="Jurisdiction identifier (e.g., 'SanDiego', 'LosAngeles')"
    )
    jurisdiction_display: str = Field(
        description="Human-readable jurisdiction name (e.g., 'San Diego County')"
    )

    # Simulator configuration
    simulator: SimulatorConfig = Field(
        description="Configuration for the ResilientSims simulator"
    )

    # S3 paths
    s3_output_base_path: str = Field(
        description="Base path for S3 output (e.g., 'pathogens/sandiego/sandiego_epidemiology')"
    )
    public_bucket: str = Field(
        description="Public S3 bucket for output data"
    )

    # S3 Monitoring
    s3_monitor: S3MonitorConfig = Field(
        description="Configuration for S3 path monitoring"
    )

    # Data Source
    data_source: DataSourceConfig = Field(
        default_factory=DataSourceConfig,
        description="Configuration for input data source"
    )

    # Templates
    templates: TemplateConfig = Field(
        default_factory=TemplateConfig,
        description="Configuration for Jinja2 templates"
    )

    # Diseases
    diseases: List[DiseaseConfig] = Field(
        default_factory=list,
        description="List of diseases to include in the forecast"
    )

    # Publishing
    publishing: PublishingConfig = Field(
        default_factory=PublishingConfig,
        description="Configuration for publishing outputs"
    )

    # Feature flags
    enable_data_extraction: bool = Field(
        default=True,
        description="Enable data extraction from source"
    )
    enable_simulation: bool = Field(
        default=True,
        description="Enable running the simulator"
    )
    enable_processing: bool = Field(
        default=True,
        description="Enable post-processing of results"
    )
    enable_llm_generation: bool = Field(
        default=False,
        description="Enable LLM content generation"
    )
    enable_github_publishing: bool = Field(
        default=False,
        description="Enable GitHub Rt data publishing"
    )
    enable_netlify_deploy: bool = Field(
        default=False,
        description="Enable Netlify deployment triggers"
    )

    # Slack
    slack_channel: str = Field(
        default="#test",
        description="Slack channel for notifications"
    )

    # Asset naming
    asset_group_name: str = Field(
        default="health",
        description="Dagster asset group name"
    )
    asset_key_prefix: Optional[str] = Field(
        default=None,
        description="Dagster asset key prefix (defaults to component name)"
    )

    def get_asset_key_prefix(self) -> str:
        """Get the asset key prefix, defaulting to component name."""
        if self.asset_key_prefix:
            return self.asset_key_prefix
        return self.name.lower().replace("-", "_")

    def get_enabled_diseases(self) -> List[DiseaseConfig]:
        """Get list of enabled diseases."""
        return [d for d in self.diseases if d.enabled]

    class Config:
        """Pydantic model configuration."""

        extra = "forbid"  # Raise error on unknown fields


# Convenience function for creating San Diego ILI config
def create_sandiego_ili_config(
    simulator_id: int = 1,
    output_bucket: str = "resilientseasonal",
    public_bucket: str = "public",
    **kwargs,
) -> EpiForecastComponentConfig:
    """
    Create a pre-configured EpiForecastComponentConfig for San Diego ILI forecasting.

    This is a convenience function that provides sensible defaults for San Diego.
    """
    return EpiForecastComponentConfig(
        name="sandiego_ili",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        s3_output_base_path="pathogens/sandiego/sandiego_epidemiology",
        public_bucket=public_bucket,
        simulator=SimulatorConfig(
            simulator_id=simulator_id,
            output_bucket=output_bucket,
        ),
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket=output_bucket,
        ),
        diseases=[
            DiseaseConfig(name="COVID", display_name="COVID-19", input_csv_suffix="COVID"),
            DiseaseConfig(name="FLU", display_name="Influenza", input_csv_suffix="FLU"),
            DiseaseConfig(name="RSV", display_name="RSV", input_csv_suffix="RSV"),
        ],
        **kwargs,
    )
