"""
Pydantic configuration models for the Epidemiology Forecast Component.

These models define the configurable aspects of the component, allowing
it to be reused for different jurisdictions, diseases, and forecast types.
"""

from typing import Optional, List
from pydantic import BaseModel, Field


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


class EpiForecastComponentConfig(BaseModel):
    """
    Root configuration for the Epidemiology Forecast Component.

    This configuration defines all aspects of the forecast pipeline,
    making it reusable across different jurisdictions and diseases.
    """

    # Jurisdiction
    jurisdiction: str = Field(
        description="Jurisdiction identifier (e.g., 'SanDiego')"
    )
    jurisdiction_display: str = Field(
        description="Human-readable jurisdiction name (e.g., 'San Diego County')"
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
    enable_llm_generation: bool = Field(
        default=True,
        description="Enable LLM content generation"
    )
    enable_github_publishing: bool = Field(
        default=True,
        description="Enable GitHub Rt data publishing"
    )
    enable_netlify_deploy: bool = Field(
        default=True,
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
        description="Dagster asset key prefix (defaults to lowercase jurisdiction)"
    )

    def get_asset_key_prefix(self) -> str:
        """Get the asset key prefix, defaulting to lowercase jurisdiction."""
        if self.asset_key_prefix:
            return self.asset_key_prefix
        return self.jurisdiction.lower()

    class Config:
        """Pydantic model configuration."""
        extra = "forbid"  # Raise error on unknown fields
