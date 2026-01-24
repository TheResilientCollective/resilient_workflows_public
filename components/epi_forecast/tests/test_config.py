"""Tests for component configuration."""

import pytest
from epi_forecast.config import (
    EpiForecastComponentConfig,
    S3MonitorConfig,
    PublishingConfig,
    DiseaseConfig,
)


def test_s3_monitor_config_defaults():
    """Test S3MonitorConfig with minimal required fields."""
    config = S3MonitorConfig(
        monitor_path="api_run/",
        monitor_bucket="test-bucket",
    )
    assert config.file_pattern == "*.csv"
    assert config.subdirectory_pattern == "ForAirTable/"
    assert config.minimum_interval_seconds == 600


def test_s3_monitor_config_custom():
    """Test S3MonitorConfig with custom values."""
    config = S3MonitorConfig(
        monitor_path="custom/path/",
        monitor_bucket="custom-bucket",
        file_pattern="*.json",
        subdirectory_pattern="output/",
        minimum_interval_seconds=300,
    )
    assert config.monitor_path == "custom/path/"
    assert config.file_pattern == "*.json"
    assert config.minimum_interval_seconds == 300


def test_disease_config():
    """Test DiseaseConfig creation."""
    config = DiseaseConfig(
        name="COVID",
        display_name="COVID-19",
        input_csv_suffix="COVID",
    )
    assert config.name == "COVID"
    assert config.report_delays_key is None


def test_publishing_config_defaults():
    """Test PublishingConfig with defaults."""
    config = PublishingConfig()
    assert config.github_output_path == "forecast_rt"
    assert config.github_repo_url is None
    assert config.netlify_preview_hook is None


def test_component_config_minimal():
    """Test EpiForecastComponentConfig with minimal fields."""
    config = EpiForecastComponentConfig(
        jurisdiction="TestJurisdiction",
        jurisdiction_display="Test Jurisdiction County",
        s3_output_base_path="test/output",
        public_bucket="test-public",
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket="test-bucket",
        ),
    )
    assert config.jurisdiction == "TestJurisdiction"
    assert config.get_asset_key_prefix() == "testjurisdiction"
    assert config.slack_channel == "#test"
    assert config.enable_github_publishing is True


def test_component_config_custom_prefix():
    """Test custom asset key prefix."""
    config = EpiForecastComponentConfig(
        jurisdiction="TestJurisdiction",
        jurisdiction_display="Test Jurisdiction County",
        s3_output_base_path="test/output",
        public_bucket="test-public",
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket="test-bucket",
        ),
        asset_key_prefix="custom_prefix",
    )
    assert config.get_asset_key_prefix() == "custom_prefix"


def test_component_config_with_diseases():
    """Test component config with disease list."""
    config = EpiForecastComponentConfig(
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        s3_output_base_path="pathogens/sandiego",
        public_bucket="public-data",
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket="forecast-bucket",
        ),
        diseases=[
            DiseaseConfig(name="COVID", display_name="COVID-19", input_csv_suffix="COVID"),
            DiseaseConfig(name="FLU", display_name="Influenza", input_csv_suffix="FLU"),
            DiseaseConfig(name="RSV", display_name="RSV", input_csv_suffix="RSV"),
        ],
    )
    assert len(config.diseases) == 3
    assert config.diseases[0].name == "COVID"
