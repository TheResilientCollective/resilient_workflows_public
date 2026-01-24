"""Tests for component configuration."""

import pytest
from epi_forecast.config import (
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    PublishingConfig,
    DiseaseConfig,
    TemplateConfig,
    DataSourceConfig,
    create_sandiego_ili_config,
)


def test_simulator_config_defaults():
    """Test SimulatorConfig with minimal required fields."""
    config = SimulatorConfig(
        simulator_id=1,
        output_bucket="test-bucket",
    )
    assert config.simulator_id == 1
    assert config.server_url == "https://sims.resilientservice.mooo.com"
    assert config.api_path == "/api/v1"
    assert config.check_interval_seconds == 30
    assert config.max_wait_seconds == 3600


def test_simulator_config_custom():
    """Test SimulatorConfig with custom values."""
    config = SimulatorConfig(
        simulator_id=5,
        server_url="https://custom.server.com",
        api_path="/api/v2",
        output_bucket="custom-bucket",
        check_interval_seconds=60,
        max_wait_seconds=7200,
    )
    assert config.simulator_id == 5
    assert config.server_url == "https://custom.server.com"
    assert config.max_wait_seconds == 7200


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
    assert config.enabled is True


def test_disease_config_disabled():
    """Test DiseaseConfig with enabled=False."""
    config = DiseaseConfig(
        name="RSV",
        display_name="RSV",
        input_csv_suffix="RSV",
        enabled=False,
    )
    assert config.enabled is False


def test_template_config_defaults():
    """Test TemplateConfig with defaults."""
    config = TemplateConfig()
    assert config.config_template == "forecast_config.yaml"
    assert config.run_template == "forecast_run.yaml"
    assert len(config.template_search_paths) == 4


def test_data_source_config_defaults():
    """Test DataSourceConfig with defaults."""
    config = DataSourceConfig()
    assert config.source_type == "s3"
    assert config.tableau_url is None


def test_publishing_config_defaults():
    """Test PublishingConfig with defaults."""
    config = PublishingConfig()
    assert config.github_output_path == "forecast_rt"
    assert config.github_repo_url is None
    assert config.netlify_preview_hook is None
    assert config.publish_to_latest is True


def test_component_config_with_simulator():
    """Test EpiForecastComponentConfig with simulator config."""
    config = EpiForecastComponentConfig(
        name="test_forecast",
        jurisdiction="TestJurisdiction",
        jurisdiction_display="Test Jurisdiction County",
        s3_output_base_path="test/output",
        public_bucket="test-public",
        simulator=SimulatorConfig(
            simulator_id=1,
            output_bucket="test-bucket",
        ),
        s3_monitor=S3MonitorConfig(
            monitor_path="api_run/",
            monitor_bucket="test-bucket",
        ),
    )
    assert config.name == "test_forecast"
    assert config.simulator.simulator_id == 1
    assert config.get_asset_key_prefix() == "test_forecast"


def test_component_config_custom_prefix():
    """Test custom asset key prefix."""
    config = EpiForecastComponentConfig(
        name="test_forecast",
        jurisdiction="TestJurisdiction",
        jurisdiction_display="Test Jurisdiction County",
        s3_output_base_path="test/output",
        public_bucket="test-public",
        simulator=SimulatorConfig(
            simulator_id=1,
            output_bucket="test-bucket",
        ),
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
        name="sandiego_ili",
        jurisdiction="SanDiego",
        jurisdiction_display="San Diego County",
        s3_output_base_path="pathogens/sandiego",
        public_bucket="public-data",
        simulator=SimulatorConfig(
            simulator_id=1,
            output_bucket="forecast-bucket",
        ),
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
    assert len(config.get_enabled_diseases()) == 3


def test_component_config_enabled_diseases_filter():
    """Test get_enabled_diseases filters correctly."""
    config = EpiForecastComponentConfig(
        name="test_forecast",
        jurisdiction="Test",
        jurisdiction_display="Test",
        s3_output_base_path="test",
        public_bucket="test",
        simulator=SimulatorConfig(simulator_id=1, output_bucket="test"),
        s3_monitor=S3MonitorConfig(monitor_path="api_run/", monitor_bucket="test"),
        diseases=[
            DiseaseConfig(name="COVID", display_name="COVID-19", input_csv_suffix="COVID", enabled=True),
            DiseaseConfig(name="FLU", display_name="Influenza", input_csv_suffix="FLU", enabled=False),
            DiseaseConfig(name="RSV", display_name="RSV", input_csv_suffix="RSV", enabled=True),
        ],
    )
    enabled = config.get_enabled_diseases()
    assert len(enabled) == 2
    assert enabled[0].name == "COVID"
    assert enabled[1].name == "RSV"


def test_multiple_simulator_configs():
    """Test creating configs for multiple simulators."""
    # ILI simulator
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

    # MPOX simulator (different simulator ID and bucket)
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

    # Verify they are independent
    assert ili_config.simulator.simulator_id == 1
    assert mpox_config.simulator.simulator_id == 2
    assert ili_config.s3_monitor.monitor_bucket == "resilientseasonal"
    assert mpox_config.s3_monitor.monitor_bucket == "resilientmpox"
    assert ili_config.get_asset_key_prefix() == "sandiego_ili"
    assert mpox_config.get_asset_key_prefix() == "sandiego_mpox"


def test_create_sandiego_ili_config_helper():
    """Test the convenience function for San Diego ILI config."""
    config = create_sandiego_ili_config(
        simulator_id=1,
        output_bucket="resilientseasonal",
        public_bucket="public",
    )
    assert config.name == "sandiego_ili"
    assert config.jurisdiction == "SanDiego"
    assert config.simulator.simulator_id == 1
    assert len(config.diseases) == 3
    assert config.diseases[0].name == "COVID"
