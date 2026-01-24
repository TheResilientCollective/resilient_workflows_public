# Epidemiology Forecast Component

A reusable Dagster component for epidemiology forecasting pipelines. This component monitors an S3 path for new forecast data and processes it through configurable assets.

## Features

- **S3 Path Monitoring**: Configurable sensor that monitors an S3 path for new forecast runs
- **Configurable Processing**: Process forecast data for any jurisdiction/disease
- **Multiple Output Formats**: Store data as CSV, JSON, GeoJSON, Parquet
- **Slack Notifications**: Automatic notifications for new runs and processing status
- **Schema.org Metadata**: Automatic metadata generation for datasets

## Installation

```bash
cd components/epi_forecast
pip install -e .
```

## Quick Start

```python
from epi_forecast import (
    create_epi_forecast_definitions,
    EpiForecastComponentConfig,
    S3MonitorConfig,
    PublishingConfig,
)

# Configure the component
config = EpiForecastComponentConfig(
    jurisdiction="SanDiego",
    jurisdiction_display="San Diego County",
    s3_output_base_path="pathogens/sandiego/epidemiology",
    public_bucket="public-data",
    s3_monitor=S3MonitorConfig(
        monitor_path="api_run/",
        monitor_bucket="forecast-data",
        file_pattern="*.csv",
        subdirectory_pattern="ForAirTable/",
        minimum_interval_seconds=600,
    ),
    publishing=PublishingConfig(
        github_repo_url="https://github.com/org/repo.git",
        netlify_preview_hook="https://api.netlify.com/build_hooks/...",
    ),
    slack_channel="#forecasts",
)

# Create Dagster definitions
defs = create_epi_forecast_definitions(config)
```

## Configuration

### EpiForecastComponentConfig

Main configuration object for the component:

| Field | Type | Description |
|-------|------|-------------|
| `jurisdiction` | str | Jurisdiction identifier (e.g., "SanDiego") |
| `jurisdiction_display` | str | Human-readable name (e.g., "San Diego County") |
| `s3_output_base_path` | str | Base S3 path for output data |
| `public_bucket` | str | S3 bucket for public data |
| `s3_monitor` | S3MonitorConfig | S3 monitoring configuration |
| `publishing` | PublishingConfig | Publishing configuration |
| `slack_channel` | str | Slack channel for notifications |

### S3MonitorConfig

Configuration for the S3 path monitoring sensor:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `monitor_path` | str | - | S3 path to monitor |
| `monitor_bucket` | str | - | S3 bucket to monitor |
| `file_pattern` | str | "*.csv" | File pattern to match |
| `subdirectory_pattern` | str | "ForAirTable/" | Subdirectory to check for files |
| `minimum_interval_seconds` | int | 600 | Sensor check interval |
| `run_path_pattern` | str | (regex) | Pattern for valid run directories |

### PublishingConfig

Configuration for publishing outputs (no Airtable):

| Field | Type | Description |
|-------|------|-------------|
| `github_repo_url` | str | GitHub repository for Rt data |
| `github_output_path` | str | Path within GitHub repo |
| `netlify_preview_hook` | str | Netlify preview webhook URL |
| `netlify_production_hook` | str | Netlify production webhook URL |

## Environment Variables

The component requires these environment variables:

```bash
# S3/MinIO
S3_BUCKET=your_bucket
S3_ADDRESS=your_s3_address
S3_PORT=443
S3_ACCESS_KEY=your_access_key
S3_SECRET_KEY=your_secret_key

# Slack
SLACK_TOKEN=xoxb-...

# GitHub (optional)
FORECAST_GITHUB_RT_TOKEN=ghp_...

# Netlify (optional)
FORECAST_NETLIFY_PREVIEW_HOOK=https://api.netlify.com/build_hooks/...
```

## Assets

The component creates these assets:

1. **`{jurisdiction}_forecast_processor`**: Processes CSV files from monitored S3 path
2. **`{jurisdiction}_forecast_latest`**: Copies processed files to "latest" directory

## Sensors

- **`{jurisdiction}_s3_monitor_sensor`**: Monitors S3 path for new forecast runs

## Custom Run Config Builder

You can customize how detected S3 runs trigger assets:

```python
from dagster import RunConfig
from epi_forecast import S3RunInfo

def custom_run_config_builder(run_info: S3RunInfo) -> RunConfig:
    return RunConfig(
        ops={
            "sandiego__forecast_processor": {
                "config": {
                    "forecast_run_path": run_info.run_path,
                    # Add custom config here
                }
            }
        }
    )

defs = create_epi_forecast_definitions(
    config,
    run_config_builder=custom_run_config_builder,
)
```

## Integrating with Existing Workflows

To merge with existing Dagster definitions:

```python
from dagster import Definitions
from epi_forecast import create_epi_forecast_definitions

# Create component definitions
epi_defs = create_epi_forecast_definitions(config)

# Merge with existing definitions
defs = Definitions(
    assets=existing_assets + list(epi_defs.assets),
    jobs=existing_jobs + list(epi_defs.jobs),
    sensors=existing_sensors + list(epi_defs.sensors),
    resources={**existing_resources, **epi_defs.resources},
)
```
