# Epidemiology Forecast Component

A reusable Dagster component for epidemiology forecasting pipelines. This component can be configured via Python or YAML to create multiple forecast instances with different simulators.

## Features

- **YAML Configuration**: Define components via `defs.yaml` files (Dagster Components)
- **Multiple Simulators**: Run different forecasts with different simulator IDs
- **S3 Path Monitoring**: Configurable sensor that monitors S3 paths for new runs
- **Configurable Processing**: Process forecast data for any jurisdiction/disease
- **Multiple Output Formats**: Store data as CSV, JSON, GeoJSON, Parquet
- **Slack Notifications**: Automatic notifications for run detection and processing

## Installation

```bash
cd components/epi_forecast
pip install -e .
```

## Quick Start

### Option 1: YAML Configuration (Dagster Components)

Create a `defs.yaml` file:

```yaml
type: epi_forecast.EpiForecastComponent
attributes:
  name: sandiego_ili
  jurisdiction: SanDiego
  jurisdiction_display: San Diego County
  s3_output_base_path: pathogens/sandiego/epidemiology
  public_bucket: "{{ env.PUBLIC_BUCKET }}"

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
```

### Option 2: Python Configuration

```python
from epi_forecast import (
    create_epi_forecast_definitions,
    EpiForecastComponentConfig,
    SimulatorConfig,
    S3MonitorConfig,
    DiseaseConfig,
)

config = EpiForecastComponentConfig(
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

defs = create_epi_forecast_definitions(config)
```

## Multiple Simulators Example

Create separate forecast pipelines with different simulators:

### San Diego ILI (defs/sandiego_ili/defs.yaml)

```yaml
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
```

### San Diego MPOX (defs/sandiego_mpox/defs.yaml)

```yaml
type: epi_forecast.EpiForecastComponent
attributes:
  name: sandiego_mpox
  jurisdiction: SanDiego
  jurisdiction_display: San Diego County
  simulator:
    simulator_id: 2  # Different simulator
    output_bucket: resilientmpox  # Different bucket
  s3_monitor:
    monitor_path: mpox_runs/  # Different path
    monitor_bucket: resilientmpox
  diseases:
    - name: MPOX
      display_name: Mpox
      input_csv_suffix: MPOX
```

## Configuration Reference

### SimulatorConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `simulator_id` | int | - | Unique identifier of the simulator |
| `server_url` | str | `https://sims.resilientservice.mooo.com` | API server URL |
| `api_path` | str | `/api/v1` | API path prefix |
| `output_bucket` | str | - | S3 bucket for simulator output |
| `username_env_var` | str | `RESILIENTSIMS_USERNAME` | Env var for username |
| `password_env_var` | str | `RESILIENTSIMS_PASSWORD` | Env var for password |
| `check_interval_seconds` | int | 30 | Status check interval |
| `max_wait_seconds` | int | 3600 | Maximum wait time for simulation |

### S3MonitorConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `monitor_path` | str | - | S3 path to monitor |
| `monitor_bucket` | str | - | S3 bucket to monitor |
| `file_pattern` | str | `*.csv` | File pattern to match |
| `subdirectory_pattern` | str | `ForAirTable/` | Subdirectory to check |
| `minimum_interval_seconds` | int | 600 | Sensor check interval |
| `run_path_pattern` | str | (regex) | Pattern for valid run directories |

### DiseaseConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `name` | str | - | Disease identifier (e.g., "COVID") |
| `display_name` | str | - | Human-readable name |
| `input_csv_suffix` | str | - | Suffix for input CSV files |
| `report_delays_key` | str | None | S3 key for report delays |
| `enabled` | bool | True | Whether disease is enabled |

### PublishingConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `github_repo_url` | str | None | GitHub repository URL |
| `github_output_path` | str | `forecast_rt` | Path in GitHub repo |
| `netlify_preview_hook` | str | None | Netlify preview webhook |
| `netlify_production_hook` | str | None | Netlify production webhook |
| `publish_to_latest` | bool | True | Copy to "latest" path |
| `latest_path_prefix` | str | `latest` | Prefix for latest paths |

### Feature Flags

| Flag | Default | Description |
|------|---------|-------------|
| `enable_data_extraction` | True | Enable data extraction |
| `enable_simulation` | True | Enable running simulator |
| `enable_processing` | True | Enable post-processing |
| `enable_github_publishing` | False | Enable GitHub publishing |
| `enable_netlify_deploy` | False | Enable Netlify deploys |
| `enable_llm_generation` | False | Enable LLM content generation |

## Jinja2 Templating

YAML configurations support Jinja2 templating:

```yaml
attributes:
  public_bucket: "{{ env.PUBLIC_BUCKET }}"
  simulator:
    output_bucket: "{{ env.RESILIENTSIMS_BUCKET | default('resilientseasonal') }}"
  slack_channel: "{{ env.SLACK_CHANNEL | default('#forecasts') }}"
```

## Environment Variables

```bash
# S3/MinIO
S3_BUCKET=your_bucket
S3_ADDRESS=your_s3_address
S3_PORT=443
S3_ACCESS_KEY=your_access_key
S3_SECRET_KEY=your_secret_key

# ResilientSims
RESILIENTSIMS_USERNAME=your_username
RESILIENTSIMS_PASSWORD=your_password
RESILIENTSIMS_BUCKET=resilientseasonal

# Slack
SLACK_TOKEN=xoxb-...

# GitHub (optional)
FORECAST_GITHUB_RT_TOKEN=ghp_...
```

## Assets & Sensors

Each component instance creates:

**Assets:**
- `{name}__forecast_processor`: Processes files from S3
- `{name}__forecast_latest`: Copies to "latest" directory

**Sensors:**
- `{name}_s3_monitor_sensor`: Monitors S3 path for new runs

**Jobs:**
- `{name}_forecast_job`: Job containing the assets

## Examples

See the `examples/` directory for complete configurations:
- `examples/sandiego_ili/defs.yaml` - San Diego ILI forecast
- `examples/sandiego_mpox/defs.yaml` - San Diego MPOX forecast
- `examples/losangeles_ili/defs.yaml` - Los Angeles ILI forecast
