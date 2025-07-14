# Resilient Sheild Public Workflows

This is a [Dagster](https://dagster.io/) project for environmental and public health monitoring in the Tijuana-San Diego border region. The system orchestrates data collection, processing, and monitoring workflows for air quality, water quality, disease surveillance, and environmental complaints.

## Overview

The public workflows system includes:

- **Air Quality**: AirNow API, PurpleAir sensors, San Diego APCD data
- **Water Quality**: Beach monitoring, IBWC spill tracking, streamflow data  
- **Public Health**: CDC disease surveillance, county-specific health data
- **Environmental Complaints**: San Diego complaint tracking and analysis
- **Weather**: OpenMeteo integration for forecasting

## Getting started

### Prerequisites

- Python 3.8+
- Access to required APIs (see Configuration section)
- Optional: MinIO or S3 for data storage

### Installation

First, install your Dagster code location as a Python package. By using the --editable flag, pip will install your Python package in ["editable mode"](https://pip.pypa.io/en/latest/topics/local-project-installs/#editable-installs) so that as you develop, local code changes will automatically apply.

```bash
pip install -e ".[dev]"
```

### Configuration

Set up environment variables for API access:

```bash
export AIRNOW_API_KEY="your_airnow_api_key"
export PURPLEAIR_API_KEY="your_purpleair_api_key"
export AIRTABLE_API_KEY="your_airtable_api_key"
export AIRTABLE_BASE_ID="your_airtable_base_id"
export SLACK_WEBHOOK_URL="your_slack_webhook_url"
export MINIO_ENDPOINT="your_minio_endpoint"
export MINIO_ACCESS_KEY="your_minio_access_key"
export MINIO_SECRET_KEY="your_minio_secret_key"
```

### Running the System

Start the Dagster UI web server:

```bash
dagster dev
```

Open http://localhost:3000 with your browser to see the project.

## Usage

### Running Individual Workflows

#### Air Quality Monitoring
```bash
# Run AirNow current conditions
dagster asset materialize --select airnow_current_conditions

# Run PurpleAir sensor data collection
dagster asset materialize --select purpleair_sensors

# Run San Diego APCD data
dagster asset materialize --select sd_apcd_current_conditions
```

#### Water Quality Monitoring
```bash
# Run beach water quality monitoring
dagster asset materialize --select beach_water_quality

# Run IBWC spills tracking
dagster asset materialize --select ibwc_spills

# Run streamflow monitoring
dagster asset materialize --select streamflow_data
```

#### Public Health Surveillance
```bash
# Run CDC NNDSS disease surveillance
dagster asset materialize --select cdc_nndss_mpox

# Run county-specific health data
dagster asset materialize --select mpox_la_county mpox_sf_county
```

### Automated Scheduling

The system includes automated schedules:

- **Daily**: Beach monitoring, air quality current conditions
- **Weekly**: Disease surveillance, county health data
- **Event-driven**: Complaint data updates, alert notifications

View and manage schedules in the Dagster UI at http://localhost:3000/schedules

### Data Access

Processed data is stored in:
- **Local files**: `./data/` directory
- **MinIO/S3**: Configured object storage
- **Airtable**: Structured reporting tables

### Monitoring and Alerts

The system includes:
- **Slack notifications**: Automated failure alerts
- **Data freshness monitoring**: Alerts for stale data
- **Quality checks**: Validation of data integrity

### Jupyter Notebooks

Explore and analyze data using included notebooks:
```bash
cd notebooks/
jupyter lab
```

Available notebooks:
- Air quality analysis
- Water quality trends
- Public health surveillance
- Environmental justice analysis

## Development


### Adding new Python dependencies

You can specify new Python dependencies in `setup.py`.

### Unit testing

Tests are in the `sheild_tests` directory and you can run tests using `pytest`:

```bash
pytest sheild_tests
```

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/concepts/partitions-schedules-sensors/schedules) or [Sensors](https://docs.dagster.io/concepts/partitions-schedules-sensors/sensors) for your jobs, the [Dagster Daemon](https://docs.dagster.io/deployment/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.

Once your Dagster Daemon is running, you can start turning on schedules and sensors for your jobs.

## Deploy on Dagster Cloud

The easiest way to deploy your Dagster project is to use Dagster Cloud.

Check out the [Dagster Cloud Documentation](https://docs.dagster.cloud) to learn more.
