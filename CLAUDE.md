# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Application
```bash
# All code locations via workspace
uv run dagster dev

# Single code location
uv run dagster dev -m tijuana
uv run dagster dev -m epidemiology
uv run dagster dev -m pathogens
uv run dagster dev -m sim
```

### Installation and Setup
```bash
# Create virtual environment
uv venv

# Install all workspace packages for development
uv sync --all-packages
source .venv/bin/activate
```

### Configuration
Environment variables are pre-configured in `workflows/.env` for development (use `workflows/.env.example` as a template). Load them using:
```bash
export $(grep -v '^#' workflows/.env | xargs)
```

The workflows/.env file contains all necessary API keys and configuration:
- **API Keys**: AIRNOW_API_KEY, PURPLE_AIR_API_KEY_READ/WRITE, OPENAI_API_KEY
- **S3/MinIO**: S3_BUCKET, S3_ADDRESS, S3_ACCESS_KEY, S3_SECRET_KEY
- **Airtable**: AIRTABLE_ACCESS_TOKEN, AIRTABLE_BASE_ID, various table IDs
- **Slack**: SLACK_TOKEN, SLACK_CHANNEL, SLACK_SIMS_CHANNEL
- **ResilientSims**: RESILIENTSIMS_* configuration variables
- **Forecast/Netlify**: Various webhook and portal URLs
- **Email**: EMAIL_ENABLED, EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT, EMAIL_FROM, EMAIL_PASSWORD, EMAIL_TO — used by `workflows/sim` (LLM forecast notification) and `netlify_triggers` (deployment approval notification). Set `EMAIL_ENABLED=true` to activate; supports Gmail SMTP and Office 365 OAuth2 (via `netlify_triggers/email_oauth.py`).

For manual setup, you can also set individual environment variables:
```bash
export AIRNOW_API_KEY="your_key"
export PURPLEAIR_API_KEY="your_key"
export AIRTABLE_ACCESS_TOKEN="your_token"
export AIRTABLE_BASE_ID="your_base_id"
export SLACK_TOKEN="your_token"
export S3_BUCKET="your_bucket"
export S3_ACCESS_KEY="your_access_key"
export S3_SECRET_KEY="your_secret_key"
```

### Asset Testing
```bash
# Materialize specific assets (tijuana code location)
dagster asset materialize --select airnow_current_conditions -m tijuana
dagster asset materialize --select beach_water_quality -m tijuana
dagster asset materialize --select ibwc_spills -m tijuana

# Materialize asset groups
dagster asset materialize --select tag:tijuana -m tijuana
dagster asset materialize --select tag:waterquality -m tijuana

# Materialize a chain of dependent assets (epidemiology code location)
uv run python -m dagster asset materialize --select sandiego/sandiego_epidemiology_testing_workbook_download,sandiego/sandiego_epidemiology_testing_hyper_extraction,sandiego/sd_testing -m epidemiology
```

### Development Server
```bash
# All code locations
uv run dagster dev

# Single code location
uv run dagster dev -m tijuana
```
Access at http://localhost:3000/

### Docker Deployment
```bash
# Production deployment with containers
cd deploy
docker compose -f dagster_core.yml -f dagster_workflows.yml up
```

## Project Overview

This is a Dagster-based data pipeline system for environmental monitoring and public health surveillance, focusing on the San Diego/Tijuana border region. The system processes data from multiple APIs and sources to create standardized datasets for air quality, water quality, health surveillance, and environmental complaints.

## Architecture Overview

### Core Structure
The project is organized as a uv workspace with multiple Dagster code locations:

- **workflows/resilient_core/**: Shared utilities, resources, and constants used across all code locations
- **workflows/tijuana/**: Regional/border environmental monitoring — air quality, water quality, beach monitoring, IBWC spills, complaints, weather (15 asset modules)
- **workflows/pathogens/**: CDC NNDSS, mpox, WAHIS disease surveillance
- **workflows/epidemiology/**: San Diego county disease surveillance and epidemiological data
- **workflows/sim/**: Epidemic forecasting via ResilientSims
- **workflows/public/**: Legacy monolithic code location (being migrated into the above projects)

Each code location is an independent Dagster project with its own `__init__.py` defining `Definitions`.

### Asset Organization
Assets are grouped by domain across the code locations:
- **airquality** (tijuana): AirNow API, PurpleAir sensors, San Diego APCD data
- **waterquality** (tijuana): Beach monitoring, IBWC spill tracking, streamflow data
- **health** (epidemiology): CDC surveillance, county health data, disease monitoring
- **complaints** (tijuana): Environmental complaint tracking and analysis
- **weather** (tijuana): OpenMeteo integration for forecasting
- **pathogens** (pathogens): CDC NNDSS, mpox, WAHIS

### Data Pipeline Patterns
All assets follow consistent patterns:
1. **Data Acquisition**: API calls or web scraping with error handling
2. **Geographic Processing**: Convert to GeoPandas with EPSG:4326 CRS
3. **Icon Assignment**: Use shared `ICONS` constants for map visualization
4. **Storage**: Multi-format export (CSV, GeoJSON, JSON) to S3 via `store_assets.geodataframe_to_s3()`
5. **Metadata**: Schema.org-compliant metadata using `store_assets.objectMetadata()`

### Key Resources Required
- **s3**: Primary data storage (required for all assets)
- **airtable**: Secondary storage and reporting
- **slack**: Automated notifications and alerts
- **openai**: Translation and AI processing tasks

### Asset Development Guidelines
When creating new assets:
1. Use consistent imports: `dagster`, `pandas`, `geopandas`, `requests`
2. Import shared utilities: `from resilient_core.utils import store_assets` and `from resilient_core.utils.constants import ICONS`
3. Require necessary resources: `s3`, `airtable`, `slack`
4. Follow naming convention: `domain_description_timeframe` (e.g., `beachwatch_closures_recent`)
5. Use geographic processing for location-based data
6. Include proper error handling and logging with `get_dagster_logger()`
7. use utils/store_assets to Store data in both raw and processed formats
8. Add automation conditions for scheduling (e.g., `AutomationCondition.eager()`)

### Data Storage Structure
- **Raw data**: `/{domain}/raw/{source}/`
- **Processed data**: `/{domain}/output/{asset_name}/`
- **Multiple formats**: Store as CSV, GeoJSON, and JSON for different use cases
- **Metadata**: Include schema.org metadata with each dataset

### Asset Scheduling
- **Real-time**: Use `AutomationCondition.eager()` for frequently updated data
- **Daily**: Beach monitoring, air quality current conditions
- **Weekly**: Disease surveillance, county health data
- **Event-driven**: Use sensors for website change detection

### Common Utility Functions
- `store_assets.geodataframe_to_s3()`: Multi-format S3 storage
- `store_assets.objectMetadata()`: Schema.org metadata creation
- Geographic utilities for coordinate processing and CRS handling
