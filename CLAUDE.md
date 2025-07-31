# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Running the Application
```bash
# Start from public workflow directory
cd workflows/public && dagster dev -m public
```

### Installation and Setup
```bash
# Create virtual environment
uv venv

# Install all dependencies with all extras for development
uv sync --all-extras
source .venv/bin/active
```

### Configuration
Set environment variables for API access:
```bash
export AIRNOW_API_KEY="your_key"
export PURPLEAIR_API_KEY="your_key"
export AIRTABLE_API_KEY="your_key"
export AIRTABLE_BASE_ID="your_base_id"
export SLACK_WEBHOOK_URL="your_webhook"
export MINIO_ENDPOINT="your_endpoint"
export MINIO_ACCESS_KEY="your_access_key"
export MINIO_SECRET_KEY="your_secret_key"
```

### Asset Management
```bash
# Materialize specific assets
cd workflows/public
dagster asset materialize --select airnow_current_conditions -m public
dagster asset materialize --select beach_water_quality -m public
dagster asset materialize --select ibwc_spills -m public

# Materialize asset groups
dagster asset materialize --select tag:tijuana -m public
dagster asset materialize --select tag:waterquality -m public
```

## Architecture Overview

### Core Structure
- **workflows/public/**: Main application code containing Dagster assets and workflows
- **assets/**: Data processing assets organized by domain (air quality, water quality, health surveillance)
- **resources/**: External service integrations (Socrata, ArcGIS, MinIO, Airtable, Foursquare)
- **utils/**: Shared utilities including `store_assets.py` for S3 operations and `constants.py` for shared values
- **notebooks/**: Jupyter notebooks for data analysis and exploration

### Asset Organization
Assets are grouped by domain:
- **airquality**: AirNow API, PurpleAir sensors, San Diego APCD data
- **waterquality**: Beach monitoring, IBWC spill tracking, streamflow data
- **health**: CDC surveillance, county health data, disease monitoring
- **complaints**: Environmental complaint tracking and analysis
- **weather**: OpenMeteo integration for forecasting

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
2. Import shared utilities: `from ..utils import store_assets` and `from ..utils.constants import ICONS`
3. Require necessary resources: `s3`, `airtable`, `slack`
4. Follow naming convention: `domain_description_timeframe` (e.g., `beachwatch_closures_recent`)
5. Use geographic processing for location-based data
6. Include proper error handling and logging with `get_dagster_logger()`
7. Store data in both raw and processed formats
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
- `ICONS[category]`: Consistent icon assignment for visualization
- Geographic utilities for coordinate processing and CRS handling
