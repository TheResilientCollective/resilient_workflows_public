# Resilient Workflows

"Software Defined Assets" developed using [Dagster](https://dagster.io)

The first step is to rewrite the 'digests' as a news and publications workflow that are
more easily maintainable and customizable, and not bound to the code project that runs the user interface


## Deployment
Deployment will be done with containers where each workfow will run in separate  code container.
This will allow for workflows to be developed independently and be isolated in case constraints come up.
### Running the Application
```bash
# Start from public workflow directory
cd deployment
docker compose -f dagster_core.yml -f dagster_workflows.yml up 
```

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
`export $(grep -v '^#' workflows/.env | xargs)`

### Asset Testing
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
### Development Server
```bash
# Materialize specific assets
cd workflows/public
dagster dev -m public
```
go to http://localhost:3000/
