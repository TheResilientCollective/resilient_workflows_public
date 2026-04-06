# Resilient Workflows

"Software Defined Assets" developed using [Dagster](https://dagster.io)

The project is organized as a uv workspace with multiple Dagster code locations:
- **tijuana** — regional/border environmental monitoring
- **pathogens** — CDC NNDSS, mpox, WAHIS surveillance
- **epidemiology** — San Diego disease surveillance
- **sim** — epidemic forecasting (ResilientSims)
- **resilient_core** — shared utilities and resources


## Deployment
Deployment will be done with containers where each workflow will run in a separate code container.
This will allow for workflows to be developed independently and be isolated in case constraints come up.
### Running the Application
```bash
cd deploy
docker compose -f dagster_core.yml -f dagster_workflows.yml up
```

## Development Commands

### Running the Application
```bash
# All code locations via workspace
uv run dagster dev

# Single code location
uv run dagster dev -m tijuana
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
# Materialize specific assets (tijuana code location)
dagster asset materialize --select airnow_current_conditions -m tijuana
dagster asset materialize --select beach_water_quality -m tijuana
dagster asset materialize --select ibwc_spills -m tijuana

# Materialize asset groups
dagster asset materialize --select tag:tijuana -m tijuana
dagster asset materialize --select tag:waterquality -m tijuana
```
### Development Server
```bash
# All code locations
uv run dagster dev

# Single code location
uv run dagster dev -m tijuana
```
go to http://localhost:3000/
