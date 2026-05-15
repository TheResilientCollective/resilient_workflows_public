# workflows/sim — Epidemic Forecasting Pipeline

This Dagster code location orchestrates epidemiological simulations for San Diego County, using the ResilientSIMS simulator to generate disease forecasts, distributing results to Airtable, GitHub, and AI-powered public-facing portals.

---

## Directory Structure

```
workflows/sim/
├── pyproject.toml
└── src/sim/
    ├── __init__.py
    ├── definitions.py                          # Dagster Definitions (assets, resources, sensors)
    ├── source_assets.py                        # Cross-location asset dependencies
    ├── assets/
    │   ├── __init__.py
    │   └── sandiego_epidemiology_forecasts.py  # All asset implementations
    ├── resources/
    │   ├── resilientsims.py                    # ResilientSIMS API client
    │   └── resilientllm.py                    # ResilientLLM API client
    └── utils/
        └── forecast_features.py               # Feature engineering for H2S forecasting
```

---

## Data Flow Overview

```
epidemiology code location
    └─ sandiego_epidemiology_hyper_extraction  (SourceAsset)
            │
            ▼
    run_epidemic_simulation  ─── calls ResilientSIMS API ──► outputs CSVs to S3 api_run/
            │
            ▼
    process_epidemiology_forecasts  ─── validates & transforms CSVs
            │  └─► Airtable (3 tables: New Cases, Rt Estimates, Hospital Admissions)
            │  └─► S3: validated_epi_schema/forecasts/
            │  └─► Slack notification
            │
            ├─► copy_rt_to_github  ──► GitHub: ResilientDataProducts/sandiego_rt/
            │
            └─► copy_forecast_latest  ──► S3: latest/sandiego_epidemiology_ili/forecast/
                        │
                        ▼
            resilientllm_by_disease_asset  ─── calls LLM API with disease data
                        │  └─► S3: output/llm/{date}/forecast_summary.json
                        │  └─► Netlify deploy trigger
                        │
                        ▼
            resilientllm_asset  ─── processes LLM output, updates portals
                        └─► Airtable Widgets & Updates tables
                        └─► S3: output/llm/{date}/(summary, summary-short, updates).md
                        └─► Netlify deploy trigger
                        └─► Slack
```

---

## Assets

All assets use `AutomationCondition.eager()` and execute in dependency order without manual scheduling.

### `run_epidemic_simulation`
**Key:** `["sandiego", "sandiego_epidemiology_forecast"]`
**Depends on:** `sandiego_epidemiology_hyper_extraction` (upstream `epidemiology` location)

Renders Jinja2 YAML templates (`forecast_config.yaml`, `forecast_run.yaml`) with current run variables, creates a simulator configuration via ResilientSIMS API, then executes the simulator workflow. Polls until completion and sends Slack updates. Stores rendered configs to S3 for audit.

### `process_epidemiology_forecasts`
**Key:** `["sandiego", "sandiego_epidemiology_airtable"]`
**Config:** `forecastsS3AssetConfig` (`forecast_run_path`, `github_rt_url`)

Lists CSV outputs from the simulator's `ForAirTable/` S3 subdirectory and dispatches each to the appropriate Airtable table based on filename suffix:

| File suffix | Airtable table |
|---|---|
| `_case_reports.csv` | New Cases |
| `_case_Rt.csv` | Rt Estimates |
| `_hosp_reports.csv` | Hospital Admissions |

Validates all records against `StatisticalExtensionSchema` (from `resilient_core`) before writing. Clears old Airtable records first, then batch-upserts new records with disease linking.

### `copy_rt_to_github`
**Key:** `["sandiego", "sandiego_epidemiology_github_rt"]`

Authenticates to GitHub using a PAT token, clones the target data products repository, downloads `_case_Rt.csv` files from S3, and commits/pushes them to the configured path (`sandiego_rt/`).

### `copy_forecast_latest`
**Key:** `["sandiego", "sandiego_epidemiology_forecast_latest"]`

Copies all `ForAirTable/` outputs to an S3 `latest/` path for easy downstream access. Renames Influenza files to the FLU naming convention.

### `resilientllm_by_disease_asset`
**Key:** `["sandiego", "resilientllm_sd_disease"]`
**Depends on:** `sandiego_epidemiology_forecast_latest`

Structures forecast CSV URLs by disease (COVID, Influenza, RSV) — each including case reports, hospitalizations, and Rt estimates — and sends them to the ResilientLLM API. Stores the resulting multi-language JSON response to S3 and triggers a Netlify deployment.

### `resilientllm_asset`
**Key:** `["sandiego", "resilientllm_sd"]`
**Depends on:** `resilientllm_sd_disease`

Parses the LLM JSON output and distributes it:
- Updates the Airtable Widgets table with LLM-generated summaries
- Upserts dated records into the Airtable Updates table, linked to configured portals
- Stores markdown versions (full summary, short summary, updates) to S3
- Triggers a second Netlify deployment for the portal website
- Posts summaries to Slack

---

## Sensor: `epidemiology_forecasts_sensor`

Polls S3 `api_run/` every 10 minutes for new run directories matching the pattern `YYYY-mm-ddTHH-MM-SS_runXX`. When a new run is detected with CSV outputs in its `ForAirTable/` subdirectory, it:

1. Updates the cursor to the new run path
2. Yields a `RunRequest` for `epidemiology_forecasts_job`
3. Passes `forecast_run_path` and `github_rt_url` via `RunConfig` to downstream assets
4. Sends a Slack notification

---

## Resources

Defined in `definitions.py` and configured via environment variables:

| Resource | Purpose | Key env vars |
|---|---|---|
| `s3` | MinIO/S3 object storage | `S3_BUCKET`, `S3_ADDRESS`, `S3_PORT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` |
| `airtable` | Record sync and portal updates | `AIRTABLE_ACCESS_TOKEN`, `AIRTABLE_BASE_ID` |
| `slack` | Notifications and alerts | `SLACK_TOKEN`, `SLACK_SIMS_CHANNEL` |
| `openai` | AI processing | `OPENAI_API_KEY`, `OPENAI_BASE_URL` |
| `resilientsims` | Simulator API client | `RESILIENTSIMS_SERVER_URL`, `RESILIENTSIMS_USERNAME`, `RESILIENTSIMS_PASSWORD`, `RESILIENTSIMS_SIMULATOR_ID`, `RESILIENTSIMS_BUCKET` |
| `resilientllm` | LLM API client | `RESILIENTLLM_API_TOKEN`, `RESILIENTLLM_WEBHOOK`, `RESILIENTLLM_WEBHOOK_UUID` |

Additional environment variables:
- `FORECAST_GITHUB_RT_TOKEN`, `FORECAST_GITHUB_RT` — GitHub Rt export
- `FORECAST_NETLIFY_*_HOOK`, `FORECAST_NETLIFY_*_URL` — Netlify deploy triggers
- `AIRTABLE_EPI_DISEASE_TABLE_ID`, `AIRTABLE_EPI_NEW_CASES_TABLE_ID`, `AIRTABLE_EPI_RT_ESTIMATES_TABLE_ID`, `AIRTABLE_EPI_HOSPITAL_ADMISSIONS_TABLE_ID` — table routing
- `FORECAST_AIRTABLE_RSV_PORTAL_RECORDID`, `FORECAST_AIRTABLE_WIDGETS_RECORDID` — portal record linking

---

## External Services

### ResilientSIMS API (`resilientsims.py`)
Session-based API client for the epidemic simulation service.

- **Auth:** Username/password → CSRF token + session cookie
- **Key methods:** `create_configuration()`, `run_simulator()`, `get_run_status()`, `monitor_run_until_completion()`, `run_simulator_workflow()`
- **Polling:** 30s interval, 3600s timeout by default

### ResilientLLM API (`resilientllm.py`)
Webhook-based client that sends forecast data to an n8n-powered LLM workflow.

- **Auth:** Bearer token
- **Methods:** `execute(report_id)` (GET), `execute_with_data(report_id, data)` (POST with disease data JSON)

### S3 Key Paths

| Path | Purpose |
|---|---|
| `api_run/` | Sensor watch target; simulator writes output here |
| `{run_path}ForAirTable/` | CSV outputs consumed by downstream assets |
| `pathogens/sandiego/sandiego_epidemiology/output/` | Processed outputs |
| `validated_epi_schema/forecasts/{run_id}/` | Schema-validated records |
| `latest/sandiego_epidemiology_ili/` | Latest forecast for portal consumption |
| `output/llm/{date}/` | LLM response JSON and markdown files |

---

## Cross-Location Dependencies

`source_assets.py` declares `sandiego_epidemiology_hyper_extraction` as a `SourceAsset`, allowing Dagster to track lineage from the `epidemiology` code location into `sim` without coupling the code locations directly.

---

## `forecast_features.py` — H2S Feature Engineering

Utility module for building ML features for hydrogen sulfide (H2S) forecasting (43 features total):

| Category | Features |
|---|---|
| Weather | temperature, wind speed/direction/gusts, precipitation, humidity, pressure, cloud cover |
| Tides | height, encoded state |
| Time | hour/month sine-cosine encoding, day/night binary flag |
| Streamflow | log-transformed, low/high category, lags, 6h/24h rolling averages |
| H2S persistence | exponential decay lags (1h, 3h, 6h), rolling averages (6h, 24h) |
| SBIWTP interactions | flow×temperature, anomaly, deficit features |

Key functions:
- `get_last_known_state()` — extracts recent observation state for persistence features
- `engineer_station_features()` — builds all features for one monitoring station
- `engineer_features()` — main entry point for all stations in a forecast run

---

## Running Locally

```bash
# Load environment variables
export $(grep -v '^#' workflows/.env | xargs)

# Start only the sim code location
uv run dagster dev -m sim
```

Access the Dagster UI at http://localhost:3000/. The sensor can be manually triggered from the UI, or individual assets materialized directly.