# Gemini Code Assistant Context

This document provides context for the Gemini code assistant to understand the project and provide better assistance.

## Project Overview

This is a Python-based data engineering project that uses the Dagster framework to create "Software Defined Assets". The project focuses on public health and environmental data, collecting and processing data from various sources, including:

*   **Air Quality:** AirNow, PurpleAir, and San Diego Air Pollution Control District (APCD)
*   **Water Quality:** San Diego beach water quality monitoring and IBWC spills
*   **Public Health:** San Diego County epidemiology data, CDC NNDS data (Mpox, Measles), and San Diego public complaints
*   **Environmental:** Streamflow data and weather forecasts from OpenMeteo
*   **GIS Data:** Subregions and tracts for spatial analysis

The project is organized as a uv workspace with multiple Dagster code locations:
*   **workflows/resilient_core/** — shared utilities, resources, and constants
*   **workflows/tijuana/** — regional/border environmental monitoring (air quality, water quality, complaints, weather)
*   **workflows/pathogens/** — CDC NNDSS, mpox, WAHIS disease surveillance
*   **workflows/epidemiology/** — San Diego county disease surveillance
*   **workflows/sim/** — epidemic forecasting (ResilientSims)

## Building and Running

### Installation and Setup

1.  **Create a virtual environment:**
    ```bash
    uv venv
    ```
2.  **Install dependencies:**
    ```bash
    uv sync --all-packages
    ```
3.  **Activate the virtual environment:**
    ```bash
    source .venv/bin/activate
    ```

### Configuration

Set the following environment variables for API access:

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

You can also add these to a `.env` file in the `workflows` directory and run `export $(grep -v '^#' workflows/.env | xargs)`.

### Running the Development Server

To run the Dagster development server and view all code locations in the UI:

```bash
uv run dagster dev
```

To run a single code location:

```bash
uv run dagster dev -m tijuana
```

You can then access the Dagster UI at http://localhost:3000.

### Materializing Assets

To materialize (i.e., run) specific assets, use the `dagster asset materialize` command with the appropriate module.

**Materialize a single asset (tijuana code location):**

```bash
dagster asset materialize --select airnow_current_conditions -m tijuana
```

**Materialize a group of assets using tags:**

```bash
dagster asset materialize --select tag:tijuana -m tijuana
```

## Development Conventions

*   **Dagster:** The project uses Dagster as the core framework for defining and managing data assets.
*   **Python:** The project is written in Python.
*   **Dependencies:** Project dependencies are managed with `uv` and defined in `pyproject.toml` files across the workspace.
*   **Testing:** The project includes a `tests` directory, but the testing strategy is not fully clear from the initial analysis.
*   **Directory Structure:** The project is a uv workspace with code locations in `workflows/tijuana/`, `workflows/pathogens/`, `workflows/epidemiology/`, `workflows/sim/`, and shared code in `workflows/resilient_core/`.
