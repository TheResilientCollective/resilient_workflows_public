# Gemini Code Assistant Context

This document provides context for the Gemini code assistant to understand the project and provide better assistance.

## Project Overview

This is a Python-based data engineering project that uses the Dagster framework to create "Software Defined Assets". The project focuses on public health and environmental data, collecting and processing data from various sources, including:

*   **Air Quality:** AirNow, PurpleAir, and San Diego Air Pollution Control District (APCD)
*   **Water Quality:** San Diego beach water quality monitoring and IBWC spills
*   **Public Health:** San Diego County epidemiology data, CDC NNDS data (Mpox, Measles), and San Diego public complaints
*   **Environmental:** Streamflow data and weather forecasts from OpenMeteo
*   **GIS Data:** Subregions and tracts for spatial analysis

The project is structured as a collection of Dagster assets, which are Python functions that produce data. These assets are organized into different modules based on their data source or domain.

## Building and Running

### Installation and Setup

1.  **Create a virtual environment:**
    ```bash
    uv venv
    ```
2.  **Install dependencies:**
    ```bash
    uv sync --all-extras
    ```
3.  **Activate the virtual environment:**
    ```bash
    source .venv/bin/active
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

To run the Dagster development server and view the assets in the UI, run the following command from the `workflows/public` directory:

```bash
dagster dev -m public
```

You can then access the Dagster UI at http://localhost:3000.

### Materializing Assets

To materialize (i.e., run) specific assets, use the `dagster asset materialize` command from the `workflows/public` directory.

**Materialize a single asset:**

```bash
dagster asset materialize --select airnow_current_conditions -m public
```

**Materialize a group of assets using tags:**

```bash
dagster asset materialize --select tag:tijuana -m public
```

## Development Conventions

*   **Dagster:** The project uses Dagster as the core framework for defining and managing data assets.
*   **Python:** The project is written in Python.
*   **Dependencies:** Project dependencies are managed with `uv` and defined in the `pyproject.toml` file.
*   **Testing:** The project includes a `tests` directory, but the testing strategy is not fully clear from the initial analysis.
*   **Directory Structure:** The main project code is located in the `workflows/public/public` directory, with subdirectories for assets, resources, schedules, etc.
