# Tijuana River H2S prediction review notebooks

[marimo](https://marimo.io) notebooks supporting the `feature/tj_prediction_redo`
planning sessions (see `docs/tj_prediction_redo/README.md`). Each notebook
answers one of the questions raised after APCD's review of the H2S forecast.

| notebook | question |
|---|---|
| `01_h2s_levels.py` | How many 30 ppb and 100 ppb events does the record hold, where, when, and for how long? Is there enough data at 100 ppb? |
| `02_complaints_vs_h2s.py` | At what H2S levels do Tijuana River Valley odour complaints occur? |
| `03_drivers.py` | Do temperature, border streamflow and SBIWTP effluent relate to high-H2S nights, and to each other? |

`tj_review_data.py` is the shared loader. All three notebooks read the same
production datasets from S3 and cache them under `.cache/` (git-ignored).

## Running

```bash
uv sync --all-packages --group analysis          # installs marimo + altair
export $(grep -v '^#' workflows/.env | xargs)    # S3_ADDRESS, S3_ACCESS_KEY, S3_SECRET_KEY
uv run marimo edit notebooks/tj_h2s_review/01_h2s_levels.py
```

`uv run marimo run <notebook>` serves it read-only for a meeting. Set
`TJ_REVIEW_BUCKET=test` to point at the development bucket instead of
`resilentpublic`.

Each notebook is also a plain script: `uv run python notebooks/tj_h2s_review/01_h2s_levels.py`
executes every cell and fails on any error, which is how they are checked.

## Datasets read

| dataset | S3 path (`latest/tijuana/…`) | producing asset |
|---|---|---|
| hourly station record | `forecast_data/modeldata_h2s_nofill.parquet` | `h2sforecast/modeldata_h2s_nofill` |
| nightly summary with complaints | `forecast_data/astronomical_day/h2s_nightly_summary_with_complaints.parquet` | `h2sforecast/h2s_nightly_summary_with_complaints` |
| complaints | `sd_complaints/complaints.parquet` | `complaints/sd_complaints` |
| border flow | `streamflow/boundary_cms/boundary_cms_{year}.parquet` | `streamflow/boundary_cms_yearly` |
| SBIWTP effluent | `effluent_flow/yearly/effluent_flow_{year}.parquet` | effluent flow assets |
