# Scripps PFM – Tijuana River Pathogen Forecast Model

Source: https://pfmweb.ucsd.edu/
Data provider: Scripps Institution of Oceanography, Cross-Border Pollution project

Daily forecasts of pathogen concentrations in the Tijuana River, based on a mechanistic
model of transport and decay. The model is calibrated using years of water quality monitoring
data. Forecasts are updated daily and are public.

---

## Dagster Assets

All assets are in `workflows/public/public/assets/scripps_pfm.py`.
Group: `tijuana`, key prefix: `oceanmodel`.
Triggered by the `scripps_pfm_sensor` (hourly, fires when pfmweb.ucsd.edu deploys new data).

### oceanmodel/pfm_site_markers
GeoJSON FeatureCollection of monitoring station Points along the TJ River / SD coast.
Known stations: "Playas de Tijuana", "Imperial Beach pier", "Silver Strand",
"Coronado Avenida Lunar".
S3 output: `tijuana/oceanmodel/output/pfm_site_markers/site_markers.geojson`
S3 raw archive: `tijuana/oceanmodel/raw/scripps_pfm/{YYYYMMDD}/site_markers.geojson`

### oceanmodel/pfm_site_timeseries
Hourly CSV timeseries with a timestamp column and one column per monitoring station.
The numeric values appear to be tidal heights (negative values observed). Their exact
relationship to pathogen concentration model output is not yet confirmed — the column
semantics should be verified against the pfmweb source code or Scripps documentation.
S3 output: `tijuana/oceanmodel/output/pfm_site_timeseries/site_timeseries_{YYYYMMDD}.csv/.json`
S3 raw archive: `tijuana/oceanmodel/raw/scripps_pfm/{YYYYMMDD}/site_timeseries.csv`

### oceanmodel/pfm_dye_contours
Five GeoJSON polygon files (`dye_contours_0` through `dye_contours_4`) plus a dense
shoreline points GeoJSON. Each file is approximately 10 MB.

**Structure: Time-batched forecast data** ✓ CONFIRMED via Playwright analysis

Each of the 5 files contains an **array of 25 time snapshots** (FeatureCollections):
- `dye_contours_0.json`: Hours 0-24 of forecast
- `dye_contours_1.json`: Hours 25-49 of forecast
- `dye_contours_2.json`: Hours 50-74 of forecast
- `dye_contours_3.json`: Hours 75-99 of forecast
- `dye_contours_4.json`: Hours 100-120 of forecast (21 steps used)

**Within each time snapshot:** 19 concentration contour polygons representing sewage
percentage levels from 0.0005% (nearly pure ocean) to 10% (high sewage). Contour
levels are defined in the `title` property as log10 ranges (e.g., "-5.50--5.25").

**Total:** 5 files × 25 time steps = 125 snapshots (121 used by slider on pfmweb.ucsd.edu)

S3 output: `tijuana/oceanmodel/output/pfm_dye_contours/dye_contours_{0-4}.geojson`
S3 output: `tijuana/oceanmodel/output/pfm_dye_contours/shoreline_points.geojson`
S3 raw archive: `tijuana/oceanmodel/raw/scripps_pfm/{YYYYMMDD}/dye_contours_{0-4}.geojson`

### oceanmodel/pfm_hour0_contours
**Static map tile for current forecast** - Extracts hour-0 (initial time) from the forecast
for use as a static map layer without needing time animation.

**Processing:**
- Reads `dye_contours_0.geojson` from S3 (32 MB, contains hours 0-24)
- Extracts first element (hour 0) → 19 concentration contour polygons
- Simplifies geometry with 20-meter tolerance
- Parses log10 concentration ranges to actual percentages (0.0005% to 10% sewage)

**Output:** 19 MultiPolygon features representing current sewage concentration forecast.
- Raw hour-0: 1.33 MB
- Simplified: 0.43 MB GeoJSON (67% reduction from raw, 98.7% from full file)

**Use case:** Display current forecast on TJ Dashboard as a static overlay without
requiring time-slider controls or loading all 121 time steps.

S3 output: `tijuana/oceanmodel/output/pfm_hour0_contours/hour0_contours.geojson`
S3 raw archive: `tijuana/oceanmodel/raw/scripps_pfm/{YYYYMMDD}/hour0_contours.geojson`

### oceanmodel/pfm_shoreline_hazard
Mobile-optimized shoreline hazard visualization asset. Converts the dense shoreline
point data into simplified colored LineStrings grouped by risk level.

**Processing:**
- Reads the 121 FeatureCollections from shoreline_points.geojson
- Each FeatureCollection contains Points with a `risk` property (red/yellow/green)
- Converts each FeatureCollection's Points into a LineString
- Simplifies geometry with 15-meter tolerance for mobile bandwidth
- Adds hex color codes for visualization: red=#FF0000, yellow=#FFFF00, green=#00FF00

**Output:** 121 LineString features, each representing a shoreline segment colored by
hazard level. Approximately 1.2 MB GeoJSON (vs. 22 MB raw points = ~95% size reduction).

S3 output: `tijuana/oceanmodel/output/pfm_shoreline_hazard/shoreline_hazard.geojson`
S3 output: `tijuana/oceanmodel/output/pfm_shoreline_hazard/shoreline_hazard.csv`

---

## File URL Pattern

The site uses Observable framework with hash-based file versioning.
Hash IDs are embedded in the page JavaScript via `registerFile()` calls and change
each daily deployment. The sensor scrapes the page to discover current IDs.

Pattern: `https://pfmweb.ucsd.edu/_file/data/pfm_his_daily/{filename}.{hash}.{ext}`

Example IDs captured on initial examination:
- site_markers.854742b3.json
- site_timeseries.e0b45ee4.csv
- computed_shoreline_points.38759e3f.json
- computed_dye_contours_0.2f5dd444.json
- computed_dye_contours_1.a15ddcc3.json
- computed_dye_contours_2.75aa5806.json
- computed_dye_contours_3.2b8f6d85.json
- computed_dye_contours_4.8d6eed68.json

---

## Future Work

- Confirm contour index semantics with Scripps and update asset metadata/labels
- Generate simplified/downsampled GeoJSON for mobile-first use on southregion.resilienthub.org
- Generate daily PMTiles tileset from contour polygons (requires `pmtiles` dependency)
- Integrate with TJ Dashboard visualization: https://github.com/TheResilientCollective/TJ-Dashboard
