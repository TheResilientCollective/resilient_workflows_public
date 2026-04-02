# Scripps PFM – Tijuana River Pathogen Forecast Model

**Source:** https://pfmweb.ucsd.edu/
**Provider:** Scripps Institution of Oceanography, Cross-Border Pollution project
**Update Frequency:** Daily (~6:30 AM Pacific)

## Overview

Daily forecasts of sewage/pathogen concentrations in the Tijuana River and San Diego coastal
waters. The model predicts sewage dispersion as a percentage (0.0005% to 10%) using a
mechanistic transport and decay model calibrated with years of water quality monitoring data.
Forecasts extend 5 days (~120 hours) into the future.

**Key Features:**
- 5-day hourly forecast (121 time steps)
- 19 concentration contour levels per time step
- 4 monitoring station locations with detailed timeseries
- Shoreline risk assessment (red/yellow/green zones)
- Mobile-optimized data layers for low-bandwidth contexts

---

## Quick Start: TJ Dashboard Integration

For **southregion.resilienthub.org**, use the `latest/` paths below. These always point to
the most recent forecast without needing date logic:

### Recommended Data Layers

**1. Current Forecast Contours** (static overlay, no animation)
```
URL: {S3_BASE}/latest/tijuana/oceanmodel/pfm_hour0_contours/hour0_contours_{YYYYMMDD}.geojson
Size: ~440 KB
Features: 19 concentration polygons (0.0005% to 10% sewage)
Use: Display current conditions without time-slider
```

**2. Shoreline Hazard Lines** (simplified risk zones)
```
URL: {S3_BASE}/latest/tijuana/oceanmodel/pfm_shoreline_hazard/shoreline_hazard_{YYYYMMDD}.geojson
Size: ~1.2 MB
Features: 121 colored line segments (red/yellow/green)
Use: Show coastal risk zones at a glance
```

**3. Monitoring Stations** (reference points)
```
URL: {S3_BASE}/latest/tijuana/oceanmodel/pfm_site_markers/site_markers_{YYYYMMDD}.geojson
Size: ~1.3 KB
Features: 4 monitoring station points
Use: Show forecast sampling locations
```

**4. Station Timeseries** (detailed forecast)
```
URL: {S3_BASE}/latest/tijuana/oceanmodel/pfm_site_timeseries/site_timeseries_{YYYYMMDD}.json
Size: ~27 KB
Data: Hourly forecast per station
Use: Charts/graphs for specific locations
```

---

## Dagster Assets

**Location:** `workflows/public/public/assets/scripps_pfm.py`
**Group:** `tijuana`
**Key Prefix:** `oceanmodel`
**Automation:** `scripps_pfm_sensor` checks pfmweb.ucsd.edu hourly for updates
**Storage:** Dated archives + `latest/` links for easy integration

### oceanmodel/pfm_site_markers
GeoJSON FeatureCollection of monitoring station Points along the TJ River / SD coast.
Known stations: "Playas de Tijuana", "Imperial Beach pier", "Silver Strand",
"Coronado Avenida Lunar".

S3 output: `tijuana/oceanmodel/output/pfm_site_markers/site_markers_{YYYYMMDD}.geojson`
S3 latest: `latest/tijuana/oceanmodel/pfm_site_markers/site_markers_{YYYYMMDD}.geojson` ⭐
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

S3 output: `tijuana/oceanmodel/output/pfm_hour0_contours/hour0_contours_{YYYYMMDD}.geojson`
S3 latest: `latest/tijuana/oceanmodel/pfm_hour0_contours/hour0_contours_{YYYYMMDD}.geojson` ⭐
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

S3 output: `tijuana/oceanmodel/output/pfm_shoreline_hazard/shoreline_hazard_{YYYYMMDD}.geojson`
S3 latest: `latest/tijuana/oceanmodel/pfm_shoreline_hazard/shoreline_hazard_{YYYYMMDD}.geojson` ⭐

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

---

## Technical Details

### Data Pipeline

```
pfmweb.ucsd.edu (Observable framework)
    ↓ (hourly sensor check)
scripps_pfm_sensor (detects hash ID changes)
    ↓ (triggers materialization)
5 Dagster Assets (fetch, process, simplify)
    ↓ (store to S3/MinIO)
Dated archives + latest/ links
    ↓ (consumed by)
TJ Dashboard (southregion.resilienthub.org)
```

### Hash-Based Versioning

The pfmweb site uses Observable framework with content-addressed files:
```
Pattern: https://pfmweb.ucsd.edu/_file/data/pfm_his_daily/{filename}.{hash}.{ext}
Example: computed_dye_contours_0.2f5dd444.json
```

Hash IDs change with each deployment (typically daily at 6:30 AM). Our sensor scrapes
the page JavaScript (`registerFile()` calls) to discover current IDs, then triggers
asset materialization when changes are detected.

### Size Optimizations

| Asset | Raw Size | Optimized | Reduction |
|-------|----------|-----------|-----------|
| Full 5-day animation | 180 MB | N/A | Not mobile-friendly |
| Hour-0 contours | 32 MB (in array) | 440 KB | 98.7% |
| Shoreline points | 22 MB | 1.2 MB (lines) | 95% |
| Site markers | 806 bytes | 1.3 KB (styled) | N/A |

**Techniques:**
- Extract hour-0 from time-batched array (avoid loading 121 snapshots)
- GeoPandas `.simplify()` with 15-20m tolerance
- Convert dense point clouds to LineStrings
- Stream large files via `BytesIO` to avoid memory issues

### File Structure (S3/MinIO)

```
tijuana/oceanmodel/
├── output/                          # Processed, dated outputs
│   ├── pfm_site_markers/
│   │   └── site_markers_20260402.geojson
│   ├── pfm_site_timeseries/
│   │   ├── site_timeseries_20260402.csv
│   │   └── site_timeseries_20260402.json
│   ├── pfm_dye_contours/
│   │   ├── dye_contours_0.geojson       (32 MB, 25 hours)
│   │   ├── dye_contours_1.geojson       (28 MB, 25 hours)
│   │   ├── dye_contours_2.geojson       (30 MB, 25 hours)
│   │   ├── dye_contours_3.geojson       (32 MB, 25 hours)
│   │   ├── dye_contours_4.geojson       (34 MB, 21 hours)
│   │   └── shoreline_points.geojson     (22 MB, 121 snapshots)
│   ├── pfm_hour0_contours/
│   │   └── hour0_contours_20260402.geojson (440 KB)
│   └── pfm_shoreline_hazard/
│       └── shoreline_hazard_20260402.geojson (1.2 MB)
├── raw/scripps_pfm/{YYYYMMDD}/      # Dated archives (original format)
└── (see latest/ below)

latest/tijuana/oceanmodel/           # Dashboard-ready links ⭐
├── pfm_site_markers/
├── pfm_site_timeseries/
├── pfm_hour0_contours/
└── pfm_shoreline_hazard/
```

---

## Integration Examples

### Leaflet/Mapbox GL JS

```javascript
// Add hour-0 contours as GeoJSON layer
fetch('{S3_BASE}/latest/tijuana/oceanmodel/pfm_hour0_contours/hour0_contours_{date}.geojson')
  .then(r => r.json())
  .then(data => {
    L.geoJSON(data, {
      style: feature => ({
        fillColor: feature.properties.fill,
        fillOpacity: feature.properties['fill-opacity'],
        color: feature.properties.stroke,
        weight: feature.properties['stroke-width']
      })
    }).addTo(map);
  });

// Add shoreline hazard with color coding
fetch('{S3_BASE}/latest/tijuana/oceanmodel/pfm_shoreline_hazard/shoreline_hazard_{date}.geojson')
  .then(r => r.json())
  .then(data => {
    L.geoJSON(data, {
      style: feature => ({
        color: feature.properties.color,
        weight: 3,
        opacity: 0.8
      })
    }).addTo(map);
  });
```

### React Component

```jsx
import { useEffect, useState } from 'react';
import { GeoJSON } from 'react-leaflet';

function ScrippsForcast({ s3Base }) {
  const [contours, setContours] = useState(null);

  useEffect(() => {
    // Fetch latest forecast (no date logic needed)
    fetch(`${s3Base}/latest/tijuana/oceanmodel/pfm_hour0_contours/`)
      .then(r => r.json())
      .then(data => setContours(data));
  }, [s3Base]);

  if (!contours) return <div>Loading forecast...</div>;

  return (
    <GeoJSON
      data={contours}
      style={feature => ({
        fillColor: feature.properties.fill,
        fillOpacity: 0.6
      })}
    />
  );
}
```

---

## Future Enhancements

### Completed ✓
- ✅ Confirm contour structure (time-batched, not concentration levels)
- ✅ Generate simplified/mobile-optimized GeoJSON
- ✅ Create static hour-0 tile for dashboard integration

### Planned
- PMTiles generation for vector tile serving (requires `pmtiles` dependency)
- Time-slider animation support (load all 121 time steps progressively)
- Concentration threshold alerts (email/Slack when >1% sewage forecast)
- Historical archive comparison (month-over-month forecast accuracy)
- Integration with real-time IBWC flow data for model calibration

---

## References

- **Scripps PFM Site:** https://pfmweb.ucsd.edu/
- **TJ Dashboard Repo:** https://github.com/TheResilientCollective/TJ-Dashboard
- **Dagster Assets:** `workflows/public/public/assets/scripps_pfm.py`
- **Contact:** ffeddersen@ucsd.edu (Scripps model questions)
