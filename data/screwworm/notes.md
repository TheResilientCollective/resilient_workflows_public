# new world screwworm

Use USDA Sources.
https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status/us-confirmed-cases-new-world
https://publicdashboards.dl.usda.gov/t/MRP_PUB/views/NewWorldScrewwormPublicReporting_17805168329840/SummaryDashboard


https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status?page=29
https://www.aphis.usda.gov/sites/default/files/nws-weekly-status.csv

Power BI:
https://app.powerbi.com/view?r=eyJrIjoiYWJmODE4MTUtNjAwYS00NjA0LTllY2UtMzhmYzE2NDFmM2EzIiwidCI6ImM1OWRjNTZhLTkzZWMtNGIwNy1iNzFkLTQzYzg0NDkyNTcxOCIsImMiOjR9

## OMSA regional Power BI dashboard (implemented)
`https://app.powerbi.com/view?r=eyJrIjoiYWJmODE4MTUtNjAwYS00NjA0LTllY2UtMzhmYzE2NDFmM2EzIiwidCI6ImM1OWRjNTZhLTkzZWMtNGIwNy1iNzFkLTQzYzg0NDkyNTcxOCIsImMiOjR9`
— "Reportes de Focos de Gusano Barrenador del Ganado en México y Centroamérica,
OMSA (2022-2026)".

- OMSA/WOAH regional data covering the **whole outbreak region** — Belize, Costa
  Rica, El Salvador, USA (EUA), Guatemala, Honduras, Mexico, Nicaragua, Panama —
  as opposed to the two APHIS sources (US-only and Mexico-only).
- It is a Power BI "publish to web" report backed by one flat table (`GBG_OMSA`)
  at the grain of a *focus* (outbreak): country, province, locality, lat/lon,
  start date, and susceptible/confirmed-case counts per species (canine, equine,
  swine, bovine, ovine, poultry, caprine, feline, buffalo, wild birds,
  terrestrial wildlife, domestic rabbit) plus totals.
- **No browser needed.** We query the Power BI public querydata API directly
  (`wabi-south-central-us-api.analysis.windows.net/public/reports/querydata`).
  The report resource key is encoded in the share URL; the dataset/report/model
  IDs come from the report's initial `conceptualschema`/`querydata` calls (browser
  Network tab) and are hardcoded — they only change if the report is republished.
  One SemanticQuery dumps the whole table (~4.7k focus rows in one page). The
  response is Power BI's DSR format (value dictionaries + repeat/null bitmasks),
  parsed by `_parse_powerbi_dsr`. Row/total counts validate against the
  dashboard tiles (4,671 focos; 21,156 casos; 3,427,959 susceptibles; 9 países).
- Asset: `pathogens` code location, key `screwworm/nws_omsa_centroamerica`.
  Dumps the raw querydata response and produces the cleaned focus line list
  `nws_omsa_focos` (CSV/JSON) plus an EPSG:4326 point layer `nws_omsa_focos_points`
  (one point per outbreak, GeoJSON/CSV). Runs 6:30pm ET daily via
  `nws_omsa_centroamerica_schedule`.
- **Schema-drift guard.** The query references `GBG_OMSA` columns by their
  source names (keys of `OMSA_COLUMN_RENAMES`); if OMSA renames/drops one, the
  querydata call silently returns nulls. Asset `screwworm/nws_omsa_columns`
  snapshots the authoritative property list from the report's `conceptualschema`
  endpoint and pickles a baseline; the asset check `nws_omsa_columns_unchanged`
  compares the live schema to the baseline (and to the queried columns) and
  fails + Slacks (channel `SLACK_CHANNEL_FAILURES`) on any add/remove — ERROR if
  a queried column goes missing. Both run in `nws_omsa_centroamerica_job` on the
  daily schedule. To accept a deliberate schema change, re-materialize
  `nws_omsa_columns` with config `update_baseline: true`.

## Unified dataset (implemented)
Merges all three sources above into one schema —
`workflows/pathogens/src/pathogens/assets/screwworm_unified.py`, asset key
`screwworm/nws_unified_events`. Depends on the three source assets, so it runs
after them (6:45pm ET, `nws_unified_schedule` / `nws_unified_job`).

- **Grain is preserved, not flattened.** `grain='focus'` rows are OMSA outbreaks
  (`total_cases`/`total_susceptible` carry the counts, one row may span species);
  `grain='case'` rows are single APHIS confirmed animals or fly-trap detections
  (`total_cases=1`). `source` is `omsa` | `aphis_us` | `aphis_mx`.
- **Outputs** (all to `SCREWWORM_OUTPUT_PATH` + the latest path):
  - `nws_unified_events` — one row per event, wide (CSV/JSON).
  - `nws_unified_events_points` — the same rows with coordinates, EPSG:4326
    (GeoJSON/CSV).
  - `nws_unified_event_species` — one row per (event, species) with
    `case_count`/`susceptible_count`, joined to the events by `event_uid`.
    OMSA's 12 per-species column pairs melt into this table (species with no
    cases *and* no susceptible animals are omitted); APHIS rows contribute a
    single row with `case_count=1`.
- **Geocoding precision is explicit** in `geo_level`: `point` (OMSA outbreak
  coordinates), `county_centroid` (APHIS US, joined from the `State Map`
  worksheet), `state_centroid` (APHIS Mexico — the CSV has only a state name, so
  `MX_STATE_CENTROIDS` supplies an *approximate* state center), or `none`.
- **Overlap is flagged, never dropped.** OMSA also covers Mexico and the US, so
  its rows overlap both APHIS sources. `duplicate_candidate` marks events whose
  country + admin1 + month is reported by more than one source — deliberately
  coarse, since the three sources use different date semantics (focus start vs
  confirmation vs report date). Consequence: **`SUM(total_cases)` across the
  whole table double-counts Mexico and the US** — filter to one `source` first.
- **Controlled vocabularies**: `SPECIES_VOCAB` (+ `SPECIES_ALIASES` for APHIS
  free text and `SPECIES_SUBSTRING_RULES` for compound values such as
  "Wildlife (American black bear)"), `COUNTRY_ISO3` for OMSA's Spanish country
  labels (`EUA`→USA, `MÉXICO`→MEX, `BELICE`→BLZ …).
- **Four in-asset checks** (declared via `check_specs`, Slacked to
  `SLACK_CHANNEL_FAILURES` on failure):
  `unified_species_vocab_covered` (WARN — a new upstream species fell to
  `other`), `unified_mx_states_geocoded` (WARN — a Mexican state with no
  centroid), `unified_totals_reconcile` (ERROR — long-table case counts must sum
  to each event's `total_cases`), `unified_required_fields` (ERROR — `event_uid`
  unique, key dimensions populated, no `UNK` countries).
- The two upstream assets `nws_aphis_us` and `nws_omsa_centroamerica` now return
  their cleaned frames (a `{"cases", "county"}` dict and the focus DataFrame)
  instead of row-count dicts, so this asset consumes them directly; the row
  counts moved to Dagster output metadata.

## NWS weekly status CSV (implemented)
`https://www.aphis.usda.gov/sites/default/files/nws-weekly-status.csv` — the "CSV"
button on the current-status page.

- Updated daily by APHIS, after ~5:30pm Eastern.
- Behind Akamai bot protection: a full browser-like header set is required, or the
  connection is reset (HTTP/2 INTERNAL_ERROR).
- Columns: Date (mm/dd/yy), State, Species, Age, Active/Inactive*,
  USDA Sterile Insect Dispersal, Approximate Miles From US.
- Confirmed cases are in Mexican states (Nuevo León, Coahuila, Tamaulipas), with
  approximate distance to the US border.
- Asset: `pathogens` code location, key `screwworm/nws_weekly_status`
  (workflows/pathogens/src/pathogens/assets/screwworm.py). Runs 6:00pm ET daily via
  `nws_weekly_status_schedule`.

## NWS Public Reporting Tableau dashboard (implemented)
`https://publicdashboards.dl.usda.gov/t/MRP_PUB/views/NewWorldScrewwormPublicReporting_17805168329840/SummaryDashboard`
— the Summary Dashboard on the us-confirmed-cases-new-world page.

- This holds the **US (Texas)** surveillance data — distinct from the weekly-status
  CSV, which only covers Mexican cases.
- It is a Tableau *Server* viz behind Akamai; full workbook (.twbx) download is
  disabled, and the VizQL config is populated by JavaScript (a plain HTTP GET sees
  an empty `tsConfigContainer`). So we drive headless Playwright once to clear the
  Akamai challenge and read the rendered config, then hand the warmed cookies +
  config to `TableauScraper` to bootstrap the session and parse each worksheet.
  (This is why `pathogens` now depends on `playwright` and `tableauscraper`; the
  deploy must run `playwright install chromium`.)
- Useful worksheets: `ExportToCSV` (per-case US line list), `State Map` (county
  counts + coordinates), plus `Summary*`/`Timeline*`/`Text description` aggregates.
- Asset: `pathogens` code location, key `screwworm/nws_dashboard`. Dumps every
  non-empty worksheet raw, and produces two cleaned datasets: `nws_us_cases`
  (line list, CSV/JSON) and `nws_us_county_summary` (county aggregates as an
  EPSG:4326 point GeoJSON — one point per county centroid — plus CSV). Runs
  6:15pm ET daily via `nws_dashboard_schedule`.
