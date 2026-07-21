# new world screwworm

Use USDA Sources.
https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status/us-confirmed-cases-new-world
https://publicdashboards.dl.usda.gov/t/MRP_PUB/views/NewWorldScrewwormPublicReporting_17805168329840/SummaryDashboard


https://www.aphis.usda.gov/animals/animal-health/livestock-and-poultry-disease/current-status?page=29
https://www.aphis.usda.gov/sites/default/files/nws-weekly-status.csv

Power BI:
https://app.powerbi.com/view?r=eyJrIjoiYWJmODE4MTUtNjAwYS00NjA0LTllY2UtMzhmYzE2NDFmM2EzIiwidCI6ImM1OWRjNTZhLTkzZWMtNGIwNy1iNzFkLTQzYzg0NDkyNTcxOCIsImMiOjR9

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
