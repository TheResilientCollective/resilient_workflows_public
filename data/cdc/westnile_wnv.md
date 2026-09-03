# West Nile Virus (WNV) Data
- **Overview** https://www.cdc.gov/west-nile-virus/index.html
- **DATA PAGE**: https://www.cdc.gov/west-nile-virus/data-maps/index.html
- **Source**: CDC ArboNET ([https://www.cdc.gov/westnilevirus/healthcare-providers/surveillance.html](https://www.cdc.gov/west-nile-virus/data-maps/current-year-data.html?state-code=08))
- **Data Type**: Weekly human case counts, mosquito pool testing results, and avian surveillance data
- **Geographic Resolution**: County-level data for the United States
- **Time Frame**: 2000 to present
- **Data Format**: CSV files with standardized column headers (e.g., `week`, `county`, `state`, `human_cases`, `mosquito_pools`, `avian_cases`)
- **Data Access**: Publicly available for download; updated weekly during mosquito season (typically May through October)
- **Data Usage**: Used for epidemiological modeling, risk assessment, and public health decision-making related to WNV transmission and outbreak prediction
- **Data Limitations**: Reporting may vary by state and county; underreporting of human cases is possible; mosquito pool testing may not cover all areas uniformly
- **Data Citation**: Centers for Disease Control and Prevention (CDC). ArboNET Surveillance Data. Available at: https://www.cdc.gov/westnilevirus/healthcare-providers/surveillance.html
- **Scientific Justification**: WNV is a mosquito-borne flavivirus that can cause severe neurological disease in humans. Surveillance data is critical for understanding transmission dynamics, identifying high-risk areas, and implementing targeted vector control measures.


##  CDC ArboNET — West Nile Virus Current Year Data (2026)

**Publisher:** CDC, National Center for Emerging and Zoonotic Infectious Diseases (NCEZID), Division of Vector-Borne Diseases
**Surveillance system:** ArboNET (national arboviral surveillance, CDC + state/territorial health departments)
**Landing page:** https://www.cdc.gov/west-nile-virus/data-maps/current-year-data.html
**License:** Public domain (US federal government work)
**Data currency:** September 1, 2026 (verified 2026-09-03). All values are preliminary current-season data.
**Update cadence:** Every one to two weeks, June through December. Files are **overwritten in place with no vintage column** — the same URL always serves the latest snapshot.
**Format:** CSV, UTF-8, quoted headers

## Access notes

- Same architecture as the historic-data page: the on-page "Download Data (CSV)" buttons produce client-side `blob:` URLs; the stable resources are the viz-backing files under `https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/`. The dashboards append a `?cacheBust=` query parameter — omit it; the bare URLs resolve. All five URLs below verified returning data on 2026-09-03.
- Two filenames contain **literal spaces** — percent-encode in configs.
- **Snapshotting is mandatory, not optional, for this page.** Unlike the historic files (refreshed annually), these are living within-season files with no date, week, or vintage field anywhere in the data. A reporting-vintage time series — case accrual curves, revision behavior, reporting-lag estimation — can only be built by capturing each release. If ingestion starts mid-season, earlier vintages are unrecoverable (CDC Stacks archives some page snapshots, but not systematically per release).
- At season close these data roll into the historic-data files (which then extend to 2026) and this page resets for the next season. Expect end-state totals here to differ from the final historic values — historic files incorporate late reports and corrections.
- `County` fields are 5-digit FIPS with leading zeros — ingest as strings.
- CDC's CDN blocks many datacenter IPs and non-browser TLS fingerprints; fetches from cloud infrastructure may 403.

## Interpretation caveats (ArboNET-standard, plus current-season specifics)

1. **Everything on this page is preliminary.** Due to reporting delays, state and local health departments may hold more current information than CDC.
2. Cases are reported by **county/state of residence, not location of exposure**.
3. Non-neuroinvasive counts are under-reported with variable completeness; within-season this is compounded by variable reporting lag across jurisdictions — apparent geographic patterns partly reflect **reporting speed**, not incidence.
4. Non-human surveillance effort varies by jurisdiction; absence of reported non-human activity ≠ absence of risk.

---

## Datasets

### 1. Human disease cases by state of residence, 2026 YTD

- **URL:** https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_current_CountbyState.csv
- **Rows:** 40 (one per state reporting ≥1 case; states with zero reports are absent)
- **Schema:** `State` (str, USPS code), `Reported Cases` (int), `Legend` (str, map bin)
- **Description:** Current-season cumulative reported WNV human disease cases (neuroinvasive + non-neuroinvasive) by state of residence. Backs the state choropleth. Absence of a state row means no reports, not a verified zero.
- **Granularity:** State / season-to-date snapshot.

### 2. Human and non-human activity by county, 2026 YTD ★

- **URL:** https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_current_hum_nonhum.csv
- **Rows:** 544
- **Schema:** `County` (str, 5-digit FIPS), `Activity` (str: Human infections / Non-human activity / Human infections and non-human activity), `Total human disease cases` (int), `Neuroinvasive disease cases` (int), `Presumptive viremic blood donors` (int)
- **Description:** County-level season-to-date record combining human case counts with categorical non-human surveillance activity (mosquito pools, birds, veterinary, sentinel). Rows with `Activity = Non-human activity` carry 0/0/0 human counts — the row's information is the activity class itself. This is the current-season counterpart of the historic county×year panel (`wnv_hist_hum_nonhum_yearly.csv`) and the natural target for within-season snapshotting: successive captures yield county-level case-accrual curves.
- **Granularity:** County (FIPS) / season-to-date snapshot.

### 3. Total human disease cases, 2026 YTD (stat tile)

- **URL:** https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_total_cases.csv
- **Rows:** 1 — single value (407 as of 2026-09-01)
- **Schema:** `Total Human Disease Cases` (int)
- **Description:** National season-to-date total backing the headline tile. Cheap sentinel for change detection: poll this one-value file to decide when a new release has landed and trigger snapshot of datasets 1–2.

### 4. Neuroinvasive disease cases, 2026 YTD (stat tile)

- **URL:** https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_current_Neuroinvasive%20Disease%20Cases.csv
- **Rows:** 1 — single value (285 as of 2026-09-01)
- **Schema:** `Neuroinvasive Disease Cases` (int)
- **Description:** National season-to-date neuroinvasive total. The more reporting-stable severity series (see caveat 3).

### 5. States reporting cases, 2026 YTD (stat tile)

- **URL:** https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_current_States%20w%20Cases.csv
- **Rows:** 1 — single value (40 as of 2026-09-01)
- **Schema:** `States` (int)
- **Description:** Count of states reporting ≥1 human disease case this season. Should equal the row count of dataset 1; a cheap internal consistency check between releases.

---

## Cross-disease note

The directory structure generalizes: parallel current-season files exist for other ArboNET diseases under sibling paths (e.g., `…/ncezid_dvbd/LAC/lac_current_hum_nonhum.csv` for La Crosse virus, verified live). If the surveillance scope expands beyond WNV, the same schema and snapshotting pattern likely applies per pathogen directory.

*Compiled 2026-09-03 from the COVE dashboard configuration (current-data.json) and network traffic of cdc.gov/west-nile-virus/data-maps/current-year-data.html. All URLs verified returning data on that date.*

## Historic Data
There are 
https://www.cdc.gov/west-nile-virus/data-maps/historic-data.html
Interpretation caveats (apply to all entries; ArboNET-standard)
Cases are reported by county/state of residence, not location of exposure.
Non-neuroinvasive (mild) disease is substantially under-reported and reporting completeness varies by place and time — CDC advises against using non-neuroinvasive counts for cross-location or temporal comparisons. Neuroinvasive disease counts and incidence are the comparable series.
Non-human surveillance (mosquito, avian, veterinary, sentinel) effort varies widely by jurisdiction; absence of reported non-human activity ≠ absence of risk.
Reporting lags exist; states may publish on different schedules than CDC.
Datasets
1. Human disease cases by year of illness onset, 1999–2025
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic%20-%20Yearly%20Data.csv
Rows: 27 (one per year)
Schema: Year (int), Reported Cases (int)
Description: National annual totals of reported WNV human disease cases (neuroinvasive + non-neuroinvasive), by year of illness onset. The canonical national epi-curve (e.g., 1999: 62; 2003: 9,862; 2012: 5,674).
Granularity: National / annual.
2. Human disease cases by age group and sex, 1999–2025
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic_Age%20Sex%20Stacke.csv
Rows: 7 (age bands: <18, 18–29, 30–39, 40–49, 50–59, 60–69, 70+)
Schema: Age (str), Male (float, % of total), Female (float, % of total)
Description: Cumulative 1999–2025 age–sex distribution of reported human cases, expressed as percent of total cases, not counts.
Granularity: National / cumulative.
3. Hospitalizations by case type and year of illness onset, 2004–2025
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic_hospitalizations.csv
Rows: 22 (one per year; series starts 2004, not 1999)
Schema: Year (int), Neuroinvasive (int), Non_neuroinvasive (int)
Description: Annual counts of hospitalized WNV cases split by case type. Useful severity denominator alongside dataset 1.
Granularity: National / annual.
4. Neuroinvasive disease average annual incidence per 100,000 by county, 1999–2025
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic_County%20Inc.csv
Rows: 2,192
Schema: Type (str), Year (str, "1999-2025"), County (str, 5-digit FIPS), Population (int), Incidence (float, avg annual per 100k), Legend (str, map bin), Notes (str)
Description: County-level average annual incidence of neuroinvasive disease over the full 1999–2025 window, with the county population denominator included. The most comparable spatial layer in this collection (neuroinvasive only; see caveat 2). Counties with zero reported neuroinvasive cases are absent.
Granularity: County (FIPS) / cumulative average.
5. Human disease cases by state of residence, by year and case type
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic%20-%20Historic%20Sta.csv
Rows: 2,229
Schema: Type (str: All disease cases / Neuroinvasive / Non-neuroinvasive), Year (str: single year or "1999-2025"), State (str, USPS code), Reported Cases (int), Legend categories (str)
Description: Long-format state × year × case-type counts backing the filterable state map. Contains both per-year rows and cumulative "1999-2025" rows — filter on Year to avoid double-counting.
Granularity: State / annual + cumulative.
6. Human disease cases by month of illness onset, by year and case type
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic_Year%20and%20Month.csv
Rows: 672
Schema: Type (str), Year (str, single year or "1999-2025"), Month (str, Jan–Dec), Reported Cases (int)
Description: National monthly seasonality of illness onset by year and case type. The finest temporal resolution CDC publishes for historic WNV. Same mixed per-year/cumulative structure as dataset 5.
Granularity: National / monthly.
7. Summary counts (cases, hospitalizations, fatalities) by year and case type
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hum_historic_Data%20Bites.csv
Rows: 56
Schema: Type (str), Year (str), Reported Cases (int), Hospitalizations (int), Fatalities (int)
Description: Headline figures backing the dashboard tiles, including the cumulative row (all disease cases 1999–2025: 63,155 cases; 30,595 hospitalizations; 3,315 deaths). The only file in the collection carrying fatalities.
Granularity: National / annual + cumulative.
8. Human and non-human activity by county, cumulative 1999–2025
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hist_hum_nonhum_cumulative.csv
Rows: 3,053
Schema: Year (str, "1999-2025"), County (str, FIPS), Activity (str: Human infections / Non-human activity / Human infections and non-human activity), Reported human cases (int), Neuroinvasive disease cases (int), Identified by Blood Donor Screening (int), Notes (str)
Description: County-level cumulative record combining human case counts with categorical non-human surveillance activity (mosquito pools, birds, veterinary, sentinel). Includes presumptive viremic blood donors as a separate count.
Granularity: County (FIPS) / cumulative.
9. Human and non-human activity by county, yearly ★
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hist_hum_nonhum_yearly.csv
Rows: 28,298
Schema: identical to dataset 8, with Year as single years (1999–2025)
Description: County × year panel of human cases, neuroinvasive cases, blood-donor detections, and non-human activity class. The workhorse file for spatiotemporal modeling — the only county-by-year series in the collection. Rows exist only where activity was reported: absence of a county-year row is "no report," not a verified zero (caveat 3 applies with force for the non-human signal).
Granularity: County (FIPS) / annual.
10. Connecticut 2022 county data (special file)
URL: https://www.cdc.gov/wcms/vizdata/live/ncezid_dvbd/WNV/wnv_hist_hum_nonhum_2022_CT.csv
Rows: 7
Schema: Year, County (FIPS), Reported human cases, Neuroinvasive disease cases, Identified by Blood Donor Screening, Activity
Description: Connecticut's 2022 data on the legacy 8-county geography, published separately because CT replaced counties with 9 Councils of Government planning regions (new FIPS codes) in the Census transition. Geographic crosswalk hazard: CT FIPS in datasets 8–9 may mix legacy (09001–09015) and COG (09110–09190) codes across years — harmonize before building CT time series.
Granularity: County (legacy CT FIPS) / single year.
Excluded: dashboard title utility file

wnv_hum_historic_Historic%20Title.csv (same directory) contains display strings for each filter combination, not data. Listed for completeness; do not catalog.
