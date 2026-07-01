# CDC Weekly COVID-19 / Respiratory (RSV, Flu, ILI) Data for Modeling

## Implementation Plan — Mapping CDC Respiratory Surveillance to the Resilient Epidemiology Schema

**Resilient Collective — Design / Planning Document**
**Date**: July 2026
**Status**: Proposed (design only — no assets built yet)
**Target branch**: `claude/cdc-nndss-covid-modeling-apl7fd`

---

## 1. Motivation

We already ingest CDC NNDSS weekly surveillance (`data.cdc.gov/resource/x9gk-5huc`) for Mpox, Measles, and general notifiable diseases, and emit it into the **resilient epidemiology schema** (`resilient_core.utils.resilient_epi_schemas`). We now want a **weekly COVID-19 / SARS-CoV-2** data feed — alongside its usual companions **RSV** and **influenza / influenza-like illness (ILI)** — to drive epidemic **modeling**.

### 1.1 The core constraint: NNDSS is no longer a live COVID case source

Aggregate COVID-19 **case** reporting to CDC / NNDSS effectively **ended in May 2024** with the close of the federal public health emergency. As a result:

- `x9gk-5huc` is **not** a reliable source of current weekly COVID case counts.
- There is **no clean weekly national COVID *case count*** stream today.
- The modern, actively-maintained weekly COVID signals are **hospitalizations**, **deaths**, **emergency-department visit share**, and **wastewater viral activity**.

This is *why* we bring in the datasets below rather than simply filtering the existing NNDSS asset for a "COVID-19" label.

---

## 2. The Resilient Epidemiology Schema (target format)

The modeling output must conform to `resilient_epi_schemas.py`, which already defines two schemas our NNDSS assets emit:

### 2.1 `BasicEpidemiologySchema` (one weekly count per jurisdiction)

| Column | Type | Notes |
|---|---|---|
| `Jurisdiction` | str | CamelCased, no spaces |
| `date_week_start` / `date_week_end` | str `YYYY-mm-dd` | Snapped to CDC epiweek (Sun–Sat) |
| `Week_Number` | int (1–53) | CDC epiweek |
| `Year` | int | |
| `Week_Year` | str `WeekNumber-Year` | |
| `Cases` | float ≥ 0 | Weekly value |
| `Week_Type` | str | Provenance / adjustment label |

### 2.2 `StatisticalExtensionSchema` (tidy long metrics — the modeling format)

Required: `Jurisdiction, date, disease, metric, observation_type`
Optional value columns: `count, rate, mean, median, lower_ci, upper_ci, lower_20/50/90, upper_20/50/90`

**Two facts that shape every decision below:**

1. **`metric` is a closed enum** (`resilient_epi_schemas.py:124`):
   `cases, deaths, hospitalizations, tests, vaccinations`.
2. **`observation_type` already includes `forecast` and `prediction`**
   (`resilient_epi_schemas.py:131`) — the schema was purpose-built to hold model
   output next to observed data. That is our modeling target row shape.

Per the scoping decision, **we are NOT extending the metric enum in this phase**. Every dataset we ingest must map onto `hospitalizations`, `deaths`, `cases`, `tests`, or `vaccinations`, using `count` and/or `rate`.

---

## 3. Datasets selected for this phase

| # | Dataset | Socrata ID | Cadence | Signal | resilient `metric` | Value column(s) |
|---|---|---|---|---|---|---|
| 1 | NHSN Weekly Hospital Respiratory Data (HRD) by Jurisdiction | `ua7e-t2fy` (data.cdc.gov) / `n3kj-exp9` (healthdata.gov) | Weekly (Fri) | COVID/Flu/RSV hospital admissions + counts, by state/age | `hospitalizations` | `count` (+ `rate` where reported) |
| 2 | NCHS Provisional COVID-19 Death Counts by Week & State | `r8kw-7aab` | Weekly (Thu) | COVID / pneumonia / influenza deaths | `deaths` | `count` (+ P&I `rate`) |
| 3 | NSSP Emergency Department Visits — COVID/Flu/RSV (state/substate trajectories) | `rdmq-nq56` | Weekly (Fri) | % of ED visits (ILI-style leading indicator) | see §5 | `rate` |

Deferred to a later phase (require enum extension): **NWSS wastewater** (`2ew6-ywp6` metric / `g653-rqe2` concentration), **RESP-NET** lab-confirmed hospitalization rates (partial stub in `respnet.py`), NSSP by-demographic (`7xva-uux8`).

### 3.1 Why these three, and their modeling roles

- **NHSN hospitalizations (`ua7e-t2fy`) — primary target.** Closest modern replacement for the retired NNDSS case series, mandatory hospital reporting since Nov 2024, weekly by state and age, clean fit to `metric='hospitalizations'`. This is the series we forecast.
- **NCHS deaths (`r8kw-7aab`) — severity target.** Authoritative weekly deaths (COVID / pneumonia / influenza, ICD-10 `U07.1`, `J09–J18.9`) by state. Lagging (≈94% complete within 8 weeks) but the canonical severity endpoint. Clean fit to `metric='deaths'`.
- **NSSP ED % (`rdmq-nq56`) — leading indicator.** Weekly percent of ED visits for COVID/Flu/RSV. Earliest of the three to move; the natural nowcast covariate. Percent-valued — see §5 for how it fits without an enum change.

---

## 4. Column mapping per dataset

> Exact source field names must be confirmed against each dataset's Socrata metadata
> at build time (`?$limit=1` probe); the mappings below are the intended targets.

### 4.1 NHSN HRD (`ua7e-t2fy`) → `metric='hospitalizations'`

| Source (expected) | Resilient target |
|---|---|
| `jurisdiction` / state | `Jurisdiction` (CamelCase) |
| `week_end_date` | `date` (+ derive epiweek `date_week_start/end`, `Week_Number`, `Year`) |
| `total_admissions_covid_confirmed` (and Flu, RSV analogues) | `count`, one row per `disease` ∈ {COVID-19, Influenza, RSV} |
| admissions per 100k (if present) | `rate` |
| — | `disease` = COVID-19 / Influenza / RSV |
| — | `metric` = `hospitalizations` |
| — | `observation_type` = `reported` |

### 4.2 NCHS deaths (`r8kw-7aab`) → `metric='deaths'`

| Source (expected) | Resilient target |
|---|---|
| `state` | `Jurisdiction` (CamelCase) |
| `week_ending_date` / `end_date` | `date` (+ epiweek fields) |
| `covid_19_deaths` | `count`, `disease='COVID-19'` |
| `pneumonia_deaths`, `influenza_deaths`, `pneumonia_influenza_covid` | additional `disease` rows |
| `percent_of_expected_deaths` / P&I % (if used) | `rate` |
| — | `metric` = `deaths`, `observation_type` = `reported` (current weeks) / `corrected` (mature) |

### 4.3 NSSP ED % (`rdmq-nq56`) → see §5

| Source (expected) | Resilient target |
|---|---|
| `geography` (state / substate) | `Jurisdiction` |
| `week_end` | `date` (+ epiweek fields) |
| `percent_visits` (COVID / Flu / RSV / Combined) | `rate` |
| `county` / HHS region granularity | retained as extra columns (raw layer) |
| — | `disease` = COVID-19 / Influenza / RSV / Combined |

---

## 5. Resolving the NSSP percent-vs-enum tension

The scoping choices are **(a)** include NSSP ED %, and **(b)** "count/rate signals only — no enum change." NSSP is a **percent of ED visits**, which is not literally cases/deaths/hospitalizations/tests/vaccinations. Two ways to honor both, in order of preference:

- **Option A (recommended): dual-layer, raw + rate.** Always store NSSP to the **raw S3 layer** (full fidelity, exactly as the other CDC assets do) and, for the statistical-extension layer, emit it with the value in the **`rate`** column under `metric='cases'` with an explicit `observation_type='reported'` and a descriptive `disease` such as `COVID-19 (ED visit %)`. The percentage lives in `rate`; `count` stays null. This passes existing validation (business rule only requires one of mean/count/rate/median present) with **zero schema change**, and the `disease` label makes the semantics unambiguous. Downside: `metric='cases'` is a loose label for a visit share.
- **Option B: NSSP raw-only this phase.** Ingest NSSP to the raw/output S3 layers for exploratory modeling, but do **not** force it through `StatisticalExtensionSchema` until we add an `ed_visit_percent` enum value in a later phase. Cleanest semantically; keeps the strict schema honest.

**Recommendation:** Option A, because it keeps NSSP queryable in the same tidy long store as hospitalizations and deaths — which is what a joint COVID/RSV/Flu model needs. Flagged here for explicit sign-off before coding. (The genuinely correct long-term fix is a one-line enum addition, deferred per scope.)

---

## 6. Asset design (follows `cdc_nnds.py` conventions)

New module: `workflows/pathogens/src/pathogens/assets/cdc_covid_respiratory.py`

Shared helpers (reused, not re-implemented):
- `resilient_core.utils.store_assets` — `dataframe_to_s3`, `store_dataframe_to_s3`, `objectMetadata`
- `resilient_core.utils.resilient_epi_schemas` — `ResilientEpiProcessor`, `create_statistical_extension_record`, `transform_to_basic_epidemiology`
- `epiweeks.Week` for CDC epiweek derivation (as in `BasicEpidemiologySchema.transform_from_source`)
- Socrata count-then-page loop pattern (`$select=count(...)`, then `$offset/$limit=1000`) exactly as `nndss_weekly` / `mpox_weekly` do.

Proposed assets (all `group_name="pathogens"`, `key_prefix="cdc"`, `required_resource_keys={"s3","airtable"}`):

| Asset | Source | Output metric |
|---|---|---|
| `covid_resp_hospitalizations_weekly` | `ua7e-t2fy` | `hospitalizations` (COVID-19, Influenza, RSV) |
| `covid_deaths_weekly` | `r8kw-7aab` | `deaths` (COVID-19, Pneumonia, Influenza) |
| `covid_resp_ed_visits_weekly` | `rdmq-nq56` | `rate` per §5 |

S3 layout (mirrors `pathogens/cdc/nndss`):
- Raw: `pathogens/cdc/respiratory/raw/<source>/…`
- Output: `pathogens/cdc/respiratory/output/<asset>/…`
- Schema layer: `pathogens/cdc/respiratory/output/validated_epi_schema/…`

Per asset:
1. Probe Socrata for count, page through JSON (these are non-geospatial → `pandas`, not `gpd`; unlike NNDSS these datasets have no `geocode` geometry).
2. Store raw.
3. Normalize state → CamelCase `Jurisdiction`; derive epiweek date fields.
4. Melt wide COVID/Flu/RSV columns → long `disease` rows.
5. Emit `StatisticalExtensionSchema` rows via `create_statistical_extension_record` (and `BasicEpidemiologySchema` for the count-valued hospitalization/death series).
6. Store output + validated schema layers with `objectMetadata` (schema.org, `source_url`).

### 6.1 Do we need `calculate_correct_count` here?

**No** for these three — NHSN admissions, NCHS deaths, and NSSP percents are reported as **discrete weekly values**, not cumulative YTD. The negative-smoothing/cumulative-differencing machinery in `cdc_nnds.py` applies only to NNDSS cumulative tables. These assets skip it and go straight to schema emission (dates may still need mild reconciliation to CDC epiweek boundaries).

---

## 7. Scheduling, jobs, checks, registration

- **Partitions**: reuse `WeeklyPartitionsDefinition(start_date="2022-01-01", tz America/Los_Angeles)` for backfill-friendly weekly assets (matching `nndss_weekly`), or run unpartitioned "full refresh" like `mpox_weekly`. Recommend unpartitioned full-refresh first (simpler; these tables are small), add partitioning if backfill volume warrants.
- **Job + schedule**: `cdc_covid_respiratory_weekly_job` on `@weekly`, added to `definitions.py` `schedules=[...]`.
- **Asset checks** (mirror the mpox/measles no-negative-count checks): non-negative `count`/`rate`, non-empty jurisdictions, and freshness (latest `date` within N weeks). Register in `definitions.py` `asset_checks=[...]`.
- **Registration**: `load_assets_from_modules` already discovers new modules under `assets/`; only jobs, schedules, and checks need explicit wiring in `definitions.py`.

---

## 8. Open decisions before implementation

1. **NSSP handling** — approve §5 Option A (`rate` under `metric='cases'` with a `disease` label) vs Option B (raw-only this phase).
2. **Diseases in scope per asset** — COVID-19 only, or the full COVID + Influenza + RSV set (recommended: full set — it's the same request and the model benefits from co-circulation covariates).
3. **Partitioned vs full-refresh** ingestion (recommend full-refresh first).
4. **Backfill horizon** — how far back to load (NHSN/NSSP go to ~2022; NCHS deaths to 2020).

---

## 9. Deferred (next phase)

- Extend the `metric` enum with `ed_visit_percent` and `wastewater_activity` (the clean home for NSSP % and NWSS).
- **NWSS wastewater** (`2ew6-ywp6` / `g653-rqe2`) — earliest leading indicator (~1–2 weeks ahead).
- **RESP-NET** lab-confirmed hospitalization **rates** with confidence intervals → ideal exercise for the statistical-extension `rate` + `lower_ci`/`upper_ci` columns; finish the `respnet.py` stub.
- Write `forecast`/`prediction` model output back into `StatisticalExtensionSchema` (the observation types already exist) to close the modeling loop.

---

## 10. Source datasets

- NHSN Weekly Hospital Respiratory Data (HRD) by Jurisdiction — `ua7e-t2fy` (data.cdc.gov), `n3kj-exp9` (healthdata.gov)
- NSSP Emergency Department Visit Trajectories by State — `rdmq-nq56`; by Demographic — `7xva-uux8`
- NCHS Provisional COVID-19 Death Counts by Week & State — `r8kw-7aab`
- NWSS SARS-CoV-2 Wastewater Metric — `2ew6-ywp6`; Concentration — `g653-rqe2` (deferred)
- RESP-NET dashboard / CDC Respiratory Illnesses Data Channel (deferred)
