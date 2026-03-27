# Deriving Accurate Weekly Case Counts from CDC NNDSS Cumulative Surveillance Data

## A Methodology for Correcting Reporting Delays and Data Revisions

**Resilient Collective — Technical Methods Paper**
**Date**: March 2026

---

## Abstract

The CDC National Notifiable Diseases Surveillance System (NNDSS) publishes weekly disease counts as cumulative year-to-date (YTD) totals rather than discrete weekly values. Converting cumulative data to weekly case counts through simple differencing produces negative values when jurisdictions submit data corrections, duplicate removals, or case reassignments. This paper describes a methodology for deriving accurate weekly case counts that addresses two fundamental challenges: (1) selecting the appropriate cumulative data source based on data maturity, and (2) smoothing negative adjustments through windowed redistribution to produce non-negative weekly time series suitable for epidemiological analysis. We apply this methodology to Mpox, Measles, and general NNDSS surveillance data and present a classification system that preserves full transparency about data provenance and adjustment history.

---

## 1. Introduction

### 1.1 The CDC NNDSS Reporting Structure

The CDC NNDSS weekly surveillance tables provide disease case counts through the Socrata API (`data.cdc.gov/resource/x9gk-5huc`). For each disease, jurisdiction, and epidemiological week, the dataset reports four key metrics:

| CDC Field | Column Name | Description |
|-----------|-------------|-------------|
| `m1` | `current_week` | Cases reported during this specific week |
| `m2` | `previous_52_weeks__max` | Maximum weekly count in the prior 52 weeks |
| `m3` | `current_YTD__cummulative` | Cumulative year-to-date case total |
| `m4` | `previous_YTD__cummulative` | Cumulative YTD total for the same period in the prior year |

### 1.2 The Reporting Delay Problem

Analysis of Mpox surveillance data for 2024 revealed that **78% of cases were reported late** — appearing not in the `current_week` field for their occurrence week, but incorporated into cumulative totals in subsequent weeks. For some jurisdictions, the late reporting rate reached 100% (e.g., California 2024: 519 cases, all reported late).

This was verified against independent state-level data:
- New York City 2025: CDC cumulative = 399, matching the independently reported total exactly
- New York State 2024: CDC cumulative = 52, matching within 3 cases
- Sum of weekly `current_week` values significantly undercount true totals

**Conclusion**: The `current_YTD__cummulative` field represents the most accurate case total, including retroactively reported cases. The `current_week` field captures only timely reports and severely undercounts true incidence.

### 1.3 The Negative Difference Problem

Deriving weekly counts from cumulative data requires computing the first difference:

$$\text{Weekly Cases}_t = \text{Cumulative}_t - \text{Cumulative}_{t-1}$$

This produces negative values when jurisdictions correct previously reported data. In the 2024 Mpox data, 30 instances of negative corrections were identified, with the largest being:

| Jurisdiction | Year | Week | Correction |
|---|---|---|---|
| California | 2024 | 40 | -203 |
| Colorado | 2024 | 42 | -146 |
| California | 2025 | 14 | -30 |
| Pennsylvania | 2024 | 18 | -12 |

These negative values represent legitimate data quality improvements (removal of duplicates, reclassification of cases, correction of reporting errors) but produce epidemiologically meaningless negative case counts that distort time series analysis.

---

## 2. Data Sources and Maturity

### 2.1 Two Cumulative Data Streams

Each record in the NNDSS dataset provides two cumulative totals:

1. **`current_YTD__cummulative`** — The running total for the current reporting year, updated weekly. This value changes as late reports arrive and corrections are applied. For the current calendar year, this is the only available cumulative figure.

2. **`previous_YTD__cummulative`** — The cumulative total for the equivalent period in the prior year. Critically, this field reflects **finalized, corrected data** from the previous year's surveillance cycle, incorporating all retroactive adjustments.

### 2.2 Data Maturity Model

The accuracy of cumulative data improves over time as corrections and late reports are incorporated:

```
Current Year (Preliminary):
  ┌─────────────────────────────────────────────┐
  │ current_YTD__cummulative                    │
  │ • Actively updated with new reports         │
  │ • Subject to ongoing corrections            │
  │ • May contain duplicates not yet resolved   │
  │ • Only available cumulative source          │
  └─────────────────────────────────────────────┘

Previous Year (Corrected):
  ┌─────────────────────────────────────────────┐
  │ previous_YTD__cummulative                   │
  │ • Finalized after year-end reconciliation   │
  │ • Corrections applied retrospectively       │
  │ • More accurate weekly rate derivation      │
  │ • Available from current year's data rows   │
  └─────────────────────────────────────────────┘
```

### 2.3 Source Selection Rule

Our methodology selects the cumulative data source based on data maturity:

- **For previous years**: Use `previous_YTD__cummulative` — the corrected, finalized cumulative total — to derive weekly case counts. This produces more accurate weekly rates because late reports and corrections have been reconciled.

- **For the current year**: Use `current_YTD__cummulative` — the only available source. Weekly rates derived from this source are considered **preliminary estimates** subject to revision.

---

## 3. Methodology

### 3.1 Weekly Difference Calculation

Weekly case counts are derived by computing the first difference of the selected cumulative column within each surveillance group (disease × jurisdiction × year):

```
Raw_Difference(t) = Effective_Cumulative(t) - Effective_Cumulative(t-1)
```

Where `Effective_Cumulative` is `previous_YTD__cummulative` for prior years and `current_YTD__cummulative` for the current year.

**Special case**: For epidemiological week 1 of each year, `Raw_Difference = Effective_Cumulative` (no prior week exists for differencing).

### 3.2 Negative Adjustment Smoothing

Negative `Raw_Difference` values indicate data corrections that removed cases from the cumulative total. Rather than simply clipping these to zero (which discards the correction signal) or preserving them (which produces invalid negative case counts), we apply a **windowed redistribution** that absorbs the correction across neighboring weeks.

#### Algorithm

For each negative value in `Raw_Difference`, the smoothing window size is determined by the magnitude of the correction:

**Small corrections** (|value| ≤ 3 cases):
- Window: 1 previous week + current week + 1 following week (3-week window)
- Rationale: Small corrections likely reflect minor reclassifications that can be absorbed locally

**Large corrections** (|value| > 3 cases):
- Window: 2 previous weeks + current week + 2 following weeks (5-week window)
- Rationale: Large corrections require broader redistribution to avoid distorting neighboring weeks

The redistribution computes the total cases across the window and distributes them evenly as whole numbers:

```
window_total = sum of all values in window
average = window_total ÷ window_size  (integer division)
remainder = window_total - (average × window_size)
```

The remainder is distributed one unit at a time to the earliest weeks in the window, ensuring the total is preserved exactly.

#### Boundary Handling

- Windows are clamped to group boundaries (disease × jurisdiction × year). A negative value in week 2 with a 5-week window would use only weeks 1–4.
- If a negative value was already resolved by a prior smoothing operation on a neighboring week, it is skipped.
- **Residual negatives**: After the smoothing pass, any values still negative are set to zero. This occurs when the window total itself is negative (i.e., corrections exceed new cases across the entire window).

### 3.3 Adjusted Output Columns

The methodology produces two primary output columns alongside the original CDC data:

| Column | Definition |
|---|---|
| `adjusted_week` | Smoothed weekly case count (non-negative integer) |
| `adjusted_YTD__cummulative` | Running cumulative sum of `adjusted_week` within each group |

The original CDC fields (`current_week`, `current_YTD__cummulative`, `previous_YTD__cummulative`) are preserved unchanged for audit and reproducibility.

Intermediate calculation columns are also retained:

| Column | Definition |
|---|---|
| `Raw_Difference` | Unsmoothed weekly difference (may be negative) |
| `Smoothed_Difference` | Post-smoothing weekly difference |
| `Cases_Added` | `Smoothed_Difference` clipped to ≥ 0 |
| `Cases_Removed` | `Raw_Difference` clipped to ≤ 0 (tracks corrections) |

---

## 4. Data Classification System

### 4.1 Week Type Classification

Each week is assigned a `Week_Type` label that describes the data provenance and any adjustments applied. This classification provides full transparency for downstream analysts.

#### Previous Years (Corrected Data)

| Week_Type | Description |
|---|---|
| `Normal` | Weekly difference is non-negative and matches reported `current_week` |
| `Adjustment` | Weekly difference is non-negative but differs from `current_week` (late reports incorporated) |
| `Adjustment_Cases_Removed` | Raw difference was negative (data correction occurred) |
| `Smoothed_Small` | Negative correction (≤ 3 cases) redistributed across 3-week window |
| `Smoothed_Large` | Negative correction (> 3 cases) redistributed across 5-week window |
| `Smoothed_Residual_Zeroed` | Negative value persisted after smoothing; clamped to zero |
| `Smoothing_Neighbor` | Week value was modified as part of a neighboring week's smoothing window |

#### Current Year (Preliminary Data)

All current-year classifications carry a `Preliminary_` prefix:

| Week_Type | Description |
|---|---|
| `Preliminary_Normal` | Current year, difference matches reported value |
| `Preliminary_Adjustment` | Current year, difference includes late reports |
| `Preliminary_Smoothed_Small` | Current year, small negative redistributed |
| `Preliminary_Smoothed_Large` | Current year, large negative redistributed |
| `Preliminary_Smoothed_Zeroed` | Current year, residual negative clamped |
| `Preliminary_Smoothing_Neighbor` | Current year, modified as smoothing neighbor |

The `Preliminary_` prefix alerts analysts that these values are derived from actively-updating cumulative data and may change as additional reports and corrections arrive.

### 4.2 Observation Type Classification

Statistical extension records classify each metric's data quality:

| Observation Type | Description |
|---|---|
| `corrected` | Adjusted values for previous years (derived from finalized cumulative data) |
| `preliminary estimate` | Adjusted values for the current year (derived from actively-updating data) |
| `reported` | Raw CDC values preserved as-is (e.g., `current_week`, cumulative fields) |

---

## 5. Output Schema

### 5.1 Basic Epidemiology Format

The primary analytical output follows a standardized weekly epidemiology schema:

| Field | Type | Description |
|---|---|---|
| `Jurisdiction` | String | Geographic jurisdiction (CamelCase, e.g., "NewYork") |
| `date_week_start` | Date | Sunday of the CDC epidemiological week |
| `date_week_end` | Date | Saturday of the CDC epidemiological week |
| `Week_Number` | Integer | Epidemiological week number (1–53) |
| `Year` | Integer | Epidemiological year |
| `Week_Year` | String | Combined identifier (e.g., "12-2025") |
| `Cases` | Integer | Adjusted weekly case count (`adjusted_week`) |
| `Week_Type` | String | Classification label (see Section 4.1) |

### 5.2 Statistical Extension Format

A companion dataset provides detailed metric-level records for each jurisdiction-week-disease combination, including:

- Original CDC reported values (`current_week`, cumulative fields)
- Derived values (`adjusted_week`, `adjusted_YTD__cummulative`)
- Correction tracking (`cases_added`, `cases_removed`, `net_cases`)
- Historical context (`previous_52_weeks__max`)

Each metric record includes an `observation_type` classification (see Section 4.2).

---

## 6. Validation

### 6.1 Non-Negativity Guarantee

The smoothing algorithm guarantees that `adjusted_week` ≥ 0 for all records. This is enforced through:

1. Windowed redistribution that absorbs most negatives into neighboring weeks
2. A final clamping pass that sets any residual negatives to zero
3. Automated asset checks that validate non-negativity after each pipeline run

### 6.2 Total Case Preservation

For windows where the total is non-negative, the redistribution preserves the exact total number of cases across the window. Integer rounding distributes remainders to the earliest weeks. When a window total is negative (correction exceeds cases in the window), the total is adjusted upward to zero, resulting in a slight overcount relative to the corrected cumulative.

### 6.3 Zero Count Preservation

Weeks with zero cases are explicitly preserved in the dataset. Automated checks validate that zero-count records are present and within expected ranges (0–95% for Mpox, 0–98% for Measles, reflecting the relative rarity of measles).

### 6.4 Audit Trail

The complete audit trail is maintained through:
- Original CDC fields preserved unchanged
- `Raw_Difference` showing the pre-smoothing weekly change
- `Cases_Removed` tracking all negative corrections
- `Week_Type` documenting every transformation applied
- `observation_type` classifying data maturity

---

## 7. Limitations

1. **Current-year preliminary estimates** are subject to revision as late reports arrive and corrections are applied. Analysts should treat `Preliminary_*` classified data with appropriate caution, particularly for jurisdictions with historically high late-reporting rates.

2. **Smoothing alters the temporal distribution** of cases within the window. While the total is preserved, the precise week of occurrence is approximate. For outbreak detection requiring exact timing, the unsmoothed `Raw_Difference` should be used with acknowledgment of negative values.

3. **The 3-week and 5-week window thresholds** (≤ 3 and > 3 cases) are heuristic choices. They perform well for the observed distribution of corrections in NNDSS data but may require adjustment for diseases with different correction patterns.

4. **Residual zeroing** introduces a small positive bias. When window totals are negative (large corrections dominate the window), clamping to zero slightly overstates the case count. In practice, this affects fewer than 1% of records.

5. **Cross-year corrections** are not handled. If a correction in January of year N affects cases from December of year N-1, the smoothing operates within year N only.

---

## 8. Application

This methodology is currently applied to three CDC NNDSS surveillance streams:

- **Mpox** (`mpox_weekly`) — Weekly state-level Mpox surveillance
- **Measles** (`measles_weekly`) — Weekly state-level Measles surveillance (Indigenous and Imported)
- **General NNDSS** (`nndss_weekly`) — Broader notifiable disease surveillance

Data is processed through an automated Dagster pipeline, validated against Pandera schemas, and stored in multiple formats (CSV, JSON, GeoJSON, Parquet) for downstream analysis.

---

## 9. Conclusion

Converting CDC NNDSS cumulative surveillance data to weekly case counts requires addressing both the data maturity gap between current and prior-year data and the presence of negative corrections from data quality improvements. Our methodology produces non-negative weekly time series suitable for epidemiological analysis while maintaining a complete audit trail of all transformations. The classification system enables analysts to distinguish between corrected historical data and preliminary current-year estimates, supporting appropriate interpretation of surveillance trends.

---

## References

1. CDC NNDSS Weekly Tables. Centers for Disease Control and Prevention. Available at: https://data.cdc.gov/browse?category=NNDSS
2. CDC NNDSS Socrata API. Dataset ID: x9gk-5huc. Available at: https://data.cdc.gov/resource/x9gk-5huc
3. CDC MMWR Epidemiological Week definitions. MMWR Weeks. Available at: https://wonder.cdc.gov/nndss/nndss_weekly_tables_menu.asp

---

## Appendix A: Late Reporting Rates by Jurisdiction (Mpox, 2024)

| Jurisdiction | Total Cases (Cumulative) | Timely Reports (current_week sum) | Late Reports | Late % |
|---|---|---|---|---|
| California | 519 | 0 | 519 | 100% |
| Colorado | 252 | 16 | 236 | 94% |
| Pennsylvania | 105 | 14 | 91 | 87% |
| New Jersey | 81 | 12 | 69 | 85% |
| North Carolina | 77 | 13 | 64 | 83% |
| Georgia | 73 | 12 | 61 | 84% |
| New York City | 420 | 132 | 288 | 69% |
| Florida | 199 | 61 | 138 | 69% |
| Texas | 289 | 112 | 177 | 61% |
| Illinois | 77 | 42 | 35 | 45% |
| **National** | **2,757** | **600** | **2,157** | **78%** |

## Appendix B: Smoothing Example

### Before Smoothing (California 2024, Weeks 39–42)

| Week | Cumulative | Raw_Difference | Cases_Added | Cases_Removed |
|---|---|---|---|---|
| 39 | 391 | 23 | 23 | 0 |
| 40 | 188 | -203 | 0 | -203 |
| 41 | 423 | 235 | 235 | 0 |

Week 40 shows a large correction (-203 cases). Since |−203| > 3, a 5-week window is applied (weeks 38–42). The total across the window is redistributed as whole numbers, and `Week_Type` is set to `Smoothed_Large` for week 40 and `Smoothing_Neighbor` for the affected neighbors.

### After Smoothing

The redistribution absorbs the correction across 5 weeks, producing non-negative values while preserving the window total. The `adjusted_week` column reflects the smoothed counts, and `adjusted_YTD__cummulative` is recalculated as the running sum.