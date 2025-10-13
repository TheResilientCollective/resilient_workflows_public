# CDC Surveillance Data Quality Checks

This document demonstrates the comprehensive data quality checks implemented for CDC surveillance data, ensuring weekly data completeness and zero count preservation.

## Overview

The asset checks validate that epidemiological surveillance data meets quality standards for:

- **Weekly Data Completeness**: No missing weeks in surveillance time series
- **Disease-Specific Validation**: Complete coverage across disease types
- **Jurisdiction Coverage**: All reporting jurisdictions have complete data
- **Zero Count Preservation**: Epidemiologically significant zero-case weeks are preserved
- **Statistical Extension Validation**: Complete metric coverage in statistical format

## Asset Checks Implemented

### 1. Weekly Completeness Checks

#### `mpox_weekly_completeness`
- **Asset**: `cdc.mpox_weekly`
- **Validates**: Basic epidemiology schema data for complete weekly coverage
- **Groups by**: Jurisdiction
- **Checks**: No missing weeks from surveillance start date to present

#### `measles_weekly_completeness`
- **Asset**: `cdc.measles_weekly`
- **Validates**: Basic epidemiology schema data for complete weekly coverage
- **Groups by**: Jurisdiction + Disease Type (Indigenous/Imported)
- **Checks**: No missing weeks for each jurisdiction-disease combination

### 2. Statistical Extension Completeness

#### `mpox_statistical_extension_completeness`
- **Asset**: `cdc.mpox_weekly`
- **Validates**: Statistical extension schema data
- **Groups by**: Jurisdiction + Metric Type
- **Checks**: Complete weekly metrics (current_week, previous_52_weeks_max, etc.)

#### `measles_statistical_extension_completeness`
- **Asset**: `cdc.measles_weekly`
- **Validates**: Statistical extension schema data
- **Groups by**: Jurisdiction + Disease Type + Metric Type
- **Checks**: Complete weekly metrics for each disease type

### 3. Zero Count Preservation Validation

#### `mpox_zero_counts_validation`
- **Asset**: `cdc.mpox_weekly`
- **Validates**: Zero case weeks are preserved (not filtered out)
- **Checks**: Reasonable percentage of zero-count records exist
- **Expected**: 0-95% zero count rate for mpox surveillance

#### `measles_zero_counts_validation`
- **Asset**: `cdc.measles_weekly`
- **Validates**: Zero case weeks are preserved for rare disease surveillance
- **Checks**: Reasonable percentage of zero-count records exist
- **Expected**: 0-98% zero count rate for measles surveillance (higher due to rarity)

## Configuration

```python
# Surveillance period configuration
SURVEILLANCE_START_DATE = "2022-01-01"  # Start of consistent surveillance
SURVEILLANCE_END_DATE = None            # None = current date
```

## Usage in Dagster

The asset checks automatically run when the corresponding assets are materialized:

```bash
# Materialize assets and run checks
dagster asset materialize --select cdc__mpox_weekly -m public
dagster asset materialize --select cdc__measles_weekly -m public
```

## Check Results

### ✅ Passing Check Example
```
✅ Mpox Weekly Data: Complete weekly data for all 52 groups. 100.0% complete.
```

### ⚠️ Warning Check Example
```
❌ Measles Weekly Data: Missing weekly data. 95.2% complete.
   • 3/52 groups have missing weeks
   • Missing: California:Measles (Indigenous): 2024-01-15, Texas:Measles (Imported): 2024-02-05, ...
```

### 📊 Metadata Provided
- Total records processed
- Completeness percentage
- Number of jurisdictions/groups checked
- Missing weeks count and details
- Disease types covered
- Check period dates

## Benefits

1. **Data Quality Assurance**: Ensures surveillance data integrity
2. **Early Gap Detection**: Identifies missing surveillance periods quickly
3. **Zero Count Validation**: Confirms epidemiologically significant zero periods are captured
4. **Multi-Disease Support**: Scales to handle multiple diseases and jurisdictions
5. **Automated Monitoring**: Integrates with Dagster's asset monitoring system

## Extensibility

The framework can be extended for:

- Additional diseases (COVID-19, influenza, etc.)
- Different surveillance frequencies (daily, monthly)
- Additional data quality metrics (geographic coverage, demographic completeness)
- Cross-dataset validation (comparing different surveillance systems)

## Technical Implementation

- **Utility Functions**: `public/utils/data_quality_checks.py`
- **Asset Checks**: `public/assets/cdc_data_quality_checks.py`
- **Data Loading**: Supports CSV and GeoJSON formats from S3
- **Error Handling**: Comprehensive error handling with detailed messages
- **Performance**: Efficient grouping and date range validation