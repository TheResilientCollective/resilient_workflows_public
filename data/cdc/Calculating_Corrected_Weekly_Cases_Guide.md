# CALCULATING CORRECTED WEEKLY CASE COUNTS FROM CUMULATIVE YTD
## Python Implementation Guide
### Handling Negative Values

---

## THE PROBLEM

When calculating weekly cases from cumulative totals using:
```python
corrected_cases = cumulative(week N) - cumulative(week N-1)
```

You may get **negative values** when:
- Data corrections remove previously reported cases
- Duplicates are identified and removed
- Cases are reassigned to different jurisdictions
- Reporting errors are fixed

**Example**: California 2024 Week 40
- Previous cumulative: 391
- Current cumulative: 188
- Difference: **-203** (large negative correction)

---

## SOLUTION APPROACHES

### 🏆 **RECOMMENDED: Approach #5 - Split Columns (Most Transparent)**

This approach separates positive additions from negative corrections, giving you full transparency.

```python
import pandas as pd
import numpy as np

def calculate_corrected_cases(df):
    """
    Calculate weekly cases from cumulative with transparency for corrections
    
    Returns columns:
    - Cases_Added: Positive increases (new cases)
    - Cases_Removed: Negative changes (corrections)  
    - Net_Cases: Total change (can be negative)
    - Week_Type: 'Normal', 'Correction', or 'Mixed'
    """
    # Sort data
    df = df.sort_values(['Reporting Area', 'Current MMWR Year', 'MMWR WEEK']).copy()
    
    # Calculate difference from previous week
    df['Raw_Difference'] = df.groupby(['Reporting Area', 'Current MMWR Year'])[
        'Cumulative YTD Current MMWR Year'].diff()
    
    # First week of each year uses cumulative value (no previous week)
    first_week_mask = df.groupby(['Reporting Area', 'Current MMWR Year']).cumcount() == 0
    df.loc[first_week_mask, 'Raw_Difference'] = df.loc[first_week_mask, 
                                                         'Cumulative YTD Current MMWR Year']
    
    # Split into positive and negative components
    df['Cases_Added'] = df['Raw_Difference'].clip(lower=0)
    df['Cases_Removed'] = df['Raw_Difference'].clip(upper=0)
    df['Net_Cases'] = df['Raw_Difference']
    
    # Flag the type of week
    df['Week_Type'] = 'Normal'
    df.loc[df['Cases_Removed'] < 0, 'Week_Type'] = 'Correction'
    df.loc[(df['Cases_Added'] > 0) & (df['Cases_Removed'] < 0), 'Week_Type'] = 'Mixed'
    
    return df

# Usage
df = pd.read_csv('NNDSS_Weekly_Data_20260202__1_.csv')
df['Cumulative YTD Current MMWR Year'] = pd.to_numeric(
    df['Cumulative YTD Current MMWR Year'], errors='coerce'
)

df_corrected = calculate_corrected_cases(df)

# Now you have:
# - Cases_Added: Use for case counts (always >= 0)
# - Cases_Removed: Track corrections (always <= 0)
# - Week_Type: Identify problematic weeks
```

**Why This Approach?**
- ✓ Completely transparent
- ✓ Preserves all information
- ✓ Easy to explain to stakeholders
- ✓ Flexible for different analyses
- ✓ Shows where data quality issues occur

---

### Alternative Approaches

#### Approach #1: Simple Floor at Zero
**Use when**: You just need positive case counts and don't care about corrections

```python
df['Corrected_Cases'] = df.groupby(['Reporting Area', 'Current MMWR Year'])[
    'Cumulative YTD Current MMWR Year'].diff()
df['Corrected_Cases'] = df['Corrected_Cases'].clip(lower=0)

# Handle first week
first_week = df.groupby(['Reporting Area', 'Current MMWR Year']).cumcount() == 0
df.loc[first_week, 'Corrected_Cases'] = df.loc[first_week, 
                                                'Cumulative YTD Current MMWR Year']
```

**Pros**: Simple, always positive
**Cons**: Hides corrections, loses information

---

#### Approach #2: Preserve Negatives
**Use when**: You need to track data quality issues

```python
df['Corrected_Cases'] = df.groupby(['Reporting Area', 'Current MMWR Year'])[
    'Cumulative YTD Current MMWR Year'].diff()

# Flag negatives
df['Is_Correction'] = df['Corrected_Cases'] < 0
df['Correction_Size'] = df['Corrected_Cases'].where(df['Is_Correction'], 0)

# Handle first week
first_week = df.groupby(['Reporting Area', 'Current MMWR Year']).cumcount() == 0
df.loc[first_week, 'Corrected_Cases'] = df.loc[first_week, 
                                                'Cumulative YTD Current MMWR Year']
```

**Pros**: Tracks all corrections
**Cons**: Negative values in case counts confusing for non-technical users

---

## REAL DATA FINDINGS

From analyzing the actual data, we found **30 instances of negative corrections**:

### Largest Corrections:

| State | Year | Week | Correction Size |
|-------|------|------|-----------------|
| CALIFORNIA | 2024 | 40 | -203 |
| COLORADO | 2024 | 42 | -146 |
| California | 2025 | 14 | -30 |
| PENNSYLVANIA | 2024 | 18 | -12 |

These represent data quality improvements where incorrect cases were removed from the system.

---

## EXAMPLE OUTPUT

### California 2026 (No Corrections):

| Week | Cumulative | Cases_Added | Cases_Removed | Net_Cases | Week_Type |
|------|------------|-------------|---------------|-----------|-----------|
| 1 | 2 | 2 | 0 | 2 | Normal |
| 2 | 10 | 8 | 0 | 8 | Normal |
| 3 | 13 | 3 | 0 | 3 | Normal |

### California 2024 Week 40 (Large Correction):

| Week | Cumulative | Cases_Added | Cases_Removed | Net_Cases | Week_Type |
|------|------------|-------------|---------------|-----------|-----------|
| 39 | 391 | 23 | 0 | 23 | Normal |
| 40 | 188 | 0 | -203 | -203 | Correction |
| 41 | 423 | 235 | 0 | 235 | Normal |

**Interpretation**: Week 40 removed 203 incorrect cases from the cumulative total.

---

## COMPLETE WORKING EXAMPLE

```python
import pandas as pd
import numpy as np

# Load your data
df = pd.read_csv('NNDSS_Weekly_Data_20260202__1_.csv')

# Convert to numeric
df['Cumulative YTD Current MMWR Year'] = pd.to_numeric(
    df['Cumulative YTD Current MMWR Year'], errors='coerce'
)

# Sort by area, year, week
df = df.sort_values(['Reporting Area', 'Current MMWR Year', 'MMWR WEEK']).copy()

# Calculate weekly change
df['Raw_Difference'] = df.groupby(['Reporting Area', 'Current MMWR Year'])[
    'Cumulative YTD Current MMWR Year'].diff()

# Handle first week of each year (no previous week to compare)
first_week = df.groupby(['Reporting Area', 'Current MMWR Year']).cumcount() == 0
df.loc[first_week, 'Raw_Difference'] = df.loc[first_week, 
                                               'Cumulative YTD Current MMWR Year']

# Split into additions and corrections
df['Cases_Added'] = df['Raw_Difference'].clip(lower=0)
df['Cases_Removed'] = df['Raw_Difference'].clip(upper=0)
df['Net_Cases'] = df['Raw_Difference']

# Classify weeks
df['Week_Type'] = 'Normal'
df.loc[df['Cases_Removed'] < 0, 'Week_Type'] = 'Correction'
df.loc[(df['Cases_Added'] > 0) & (df['Cases_Removed'] < 0), 'Week_Type'] = 'Mixed'

# Flag corrections
df['Has_Correction'] = df['Cases_Removed'] < 0

# Save results
df.to_csv('corrected_weekly_cases.csv', index=False)

# Analysis examples:

# 1. Total cases for each state (use Cases_Added)
state_totals = df.groupby('Reporting Area')['Cases_Added'].sum()

# 2. Find weeks with corrections
corrections = df[df['Has_Correction']]
print(f"Found {len(corrections)} weeks with corrections")

# 3. States with most corrections
correction_counts = df.groupby('Reporting Area')['Has_Correction'].sum()
print("\nStates with most correction weeks:")
print(correction_counts.sort_values(ascending=False).head(10))

# 4. Visualize California 2026
ca_2026 = df[(df['Reporting Area'] == 'California') & 
             (df['Current MMWR Year'] == 2026)]
print("\nCalifornia 2026:")
print(ca_2026[['MMWR WEEK', 'Cumulative YTD Current MMWR Year', 
               'Cases_Added', 'Cases_Removed', 'Week_Type']])
```

---

## DECISION GUIDE

### Choose Your Approach:

**For Public Health Reports** → Use **Cases_Added** column
- Always positive (no confusing negatives)
- Represents new cases identified each week
- Easy to communicate

**For Academic Publications** → Use **Split Columns** approach
- Shows full transparency
- Documents data quality
- Reviewers can see corrections

**For Trend Analysis** → Use **Cases_Added** column
- Creates smooth time series
- Removes noise from corrections
- Better for forecasting

**For Data Quality Monitoring** → Use **Preserve Negatives** approach
- Track where problems occur
- Identify states with issues
- Monitor improvement over time

---

## KEY TAKEAWAYS

1. **Negatives are EXPECTED** - they represent data improvements
2. **Don't ignore them** - use split columns to show transparency
3. **For case counts** - use the `Cases_Added` column (always >= 0)
4. **For QA/QC** - monitor `Cases_Removed` to find problem areas
5. **Document your choice** - explain which approach you used and why

---

## APPENDIX: States with Corrections

Based on the current data, these states had the most weeks with corrections:

- California (multiple large corrections in 2024-2025)
- Colorado (major correction in 2024 week 42)
- Pennsylvania (correction in 2024 week 18)
- Ohio, Texas, Florida (occasional small corrections)

**This is normal** - it shows the data quality is being actively maintained and improved.

---

**Updated**: February 2, 2026
**Superseded**: March 27, 2026

---

## ⚠️ SUPERSEDED — Updated Methodology (March 2026)

This guide describes the initial split-column approach. The pipeline has since been enhanced with:

1. **Use `previous_YTD__cummulative` for prior years** — Provides corrected/finalized cumulative data, producing more accurate weekly rates than `current_YTD__cummulative` for historical years.

2. **Windowed smoothing of negative adjustments** — Instead of simply clipping negatives to zero, the pipeline now redistributes negative corrections across neighboring weeks:
   - Small negatives (|value| ≤ 3): averaged across a 3-week window
   - Large negatives (|value| > 3): averaged across a 5-week window
   - Residual negatives clamped to zero after smoothing

3. **New primary columns**:
   - **`adjusted_week`** — Smoothed weekly case count (replaces `Cases_Added` as the primary analytical column)
   - **`adjusted_YTD__cummulative`** — Recalculated cumulative from adjusted weekly counts

4. **Week_Type classification** — Now includes smoothing labels (`Smoothed_Small`, `Smoothed_Large`, `Smoothing_Neighbor`, `Smoothed_Residual_Zeroed`) and a `Preliminary_` prefix for current-year data.

5. **Observation type classification** — Statistical extension records now use `corrected` (previous years), `preliminary estimate` (current year), and `reported` (raw CDC values).

**For the full updated methodology, see: `docs/cdc_weekly_case_rate_methodology.md`**
