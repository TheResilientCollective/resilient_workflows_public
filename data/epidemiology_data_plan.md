# Plan to standardized Epidemiology output

We want to create a base model, and allow for model extension  for the output of epidemiology data.
We want to use the pandera library
While this is for san diego, let's just call them resilient epi schemas.

The model for the output that the San Diego County wants use as inputs:
- Jurisdiction: String, No spaces, CamelCased Location names
- date_week_start: Date from dataset
- date_week_end: Date + Seven Days
- Week_Number: Week Number of Date
- Year: Year of Date
- Week_Year: f'{Week_Number}-{Year}'
- Cases: Counts

A Different Model would be used for statistical model output
A statistical extension should:
- "Jurisdiction"      Geographic jurisdiction
- "date"           iso_date  YYYY-mm-dd
- "disease"       name of the pathogen
- "metric"           cases, deaths, hospitalizations  (tests, vaccinations?)
- "observation_type"    actual, partial-data estimate, prediction, forecast
- "mean" (optional_1)
- "count"  (optional_1) - actual observation 
- "rate"  (optional_1)
- "median"  (optional_1)
- "lower_ci"    (optional_2_1)      Lower CI for prediction
- "upper_ci"   (optional_2_1)   Upper CI for prediction

- "lower_20" (optional_2_2) 
- "upper_20" (optional_2_2) 
- "lower_50" (optional_2_3) 
- "upper_50" (optional_2_3) 
- "lower_90" (optional_2_4) 
- "upper_90" (optional_2_4) 
Must have at least one of the optional_1 fields.
The optional_2 properties are pairs of values. Both of the values must be present.

This may part of a new workflows/public/public/utils class
I might accept a dataframe, validate it, then write it out.

I would like to implement this in these assets:
* workflows/public/public/assets/sandiego_epidemiology_mpox.py
* workflows/public/public/assets/sandiego_epidemiology.py
* workflows/public/public/assets/cdc_nnds.py
* workflows/public/public/assets/mpox_counties.py

The Statistical Model output should be used for 
* workflows/public/public/assets/sandiego_epidemiology_forecasts.py
