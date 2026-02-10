"""Convert Guam FLU daily CSV to basic_epi_schema weekly format."""

import pandas as pd
from epiweeks import Week

# Read daily data
df = pd.read_csv("FLU_Guam_26NOV2025.csv", parse_dates=["epiweek_start"])

# Compute CDC epi-week for each daily record and aggregate into weekly totals
df["epi_week"] = df["epiweek_start"].apply(lambda d: Week.fromdate(d, system="cdc"))
df["week_start"] = df["epi_week"].apply(lambda w: w.startdate())
weekly = df.groupby("week_start", as_index=False)["cases"].sum()
weekly = weekly.sort_values("week_start").reset_index(drop=True)

# Build basic_epi_schema columns
weekly["week_start"] = pd.to_datetime(weekly["week_start"])
result = pd.DataFrame(index=weekly.index)

result["Jurisdiction"] = "Guam"
result["date_week_start"] = weekly["week_start"].dt.strftime("%Y-%m-%d")
result["date_week_end"] = (weekly["week_start"] + pd.Timedelta(days=6)).dt.strftime("%Y-%m-%d")
result["Week_Number"] = weekly["week_start"].apply(lambda d: Week.fromdate(d, system="cdc").week)
result["Year"] = weekly["week_start"].apply(lambda d: Week.fromdate(d, system="cdc").year)
result["Week_Year"] = result["Week_Number"].astype(str) + "-" + result["Year"].astype(str)
result["Cases"] = weekly["cases"].astype(int)
result["Week_Type"] = "Original"

# Save
output_path = "FLU_Guam_26NOV2025_epi_schema.csv"
result.to_csv(output_path, index=False)
print(f"Wrote {len(result)} weekly records to {output_path}")
print(result.head(10).to_string())
