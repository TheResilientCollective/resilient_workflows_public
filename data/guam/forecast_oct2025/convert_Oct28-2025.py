"""
Convert daily infection/R model output (EpiNow2-style) into the
COVID_case_reports.csv + COVID_case_Rt.csv format used by the user's pipeline.

Inputs:
  data-cases-Oct28-2025.csv  — daily `infections`, Estimate|Estimate based on preliminary data|Forecast
  data-R-Oct28-2025.csv      — daily `R`, same type categories

Outputs:
  case_reports_from_Oct28-2025.csv   (weekly, COVID_case_reports schema)
  case_Rt_from_Oct28-2025.csv        (daily, COVID_case_Rt schema)

Epiweek convention: MMWR (Sunday-start).
"""

import csv
from collections import defaultdict
from datetime import date, timedelta

SRC_CASES = "/mnt/user-data/uploads/data-cases-Oct28-2025.csv"
SRC_R     = "/mnt/user-data/uploads/data-R-Oct28-2025.csv"

OUT_CASES = "/home/claude/out/case_reports_from_Oct28-2025.csv"
OUT_RT    = "/home/claude/out/case_Rt_from_Oct28-2025.csv"

DISEASE = "Infections"

# --- type mapping: source → target
TYPE_MAP = {
    "Estimate":                           "estimate",
    "Estimate based on preliminary data": "partial-data estimate",
    "Forecast":                           "forecast",
}


# ======================================================================
# Helpers
# ======================================================================
def strip_q(s):
    return s.strip().strip('"') if s is not None else s


def mmwr_week_start(d):
    """Return the Sunday that starts the MMWR epiweek containing d."""
    # Python's weekday(): Mon=0..Sun=6
    days_since_sun = (d.weekday() + 1) % 7   # Sun→0, Mon→1, ..., Sat→6
    return d - timedelta(days=days_since_sun)


def parse_float_or_na(s):
    s = strip_q(s)
    if s == "" or s.upper() == "NA":
        return None
    return float(s)


# ======================================================================
# Read daily cases file (infections)
# ======================================================================
daily_rows = []
with open(SRC_CASES, newline="") as f:
    reader = csv.DictReader(f)
    for r in reader:
        # Strip any stray quotes from values and carriage returns
        cleaned = {k: strip_q(v).rstrip("\r") for k, v in r.items()}
        daily_rows.append({
            "date":     date.fromisoformat(cleaned["date"]),
            "type":     TYPE_MAP[cleaned["type"]],
            "median":   float(cleaned["median"]),
            "mean":     float(cleaned["mean"]),
            "sd":       float(cleaned["sd"]),
            "lower_90": float(cleaned["lower_90"]),
            "lower_50": float(cleaned["lower_50"]),
            "lower_20": float(cleaned["lower_20"]),
            "upper_20": float(cleaned["upper_20"]),
            "upper_50": float(cleaned["upper_50"]),
            "upper_90": float(cleaned["upper_90"]),
        })


# ======================================================================
# Aggregate to MMWR weeks
# ======================================================================
by_week = defaultdict(list)
for r in daily_rows:
    by_week[mmwr_week_start(r["date"])].append(r)

weekly_rows = []
for ws in sorted(by_week.keys()):
    days = by_week[ws]
    n_days = len(days)

    # Weekly quantiles = sum of daily quantiles (preserves CI-to-median ratio
    # when that ratio is stable across days, which it is here; far simpler
    # than refitting a distribution).
    def ssum(key):
        return sum(d[key] for d in days)

    # Type: use the dominant type across the week; ties break toward "worse"
    # (forecast > partial-data estimate > estimate) since that's the
    # conservative display choice.
    type_counts = defaultdict(int)
    for d in days:
        type_counts[d["type"]] += 1
    rank = {"estimate": 0, "partial-data estimate": 1, "forecast": 2}
    week_type = max(type_counts.keys(),
                    key=lambda t: (type_counts[t], rank[t]))

    median_val = ssum("median")

    # cases: use rounded weekly median for estimate/partial, blank for forecast
    cases_val = "" if week_type == "forecast" else int(round(median_val))

    weekly_rows.append({
        "epiweek_start": ws.isoformat(),
        "cases":         cases_val,
        "Type":          week_type,
        "Median":  int(round(median_val)),
        "Lower90": int(round(ssum("lower_90"))),
        "Lower50": int(round(ssum("lower_50"))),
        "Lower20": int(round(ssum("lower_20"))),
        "Upper20": int(round(ssum("upper_20"))),
        "Upper50": int(round(ssum("upper_50"))),
        "Upper90": int(round(ssum("upper_90"))),
        "_n_days": n_days,            # not written out, just for reporting
    })


# ======================================================================
# Write cases CSV (COVID_case_reports.csv schema + quoting)
# ======================================================================
CASE_HDR = ["Disease","epiweek_start","cases","Type","Median",
            "Lower90","Lower50","Lower20","Upper20","Upper50","Upper90"]

with open(OUT_CASES, "w", newline="") as f:
    f.write(",".join(f'"{h}"' for h in CASE_HDR) + "\n")
    for r in weekly_rows:
        parts = [
            f'"{DISEASE}"',
            r["epiweek_start"],
            str(r["cases"]) if r["cases"] != "" else "",
            f'"{r["Type"]}"',
            str(r["Median"]),
            str(r["Lower90"]), str(r["Lower50"]), str(r["Lower20"]),
            str(r["Upper20"]), str(r["Upper50"]), str(r["Upper90"]),
        ]
        f.write(",".join(parts) + "\n")

print(f"Wrote {len(weekly_rows)} weekly rows → {OUT_CASES}")


# ======================================================================
# Read daily R file and reformat
# ======================================================================
rt_rows = []
with open(SRC_R, newline="") as f:
    reader = csv.DictReader(f)
    for r in reader:
        cleaned = {k: strip_q(v).rstrip("\r") for k, v in r.items()}
        rt_rows.append({
            "date":     cleaned["date"],
            "variable": cleaned["variable"],         # "R"
            "type":     TYPE_MAP[cleaned["type"]],
            "median":   cleaned["median"],
            "mean":     cleaned["mean"],
            "sd":       cleaned["sd"],
            "lower_90": cleaned["lower_90"],
            "lower_50": cleaned["lower_50"],
            "lower_20": cleaned["lower_20"],
            "upper_20": cleaned["upper_20"],
            "upper_50": cleaned["upper_50"],
            "upper_90": cleaned["upper_90"],
        })

RT_HDR = ["Disease","date","variable","type","median","mean","sd",
          "lower_90","lower_50","lower_20","upper_20","upper_50","upper_90"]

with open(OUT_RT, "w", newline="") as f:
    f.write(",".join(f'"{h}"' for h in RT_HDR) + "\n")
    for r in rt_rows:
        f.write(",".join([
            f'"{DISEASE}"',
            r["date"],
            f'"{r["variable"]}"',
            f'"{r["type"]}"',
            r["median"], r["mean"], r["sd"],
            r["lower_90"], r["lower_50"], r["lower_20"],
            r["upper_20"], r["upper_50"], r["upper_90"],
        ]) + "\n")

print(f"Wrote {len(rt_rows)} daily Rt rows → {OUT_RT}")


# ======================================================================
# Summary
# ======================================================================
from collections import Counter
print("\nWeekly breakdown:")
for r in weekly_rows:
    partial = "" if r["_n_days"] == 7 else f"  [PARTIAL: {r['_n_days']} days]"
    print(f"  {r['epiweek_start']}  cases={str(r['cases']):>4}  "
          f"median={r['Median']:>4}  [{r['Lower90']:>4},{r['Upper90']:>4}]  "
          f"{r['Type']}{partial}")
print("\nWeekly Type counts:", Counter(r["Type"] for r in weekly_rows))
print("Rt Type counts:     ", Counter(r["type"] for r in rt_rows))
