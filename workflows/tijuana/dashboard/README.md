# Tijuana River dashboard

A static dashboard of H2S at the three APCD monitors with wind, the H2S forecast and how well it
warns, river flow at the border, South Bay plant flows, weather and spills.

Published page: https://oss.resilientservice.mooo.com/resilentpublic/latest/tijuana/dashboard/index.html

| File | What it is |
|---|---|
| `build_data.py` | Reads the pipeline's public outputs and writes one JSON file per dataset (standard library only) |
| `page.html` | The dashboard page; the same file is used by the Claude dashboard artifact |
| `standalone.html` | Wraps `page.html` for S3 with design tokens and a small data loader |
| `publish_s3.py` | Uploads `index.html` and `data/*.json` to the bucket (needs `minio` and the `S3_*` variables) |
| `map_outlines.json` | Simplified land, border and river lines for the map |

```bash
python3 -I build_data.py /tmp/tjdash/in /tmp/tjdash/out
python3 publish_s3.py /tmp/tjdash/out            # --bucket test to try it on the development bucket
```

The forecast section reads the 30 ppb backtest (`h2sforecast/h2s_backtest_30ppb`), the live
performance report (`forecast_performance_latest.json`) and keeps `data/forecast_log.json`, a
running record of each day's published forecast, so recent nights can be checked against the
measured sunset-to-sunrise peak. A scheduled job runs both scripts at 10:45 am and 7:45 pm Pacific.
