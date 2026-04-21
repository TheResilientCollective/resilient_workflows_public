# h2s_accuracy

Monthly Quarto report and weekly Slack scorecard for the Tijuana River Valley
H2S forecast.

Both artifacts consume the rollup JSON published by `accuracy_reporting_job`
in [`tj_h2s_prediction`][tj]:

```
{ACCURACY_URL}/rolling/{7d,30d,90d}/scorecard.json
{ACCURACY_URL}/monthly/{YYYY-MM}/scorecard.json
{ACCURACY_URL}/alert_performance/30d.json
{ACCURACY_URL}/latest.json
```

`ACCURACY_URL` defaults to the public MinIO URL:
`https://oss.resilientservice.mooo.com/resilentpublic/latest/tijuana/forecast/accuracy_reports`.

## Monthly report

```sh
quarto render report.qmd --to html --output-dir _site
# Or for a PDF:
quarto render report.qmd --to pdf
```

Publish via the existing Netlify + Slack approval flow in
`netlify_triggers/` — the Dagster asset should produce `_site/index.html`
and post a preview link for stakeholder sign-off.

## Weekly Slack scorecard

```sh
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
    python -m workflows.h2s_accuracy.weekly_scorecard
```

Ship as a Dagster schedule (Mondays 09:00 PT). The card shows the 7-day
rolling window with arrows comparing against the 30-day baseline.

[tj]: https://github.com/TheResilientCollective/tj_h2s_prediction
