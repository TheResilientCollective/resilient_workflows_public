"""Weekly H2S accuracy scorecard posted to Slack.

Reads `rolling/7d/scorecard.json` published by the `accuracy_reporting_job`
in `tj_h2s_prediction` and posts a compact Block Kit card to Slack.

Scheduled via the Dagster stack in `deploy/dagster_workflows*.yml`:
    SLACK_WEBHOOK_URL   — incoming webhook, channel #h2s-alerts
    ACCURACY_URL        — public MinIO URL, defaults to oss.resilientservice.mooo.com

Run manually:
    python -m workflows.h2s_accuracy.weekly_scorecard
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

ACCURACY_URL = os.environ.get(
    "ACCURACY_URL",
    "https://oss.resilientservice.mooo.com/resilentpublic/latest/tijuana/forecast/accuracy_reports",
)


def _fetch(url: str) -> dict[str, Any]:
    with urlopen(url, timeout=15) as resp:
        return json.loads(resp.read())


def _pct(v: float | None) -> str:
    return "—" if v is None else f"{100 * v:.1f}%"


def _arrow(new: float | None, old: float | None) -> str:
    if new is None or old is None:
        return ""
    diff = new - old
    if abs(diff) < 0.005:
        return "➡️"
    return "⬆️" if diff > 0 else "⬇️"


def build_blocks(weekly: dict[str, Any], monthly: dict[str, Any]) -> list[dict[str, Any]]:
    w = weekly.get("overall") or {}
    m = monthly.get("overall") or {}
    header = {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"H2S forecast scorecard — week of {weekly.get('period_start')}",
        },
    }
    fields = [
        {"type": "mrkdwn",
         "text": f"*Balanced accuracy*\n{_pct(w.get('balanced_accuracy'))} "
                 f"{_arrow(w.get('balanced_accuracy'), m.get('balanced_accuracy'))}"},
        {"type": "mrkdwn",
         "text": f"*Orange recall*\n{_pct(w.get('orange_recall'))} "
                 f"{_arrow(w.get('orange_recall'), m.get('orange_recall'))}"},
        {"type": "mrkdwn",
         "text": f"*False-alarm rate*\n{_pct(w.get('false_alarm_rate'))} "
                 f"{_arrow(m.get('false_alarm_rate'), w.get('false_alarm_rate'))}"},
        {"type": "mrkdwn",
         "text": f"*Matched observations*\n{w.get('n_matched_observations', 0)}"},
    ]
    body = {"type": "section", "fields": fields}
    footer = {
        "type": "context",
        "elements": [{
            "type": "mrkdwn",
            "text": "_Arrows compare the 7-day window against the 30-day baseline. "
                    f"Period end: {weekly.get('period_end')}._",
        }],
    }
    return [header, body, footer]


def post_to_slack(webhook_url: str, blocks: list[dict[str, Any]]) -> None:
    body = json.dumps({"blocks": blocks}).encode("utf-8")
    req = Request(webhook_url, data=body, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=10) as resp:
        if resp.status >= 300:
            raise RuntimeError(f"Slack webhook returned {resp.status}")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        raise SystemExit("SLACK_WEBHOOK_URL is not set")

    weekly = _fetch(f"{ACCURACY_URL}/rolling/7d/scorecard.json")
    monthly = _fetch(f"{ACCURACY_URL}/rolling/30d/scorecard.json")
    blocks = build_blocks(weekly, monthly)
    post_to_slack(webhook, blocks)
    log.info("posted weekly scorecard: %s → %s",
             weekly.get("period_start"), weekly.get("period_end"))


if __name__ == "__main__":
    main()
