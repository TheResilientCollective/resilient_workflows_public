"""Freshness monitoring for ``sandiego/sandiego_epidemiology_forecast``.

The forecast normally runs weekly. If no materialization has happened in
``FORECAST_STALE_AFTER`` (7.5 days), the pipeline that feeds it has stalled
somewhere upstream and nobody is told unless something watches the clock.

Two pieces:

* ``FORECAST_FRESHNESS_POLICY`` -- attached to the asset so the stall shows as a
  freshness failure on the asset in the Dagster UI (no sensor needed).
* ``forecast_freshness_alert_sensor`` -- posts to Slack (``SLACK_SIMS_CHANNEL``)
  and emails (``EMAIL_*``) when the asset is stale. It re-alerts at most once per
  ``FORECAST_FRESHNESS_REALERT_HOURS`` so a long outage does not spam the channel.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from dagster import (
    AssetKey,
    FreshnessPolicy,
    SensorEvaluationContext,
    SkipReason,
    get_dagster_logger,
    sensor,
)

FORECAST_ASSET_KEY = AssetKey(["sandiego", "sandiego_epidemiology_forecast"])
FORECAST_STALE_AFTER = timedelta(days=7.5)
FORECAST_FRESHNESS_POLICY = FreshnessPolicy.time_window(fail_window=FORECAST_STALE_AFTER)
FORECAST_FRESHNESS_REALERT_HOURS = float(os.environ.get("FORECAST_FRESHNESS_REALERT_HOURS", "24"))
DAGSTER_URL = f'https://{os.environ.get("SCHED_HOSTNAME", "sched")}.{os.environ.get("HOST", "local")}/'


def forecast_staleness(
    last_materialized: datetime | None,
    now: datetime,
    stale_after: timedelta = FORECAST_STALE_AFTER,
) -> tuple[bool, float | None, str]:
    """Classify the forecast asset's age.

    Returns ``(is_stale, age_days, reason)``. ``age_days`` is ``None`` when the
    asset has never materialized, which counts as stale.
    """
    threshold_days = stale_after.total_seconds() / 86400
    if last_materialized is None:
        return True, None, "asset has never been materialized"
    age = now - last_materialized
    age_days = age.total_seconds() / 86400
    if age <= stale_after:
        return False, age_days, f"last run {age_days:.1f} days ago, within the {threshold_days:g}-day window"
    return (
        True,
        age_days,
        f"last run {age_days:.1f} days ago, beyond the {threshold_days:g}-day window",
    )


def should_realert(last_alert: datetime | None, now: datetime, realert_hours: float) -> bool:
    """True when no alert has been sent, or the last one is older than ``realert_hours``."""
    if last_alert is None:
        return True
    return now - last_alert >= timedelta(hours=realert_hours)


# --- Notification sensor --------------------------------------------------------

def _format_messages(reason: str, last_materialized: datetime | None) -> tuple[str, str, str]:
    """Return ``(slack_text, email_subject, email_body)``."""
    last_str = last_materialized.strftime("%Y-%m-%d %H:%M UTC") if last_materialized else "never"
    asset_path = FORECAST_ASSET_KEY.to_user_string()
    slack_text = (
        f":warning: *San Diego epidemiology forecast is stale*\n"
        f"Asset `{asset_path}` has not run in over {FORECAST_STALE_AFTER.total_seconds() / 86400:g} days "
        f"({reason}).\nLast materialization: {last_str}\n"
        f"Dagster: {DAGSTER_URL}"
    )
    subject = f"[Dagster] San Diego epidemiology forecast stale ({reason})"
    body = (
        f"The Dagster asset {asset_path} has not materialized within the expected "
        f"{FORECAST_STALE_AFTER.total_seconds() / 86400:g}-day window.\n\n"
        f"Status: {reason}\n"
        f"Last materialization: {last_str}\n"
        f"Dagster: {DAGSTER_URL}\n\n"
        "Check the upstream sandiego_epidemiology_hyper_extraction asset and the "
        "ResilientSims run queue."
    )
    return slack_text, subject, body


@sensor(
    name="forecast_freshness_alert_sensor",
    minimum_interval_seconds=3600,
    required_resource_keys={"slack"},
    description=(
        f"Alerts Slack and email when sandiego/sandiego_epidemiology_forecast has not "
        f"materialized in {FORECAST_STALE_AFTER.total_seconds() / 86400:g} days."
    ),
)
def forecast_freshness_alert_sensor(context: SensorEvaluationContext):
    from .sandiego_epidemiology_forecasts import SLACK_CHANNEL, send_email

    logger = get_dagster_logger()
    now = datetime.now(timezone.utc)

    event = context.instance.get_latest_materialization_event(FORECAST_ASSET_KEY)
    last_materialized = (
        datetime.fromtimestamp(event.timestamp, tz=timezone.utc) if event else None
    )
    is_stale, age_days, reason = forecast_staleness(last_materialized, now)
    if not is_stale:
        return SkipReason(f"forecast is fresh: {reason}")

    last_alert = datetime.fromisoformat(context.cursor) if context.cursor else None
    if not should_realert(last_alert, now, FORECAST_FRESHNESS_REALERT_HOURS):
        return SkipReason(
            f"forecast is stale ({reason}) but already alerted at {last_alert.isoformat()}; "
            f"next alert after {FORECAST_FRESHNESS_REALERT_HOURS:g}h"
        )

    logger.warning(f"Forecast stale: {reason}")
    slack_text, subject, body = _format_messages(reason, last_materialized)

    slack_sent = False
    try:
        context.resources.slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=slack_text)
        slack_sent = True
    except Exception as e:
        logger.error(f"Error sending Slack freshness alert: {e}")

    email_sent = send_email(subject, body)

    if slack_sent or email_sent:
        context.update_cursor(now.isoformat())
    return SkipReason(
        f"alert sent (slack={slack_sent}, email={email_sent}); age_days={age_days}"
    )
