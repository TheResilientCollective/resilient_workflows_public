"""Poll an asynchronous ResilientLLM run and launch the parse asset when it is done.

``resilientllm_sd_disease_request`` submits the run and records its id in the
materialization metadata. This sensor reads that id, polls ``{webhook}/status``,
and:

* pending  -- keeps waiting;
* done     -- requests ``resilientllm_sd_disease_job`` with the run id as config,
              using the run id as ``run_key`` so the job launches once per run;
* error, done-without-data, or pending past ``RESILIENTLLM_TIMEOUT_HOURS``
           -- alerts Slack and email, then stops polling that run.

The cursor is a JSON record ``{"run_id", "state", "first_seen"}`` so the sensor
survives restarts without re-launching or re-alerting.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from dagster import (
    RunConfig,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    get_dagster_logger,
    sensor,
)

from ..resources.resilientllm import LLMRunHandle
from .sandiego_epidemiology_forecasts import (
    LLM_REQUEST_ASSET_KEY,
    SLACK_CHANNEL,
    LLMRunConfig,
    resilientllm_sd_disease_job,
    send_email,
)

RESILIENTLLM_POLL_SECONDS = int(os.environ.get("RESILIENTLLM_POLL_SECONDS", "300"))
RESILIENTLLM_TIMEOUT_HOURS = float(os.environ.get("RESILIENTLLM_TIMEOUT_HOURS", "6"))
PARSE_OP_NAME = "sandiego__resilientllm_sd_disease"

STATE_POLLING = "polling"
STATE_LAUNCHED = "launched"
STATE_FAILED = "failed"


@dataclass
class PollCursor:
    run_id: str
    state: str
    first_seen: str  # ISO timestamp of the first poll for this run_id

    @classmethod
    def load(cls, raw: str | None) -> "PollCursor | None":
        if not raw:
            return None
        return cls(**json.loads(raw))

    def dump(self) -> str:
        return json.dumps(asdict(self))

    @property
    def finished(self) -> bool:
        return self.state in (STATE_LAUNCHED, STATE_FAILED)


@dataclass
class PollDecision:
    action: str  # "skip" | "launch" | "fail"
    cursor: PollCursor
    reason: str


def decide(
    run_id: str,
    handle: LLMRunHandle | None,
    cursor: PollCursor | None,
    now: datetime,
    timeout: timedelta,
) -> PollDecision:
    """Pure state transition for one sensor tick.

    ``handle`` is ``None`` when the status call itself failed; that counts as a
    pending poll so a transient network error does not fail the run.
    """
    if cursor is None or cursor.run_id != run_id:
        cursor = PollCursor(run_id=run_id, state=STATE_POLLING, first_seen=now.isoformat())
    if cursor.finished:
        return PollDecision("skip", cursor, f"run {run_id} already {cursor.state}")

    if handle is not None and handle.is_done:
        if handle.data:
            return PollDecision("launch", PollCursor(run_id, STATE_LAUNCHED, cursor.first_seen),
                                f"run {run_id} is done")
        return PollDecision("fail", PollCursor(run_id, STATE_FAILED, cursor.first_seen),
                            f"run {run_id} is done but returned no data")
    if handle is not None and handle.is_error:
        return PollDecision("fail", PollCursor(run_id, STATE_FAILED, cursor.first_seen),
                            f"run {run_id} reported status error")

    waited = now - datetime.fromisoformat(cursor.first_seen)
    if waited > timeout:
        return PollDecision("fail", PollCursor(run_id, STATE_FAILED, cursor.first_seen),
                            f"run {run_id} still pending after {waited.total_seconds() / 3600:.1f}h")
    status = handle.status if handle is not None else "unknown (status call failed)"
    return PollDecision("skip", cursor,
                        f"run {run_id} pending (status={status}, waited {waited.total_seconds() / 60:.0f} min)")


def _latest_run_id(context: SensorEvaluationContext) -> str | None:
    event = context.instance.get_latest_materialization_event(LLM_REQUEST_ASSET_KEY)
    if event is None:
        return None
    entry = event.asset_materialization.metadata.get("llm_run_id")
    return str(entry.value) if entry is not None else None


def _notify_failure(context: SensorEvaluationContext, reason: str) -> None:
    logger = get_dagster_logger()
    text = (f":x: *ResilientLLM by-disease summary failed*\n{reason}\n"
            "The San Diego forecast summary was not published. Re-materialize "
            f"`{LLM_REQUEST_ASSET_KEY.to_user_string()}` to request a new run.")
    try:
        context.resources.slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=text)
    except Exception as e:
        logger.error(f"Error sending Slack LLM failure alert: {e}")
    send_email("[Dagster] ResilientLLM by-disease summary failed", text.replace("*", "").replace("`", ""))


@sensor(
    name="resilientllm_status_sensor",
    job=resilientllm_sd_disease_job,
    minimum_interval_seconds=RESILIENTLLM_POLL_SECONDS,
    required_resource_keys={"resilientllm", "slack"},
    description="Polls the asynchronous ResilientLLM run and launches resilientllm_sd_disease when it is done.",
)
def resilientllm_status_sensor(context: SensorEvaluationContext):
    logger = get_dagster_logger()
    now = datetime.now(timezone.utc)

    run_id = _latest_run_id(context)
    if run_id is None:
        return SkipReason("no resilientllm_sd_disease_request materialization yet")

    cursor = PollCursor.load(context.cursor)
    handle = None
    if cursor is None or cursor.run_id != run_id or not cursor.finished:
        try:
            handle = context.resources.resilientllm.get_status(run_id)
        except Exception as e:
            logger.warning(f"ResilientLLM status call failed for {run_id}: {e}")

    decision = decide(run_id, handle, cursor, now, timedelta(hours=RESILIENTLLM_TIMEOUT_HOURS))
    context.update_cursor(decision.cursor.dump())

    if decision.action == "launch":
        logger.info(decision.reason)
        return RunRequest(
            run_key=run_id,
            run_config=RunConfig(ops={PARSE_OP_NAME: LLMRunConfig(run_id=run_id)}),
            tags={"llm_run_id": run_id},
        )
    if decision.action == "fail":
        logger.error(decision.reason)
        _notify_failure(context, decision.reason)
        return SkipReason(decision.reason)
    return SkipReason(decision.reason)
