"""Poll asynchronous ResilientLLM runs and launch the parse asset for each one that finishes.

``resilientllm_sd_disease_request`` submits a run and records its id (and the
report id it was submitted to) in the materialization metadata. This sensor:

1. reads the recent materializations of that asset and adds any run id it has
   not seen yet to its tracked list, so several runs can be in flight at once
   (for example, when nobody approves the first before the next week's request);
2. polls ``{webhook}/{report_id}/v3/status`` for every run still pending;
3. for each run that is ``done`` with ``data``, requests
   ``resilientllm_sd_disease_job`` with the run id as config, using the run id
   as ``run_key`` so the job launches once per run;
4. on ``error``, done-without-data, or pending past ``RESILIENTLLM_TIMEOUT_HOURS``,
   alerts Slack and email and stops polling that run.

The cursor is JSON ``{"runs": [{"run_id", "report_id", "state", "first_seen"}, ...]}``
so the sensor survives restarts without re-launching or re-alerting. Finished
runs are kept while their request materialization is still inside the discovery
window (the last ``RESILIENTLLM_MAX_TRACKED_RUNS`` materializations) and for at
least ``RETENTION_DAYS``, so a run that is already launched or failed is never
rediscovered and polled again. Only once both conditions lapse is it dropped.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
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

RESILIENTLLM_POLL_SECONDS = int(os.environ.get("RESILIENTLLM_POLL_SECONDS", "1800"))
RESILIENTLLM_TIMEOUT_HOURS = float(os.environ.get("RESILIENTLLM_TIMEOUT_HOURS", "144"))# six days for a weekly run
RESILIENTLLM_MAX_TRACKED_RUNS = int(os.environ.get("RESILIENTLLM_MAX_TRACKED_RUNS", "20"))
RETENTION_DAYS = 14
PARSE_OP_NAME = "sandiego__resilientllm_sd_disease"

STATE_POLLING = "polling"
STATE_LAUNCHED = "launched"
STATE_FAILED = "failed"


@dataclass
class TrackedRun:
    run_id: str
    report_id: str
    state: str
    first_seen: str  # ISO timestamp of when the sensor first saw this run id

    @property
    def finished(self) -> bool:
        return self.state in (STATE_LAUNCHED, STATE_FAILED)

    def with_state(self, state: str) -> "TrackedRun":
        return TrackedRun(self.run_id, self.report_id, state, self.first_seen)


@dataclass
class PollCursor:
    runs: list[TrackedRun] = field(default_factory=list)

    @classmethod
    def load(cls, raw: str | None) -> "PollCursor":
        if not raw:
            return cls()
        obj = json.loads(raw)
        if "runs" not in obj:
            # Pre-array cursor: a single {"run_id", "state", "first_seen"} record.
            obj = {"runs": [{"report_id": "", **obj}]}
        return cls(runs=[TrackedRun(**r) for r in obj["runs"]])

    def dump(self) -> str:
        return json.dumps({"runs": [asdict(r) for r in self.runs]})

    def get(self, run_id: str) -> TrackedRun | None:
        return next((r for r in self.runs if r.run_id == run_id), None)

    def upsert(self, run: TrackedRun) -> None:
        self.runs = [r for r in self.runs if r.run_id != run.run_id] + [run]

    @property
    def pending(self) -> list[TrackedRun]:
        return [r for r in self.runs if not r.finished]

    def prune(self, now: datetime, discoverable: set[str] | None = None,
              retention: timedelta = timedelta(days=RETENTION_DAYS)) -> None:
        """Drop finished runs that are older than ``retention`` and no longer discoverable.

        ``discoverable`` is the set of run ids currently visible in the request
        asset's recent materializations. A finished run still in that set must
        stay in the cursor, however old, or ``discover`` would re-add it as
        pending on the next tick and poll it again. Pending runs are always kept.
        """
        discoverable = discoverable or set()
        self.runs = [
            r for r in self.runs
            if not r.finished
            or r.run_id in discoverable
            or now - datetime.fromisoformat(r.first_seen) <= retention
        ]


@dataclass
class MaterializedRun:
    """A run id recorded by a ``resilientllm_sd_disease_request`` materialization."""
    run_id: str
    report_id: str
    materialized_at: datetime


@dataclass
class PollDecision:
    action: str  # "skip" | "launch" | "fail"
    run: TrackedRun
    reason: str


def discover(cursor: PollCursor, materialized: list[MaterializedRun]) -> list[TrackedRun]:
    """Add every materialized run id the cursor has not seen; return the new entries."""
    new = []
    for m in materialized:
        if cursor.get(m.run_id) is None:
            run = TrackedRun(m.run_id, m.report_id, STATE_POLLING, m.materialized_at.isoformat())
            cursor.upsert(run)
            new.append(run)
    return new


def decide(run: TrackedRun, handle: LLMRunHandle | None, now: datetime, timeout: timedelta) -> PollDecision:
    """Pure state transition for one tracked run on one sensor tick.

    ``handle`` is ``None`` when the status call itself failed; that counts as a
    pending poll so a transient network error does not fail the run.
    """
    if run.finished:
        return PollDecision("skip", run, f"run {run.run_id} already {run.state}")

    if handle is not None and handle.is_done:
        if handle.data:
            return PollDecision("launch", run.with_state(STATE_LAUNCHED), f"run {run.run_id} is done")
        return PollDecision("fail", run.with_state(STATE_FAILED), f"run {run.run_id} is done but returned no data")
    if handle is not None and handle.is_error:
        return PollDecision("fail", run.with_state(STATE_FAILED),
                            f"run {run.run_id} reported status {handle.status!r}")

    waited = now - datetime.fromisoformat(run.first_seen)
    if waited > timeout:
        return PollDecision("fail", run.with_state(STATE_FAILED),
                            f"run {run.run_id} still pending after {waited.total_seconds() / 3600:.1f}h")
    status = handle.status if handle is not None else "unknown (status call failed)"
    return PollDecision("skip", run,
                        f"run {run.run_id} pending (status={status}, waited {waited.total_seconds() / 60:.0f} min)")


def _materialized_runs(context: SensorEvaluationContext, default_report_id: str) -> list[MaterializedRun]:
    """Run ids from the most recent request materializations, oldest first."""
    result = context.instance.fetch_materializations(LLM_REQUEST_ASSET_KEY, limit=RESILIENTLLM_MAX_TRACKED_RUNS)
    runs = []
    for record in result.records:
        metadata = record.asset_materialization.metadata
        run_entry = metadata.get("llm_run_id")
        if run_entry is None or not run_entry.value:
            continue
        report_entry = metadata.get("llm_report_id")
        report_id = str(report_entry.value) if report_entry is not None and report_entry.value else default_report_id
        runs.append(MaterializedRun(
            run_id=str(run_entry.value),
            report_id=report_id,
            materialized_at=datetime.fromtimestamp(record.timestamp, tz=timezone.utc),
        ))
    runs.reverse()
    return runs


def _notify_failure(context: SensorEvaluationContext, reason: str) -> None:
    logger = get_dagster_logger()
    text = (f":x: *ResilientLLM by-disease summary failed*\n{reason}\n"
            "The San Diego forecast summary was not published. Re-materialize "
            f"`{LLM_REQUEST_ASSET_KEY.to_user_string()}` to request a new run.")
    try:
        context.resources.slack.get_client().chat_postMessage(channel=SLACK_CHANNEL, text=text)
    except Exception as e:
        logger.error(f"Error sending Slack LLM failure alert: {e}")
   # send_email("[Dagster] ResilientLLM by-disease summary failed", text.replace("*", "").replace("`", ""))


@sensor(
    name="resilientllm_status_sensor",
    job=resilientllm_sd_disease_job,
    minimum_interval_seconds=RESILIENTLLM_POLL_SECONDS,
    required_resource_keys={"resilientllm", "slack"},
    description="Polls every in-flight ResilientLLM run and launches resilientllm_sd_disease for each one that is done.",
)
def resilientllm_status_sensor(context: SensorEvaluationContext):
    logger = get_dagster_logger()
    llm = context.resources.resilientllm
    now = datetime.now(timezone.utc)
    timeout = timedelta(hours=RESILIENTLLM_TIMEOUT_HOURS)

    cursor = PollCursor.load(context.cursor)
    materialized = _materialized_runs(context, llm.webhook_uuid)
    discoverable = {m.run_id for m in materialized}
    new_runs = discover(cursor, materialized)
    for run in new_runs:
        logger.info(f"Tracking new ResilientLLM run {run.run_id} (report {run.report_id})")

    pending = cursor.pending
    if not pending:
        cursor.prune(now, discoverable)
        context.update_cursor(cursor.dump())
        return SkipReason("no ResilientLLM runs in flight")

    run_requests = []
    reasons = []
    for run in pending:
        handle = None
        try:
            handle = llm.get_status(run.report_id, run.run_id)
        except Exception as e:
            logger.warning(f"ResilientLLM status call failed for {run.run_id}: {e}")

        decision = decide(run, handle, now, timeout)
        cursor.upsert(decision.run)
        reasons.append(decision.reason)

        if decision.action == "launch":
            logger.info(decision.reason)
            run_requests.append(RunRequest(
                run_key=run.run_id,
                run_config=RunConfig(ops={PARSE_OP_NAME: LLMRunConfig(run_id=run.run_id, report_id=run.report_id)}),
                tags={"llm_run_id": run.run_id},
            ))
        elif decision.action == "fail":
            logger.error(decision.reason)
            _notify_failure(context, decision.reason)

    cursor.prune(now, discoverable)
    context.update_cursor(cursor.dump())

    if run_requests:
        return run_requests
    return SkipReason("; ".join(reasons))
