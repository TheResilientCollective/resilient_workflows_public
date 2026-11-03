"""End-to-end tests of the asynchronous ResilientLLM by-disease flow.

These run the real request asset, status sensor and parse asset against the
real ``ResilientLLMResource`` with only HTTP, S3 writes, email and the Netlify
deploy stubbed. They exist to catch call-signature drift between the three
pieces, which pure-function tests cannot see.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from dagster import (
    AssetMaterialization,
    DagsterInstance,
    build_asset_context,
    build_sensor_context,
)

from sim.assets import resilientllm_status as status_mod
from sim.assets import sandiego_epidemiology_forecasts as sd
from sim.assets.resilientllm_status import PARSE_OP_NAME, PollCursor, resilientllm_status_sensor
from sim.definitions import defs
from sim.resources.resilientllm import ResilientLLMResource

REPORT_ID = "report-uuid"
RUN_URL = f"https://agents.example.org/webhook/{REPORT_ID}/v3"
STATUS_URL = f"{RUN_URL}/status"
LLM_DATA = {"en": {"summary": "ok"}, "es": {"summary": "bien"}}


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class FakeSlack:
    def __init__(self):
        self.messages = []

    def get_client(self):
        outer = self

        class Client:
            def chat_postMessage(self, **kw):
                outer.messages.append(kw)

        return Client()


class FakeS3:
    S3_BUCKET = "test-bucket"


class FakeAPI:
    """Records every POST and answers like the ResilientLLM webhook."""

    def __init__(self, statuses: dict[str, str]):
        self.statuses = statuses
        self.posts = []
        self.next_id = iter(statuses)

    def post(self, url, headers=None, json=None, timeout=None):
        self.posts.append((url, json))
        assert headers["Authorization"] == "Bearer test-token"
        if url == STATUS_URL:
            run_id = json["id"]
            status = self.statuses[run_id]
            return FakeResponse({"id": run_id, "status": status, "data": LLM_DATA if status == "done" else None})
        assert url == RUN_URL
        return FakeResponse([{"_id": next(self.next_id), "status": "pending", "data": None}])


@pytest.fixture
def llm():
    return ResilientLLMResource(token="test-token", llm_endpoint="https://agents.example.org/webhook/",
                                webhook_uuid=REPORT_ID, poll_interval_seconds=0, timeout_seconds=1)


@pytest.fixture
def resources(llm):
    return {"resilientllm": llm, "slack": FakeSlack(), "s3": FakeS3()}


# ------------------------------------------------------------- request asset

def test_request_asset_submits_and_records_run_and_report_id(resources):
    api = FakeAPI({"run-1": "pending"})
    with patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         patch.object(sd.store_assets, "text_to_s3") as to_s3, \
         patch.object(sd, "send_email"):
        out = sd.resilientllm_by_disease_request(build_asset_context(resources=resources))

    assert api.posts == [(RUN_URL, sd.DISEASES_CONFIG)]  # submit only, no polling
    assert out.metadata["llm_run_id"].value == "run-1"
    assert out.metadata["llm_report_id"].value == REPORT_ID
    assert out.value["id"] == "run-1" and out.value["report_id"] == REPORT_ID
    assert to_s3.call_count == 1


# --------------------------------------------------------------- parse asset

def test_parse_asset_fetches_finished_run_by_report_and_run_id(resources):
    api = FakeAPI({"run-1": "done"})
    with patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         patch.object(sd.store_assets, "text_to_s3") as to_s3, \
         patch.object(sd, "trigger_deploy"):
        out = sd.resilientllm_by_disease_asset(
            build_asset_context(resources=resources),
            config=sd.LLMRunConfig(run_id="run-1", report_id=REPORT_ID),
        )

    assert api.posts == [(STATUS_URL, {"id": "run-1"})]
    assert out.metadata["llm_run_id"].value == "run-1"
    assert sd.json.loads(out.value) == LLM_DATA
    assert to_s3.call_count == 2  # dated + latest


def test_parse_asset_defaults_report_id_to_resource_webhook(resources):
    api = FakeAPI({"run-1": "done"})
    with patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         patch.object(sd.store_assets, "text_to_s3"), patch.object(sd, "trigger_deploy"):
        sd.resilientllm_by_disease_asset(build_asset_context(resources=resources),
                                         config=sd.LLMRunConfig(run_id="run-1"))
    assert api.posts[0][0] == STATUS_URL


def test_parse_asset_refuses_unfinished_run(resources):
    api = FakeAPI({"run-1": "running"})
    with patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         pytest.raises(Exception, match="not done"):
        sd.resilientllm_by_disease_asset(build_asset_context(resources=resources),
                                         config=sd.LLMRunConfig(run_id="run-1", report_id=REPORT_ID))
    assert resources["slack"].messages  # failure was reported


# -------------------------------------------------------------------- sensor

def _record_request(instance, run_id, report_id=REPORT_ID):
    """Mimic what the request asset leaves behind in the event log."""
    instance.report_runless_asset_event(AssetMaterialization(
        asset_key=sd.LLM_REQUEST_ASSET_KEY,
        metadata={"llm_run_id": run_id, "llm_report_id": report_id},
    ))


def _tick(instance, cursor, resources):
    ctx = build_sensor_context(instance=instance, cursor=cursor, resources=resources)
    return resilientllm_status_sensor.evaluate_tick(ctx)


def test_sensor_tracks_two_runs_and_launches_each_once(resources):
    api = FakeAPI({"run-1": "running", "run-2": "running"})
    with DagsterInstance.ephemeral() as instance, \
         patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         patch.object(status_mod, "send_email"):
        _record_request(instance, "run-1")
        _record_request(instance, "run-2")

        res = _tick(instance, None, resources)
        assert not res.run_requests
        assert [(r.run_id, r.state) for r in PollCursor.load(res.cursor).runs] == \
               [("run-1", "polling"), ("run-2", "polling")]
        assert sorted(api.posts, key=lambda p: p[1]["id"]) == [(STATUS_URL, {"id": "run-1"}), (STATUS_URL, {"id": "run-2"})]

        api.statuses["run-1"] = "done"
        res = _tick(instance, res.cursor, resources)
        assert [r.run_key for r in res.run_requests] == ["run-1"]
        assert res.run_requests[0].run_config["ops"] == {
            PARSE_OP_NAME: {"config": {"run_id": "run-1", "report_id": REPORT_ID}}
        }

        res = _tick(instance, res.cursor, resources)  # run-1 is not launched again
        assert not res.run_requests and "run-2" in res.skip_message

        api.statuses["run-2"] = "error"
        res = _tick(instance, res.cursor, resources)
        assert not res.run_requests
        assert [(r.run_id, r.state) for r in PollCursor.load(res.cursor).runs] == \
               [("run-1", "launched"), ("run-2", "failed")]
        assert any("run-2" in m["text"] for m in resources["slack"].messages)

        res = _tick(instance, res.cursor, resources)
        assert res.skip_message == "no ResilientLLM runs in flight"


def test_sensor_run_request_executes_the_parse_job(resources):
    """The config the sensor emits must be accepted by the job it targets."""
    api = FakeAPI({"run-1": "done"})
    with DagsterInstance.ephemeral() as instance, \
         patch("sim.resources.resilientllm.requests.post", side_effect=api.post), \
         patch.object(sd.store_assets, "text_to_s3"), patch.object(sd, "trigger_deploy"):
        _record_request(instance, "run-1")
        res = _tick(instance, None, resources)
        (request,) = res.run_requests

        job = defs.get_repository_def().get_job(resilientllm_status_sensor.job.name)
        result = job.execute_in_process(run_config=request.run_config, resources=resources, instance=instance)

    assert result.success
    mats = result.asset_materializations_for_node(PARSE_OP_NAME)
    assert mats[0].metadata["llm_run_id"].value == "run-1"
