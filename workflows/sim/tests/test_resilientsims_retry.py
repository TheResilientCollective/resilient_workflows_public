"""Retry behaviour of ResilientSimsResource and the email-on-failure sensor."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from dagster import AssetKey

from sim.resources.resilientsims import ResilientSimsResource, is_transient_request_error


def make_resource(**overrides) -> ResilientSimsResource:
    params = dict(
        RESILIENTSIMS_USERNAME="u",
        RESILIENTSIMS_PASSWORD="p",
        RESILIENTSIMS_SIMULATOR_ID=1,
        RESILIENTSIMS_BUCKET="b",
        RESILIENTSIMS_RETRY_ATTEMPTS=3,
        RESILIENTSIMS_RETRY_FIRST_WAIT_SECONDS=0,
        RESILIENTSIMS_RETRY_MAX_WAIT_SECONDS=0,
    )
    params.update(overrides)
    return ResilientSimsResource(**params)


def test_dns_connection_error_is_transient():
    assert is_transient_request_error(requests.ConnectionError("Failed to resolve"))
    assert is_transient_request_error(requests.Timeout())
    resp = MagicMock(status_code=503)
    assert is_transient_request_error(requests.HTTPError(response=resp))
    resp404 = MagicMock(status_code=404)
    assert not is_transient_request_error(requests.HTTPError(response=resp404))
    assert not is_transient_request_error(ValueError("x"))


def test_create_configuration_retries_after_dns_failure_then_succeeds():
    res = make_resource()
    attempts = {"n": 0}

    def fake_auth():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise requests.ConnectionError("Name or service not known")

    response = MagicMock()
    response.json.return_value = {"id": 42}
    session = MagicMock()
    session.post.return_value = response

    with patch.object(ResilientSimsResource, "_authenticate", side_effect=fake_auth), \
         patch("sim.resources.resilientsims.requests.Session", return_value=session):
        result = res.create_configuration(1, {"a": 1})

    assert result == {"id": 42}
    assert attempts["n"] == 2  # first login failed, second attempt re-authenticated


def test_gives_up_after_configured_attempts():
    res = make_resource(RESILIENTSIMS_RETRY_ATTEMPTS=2)
    with patch.object(ResilientSimsResource, "_authenticate",
                      side_effect=requests.ConnectionError("dns")) as auth:
        with pytest.raises(requests.ConnectionError):
            res.list_simulators()
    assert auth.call_count == 2


def test_client_errors_are_not_retried():
    res = make_resource()
    response = MagicMock(status_code=400)
    response.raise_for_status.side_effect = requests.HTTPError(response=response)
    session = MagicMock()
    session.get.return_value = response
    with patch.object(ResilientSimsResource, "_authenticate", return_value=True), \
         patch("sim.resources.resilientsims.requests.Session", return_value=session):
        with pytest.raises(requests.HTTPError):
            res.list_simulators()
    assert session.get.call_count == 1


def _failure_context(step_key: str, asset_selection=None):
    ctx = MagicMock()
    ctx.dagster_run.run_id = "abc123"
    ctx.dagster_run.job_name = "__ASSET_JOB"
    ctx.dagster_run.asset_selection = asset_selection
    ctx.failure_event.message = "ConnectionError: dns"
    event = MagicMock()
    event.step_key = step_key
    ctx.get_step_failure_events.return_value = [event]
    return ctx


def test_email_sensor_sends_for_forecast_asset_with_run_link():
    from sim import definitions as d

    ctx = _failure_context("sandiego__sandiego_epidemiology_forecast")
    with patch.object(d, "send_email") as send:
        d.email_forecast_failure(ctx)
    send.assert_called_once()
    subject, body, html = send.call_args.args
    assert "sandiego/sandiego_epidemiology_forecast" in subject
    assert f"{d.WEBSERVER_BASE_URL}/runs/abc123" in body
    assert f"{d.WEBSERVER_BASE_URL}/runs/abc123" in html


def test_email_sensor_ignores_other_assets():
    from sim import definitions as d

    ctx = _failure_context("sandiego__something_else",
                           asset_selection={AssetKey(["sandiego", "something_else"])})
    with patch.object(d, "send_email") as send:
        d.email_forecast_failure(ctx)
    send.assert_not_called()


# ── S3 output discovery / asset metadata ──────────────────────────────────────

def _obj(name, size=0, is_dir=False):
    o = MagicMock()
    o.object_name = name
    o.size = size
    o.is_dir = is_dir
    return o


def test_find_run_directory_matches_run_id_suffix():
    from sim.assets.sandiego_epidemiology_forecasts import find_run_directory

    s3 = MagicMock()
    s3.listPath.return_value = [
        _obj("api_run/2026-11-01T01-00-00_run7/"),
        _obj("api_run/2026-11-10T01-00-00_run71/"),
        _obj("api_run/2026-11-11T01-00-00_run8/"),
    ]
    assert find_run_directory(s3, 7) == "api_run/2026-11-01T01-00-00_run7/"
    assert find_run_directory(s3, 99) is None


def test_list_run_files_builds_public_urls():
    from sim.assets.sandiego_epidemiology_forecasts import list_run_files

    s3 = MagicMock()
    s3.publicUrl.side_effect = lambda path, bucket=None: f"https://oss/{bucket}/{path}"
    s3.getClient.return_value.list_objects.return_value = [
        _obj("api_run/x_run7/", is_dir=True),
        _obj("api_run/x_run7/forecast.csv", size=12),
    ]
    files = list_run_files(s3, "api_run/x_run7/", bucket="resilientseasonal")
    assert files == [{
        "path": "api_run/x_run7/forecast.csv",
        "url": "https://oss/resilientseasonal/api_run/x_run7/forecast.csv",
        "size_bytes": 12,
    }]


def test_workflow_result_includes_duration():
    res = make_resource()
    with patch.object(ResilientSimsResource, "verify_simulator_exists", return_value={"id": 1, "name": "epi"}), \
         patch.object(ResilientSimsResource, "run_simulator", return_value={"id": 7}), \
         patch.object(ResilientSimsResource, "monitor_run_until_completion", return_value={"task_status": "SUCCESS"}):
        result = res.run_simulator_workflow(1, run_data={}, slack_resource=MagicMock())
    assert result["simulator_name"] == "epi"
    assert result["duration_seconds"] >= 0
    assert result["started_at"]


def test_forecast_asset_metadata_has_runtime_and_s3_links(monkeypatch):
    import sim.assets.sandiego_epidemiology_forecasts as m
    from dagster import build_asset_context

    sims = MagicMock()
    sims.RESILIENTSIMS_SIMULATOR_ID = 1
    sims.RESILIENTSIMS_BUCKET = "resilientseasonal"
    sims.create_configuration.return_value = {"id": 5}
    sims.run_simulator_workflow.return_value = {
        "simulator_name": "epi",
        "run_info": {"id": 7},
        "final_status": {"task_status": "SUCCESS"},
        "started_at": "2026-11-12T00:00:00+00:00",
        "duration_seconds": 90.0,
    }
    s3 = MagicMock()
    s3.publicUrl.side_effect = lambda path, bucket=None: f"https://oss/{bucket or 'test-bucket'}/{path}"
    s3.listPath.return_value = [_obj("api_run/2026-11-12T00-00-00_run7/")]
    s3.getClient.return_value.list_objects.return_value = [
        _obj("api_run/2026-11-12T00-00-00_run7/forecast.csv", size=3),
    ]
    monkeypatch.setattr(m.store_assets, "text_to_s3", lambda *a, **k: None)
    # templates are resolved from relative search paths; run from the sim package root
    monkeypatch.chdir(Path(m.__file__).resolve().parents[3])

    ctx = build_asset_context(resources={"resilientsims": sims, "s3": s3, "slack": MagicMock()})
    out = m.run_epidemic_simulation(ctx, {"date_path": "2026-11-12"})

    md = out.metadata
    assert md["runtime_seconds"].value == 90.0
    assert md["runtime_minutes"].value == 1.5
    assert md["s3_run_url"].value == "https://oss/resilientseasonal/api_run/2026-11-12T00-00-00_run7/"
    assert "forecast.csv" in md["s3_files"].value
    assert md["config_s3_url"].value.endswith("sims/configs/epi2/config_2026-11-12.yaml")
    assert out.value["run_dir"] == "api_run/2026-11-12T00-00-00_run7/"


def test_mpox_simulation_metadata_has_runtime_and_s3_links():
    import sim.assets.mpox_forecasts as m
    from dagster import build_asset_context

    sims = MagicMock()
    sims.RESILIENTSIMS_SIMULATOR_ID = 49
    sims.run_simulator_workflow.return_value = {
        "simulator_name": "mpox",
        "run_info": {"id": 3},
        "final_status": {"task_status": "SUCCESS"},
        "started_at": "2026-11-12T00:00:00+00:00",
        "duration_seconds": 120.0,
    }
    s3 = MagicMock()
    s3.publicUrl.side_effect = lambda path, bucket=None: f"https://oss/{bucket}/{path}"
    s3.listPath.return_value = [
        _obj("api_run/2026-11-11T00-00-00_run2/"),
        _obj("api_run/2026-11-12T00-00-00_run3/"),
    ]
    s3.getClient.return_value.list_objects.return_value = [
        _obj("api_run/2026-11-12T00-00-00_run3/out.json", size=5),
    ]
    ctx = build_asset_context(resources={"mpox_resilientsims": sims, "s3": s3, "slack": MagicMock()})
    out = m.run_mpox_simulation(ctx)

    assert out.value == "api_run/2026-11-12T00-00-00_run3/"
    md = out.metadata
    assert md["runtime_seconds"].value == 120.0
    assert md["s3_run_url"].value == "https://oss/resilientmpox/api_run/2026-11-12T00-00-00_run3/"
    assert "out.json" in md["s3_files"].value
    assert md["run_path"].value == out.value
