from unittest.mock import patch

import pytest

from sim.resources.resilientllm import ResilientLLMError, ResilientLLMResource


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


def make_resource(**overrides):
    kwargs = dict(
        token="t",
        llm_endpoint="https://agents.example.org/webhook/",
        poll_interval_seconds=0,
        timeout_seconds=5,
    )
    kwargs.update(overrides)
    return ResilientLLMResource(**kwargs)


def test_endpoints():
    r = make_resource()
    assert r.run_endpoint("abc") == "https://agents.example.org/webhook/abc/v3"
    assert r.status_endpoint("abc") == "https://agents.example.org/webhook/abc/v3/status"


def test_execute_with_data_polls_until_done():
    r = make_resource()
    statuses = iter([
        {"_id": "run1", "status": "generating report for each disease", "data": {"en": {"partial": True}}},
        {"_id": "run1", "status": "done", "data": {"en": {"summary": "ok"}, "es": {"summary": "bien"}}},
    ])
    posted = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posted.append((url, json))
        if url.endswith("/status"):
            return FakeResponse(next(statuses))
        return FakeResponse([{"status": "started", "data": None, "_id": "run1", "id": "run1"}])

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post), \
         patch("sim.resources.resilientllm.time.sleep"):
        result = r.execute_with_data("abc", {"covid": []})

    assert result == {"en": {"summary": "ok"}, "es": {"summary": "bien"}}
    assert posted[0] == ("https://agents.example.org/webhook/abc/v3", {"covid": []})
    assert posted[1] == ("https://agents.example.org/webhook/abc/v3/status", {"id": "run1"})
    assert len(posted) == 3


def test_data_before_done_is_ignored():
    """A non-null data field must not short-circuit polling; only status=='done' does."""
    r = make_resource()
    statuses = iter([
        {"_id": "run1", "status": "working", "data": {"en": {"summary": "partial"}}},
        {"_id": "run1", "status": "done", "data": {"en": {"summary": "final"}}},
    ])

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/status"):
            return FakeResponse(next(statuses))
        return FakeResponse({"id": "run1", "status": "started", "data": None})

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post), \
         patch("sim.resources.resilientllm.time.sleep"):
        assert r.execute_with_data("abc", {})["en"]["summary"] == "final"


def test_failed_status_raises():
    r = make_resource()

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/status"):
            return FakeResponse({"_id": "run1", "status": "error", "data": None})
        return FakeResponse([{"id": "run1", "status": "started", "data": None}])

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post), \
         patch("sim.resources.resilientllm.time.sleep"), \
         pytest.raises(ResilientLLMError, match="failed"):
        r.execute_with_data("abc", {})


def test_timeout_raises():
    r = make_resource(timeout_seconds=0)

    def fake_post(url, headers=None, json=None, timeout=None):
        if url.endswith("/status"):
            return FakeResponse({"_id": "run1", "status": "working", "data": None})
        return FakeResponse([{"id": "run1", "status": "started", "data": None}])

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post), \
         patch("sim.resources.resilientllm.time.sleep"), \
         pytest.raises(ResilientLLMError, match="did not reach"):
        r.execute_with_data("abc", {})


def test_missing_run_id_raises():
    r = make_resource()
    with patch("sim.resources.resilientllm.requests.post", return_value=FakeResponse([{"status": "started"}])), \
         pytest.raises(ResilientLLMError, match="run id"):
        r.submit("abc", {})


def test_submit_returns_handle_without_polling():
    r = make_resource()
    posted = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posted.append(url)
        return FakeResponse({"_id": "run9", "status": "started", "data": None})

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post):
        handle = r.submit("abc", {"covid": []})

    assert handle.id == "run9" and handle.is_pending and handle.data is None
    assert posted == ["https://agents.example.org/webhook/abc/v3"]


def test_submit_requires_report_id():
    r = make_resource()
    with pytest.raises(ResilientLLMError, match="report id"):
        r.submit("", {})


def test_get_status_posts_run_id():
    r = make_resource()
    posted = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posted.append((url, json))
        return FakeResponse([{"id": "run9", "status": "done", "data": {"en": {"summary": "ok"}}}])

    with patch("sim.resources.resilientllm.requests.post", side_effect=fake_post):
        handle = r.get_status("abc", "run9")

    assert handle.is_done and handle.data == {"en": {"summary": "ok"}}
    assert posted == [("https://agents.example.org/webhook/abc/v3/status", {"id": "run9"})]
