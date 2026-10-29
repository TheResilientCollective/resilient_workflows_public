"""Unit tests for the ResilientLLM polling state machine and run-handle parsing."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from sim.assets.resilientllm_status import (
    STATE_FAILED,
    STATE_LAUNCHED,
    STATE_POLLING,
    PollCursor,
    decide,
)
from sim.resources.resilientllm import LLMRunHandle

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
TIMEOUT = timedelta(hours=6)


def handle(status, data=None):
    return LLMRunHandle(id="run1", status=status, data=data)


def test_handle_prefers_id_over_underscore_id():
    h = LLMRunHandle.from_response({"status": "pending", "_id": "a", "id": "b"})
    assert h.id == "b" and h.is_pending


def test_handle_falls_back_to_underscore_id():
    assert LLMRunHandle.from_response({"status": "done", "_id": "a"}).id == "a"


def test_handle_requires_an_id():
    with pytest.raises(ValueError):
        LLMRunHandle.from_response({"status": "done"})


def test_first_tick_pending_starts_polling():
    d = decide("run1", handle("pending"), None, NOW, TIMEOUT)
    assert d.action == "skip"
    assert d.cursor.state == STATE_POLLING and d.cursor.run_id == "run1"


def test_done_with_data_launches_once():
    cur = PollCursor("run1", STATE_POLLING, NOW.isoformat())
    d = decide("run1", handle("done", {"en": {}}), cur, NOW, TIMEOUT)
    assert d.action == "launch" and d.cursor.state == STATE_LAUNCHED
    again = decide("run1", handle("done", {"en": {}}), d.cursor, NOW, TIMEOUT)
    assert again.action == "skip"


def test_done_without_data_fails():
    cur = PollCursor("run1", STATE_POLLING, NOW.isoformat())
    d = decide("run1", handle("done", None), cur, NOW, TIMEOUT)
    assert d.action == "fail" and d.cursor.state == STATE_FAILED


def test_error_status_fails_and_stops():
    cur = PollCursor("run1", STATE_POLLING, NOW.isoformat())
    d = decide("run1", handle("error"), cur, NOW, TIMEOUT)
    assert d.action == "fail"
    assert decide("run1", handle("error"), d.cursor, NOW, TIMEOUT).action == "skip"


def test_timeout_fails():
    cur = PollCursor("run1", STATE_POLLING, (NOW - timedelta(hours=7)).isoformat())
    d = decide("run1", handle("pending"), cur, NOW, TIMEOUT)
    assert d.action == "fail" and "still pending" in d.reason


def test_status_call_failure_counts_as_pending():
    cur = PollCursor("run1", STATE_POLLING, NOW.isoformat())
    d = decide("run1", None, cur, NOW, TIMEOUT)
    assert d.action == "skip" and d.cursor.state == STATE_POLLING


def test_new_run_id_resets_finished_cursor():
    cur = PollCursor("run1", STATE_LAUNCHED, NOW.isoformat())
    d = decide("run2", handle("pending"), cur, NOW, TIMEOUT)
    assert d.action == "skip" and d.cursor.run_id == "run2" and d.cursor.state == STATE_POLLING


def test_cursor_roundtrip():
    cur = PollCursor("run1", STATE_POLLING, NOW.isoformat())
    assert PollCursor.load(cur.dump()) == cur
    assert PollCursor.load(None) is None
