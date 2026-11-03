"""Unit tests for the ResilientLLM polling state machine, run discovery and cursor."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from sim.assets.resilientllm_status import (
    STATE_FAILED,
    STATE_LAUNCHED,
    STATE_POLLING,
    MaterializedRun,
    PollCursor,
    TrackedRun,
    decide,
    discover,
)
from sim.resources.resilientllm import LLMRunHandle, ResilientLLMError

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)
TIMEOUT = timedelta(hours=6)


def handle(status, data=None):
    return LLMRunHandle(id="run1", status=status, data=data)


def tracked(run_id="run1", state=STATE_POLLING, first_seen=NOW):
    return TrackedRun(run_id, "report", state, first_seen.isoformat())


# ---------------------------------------------------------------- run handle

def test_handle_prefers_id_over_underscore_id():
    h = LLMRunHandle.from_response({"status": "pending", "_id": "a", "id": "b"})
    assert h.id == "b" and h.is_pending


def test_handle_falls_back_to_underscore_id():
    assert LLMRunHandle.from_response({"status": "done", "_id": "a"}).id == "a"


def test_handle_unwraps_single_element_list():
    assert LLMRunHandle.from_response([{"status": "done", "id": "a"}]).id == "a"


def test_handle_requires_an_id():
    with pytest.raises(ResilientLLMError):
        LLMRunHandle.from_response({"status": "done"})


def test_handle_error_statuses():
    assert LLMRunHandle(id="a", status="error").is_error
    assert LLMRunHandle(id="a", status="Failed").is_error
    assert not LLMRunHandle(id="a", status="working").is_error


# ------------------------------------------------------------------ decide

def test_pending_keeps_polling():
    d = decide(tracked(), handle("pending"), NOW, TIMEOUT)
    assert d.action == "skip" and d.run.state == STATE_POLLING


def test_done_with_data_launches_once():
    d = decide(tracked(), handle("done", {"en": {}}), NOW, TIMEOUT)
    assert d.action == "launch" and d.run.state == STATE_LAUNCHED
    assert decide(d.run, handle("done", {"en": {}}), NOW, TIMEOUT).action == "skip"


def test_done_without_data_fails():
    d = decide(tracked(), handle("done", None), NOW, TIMEOUT)
    assert d.action == "fail" and d.run.state == STATE_FAILED


def test_error_status_fails_and_stops():
    d = decide(tracked(), handle("error"), NOW, TIMEOUT)
    assert d.action == "fail"
    assert decide(d.run, handle("error"), NOW, TIMEOUT).action == "skip"


def test_timeout_fails():
    d = decide(tracked(first_seen=NOW - timedelta(hours=7)), handle("pending"), NOW, TIMEOUT)
    assert d.action == "fail" and "still pending" in d.reason


def test_status_call_failure_counts_as_pending():
    d = decide(tracked(), None, NOW, TIMEOUT)
    assert d.action == "skip" and d.run.state == STATE_POLLING


# ---------------------------------------------------------------- discover

def test_discover_adds_unseen_runs_only():
    cursor = PollCursor([tracked("run1", STATE_LAUNCHED)])
    new = discover(cursor, [
        MaterializedRun("run1", "report", NOW),
        MaterializedRun("run2", "report", NOW),
        MaterializedRun("run3", "other-report", NOW + timedelta(days=7)),
    ])
    assert [r.run_id for r in new] == ["run2", "run3"]
    assert new[1].report_id == "other-report"
    assert all(r.state == STATE_POLLING for r in new)
    assert cursor.get("run1").state == STATE_LAUNCHED  # untouched
    assert [r.run_id for r in cursor.pending] == ["run2", "run3"]


def test_two_runs_in_flight_progress_independently():
    cursor = PollCursor()
    discover(cursor, [MaterializedRun("run1", "r", NOW), MaterializedRun("run2", "r", NOW + timedelta(days=7))])
    now = NOW + timedelta(days=7, hours=1)
    d1 = decide(cursor.get("run1"), LLMRunHandle(id="run1", status="done", data={"en": {}}), now, TIMEOUT)
    d2 = decide(cursor.get("run2"), LLMRunHandle(id="run2", status="working"), now, TIMEOUT)
    cursor.upsert(d1.run)
    cursor.upsert(d2.run)
    assert d1.action == "launch" and d2.action == "skip"
    assert [r.run_id for r in cursor.pending] == ["run2"]


# ------------------------------------------------------------------ cursor

def test_cursor_roundtrip():
    cursor = PollCursor([tracked("run1"), tracked("run2", STATE_FAILED)])
    assert PollCursor.load(cursor.dump()) == cursor
    assert PollCursor.load(None) == PollCursor()


def test_cursor_loads_legacy_single_record():
    legacy = json.dumps({"run_id": "run1", "state": STATE_POLLING, "first_seen": NOW.isoformat()})
    cursor = PollCursor.load(legacy)
    assert [r.run_id for r in cursor.runs] == ["run1"]
    assert cursor.runs[0].report_id == ""


def test_prune_drops_old_finished_runs_but_keeps_pending():
    old = NOW - timedelta(days=30)
    cursor = PollCursor([
        tracked("old-done", STATE_LAUNCHED, old),
        tracked("old-pending", STATE_POLLING, old),
        tracked("new-done", STATE_LAUNCHED, NOW),
    ])
    cursor.prune(NOW)
    assert sorted(r.run_id for r in cursor.runs) == ["new-done", "old-pending"]


def test_prune_keeps_old_finished_runs_still_in_discovery_window():
    old = NOW - timedelta(days=30)
    cursor = PollCursor([
        tracked("old-launched", STATE_LAUNCHED, old),
        tracked("old-failed", STATE_FAILED, old),
        tracked("old-gone", STATE_LAUNCHED, old),
    ])
    cursor.prune(NOW, discoverable={"old-launched", "old-failed"})
    assert sorted(r.run_id for r in cursor.runs) == ["old-failed", "old-launched"]


def test_pruned_run_is_not_rediscovered_while_materialization_is_visible():
    """Weekly requests stay inside the 20-record window far longer than 14 days."""
    old = NOW - timedelta(days=30)
    materialized = [MaterializedRun("run1", "report", old)]
    cursor = PollCursor([tracked("run1", STATE_LAUNCHED, old)])

    cursor.prune(NOW, {m.run_id for m in materialized})
    assert discover(cursor, materialized) == []
    assert cursor.get("run1").state == STATE_LAUNCHED
