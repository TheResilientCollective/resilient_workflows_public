"""Unit tests for the forecast freshness verdict and re-alert throttle."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sim.assets.forecast_freshness import (
    FORECAST_STALE_AFTER,
    forecast_staleness,
    should_realert,
)

NOW = datetime(2026, 10, 29, 12, 0, tzinfo=timezone.utc)


def test_recent_run_is_fresh():
    is_stale, age, _ = forecast_staleness(NOW - timedelta(days=6), NOW)
    assert not is_stale and age == 6


def test_exactly_at_threshold_is_fresh():
    is_stale, _, _ = forecast_staleness(NOW - FORECAST_STALE_AFTER, NOW)
    assert not is_stale


def test_beyond_threshold_is_stale():
    is_stale, age, reason = forecast_staleness(NOW - timedelta(days=8), NOW)
    assert is_stale and age == 8 and "beyond" in reason


def test_never_materialized_is_stale():
    is_stale, age, reason = forecast_staleness(None, NOW)
    assert is_stale and age is None and "never" in reason


def test_realert_when_no_prior_alert():
    assert should_realert(None, NOW, 24)


def test_no_realert_within_window():
    assert not should_realert(NOW - timedelta(hours=5), NOW, 24)


def test_realert_after_window():
    assert should_realert(NOW - timedelta(hours=25), NOW, 24)
