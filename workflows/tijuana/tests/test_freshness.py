"""Unit tests for the two-tier freshness verdict."""

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_UTILS = Path(__file__).resolve().parents[1] / "src" / "tijuana" / "utils"
_PKG = "_tijuana_utils_under_test"
if _PKG not in sys.modules:
    _pkg = types.ModuleType(_PKG)
    _pkg.__path__ = [str(_UTILS)]
    sys.modules[_PKG] = _pkg

_spec = importlib.util.spec_from_file_location(f"{_PKG}.freshness", _UTILS / "freshness.py")
fr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fr)

WARN, FAIL = 10, 21


def verdict(age):
    return fr.freshness_verdict(age, WARN, FAIL)


def test_normal_publication_lag_passes():
    """The observed source lag of ~6.6 days must not fail the check."""
    passed, is_error, _ = verdict(6.6)
    assert passed and not is_error


def test_boundary_of_normal_lag_passes():
    passed, is_error, _ = verdict(WARN)
    assert passed and not is_error


def test_a_skipped_publication_warns_but_does_not_error():
    passed, is_error, reason = verdict(14.0)
    assert not passed
    assert not is_error
    assert "under the 21-day" in reason


def test_boundary_of_the_failure_threshold_still_warns():
    passed, is_error, _ = verdict(FAIL)
    assert not passed and not is_error


def test_a_stopped_feed_errors():
    passed, is_error, reason = verdict(30.0)
    assert not passed
    assert is_error
    assert "likely stopped" in reason


def test_fresh_data_passes():
    passed, is_error, _ = verdict(0.2)
    assert passed and not is_error


def test_thresholds_must_be_ordered():
    with pytest.raises(ValueError, match="below warn_after_days"):
        fr.freshness_verdict(1.0, 21, 10)


def test_reason_always_reports_the_age():
    for age in (0.5, 12.0, 40.0):
        assert f"{age:.1f} days old" in verdict(age)[2]
