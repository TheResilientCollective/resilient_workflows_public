"""Tests for S3 monitoring sensor."""

import pytest
from datetime import datetime
from epi_forecast.sensors.s3_monitor import parse_run_id, S3RunInfo


def test_parse_run_id_valid():
    """Test parsing a valid run ID."""
    run_datetime, run_id = parse_run_id("2025-09-07T00-07-25_run15")

    assert run_datetime is not None
    assert run_datetime.year == 2025
    assert run_datetime.month == 9
    assert run_datetime.day == 7
    assert run_id == "run15"


def test_parse_run_id_with_path():
    """Test parsing run ID from a full path."""
    run_datetime, run_id = parse_run_id("api_run/2025-01-15T10-30-45_run42/")

    assert run_datetime is not None
    assert run_datetime.year == 2025
    assert run_datetime.month == 1
    assert run_id == "run42"


def test_parse_run_id_invalid():
    """Test parsing an invalid run ID format."""
    run_datetime, run_id = parse_run_id("invalid_format")

    assert run_datetime is None
    assert run_id == "invalid_format"


def test_parse_run_id_partial():
    """Test parsing a partially valid format."""
    run_datetime, run_id = parse_run_id("some_prefix_run99")

    assert run_datetime is None
    assert run_id == "some_prefix_run99"


def test_s3_run_info_dataclass():
    """Test S3RunInfo dataclass."""
    run_info = S3RunInfo(
        run_path="api_run/2025-01-15T10-30-45_run42/",
        run_datetime=datetime(2025, 1, 15, 10, 30, 45),
        run_id="run42",
        files_found=5,
        bucket="test-bucket",
    )

    assert run_info.run_path == "api_run/2025-01-15T10-30-45_run42/"
    assert run_info.files_found == 5
    assert run_info.bucket == "test-bucket"
