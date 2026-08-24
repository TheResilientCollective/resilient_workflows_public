"""Tests for the wind-grid geometry and the speed/direction to u/v conversion.

The conversion is the kind of code that fails silently: a sign error still
produces a plausible-looking arrow field, just one pointing the wrong way. These
tests pin the meteorological convention (direction is where the wind comes
*from*) and the scan order the published JSON promises in its header.
"""

import math

import pandas as pd
import pytest

from tijuana.assets.wind_grid import (
    GRID_EAST,
    GRID_NORTH,
    GRID_NX,
    GRID_NY,
    GRID_SOUTH,
    GRID_WEST,
    grid_points,
    uv_grid_json,
    wind_to_uv,
)


def test_grid_covers_the_declared_bounds():
    points = grid_points()

    assert len(points) == GRID_NX * GRID_NY
    assert points["lon"].min() == pytest.approx(GRID_WEST)
    assert points["lon"].max() == pytest.approx(GRID_EAST)
    assert points["lat"].min() == pytest.approx(GRID_SOUTH)
    assert points["lat"].max() == pytest.approx(GRID_NORTH)


def test_grid_scans_northwest_first_east_then_south():
    """The published header claims this scan order; velocity layers rely on it."""
    points = grid_points(nx=3, ny=2)

    first = points.iloc[0]
    assert first["lat"] == pytest.approx(GRID_NORTH)
    assert first["lon"] == pytest.approx(GRID_WEST)

    # First row runs west to east at the northern edge.
    assert points.iloc[2]["lon"] == pytest.approx(GRID_EAST)
    assert points.iloc[2]["lat"] == pytest.approx(GRID_NORTH)

    # Second row steps south, back to the western edge.
    assert points.iloc[3]["lat"] == pytest.approx(GRID_SOUTH)
    assert points.iloc[3]["lon"] == pytest.approx(GRID_WEST)

    last = points.iloc[-1]
    assert last["lat"] == pytest.approx(GRID_SOUTH)
    assert last["lon"] == pytest.approx(GRID_EAST)


def test_grid_rejects_a_degenerate_shape():
    with pytest.raises(ValueError):
        grid_points(nx=1, ny=4)


@pytest.mark.parametrize(
    "direction_deg, expected_u, expected_v",
    [
        # A northerly (from the north) pushes air southward: v negative.
        (0.0, 0.0, -10.0),
        # An easterly (from the east) pushes air westward: u negative.
        (90.0, -10.0, 0.0),
        # A southerly pushes air northward.
        (180.0, 0.0, 10.0),
        # A westerly pushes air eastward — the sea breeze that ventilates the valley.
        (270.0, 10.0, 0.0),
    ],
)
def test_wind_direction_is_the_direction_it_blows_from(direction_deg, expected_u, expected_v):
    u, v = wind_to_uv(10.0, direction_deg)

    assert u == pytest.approx(expected_u, abs=1e-9)
    assert v == pytest.approx(expected_v, abs=1e-9)


def test_uv_magnitude_matches_wind_speed():
    u, v = wind_to_uv(7.5, 217.0)

    assert math.hypot(u, v) == pytest.approx(7.5)


def test_missing_wind_is_null_not_calm():
    """A missing value must never become a zero — a calm night is a real signal."""
    assert wind_to_uv(None, 180.0) == (None, None)
    assert wind_to_uv(5.0, None) == (None, None)
    assert wind_to_uv(float("nan"), 180.0) == (None, None)
    assert wind_to_uv(5.0, float("nan")) == (None, None)


def _synthetic_frame(nx: int, ny: int) -> pd.DataFrame:
    """SYNTHETIC_FIXTURE: a uniform westerly over a small grid."""
    points = grid_points(nx=nx, ny=ny)
    u, v = wind_to_uv(4.0, 270.0)
    return points.assign(u=u, v=v)


def test_uv_grid_json_emits_two_records_with_matching_headers():
    ref = pd.Timestamp("2026-08-23T12:00:00Z").to_pydatetime()
    records = uv_grid_json(_synthetic_frame(GRID_NX, GRID_NY), ref, ref)

    assert [r["header"]["parameterNumber"] for r in records] == [2, 3]
    assert [r["header"]["parameterNumberName"] for r in records] == [
        "eastward_wind",
        "northward_wind",
    ]

    for record in records:
        header = record["header"]
        assert header["nx"] * header["ny"] == len(record["data"])
        # Scan starts at the northwest corner.
        assert header["la1"] == pytest.approx(GRID_NORTH)
        assert header["lo1"] == pytest.approx(GRID_WEST)
        assert header["refTime"] == "2026-08-23T12:00:00Z"

    eastward, northward = records
    assert eastward["data"][0] == pytest.approx(4.0)
    assert northward["data"][0] == pytest.approx(0.0)


def test_uv_grid_json_rejects_a_frame_of_the_wrong_size():
    ref = pd.Timestamp("2026-08-23T12:00:00Z").to_pydatetime()
    short = _synthetic_frame(GRID_NX, GRID_NY).iloc[:-1]

    with pytest.raises(ValueError, match="grid nodes"):
        uv_grid_json(short, ref, ref)
