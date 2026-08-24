"""Tests for the SBIWTP treatment-deficit signal.

The deficit drives channel colouring on a public map, so the failure modes that
matter are the ones that would mislead a resident: a sign flip turning a healthy
surplus into apparent risk, a lag applied in the wrong direction, or a colour
index that pins to one end of its range.
"""

import numpy as np
import pandas as pd
import pytest

from tijuana.assets.effluent_deficit import (
    BASELINE_WINDOW_DAYS,
    DEFICIT_SATURATION_MGD,
    daily_series,
    deficit_index,
    effluent_deficit,
)


def _synthetic_daily(values: list[float], start: str = "2026-01-01") -> pd.Series:
    """SYNTHETIC_FIXTURE: a daily MGD series."""
    idx = pd.date_range(start, periods=len(values), freq="D", tz="Etc/GMT+8")
    return pd.Series(values, index=idx, name="flow_mgd")


def test_steady_flow_has_no_deficit():
    frame = effluent_deficit(_synthetic_daily([25.0] * 40))

    assert frame["deficit_mgd"].dropna().max() == pytest.approx(0.0)


def test_surplus_never_reads_as_risk():
    """A plant running *above* baseline must floor at zero, not go negative."""
    frame = effluent_deficit(_synthetic_daily([25.0] * 30 + [40.0] * 5))

    assert frame["deficit_mgd"].dropna().min() >= 0.0
    assert frame["deficit_mgd"].dropna().max() == pytest.approx(0.0)
    # The anomaly, unlike the deficit, keeps its sign so a surplus stays visible.
    assert frame["anomaly"].dropna().max() > 0


def test_a_shortfall_produces_a_deficit_of_the_right_size():
    # 30 steady days at 30 MGD, a drop to 10, then one more day so the lagged
    # drop has a row to surface on.
    frame = effluent_deficit(_synthetic_daily([30.0] * 30 + [10.0, 30.0]))

    row = frame.iloc[31]
    assert row["flow_mgd"] == pytest.approx(10.0)
    # The rolling window immediately precedes the row, so it already contains
    # the drop day; the baseline is pulled slightly below the steady 30 rather
    # than sitting exactly on it.
    assert 29.0 < row["baseline_mgd"] < 30.0
    assert row["deficit_mgd"] == pytest.approx(row["baseline_mgd"] - row["flow_mgd"])
    assert row["deficit_mgd"] > 19.0
    assert row["anomaly"] == pytest.approx(
        (row["flow_mgd"] - row["baseline_mgd"]) / row["baseline_mgd"]
    )


def test_the_deficit_is_lagged_by_a_day():
    """Today's published deficit describes yesterday's plant, per the model feature."""
    frame = effluent_deficit(_synthetic_daily([30.0] * 30 + [10.0, 30.0]))

    # The drop happened on day 31; it must surface on day 32's row.
    assert frame["flow_mgd"].iloc[30] == pytest.approx(30.0)
    assert frame["flow_mgd"].iloc[31] == pytest.approx(10.0)
    assert frame["deficit_mgd"].iloc[31] > 0


def test_first_day_has_no_baseline_to_compare_against():
    frame = effluent_deficit(_synthetic_daily([25.0] * 5))

    assert np.isnan(frame["flow_mgd"].iloc[0])
    assert np.isnan(frame["deficit_mgd"].iloc[0])


def test_zero_flow_does_not_produce_an_infinite_anomaly():
    frame = effluent_deficit(_synthetic_daily([0.0, 0.0, 0.0]))

    assert not np.isinf(frame["anomaly"]).any()


def test_empty_input_raises_rather_than_inventing_a_baseline():
    with pytest.raises(ValueError):
        effluent_deficit(pd.Series(dtype=float))


def test_deficit_index_spans_the_usable_range():
    idx = deficit_index(pd.Series([0.0, DEFICIT_SATURATION_MGD / 2, DEFICIT_SATURATION_MGD]))

    assert list(idx) == pytest.approx([0.0, 0.5, 1.0])


def test_deficit_index_clamps_beyond_saturation():
    idx = deficit_index(pd.Series([DEFICIT_SATURATION_MGD * 3]))

    assert idx.iloc[0] == pytest.approx(1.0)


def test_saturation_is_reachable_by_the_observed_record():
    """A saturation no day can reach would render every day pale.

    The published record (2020-2026) tops out at 14.51 MGD, so the constant must
    sit below that or the colour ramp wastes its upper half.
    """
    assert 0 < DEFICIT_SATURATION_MGD < 14.51


def test_deficit_index_rejects_a_nonsense_saturation():
    with pytest.raises(ValueError):
        deficit_index(pd.Series([1.0]), saturation=0)


def test_daily_series_reads_the_portal_export_shape():
    """SYNTHETIC_FIXTURE: the IBWC portal's column naming, two readings a day."""
    raw = pd.DataFrame({
        "Timestamp (UTC-08:00)": [
            "2026-01-01 00:00:00", "2026-01-01 12:00:00",
            "2026-01-02 00:00:00", "2026-01-02 12:00:00",
        ],
        "Value (M US Gal/d)": [20.0, 30.0, 10.0, 10.0],
        "Unnamed: 2": [-1, -1, -1, -1],
    })

    daily = daily_series(raw)

    assert len(daily) == 2
    assert daily.iloc[0] == pytest.approx(25.0)   # mean of the day's readings
    assert daily.iloc[1] == pytest.approx(10.0)


def test_daily_series_treats_the_stamp_as_a_fixed_offset():
    """`UTC-08:00` is a fixed offset year-round, not a Pacific local clock.

    Localising to America/Los_Angeles would shift summer readings by an hour and
    smear values across the date boundary.
    """
    raw = pd.DataFrame({
        "Timestamp (UTC-08:00)": ["2026-07-01 23:30:00"],
        "Value (M US Gal/d)": [25.0],
    })

    daily = daily_series(raw)

    assert str(daily.index[0].tz) in {"Etc/GMT+8", "UTC-08:00"}
    assert daily.index[0].strftime("%Y-%m-%d") == "2026-07-01"


def test_daily_series_rejects_a_frame_it_cannot_read():
    with pytest.raises(ValueError, match="timestamp/value"):
        daily_series(pd.DataFrame({"when": ["2026-01-01"], "how_much": [1.0]}))

    with pytest.raises(ValueError, match="empty"):
        daily_series(pd.DataFrame())


def test_baseline_window_matches_the_model_feature():
    """Drift here silently desynchronises the map from the forecast models."""
    assert BASELINE_WINDOW_DAYS == 30
