"""Tests for the SBIWTP plant balance.

Three measures feed a public dataset and candidate H2S model features, so the
failures that matter are the ones that would state the wrong thing about the
plant: a sign flip on the influent/effluent difference, a capacity flag that
fires on the wrong side of the rating (or on a day whose flow is unknown), a
unit slip on the border gauge, or a lag applied in the wrong direction.
"""

import logging

import numpy as np
import pandas as pd
import pytest

from tijuana.assets.plant_balance import (
    CMS_TO_MGD,
    LAG_DAYS,
    PLANT_BALANCE_FEATURE_COLUMNS,
    PLANT_CAPACITY_MGD,
    add_plant_balance_features,
    border_hourly_mgd,
    capacity_series,
    plant_balance,
)

LOGGER = logging.getLogger("test_plant_balance")


def _daily(values, start="2026-01-01") -> pd.Series:
    """SYNTHETIC_FIXTURE: a daily MGD series on the IBWC fixed offset."""
    idx = pd.date_range(start, periods=len(values), freq="D", tz="Etc/GMT+8")
    return pd.Series(values, index=idx, dtype=float)


def _hourly_border(daily_mgd, start="2026-01-01") -> pd.Series:
    """SYNTHETIC_FIXTURE: an hourly border series, one flat value per day, in MGD."""
    idx = pd.date_range(start, periods=len(daily_mgd) * 24, freq="h", tz="Etc/GMT+8")
    return pd.Series(np.repeat(daily_mgd, 24), index=idx, dtype=float)


# --- unit conversion -------------------------------------------------------

def test_one_cubic_metre_per_second_is_about_22_8_mgd():
    """264.172 gal/m³ × 86,400 s/day. A slip here mis-scales every border figure."""
    assert CMS_TO_MGD == pytest.approx(264.172052 * 86400 / 1e6, rel=1e-6)


def test_border_export_is_converted_and_localised():
    """SYNTHETIC_FIXTURE: the portal's column naming, timestamp as the index."""
    raw = pd.DataFrame({
        "Start of Interval (UTC-08:00)": ["2026-07-01 00:00:00", "2026-07-01 01:00:00"],
        "End of Interval (UTC-08:00)": ["2026-07-01 01:00:00", "2026-07-01 02:00:00"],
        "Average (m^3/s)": [1.0, "NaN"],
    }).set_index("Start of Interval (UTC-08:00)")

    series = border_hourly_mgd(raw)

    assert len(series) == 1                                   # the NaN hour is dropped
    assert series.iloc[0] == pytest.approx(CMS_TO_MGD)
    assert str(series.index[0].tz) in {"Etc/GMT+8", "UTC-08:00"}
    assert series.index[0].hour == 0                          # not shifted to PDT


def test_border_export_rejects_a_frame_it_cannot_read():
    with pytest.raises(ValueError, match="Start of Interval"):
        border_hourly_mgd(pd.DataFrame({"when": ["2026-01-01"], "Average (m^3/s)": [1.0]}))
    with pytest.raises(ValueError, match="empty"):
        border_hourly_mgd(pd.DataFrame())


# --- influent minus effluent -----------------------------------------------

def test_net_flow_is_influent_minus_effluent():
    frame = plant_balance(_daily([30.0, 30.0]), _daily([25.0, 32.0]))

    assert list(frame["net_mgd"]) == pytest.approx([5.0, -2.0])
    assert frame["net_fraction"].iloc[0] == pytest.approx(5.0 / 30.0)


def test_a_day_missing_from_one_series_is_kept_not_dropped():
    frame = plant_balance(_daily([30.0, 30.0, 30.0]), _daily([25.0, 25.0]))

    assert len(frame) == 3
    assert np.isnan(frame["effluent_mgd"].iloc[2])
    assert np.isnan(frame["net_mgd"].iloc[2])
    assert frame["influent_mgd"].iloc[2] == pytest.approx(30.0)


def test_zero_influent_does_not_produce_an_infinite_fraction():
    frame = plant_balance(_daily([0.0]), _daily([5.0]))

    assert not np.isinf(frame["net_fraction"]).any()


def test_zero_influent_while_the_plant_discharges_is_suspect_not_a_deficit():
    """The 2026 record has two months of 0.00 influent against ~34 MGD effluent:
    a meter outage. It must not publish as the plant discharging 34 MGD more
    than it took in."""
    frame = plant_balance(_daily([30.0, 0.0, 0.0]), _daily([25.0, 34.0, 0.0]))

    assert list(frame["influent_suspect"]) == [False, True, False]   # 0 in, 0 out is not suspect
    assert list(frame["influent_reported_mgd"]) == pytest.approx([30.0, 0.0, 0.0])
    assert np.isnan(frame["influent_mgd"].iloc[1])
    assert np.isnan(frame["net_mgd"].iloc[1])
    assert frame["influent_over_capacity"].iloc[1] is pd.NA
    assert frame["net_mgd"].iloc[2] == pytest.approx(0.0)


def test_both_series_empty_raises():
    with pytest.raises(ValueError):
        plant_balance(pd.Series(dtype=float), pd.Series(dtype=float))


# --- capacity ------------------------------------------------------------

def test_capacity_flags_fire_strictly_above_the_rating():
    cap = PLANT_CAPACITY_MGD
    frame = plant_balance(
        _daily([cap - 1, cap, cap + 0.1, 20.0]),
        _daily([20.0, 20.0, 20.0, cap + 5]),
    )

    assert list(frame["influent_over_capacity"]) == [False, False, True, False]
    assert list(frame["effluent_over_capacity"]) == [False, False, False, True]
    assert list(frame["over_capacity"]) == [False, False, True, True]
    assert list(frame["influent_excess_mgd"]) == pytest.approx([0.0, 0.0, 0.1, 0.0])
    assert list(frame["effluent_excess_mgd"]) == pytest.approx([0.0, 0.0, 0.0, 5.0])
    assert frame["capacity_utilisation"].iloc[1] == pytest.approx(1.0)


def test_an_unknown_flow_gives_an_unknown_flag_not_a_false_one():
    """A missing reading must not read as 'within capacity'."""
    frame = plant_balance(_daily([40.0, 40.0]), _daily([20.0]))

    assert frame["effluent_over_capacity"].iloc[1] is pd.NA
    # Influent alone was over, so the combined flag is still known.
    assert frame["over_capacity"].iloc[1] == True  # noqa: E712 — nullable boolean


def test_capacity_can_be_overridden_per_run():
    frame = plant_balance(_daily([40.0]), _daily([20.0]), capacity=50.0)

    assert frame["capacity_mgd"].iloc[0] == 50.0
    assert frame["influent_over_capacity"].iloc[0] == False  # noqa: E712


def test_a_dated_capacity_change_applies_from_its_effective_date():
    """The rating is changing soon; history must keep the rating of its day."""
    idx = pd.date_range("2026-01-01", periods=4, freq="D", tz="Etc/GMT+8")

    cap = capacity_series(idx, base=35.0, changes=[("2026-01-03", 50.0)])

    assert list(cap) == [35.0, 35.0, 50.0, 50.0]


def test_capacity_changes_need_not_be_ordered():
    idx = pd.date_range("2026-01-01", periods=5, freq="D", tz="Etc/GMT+8")

    cap = capacity_series(idx, base=25.0, changes=[("2026-01-04", 50.0), ("2026-01-02", 35.0)])

    assert list(cap) == [25.0, 35.0, 35.0, 50.0, 50.0]


def test_capacity_rejects_a_nonsense_rating():
    idx = pd.date_range("2026-01-01", periods=1, freq="D", tz="Etc/GMT+8")
    with pytest.raises(ValueError):
        capacity_series(idx, base=0)
    with pytest.raises(ValueError):
        capacity_series(idx, base=35.0, changes=[("2026-01-01", -1.0)])


# --- border vs effluent --------------------------------------------------

def test_border_above_effluent_is_flagged_on_the_daily_mean():
    border = _hourly_border([30.0, 10.0])
    frame = plant_balance(_daily([30.0, 30.0]), _daily([25.0, 25.0]), border_mgd=border)

    assert list(frame["border_flow_mgd"]) == pytest.approx([30.0, 10.0])
    assert list(frame["border_minus_effluent_mgd"]) == pytest.approx([5.0, -15.0])
    assert list(frame["border_over_effluent"]) == [True, False]
    assert frame["border_effluent_ratio"].iloc[0] == pytest.approx(1.2)


def test_hours_over_effluent_count_the_hours_not_the_mean():
    """A day whose mean sits below the effluent can still have hours above it."""
    hourly = np.array([40.0] * 6 + [10.0] * 18)             # mean 17.5, 6 hours over 25
    idx = pd.date_range("2026-01-01", periods=24, freq="h", tz="Etc/GMT+8")
    border = pd.Series(hourly, index=idx)

    frame = plant_balance(_daily([30.0]), _daily([25.0]), border_mgd=border)

    assert frame["border_over_effluent"].iloc[0] == False  # noqa: E712
    assert frame["border_over_effluent_hours"].iloc[0] == 6
    assert frame["border_hours_reported"].iloc[0] == 24
    assert frame["border_over_effluent_fraction"].iloc[0] == pytest.approx(0.25)


def test_border_comparison_is_unknown_without_an_effluent_reading():
    border = _hourly_border([30.0, 30.0])
    frame = plant_balance(_daily([30.0, 30.0]), _daily([25.0]), border_mgd=border)

    assert frame["border_over_effluent"].iloc[1] is pd.NA
    assert np.isnan(frame["border_over_effluent_hours"].iloc[1])
    assert frame["border_flow_mgd"].iloc[1] == pytest.approx(30.0)   # the gauge itself is still published


def test_without_a_border_series_the_plant_columns_still_publish():
    frame = plant_balance(_daily([30.0]), _daily([25.0]), border_mgd=None)

    assert frame["net_mgd"].iloc[0] == pytest.approx(5.0)
    assert np.isnan(frame["border_flow_mgd"].iloc[0])
    assert frame["border_over_effluent"].iloc[0] is pd.NA


def test_border_in_pacific_time_lands_on_the_same_day():
    """The model data carries the gauge in America/Los_Angeles; the day boundary
    must still be the plant's UTC-8 day, not smeared across two."""
    idx = pd.date_range("2026-07-01 00:00", periods=24, freq="h", tz="Etc/GMT+8").tz_convert("America/Los_Angeles")
    border = pd.Series(30.0, index=idx)

    frame = plant_balance(_daily([30.0], start="2026-07-01"), _daily([25.0], start="2026-07-01"), border_mgd=border)

    assert len(frame) == 1
    assert frame["border_hours_reported"].iloc[0] == 24


# --- model features ------------------------------------------------------

def _hourly_frame(days=3, start="2026-03-01", border_cms=1.0, effluent_lagged=25.0):
    """SYNTHETIC_FIXTURE: the shape modeldata_h2s has when the features are added."""
    t = pd.date_range(start, periods=24 * days, freq="h", tz="America/Los_Angeles")
    return pd.DataFrame({
        "time": t,
        "Flow (m^3/s)--Border": border_cms,
        "sbiwtp_flow_mgd": effluent_lagged,
    })


def test_daily_plant_features_are_lagged_one_day():
    """Today's rows describe yesterday's plant, as the existing SBIWTP features do."""
    influent = _daily([31.0, 32.0, 33.0, 34.0], start="2026-02-28")
    effluent = _daily([21.0, 22.0, 23.0, 24.0], start="2026-02-28")

    out = add_plant_balance_features(_hourly_frame(days=3), influent, effluent, LOGGER)

    march_1 = out[out["time"].dt.day == 1]
    assert (march_1["sbiwtp_influent_mgd"] == 31.0).all()          # Feb 28's influent
    assert np.allclose(march_1["sbiwtp_net_mgd"], 10.0)
    march_3 = out[out["time"].dt.day == 3]
    assert (march_3["sbiwtp_influent_mgd"] == 33.0).all()
    assert LAG_DAYS == 1


def test_over_capacity_feature_is_numeric_and_lagged():
    influent = _daily([40.0, 20.0], start="2026-02-28")
    effluent = _daily([20.0, 20.0], start="2026-02-28")

    out = add_plant_balance_features(_hourly_frame(days=2), influent, effluent, LOGGER)

    assert out.loc[out["time"].dt.day == 1, "sbiwtp_over_capacity"].unique().tolist() == [1.0]
    assert out.loc[out["time"].dt.day == 2, "sbiwtp_over_capacity"].unique().tolist() == [0.0]
    assert out["sbiwtp_capacity_mgd"].dropna().unique().tolist() == [PLANT_CAPACITY_MGD]


def test_border_feature_compares_the_hour_with_the_lagged_effluent():
    out = add_plant_balance_features(
        _hourly_frame(days=1, border_cms=2.0, effluent_lagged=25.0),
        _daily([30.0]), _daily([25.0]), LOGGER,
    )

    assert np.allclose(out["border_flow_mgd"], 2.0 * CMS_TO_MGD)
    assert np.allclose(out["border_minus_effluent_mgd"], 2.0 * CMS_TO_MGD - 25.0)
    assert (out["border_over_effluent"] == 1.0).all()


def test_border_feature_is_missing_where_the_gauge_is():
    frame = _hourly_frame(days=1, border_cms=2.0)          # 45.6 MGD against 25 MGD effluent
    frame.loc[0, "Flow (m^3/s)--Border"] = np.nan

    out = add_plant_balance_features(frame, _daily([30.0]), _daily([25.0]), LOGGER)

    assert np.isnan(out["border_over_effluent"].iloc[0])
    assert out["border_over_effluent"].iloc[1] == 1.0


def test_missing_inputs_leave_every_feature_column_present_and_nan():
    frame = _hourly_frame(days=1).drop(columns=["Flow (m^3/s)--Border"])

    out = add_plant_balance_features(frame, pd.Series(dtype=float), _daily([25.0]), LOGGER)

    for col in PLANT_BALANCE_FEATURE_COLUMNS:
        assert col in out.columns
        assert out[col].isna().all()


def test_feature_columns_are_not_in_the_model_feature_list():
    """Candidates for evaluation, not a silent change to what the models see."""
    from tijuana.utils.forecast_features import MODEL_FEATURES

    assert not set(PLANT_BALANCE_FEATURE_COLUMNS) & set(MODEL_FEATURES)
