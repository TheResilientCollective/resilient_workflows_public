"""resilient_core.forecasting: metrics, backtests and renewal Rt without model downloads.

Model-loading tests download weights from Hugging Face and need ``resilient_core[tabular]``;
set RESILIENT_CORE_MODEL_TESTS=1 to run them.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys

import numpy as np
import pandas as pd
import pytest

from resilient_core.forecasting import (
    TABPFN_TS_LEVELS,
    Forecast,
    backtest,
    forecast_path_rows,
    interval,
    interval_coverage,
    mase_scale,
    naive_predictor,
    path_table,
    predict_dated,
    rolling_origins,
    seasonal_naive_predictor,
    summarise,
    weighted_quantile_loss,
)

MODEL_LIBRARIES = ("torch", "timesfm3", "timestral", "tabpfn", "tabpfn_time_series", "tabpfn_extensions")


def quantile_forecast() -> Forecast:
    return Forecast(
        median=np.array([10.0, 20.0]),
        quantiles=np.array([[8.0, 10.0, 12.0], [15.0, 20.0, 25.0]]),
        levels=(0.1, 0.5, 0.9),
    )


# --- metrics ---------------------------------------------------------------


def test_weighted_quantile_loss_known_value():
    # Pinball sums per level: 0.1 -> 0.2 + 0.5, 0.5 -> 0, 0.9 -> 0.2 + 0.5; |actual| sums to 30.
    wql = weighted_quantile_loss(np.array([10.0, 20.0]), quantile_forecast())
    assert wql == pytest.approx(1.4 / (3 * 30))


def test_weighted_quantile_loss_is_nan_without_quantiles_or_signal():
    assert np.isnan(weighted_quantile_loss(np.array([1.0, 2.0]), Forecast(median=np.array([1.0, 2.0]))))
    assert np.isnan(weighted_quantile_loss(np.zeros(2), quantile_forecast()))


def test_interval_coverage_known_value():
    forecast = quantile_forecast()
    assert interval_coverage(np.array([10.0, 30.0]), forecast, 0.1, 0.9) == pytest.approx(0.5)
    assert interval_coverage(np.array([9.0, 24.0]), forecast, 0.1, 0.9) == pytest.approx(1.0)
    low, high = interval(forecast, 0.1, 0.9)
    np.testing.assert_array_equal(low, [8.0, 15.0])
    np.testing.assert_array_equal(high, [12.0, 25.0])


def test_interval_coverage_is_nan_for_missing_levels():
    assert np.isnan(interval_coverage(np.array([10.0, 20.0]), quantile_forecast(), 0.05, 0.95))


def test_mase_scale():
    assert mase_scale(np.array([1.0, 3.0, 6.0, 10.0])) == pytest.approx(3.0)
    assert mase_scale(np.array([5.0])) == 0.0


def test_path_tables():
    levels = TABPFN_TS_LEVELS
    quantiles = np.tile(np.array(levels) * 100, (3, 1))
    forecast = Forecast(median=quantiles[:, levels.index(0.5)], quantiles=quantiles, levels=levels)
    last_week = pd.Timestamp("2025-01-04")

    table = path_table(forecast, last_week)
    assert table["week"].tolist() == [pd.Timestamp("2025-01-11"), pd.Timestamp("2025-01-18"), pd.Timestamp("2025-01-25")]
    assert table["lower_80"].tolist() == pytest.approx([10.0] * 3)
    assert table["upper_90"].tolist() == pytest.approx([95.0] * 3)

    rows = forecast_path_rows(forecast, last_week, pd.Timestamp("2025-01-18"), series="flu")
    assert [row["weeks_from_target"] for row in rows] == [-1, 0, 1]
    assert rows[0] == {
        "series": "flu",
        "week": "2025-01-11",
        "step": 1,
        "weeks_from_target": -1,
        "median": pytest.approx(50.0),
        "lower_80": pytest.approx(10.0),
        "upper_80": pytest.approx(90.0),
        "lower_90": pytest.approx(5.0),
        "upper_90": pytest.approx(95.0),
    }


# --- rolling origins and backtest ------------------------------------------


def test_rolling_origins():
    assert rolling_origins(100, 4, 4, 52) == [96, 92, 88, 84]
    assert rolling_origins(60, 4, 4, 52) == [56, 52]
    assert rolling_origins(50, 4, 4, 52) == []


def test_naive_and_seasonal_naive_predictors():
    history = np.arange(60, dtype=float)
    assert naive_predictor([history], 3)[0].median.tolist() == [59.0, 59.0, 59.0]
    assert seasonal_naive_predictor([history], 3)[0].median.tolist() == [8.0, 9.0, 10.0]
    # Shorter than a season: falls back to naive.
    assert seasonal_naive_predictor([history[:10]], 2)[0].median.tolist() == [9.0, 9.0]
    assert predict_dated(naive_predictor, [history], 2, ends=[pd.Timestamp("2025-01-04")])[0].median.tolist() == [59.0, 59.0]


def test_backtest_with_naive_baselines():
    series = {"linear": np.arange(60, dtype=float)}
    results = backtest(
        series,
        horizon=4,
        predictors={"naive": naive_predictor, "seasonal_naive": seasonal_naive_predictor},
        n_windows=2,
        min_context=52,
    )
    assert len(results) == 4
    assert sorted(results["origin"].unique().tolist()) == [52, 56]

    naive = results[results["model"] == "naive"]
    # Last value repeated against a +1/week trend: errors 1..4, mean 2.5; MASE scale is 1.
    assert naive["mae"].tolist() == pytest.approx([2.5, 2.5])
    assert naive["mase"].tolist() == pytest.approx([2.5, 2.5])
    assert naive["wql"].isna().all() and naive["coverage_80"].isna().all()

    seasonal = results[results["model"] == "seasonal_naive"]
    assert seasonal["mae"].tolist() == pytest.approx([52.0, 52.0])

    summary = summarise(results)
    assert summary.index.tolist() == ["naive", "seasonal_naive"]
    assert summary.loc["naive", "windows"] == 2


def test_backtest_batches_dated_predictors_with_window_end_weeks():
    calls = []

    def dated(histories, horizon, ends=None):
        calls.append((len(histories), list(ends)))
        return [
            Forecast(
                median=np.repeat(h[-1], horizon),
                quantiles=np.tile([h[-1] - 1, h[-1], h[-1] + 1], (horizon, 1)),
                levels=(0.1, 0.5, 0.9),
            )
            for h in histories
        ]

    dated.uses_dates = True
    values = np.arange(60, dtype=float)
    weeks = pd.date_range("2024-01-06", periods=60, freq="7D")
    results = backtest({"s": values}, 4, {"dated": dated}, n_windows=2, weeks={"s": weeks})

    assert calls == [(2, [weeks[55], weeks[51]])]
    assert results["coverage_80"].tolist() == pytest.approx([0.25, 0.25])
    assert results["wql"].notna().all()


# --- renewal Rt -------------------------------------------------------------


@pytest.fixture
def renewal():
    pytest.importorskip("scipy")
    from resilient_core.forecasting import renewal

    return renewal


def weekly_series(growth: float, weeks: int = 30) -> pd.Series:
    index = pd.date_range("2024-01-06", periods=weeks, freq="7D")
    return pd.Series(1e4 * growth ** np.arange(weeks), index=index)


def test_generation_interval_is_a_distribution(renewal):
    for mean, sd in renewal.GENERATION_INTERVALS.values():
        gi = renewal.generation_interval(mean, sd)
        assert len(gi) == renewal.MAX_GI_DAYS
        assert gi.sum() == pytest.approx(1.0)
        assert (gi >= 0).all()


def test_estimate_rt_matches_growth_rate(renewal):
    gi = renewal.generation_interval(*renewal.GENERATION_INTERVALS["FLU"])
    growth = 1.2
    rt = renewal.estimate_rt(weekly_series(growth), gi)

    # The first four weeks lack a full generation-interval window and are dropped.
    assert len(rt) == 26
    assert list(rt.columns) == ["week", "cases", "rt", "rt_lower_90", "rt_upper_90"]
    # Wallinga-Lipsitch: R = 1 / sum_k g_k exp(-r k) for daily growth rate r.
    daily_rate = np.log(growth) / 7
    expected = 1.0 / np.sum(gi * np.exp(-daily_rate * np.arange(1, len(gi) + 1)))
    assert rt["rt"].iloc[-1] == pytest.approx(expected, rel=0.02)
    assert (rt["rt_lower_90"] < rt["rt"]).all() and (rt["rt"] < rt["rt_upper_90"]).all()


def test_estimate_rt_flat_and_declining(renewal):
    gi = renewal.generation_interval(*renewal.GENERATION_INTERVALS["COVID"])
    flat = renewal.estimate_rt(weekly_series(1.0), gi)
    assert flat["rt"].to_numpy() == pytest.approx(1.0, abs=1e-3)
    declining = renewal.estimate_rt(weekly_series(0.8, weeks=12), gi)
    assert (declining["rt"] < 1.0).all()


def test_renewal_fit_recovers_level_with_grid(renewal):
    gi = renewal.generation_interval(*renewal.GENERATION_INTERVALS["RSV"])
    weekly = np.asarray(weekly_series(1.0, weeks=20))
    objective = renewal.renewal_objective(weekly, gi, fit_weeks=6)
    best, value = renewal.grid_maximise(objective, points=31)
    level, trend = renewal.scale_params(best)
    # A flat series is best explained by Rt = 1 with no trend (both on the 31-point grid).
    assert level == pytest.approx(1.0)
    assert trend == pytest.approx(0.0, abs=1e-9)
    assert value == pytest.approx(0.0, abs=1e-3)
    assert renewal.rt_path(1.0, 0.0, 4, extra_weeks=2).tolist() == [1.0] * 6


# --- import boundary --------------------------------------------------------


def test_import_does_not_load_model_libraries_or_scipy():
    script = (
        "import sys\n"
        "import resilient_core.forecasting\n"
        f"loaded = [m for m in {MODEL_LIBRARIES + ('scipy',)!r} if m in sys.modules]\n"
        "assert not loaded, loaded\n"
        "print('ok')\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"


@pytest.mark.skipif(importlib.util.find_spec("timestral") is not None, reason="timestral is installed")
def test_loader_without_tabular_extra_names_the_extra():
    from resilient_core.forecasting import load_timestral

    with pytest.raises(ImportError, match=r"resilient_core\[tabular\]"):
        load_timestral()


# --- opt-in: real model weights ---------------------------------------------

model_tests = pytest.mark.skipif(
    os.environ.get("RESILIENT_CORE_MODEL_TESTS") != "1",
    reason="downloads model weights; set RESILIENT_CORE_MODEL_TESTS=1 (needs resilient_core[tabular])",
)


def seasonal_counts(weeks: int = 120) -> np.ndarray:
    t = np.arange(weeks)
    return 100 + 80 * np.sin(2 * np.pi * t / 52) ** 2 + t * 0.5


def assert_quantile_backtest(results: pd.DataFrame) -> None:
    assert len(results) == 2
    assert results["mae"].notna().all()
    assert results["wql"].notna().all()


@model_tests
def test_timestral_model():
    from resilient_core.forecasting import load_timestral, timestral_predictor

    predict = timestral_predictor(load_timestral())
    forecast = predict([seasonal_counts()], 4)[0]
    assert forecast.median.shape == (4,)
    assert (forecast.quantiles >= 0).all()
    assert_quantile_backtest(backtest({"s": seasonal_counts()}, 4, {"timestral": predict}, n_windows=2))


@model_tests
def test_timesfm_model():
    from resilient_core.forecasting import load_timesfm, timesfm_predictor

    evaluator, levels = load_timesfm()
    predict = timesfm_predictor(evaluator, levels)
    forecast = predict([seasonal_counts()], 4)[0]
    assert forecast.median.shape == (4,)
    assert forecast.quantiles.shape == (4, len(levels))
    assert_quantile_backtest(backtest({"s": seasonal_counts()}, 4, {"timesfm": predict}, n_windows=2))


@model_tests
def test_tabpfn_ts_model():
    from resilient_core.forecasting import load_tabpfn_ts, tabpfn_ts_predictor

    predict = tabpfn_ts_predictor(load_tabpfn_ts())
    weeks = pd.date_range("2023-01-07", periods=120, freq="7D")
    results = backtest({"s": seasonal_counts()}, 4, {"tabpfn": predict}, n_windows=2, weeks={"s": weeks})
    assert_quantile_backtest(results)
