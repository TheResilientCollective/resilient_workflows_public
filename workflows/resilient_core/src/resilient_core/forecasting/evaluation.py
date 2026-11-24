"""Forecast metrics, rolling-origin backtests and forecast-path tables (numpy/pandas only)."""

from __future__ import annotations

import numpy as np
import pandas as pd

from resilient_core.forecasting.predictors import Forecast, Predictor


def pinball(actual: np.ndarray, predicted: np.ndarray, level: float) -> np.ndarray:
    diff = actual - predicted
    return np.maximum(level * diff, (level - 1) * diff)


def weighted_quantile_loss(actual: np.ndarray, forecast: Forecast) -> float:
    denominator = np.abs(actual).sum()
    if forecast.quantiles is None or denominator == 0:
        return np.nan
    losses = [
        pinball(actual, forecast.quantiles[:, i], level).sum()
        for i, level in enumerate(forecast.levels)
    ]
    return float(sum(losses) / (len(forecast.levels) * denominator))


def interval(forecast: Forecast, lower: float, upper: float) -> tuple[np.ndarray, np.ndarray]:
    horizon = len(forecast.median)
    levels = np.asarray(forecast.levels)
    low_idx = np.flatnonzero(np.isclose(levels, lower))
    high_idx = np.flatnonzero(np.isclose(levels, upper))
    if forecast.quantiles is None or len(low_idx) == 0 or len(high_idx) == 0:
        return np.full(horizon, np.nan), np.full(horizon, np.nan)
    return forecast.quantiles[:, low_idx[0]], forecast.quantiles[:, high_idx[0]]


def interval_coverage(actual: np.ndarray, forecast: Forecast, lower: float, upper: float) -> float:
    low, high = interval(forecast, lower, upper)
    if np.isnan(low).all():
        return np.nan
    return float(np.mean((actual >= low) & (actual <= high)))


def path_table(forecast: Forecast, last_week: pd.Timestamp) -> pd.DataFrame:
    steps = np.arange(1, len(forecast.median) + 1)
    frame = pd.DataFrame(
        {
            "week": [last_week + pd.Timedelta(weeks=int(step)) for step in steps],
            "step": steps,
            "median": forecast.median,
        }
    )
    for label, lower, upper in [("80", 0.1, 0.9), ("90", 0.05, 0.95)]:
        low, high = interval(forecast, lower, upper)
        frame[f"lower_{label}"] = low
        frame[f"upper_{label}"] = high
    return frame


def mase_scale(history: np.ndarray) -> float:
    return float(np.mean(np.abs(np.diff(history)))) if len(history) > 1 else 0.0


def rolling_origins(length: int, horizon: int, n_windows: int, min_context: int) -> list[int]:
    origins = [length - horizon - k * horizon for k in range(n_windows)]
    return [origin for origin in origins if origin >= min_context]


def backtest(
    series: dict[str, np.ndarray],
    horizon: int,
    predictors: dict[str, Predictor],
    n_windows: int = 4,
    min_context: int = 52,
    weeks: dict[str, pd.DatetimeIndex] | None = None,
) -> pd.DataFrame:
    series = {name: np.asarray(values, dtype=float) for name, values in series.items()}
    windows = [
        (name, origin)
        for name, values in series.items()
        for origin in rolling_origins(len(values), horizon, n_windows, min_context)
    ]
    batched = {}
    for model_name, predict in predictors.items():
        if windows and getattr(predict, "uses_dates", False):
            ends = [weeks[name][origin - 1] for name, origin in windows] if weeks is not None else None
            outputs = predict([series[name][:origin] for name, origin in windows], horizon, ends=ends)
            batched[model_name] = dict(zip(windows, outputs))

    rows = []
    for name, values in series.items():
        origins = rolling_origins(len(values), horizon, n_windows, min_context)
        for origin in origins:
            history = values[:origin]
            actual = values[origin : origin + horizon]
            scale = mase_scale(history)
            for model_name, predict in predictors.items():
                if model_name in batched:
                    forecast = batched[model_name][(name, origin)]
                else:
                    forecast = predict([history], horizon)[0]
                mae = float(np.mean(np.abs(actual - forecast.median)))
                rows.append(
                    {
                        "series": name,
                        "origin": origin,
                        "model": model_name,
                        "mae": mae,
                        "mase": mae / scale if scale > 0 else np.nan,
                        "wql": weighted_quantile_loss(actual, forecast),
                        "coverage_80": interval_coverage(actual, forecast, 0.1, 0.9),
                        "coverage_90": interval_coverage(actual, forecast, 0.05, 0.95),
                    }
                )
    return pd.DataFrame(rows)


def summarise(results: pd.DataFrame) -> pd.DataFrame:
    return (
        results.groupby("model")
        .agg(
            mae=("mae", "mean"),
            mase=("mase", "mean"),
            wql=("wql", "mean"),
            coverage_80=("coverage_80", "mean"),
            coverage_90=("coverage_90", "mean"),
            windows=("mae", "size"),
        )
        .sort_values("mae")
    )


def forecast_path_rows(forecast: Forecast, last_week: pd.Timestamp, target_week: pd.Timestamp, **keys) -> list[dict]:
    rows = []
    for step in range(len(forecast.median)):
        week = last_week + pd.Timedelta(weeks=step + 1)
        row = {
            **keys,
            "week": week.strftime("%Y-%m-%d"),
            "step": step + 1,
            "weeks_from_target": (week - target_week).days // 7,
            "median": float(forecast.median[step]),
        }
        for label, lower, upper in [("80", 0.1, 0.9), ("90", 0.05, 0.95)]:
            low, high = interval(forecast, lower, upper)
            row[f"lower_{label}"] = float(low[step])
            row[f"upper_{label}"] = float(high[step])
        rows.append(row)
    return rows
