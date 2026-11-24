"""Renewal-equation Rt for weekly counts: generation intervals, Rt estimation and simulation.

Needs scipy (``resilient_core[renewal]``, included in ``[tabular]``). ``bo_maximise``
additionally needs torch, tabpfn and tabpfn-extensions (``resilient_core[tabular]``),
imported when it is called.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd
from scipy import stats

GENERATION_INTERVALS = {
    "COVID": (3.5, 2.5),
    "FLU": (3.0, 1.5),
    "RSV": (7.5, 2.1),
}
MAX_GI_DAYS = 28
LEVEL_BOUNDS = (0.5, 2.0)
TREND_BOUNDS = (-0.15, 0.15)


def generation_interval(mean: float, sd: float, max_days: int = MAX_GI_DAYS) -> np.ndarray:
    shape = (mean / sd) ** 2
    scale = sd**2 / mean
    edges = np.arange(0, max_days + 1)
    mass = np.diff(stats.gamma.cdf(edges + 0.5, a=shape, scale=scale))
    return mass / mass.sum()


def daily_incidence(weekly: np.ndarray) -> np.ndarray:
    return np.repeat(np.asarray(weekly, dtype=float) / 7.0, 7)


def infection_pressure(daily: np.ndarray, gi: np.ndarray) -> np.ndarray:
    pressure = np.full(len(daily), np.nan)
    for day in range(len(gi), len(daily)):
        pressure[day] = float(np.dot(gi, daily[day - 1 :: -1][: len(gi)]))
    return pressure


def estimate_rt(weekly: pd.Series, gi: np.ndarray, prior_shape: float = 1.0, prior_scale: float = 5.0) -> pd.DataFrame:
    daily = daily_incidence(weekly.to_numpy())
    pressure = infection_pressure(daily, gi)
    cases = daily.reshape(-1, 7).sum(axis=1)
    total_pressure = pressure.reshape(-1, 7).sum(axis=1)
    shape = prior_shape + cases
    rate = 1.0 / prior_scale + total_pressure
    frame = pd.DataFrame(
        {
            "week": weekly.index,
            "cases": cases,
            "rt": shape / rate,
            "rt_lower_90": stats.gamma.ppf(0.05, a=shape, scale=1.0 / rate),
            "rt_upper_90": stats.gamma.ppf(0.95, a=shape, scale=1.0 / rate),
        }
    )
    return frame[np.isfinite(total_pressure)].reset_index(drop=True)


def simulate_weekly(history_daily: np.ndarray, gi: np.ndarray, rt_weekly: np.ndarray) -> np.ndarray:
    daily = list(history_daily[-len(gi) :])
    for rt_value in rt_weekly:
        for _ in range(7):
            recent = np.asarray(daily[-len(gi) :])[::-1]
            daily.append(float(rt_value) * float(np.dot(gi, recent)))
    return np.asarray(daily[len(gi) :]).reshape(-1, 7).sum(axis=1)


def rt_path(level: float, trend: float, fit_weeks: int, extra_weeks: int = 0) -> np.ndarray:
    centre = (fit_weeks - 1) / 2.0
    return level * np.exp(trend * (np.arange(fit_weeks + extra_weeks) - centre))


def log_rmse(observed: np.ndarray, expected: np.ndarray) -> float:
    return float(np.sqrt(np.mean((np.log1p(observed) - np.log1p(expected)) ** 2)))


def scale_params(unit: np.ndarray) -> tuple[float, float]:
    level = LEVEL_BOUNDS[0] + float(unit[0]) * (LEVEL_BOUNDS[1] - LEVEL_BOUNDS[0])
    trend = TREND_BOUNDS[0] + float(unit[1]) * (TREND_BOUNDS[1] - TREND_BOUNDS[0])
    return level, trend


def renewal_objective(weekly: np.ndarray, gi: np.ndarray, fit_weeks: int) -> Callable[[np.ndarray], float]:
    weekly = np.asarray(weekly, dtype=float)
    history_daily = daily_incidence(weekly[:-fit_weeks])
    observed = weekly[-fit_weeks:]

    def objective(unit: np.ndarray) -> float:
        level, trend = scale_params(unit)
        expected = simulate_weekly(history_daily, gi, rt_path(level, trend, fit_weeks))
        return -log_rmse(observed, expected)

    return objective


def bo_maximise(
    objective: Callable[[np.ndarray], float],
    dim: int = 2,
    n_init: int = 8,
    n_iter: int = 40,
    rounds: int = 1,
    shrink: float = 0.2,
    seed: int = 0,
) -> pd.DataFrame:
    try:
        import torch
        from tabpfn import TabPFNRegressor
        from tabpfn_extensions.bayesian_optimization.bo import propose_next_point
    except ImportError as error:
        raise ImportError(f"bo_maximise needs resilient_core[tabular]: {error}") from error

    torch.manual_seed(seed)
    regressor = TabPFNRegressor(differentiable_input=True, device="cpu", random_state=seed)
    low, high = np.zeros(dim), np.ones(dim)
    frames = []
    best_x, best_value = None, -np.inf
    for round_index in range(rounds):
        sobol = torch.quasirandom.SobolEngine(dim, scramble=True, seed=seed + round_index)
        train_x = sobol.draw(n_init).to(torch.float32)
        points = [low + x.numpy() * (high - low) for x in train_x]
        values = [objective(point) for point in points]
        for _ in range(n_iter):
            train_y = torch.tensor(values, dtype=torch.float32)
            candidate = propose_next_point(regressor, train_x, train_y).to(torch.float32).cpu()
            train_x = torch.cat([train_x, candidate[None, :]])
            points.append(low + candidate.numpy() * (high - low))
            values.append(objective(points[-1]))
        frame = pd.DataFrame(np.asarray(points), columns=[f"x{i}" for i in range(dim)])
        frame["objective"] = values
        frame["phase"] = ["init"] * n_init + ["bo"] * n_iter
        frame["round"] = round_index
        frames.append(frame)
        if max(values) > best_value:
            best_value = max(values)
            best_x = points[int(np.argmax(values))]
        width = shrink ** (round_index + 1)
        low = np.clip(best_x - width / 2, 0.0, 1.0 - width)
        high = low + width
    return pd.concat(frames, ignore_index=True)


def grid_maximise(objective: Callable[[np.ndarray], float], points: int = 61) -> tuple[np.ndarray, float]:
    axis = np.linspace(0.0, 1.0, points)
    best_x, best_value = None, -np.inf
    for a in axis:
        for b in axis:
            value = objective(np.array([a, b]))
            if value > best_value:
                best_x, best_value = np.array([a, b]), value
    return best_x, best_value
