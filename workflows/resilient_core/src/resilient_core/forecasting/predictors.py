"""Forecast container and predictors for weekly count series.

A predictor maps a batch of histories and a horizon to one ``Forecast`` per history.
Model libraries (torch, timestral, timesfm, tabpfn-time-series) are imported inside
the ``load_*`` functions only, so this module imports with numpy and pandas alone.
Install ``resilient_core[tabular]`` to use the model loaders.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd

TIMESTRAL_CHECKPOINT = "mistralai/Timestral-5M"
TIMESFM_CHECKPOINT = "google/timesfm-3.0-pytorch"
SEASON_LENGTH = 52
PATH_EXTRA_WEEKS = 3
TABPFN_TS_LEVELS = (0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95)
TABPFN_TS_DEFAULT_END = pd.Timestamp("2026-01-04")
TABPFN_TOKEN_ENV = "TABPFN_TOKEN"
TABULAR_EXTRA_HINT = "install resilient_core[tabular]"


@dataclass
class Forecast:
    median: np.ndarray
    quantiles: np.ndarray | None = None
    levels: tuple[float, ...] = ()


Predictor = Callable[[list[np.ndarray], int], list[Forecast]]


def _missing_extra(package: str, error: ImportError) -> ImportError:
    return ImportError(f"{package} is not available ({TABULAR_EXTRA_HINT}): {error}")


def load_timestral(checkpoint: str = TIMESTRAL_CHECKPOINT, device: str = "cpu"):
    try:
        from timestral import TimestralForecaster
    except ImportError as error:
        raise _missing_extra("timestral", error) from error

    return TimestralForecaster.from_pretrained(checkpoint, device=device)


def load_timesfm(device: str = "cpu"):
    """Load TimesFM 3; returns ``(evaluator, quantile_levels)``.

    ``ModelConfig`` downloads ``TIMESFM_CHECKPOINT`` by default.
    """
    try:
        from timesfm3 import ModelConfig, TimesFM3Evaluator
    except ImportError as error:
        raise _missing_extra("timesfm[torch]", error) from error

    config = ModelConfig(device=device)
    return TimesFM3Evaluator(config), tuple(config.quantiles)


def timestral_predictor(model) -> Predictor:
    def predict(histories: list[np.ndarray], horizon: int) -> list[Forecast]:
        levels = tuple(model.quantile_levels)
        outputs = []
        for history in histories:
            result = model.forecast(np.asarray(history, dtype=np.float32), prediction_length=horizon)
            quantiles = np.clip(result.quantiles[0, 0].cpu().numpy(), 0, None)
            outputs.append(Forecast(median=quantiles[:, model.median_index], quantiles=quantiles, levels=levels))
        return outputs

    return predict


def timesfm_predictor(evaluator, levels: tuple[float, ...]) -> Predictor:
    def predict(histories: list[np.ndarray], horizon: int) -> list[Forecast]:
        arrays = [np.asarray(history, dtype=np.float32) for history in histories]
        outputs = list(evaluator.predict_batch(arrays, horizon=horizon, return_quantiles=True))
        return [
            Forecast(
                median=np.clip(np.asarray(out.forecast), 0, None),
                quantiles=np.clip(np.asarray(out.quantiles), 0, None),
                levels=levels,
            )
            for out in outputs
        ]

    return predict


def has_tabpfn_token(name: str = TABPFN_TOKEN_ENV) -> bool:
    """Whether the TabPFN token is set in the process environment.

    Credentials come from the environment of whoever runs the code; no ``.env`` file is read.
    """
    return bool(os.environ.get(name))


def load_tabpfn_ts():
    try:
        from tabpfn_time_series import TabPFNMode, TabPFNTSPipeline
    except ImportError as error:
        raise _missing_extra("tabpfn-time-series", error) from error

    return TabPFNTSPipeline(tabpfn_mode=TabPFNMode.LOCAL)


def tabpfn_ts_predictor(pipeline, levels: tuple[float, ...] = TABPFN_TS_LEVELS, clip: bool = True) -> Predictor:
    median_index = levels.index(0.5)

    def predict(histories: list[np.ndarray], horizon: int, ends: list[pd.Timestamp] | None = None) -> list[Forecast]:
        frames = []
        for item, history in enumerate(histories):
            end = pd.Timestamp(ends[item]) if ends is not None else TABPFN_TS_DEFAULT_END
            frames.append(
                pd.DataFrame(
                    {
                        "item_id": item,
                        "timestamp": pd.date_range(end=end, periods=len(history), freq="7D"),
                        "target": np.asarray(history, dtype=float),
                    }
                )
            )
        result = pipeline.predict_df(pd.concat(frames, ignore_index=True), prediction_length=horizon, quantiles=list(levels))
        outputs = []
        for item in range(len(histories)):
            quantiles = result.xs(item, level="item_id")[list(levels)].to_numpy(dtype=float)
            if clip:
                quantiles = np.clip(quantiles, 0, None)
            outputs.append(Forecast(median=quantiles[:, median_index], quantiles=quantiles, levels=levels))
        return outputs

    predict.uses_dates = True
    return predict


def predict_dated(predict: Predictor, histories: list[np.ndarray], horizon: int, ends: list[pd.Timestamp]) -> list[Forecast]:
    if getattr(predict, "uses_dates", False):
        return predict(histories, horizon, ends=ends)
    return predict(histories, horizon)


def naive_predictor(histories: list[np.ndarray], horizon: int) -> list[Forecast]:
    return [Forecast(median=np.repeat(float(history[-1]), horizon)) for history in histories]


def seasonal_naive_predictor(histories: list[np.ndarray], horizon: int) -> list[Forecast]:
    outputs = []
    for history in histories:
        if len(history) < SEASON_LENGTH:
            outputs.append(Forecast(median=np.repeat(float(history[-1]), horizon)))
            continue
        last_season = history[-SEASON_LENGTH:]
        outputs.append(Forecast(median=last_season[np.arange(horizon) % SEASON_LENGTH]))
    return outputs
