"""Tabular forecasting: foundation-model predictors, backtests and renewal-equation Rt.

Shared by the epimodel harness, ResilientSIMS and the Dagster workflows so that all of
them produce the same forecast from the same data.

- ``resilient_core.forecasting.predictors``: ``Forecast``, the ``load_*`` model loaders
  and ``*_predictor`` adapters, naive baselines, ``predict_dated``.
- ``resilient_core.forecasting.evaluation``: metrics, ``rolling_origins``, ``backtest``,
  ``summarise``, forecast-path tables.
- ``resilient_core.forecasting.renewal``: renewal-equation Rt (needs scipy, the
  ``renewal`` extra); not re-exported here so this package imports without scipy.

This package imports with numpy and pandas only; the model libraries are imported by
the loaders and need ``resilient_core[tabular]``.
"""

from resilient_core.forecasting.evaluation import (
    backtest,
    forecast_path_rows,
    interval,
    interval_coverage,
    mase_scale,
    path_table,
    pinball,
    rolling_origins,
    summarise,
    weighted_quantile_loss,
)
from resilient_core.forecasting.predictors import (
    PATH_EXTRA_WEEKS,
    SEASON_LENGTH,
    TABPFN_TS_DEFAULT_END,
    TABPFN_TS_LEVELS,
    TIMESFM_CHECKPOINT,
    TIMESTRAL_CHECKPOINT,
    Forecast,
    Predictor,
    has_tabpfn_token,
    load_tabpfn_ts,
    load_timesfm,
    load_timestral,
    naive_predictor,
    predict_dated,
    seasonal_naive_predictor,
    tabpfn_ts_predictor,
    timesfm_predictor,
    timestral_predictor,
)

__all__ = [
    "PATH_EXTRA_WEEKS",
    "SEASON_LENGTH",
    "TABPFN_TS_DEFAULT_END",
    "TABPFN_TS_LEVELS",
    "TIMESFM_CHECKPOINT",
    "TIMESTRAL_CHECKPOINT",
    "Forecast",
    "Predictor",
    "backtest",
    "forecast_path_rows",
    "has_tabpfn_token",
    "interval",
    "interval_coverage",
    "load_tabpfn_ts",
    "load_timesfm",
    "load_timestral",
    "mase_scale",
    "naive_predictor",
    "path_table",
    "pinball",
    "predict_dated",
    "rolling_origins",
    "seasonal_naive_predictor",
    "summarise",
    "tabpfn_ts_predictor",
    "timesfm_predictor",
    "timestral_predictor",
    "weighted_quantile_loss",
]
