"""Shared H2S threshold-exceedance aggregation.

One implementation used by both the clock-based ``h2s_peaks`` asset and the
astronomical ``h2s_peaks_astronomical_day`` asset, so the two cannot drift apart
in how they treat gap-filled values -- which is precisely how the original
double-counting defect arose.

Pure functions, no Dagster or S3 imports.
"""

from __future__ import annotations

import pandas as pd

#: H2S exceedance thresholds in ppb. 5 is the odour/nuisance level, 30 the higher
#: health-referenced level.
THRESHOLDS = (5, 30)


def aggregate_exceedances(
    df: pd.DataFrame,
    group_keys: list[str],
    thresholds: tuple[int, ...] = THRESHOLDS,
    value_col: str = "H2S",
    measured_col: str = "h2s_measured",
) -> pd.DataFrame:
    """Count threshold exceedances per group, over measured observations only.

    Gap-filled values are excluded from the exceedance counts **and** from
    ``max_h2s`` / ``mean_h2s``. A synthetic value must never be reported as an
    observed exceedance, and leaving it in the max/mean while excluding it from
    the counts would make the table self-contradictory -- ``max_h2s`` of 40 next
    to ``count_exceeds_30`` of 0.

    Gap-filled rows are still counted, in ``count_filled``, so coverage stays
    visible. ``total_measurements`` keeps its original meaning of all non-null
    rows, so ``total_measurements == measured_observations + count_filled``.

    When ``measured_col`` is absent every row is treated as measured, which is
    correct for inputs like ``modeldata_h2s_nofill`` where unmeasured values are
    already null.
    """
    if df.empty:
        return pd.DataFrame()

    valid = df[df[value_col].notna()].copy()
    if valid.empty:
        return pd.DataFrame()

    if measured_col in valid.columns:
        measured = valid[measured_col].fillna(False).astype(bool)
    else:
        measured = pd.Series(True, index=valid.index)
    valid["_measured"] = measured
    # Filled values are masked out, so every statistic below sees observations only.
    valid["_observed"] = valid[value_col].where(measured)

    for thr in thresholds:
        valid[f"_exceeds_{thr}"] = valid["_observed"] > thr

    named = {
        f"count_exceeds_{thr}": (f"_exceeds_{thr}", "sum") for thr in thresholds
    }
    named.update(
        {
            "total_measurements": (value_col, "count"),
            "max_h2s": ("_observed", "max"),
            "mean_h2s": ("_observed", "mean"),
            "count_filled": ("_measured", lambda x: int((~x).sum())),
            "measured_observations": ("_measured", "sum"),
        }
    )
    out = valid.groupby(group_keys, dropna=False).agg(**named).reset_index()

    for col in [f"count_exceeds_{thr}" for thr in thresholds] + [
        "total_measurements",
        "count_filled",
        "measured_observations",
    ]:
        out[col] = out[col].astype(int)
    return out
