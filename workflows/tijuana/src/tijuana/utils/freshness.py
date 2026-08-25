"""Freshness verdicts for assets fed by sources that publish on a lag.

A single threshold forces a false choice: set it to the source's publication
cadence and a genuine stall goes unnoticed for as long as that cadence, or set it
tight and the check fails routinely on lag the pipeline cannot control. Two tiers
separate "the source has not published yet" from "the feed has stopped".
"""

from __future__ import annotations


def freshness_verdict(
    age_days: float, warn_after_days: float, fail_after_days: float
) -> tuple[bool, bool, str]:
    """Classify the age of the newest record.

    Returns ``(passed, is_error, reason)``:

    * within ``warn_after_days`` -- passed, normal publication lag;
    * between the two -- not passed at WARN severity, the source is later than
      usual but not obviously broken;
    * beyond ``fail_after_days`` -- not passed at ERROR severity.

    A caller maps ``is_error`` onto its severity type.
    """
    if fail_after_days < warn_after_days:
        raise ValueError(
            f"fail_after_days {fail_after_days} is below warn_after_days {warn_after_days}"
        )
    if age_days <= warn_after_days:
        return True, False, f"newest record is {age_days:.1f} days old, within normal lag"
    if age_days <= fail_after_days:
        return (
            False,
            False,
            f"newest record is {age_days:.1f} days old, beyond the usual "
            f"{warn_after_days:g}-day publication lag but under the {fail_after_days:g}-day "
            "failure threshold",
        )
    return (
        False,
        True,
        f"newest record is {age_days:.1f} days old, beyond the {fail_after_days:g}-day "
        "threshold - the source feed has likely stopped",
    )
