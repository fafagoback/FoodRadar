"""Prevent A-B-A price oscillations from inventing seven-day discounts.

A reversal is evidence of an unstable price comparison, not proof that
either the high or the low Uber quote is wrong. Never alter the observations.
This detector needs only the last fourteen days of already-verified prices:
seven days for current comparisons and seven for detecting reversals whose
first leg has just left the comparison window.
"""
from __future__ import annotations

from datetime import timedelta


def has_disputed_high_in_week(observations, now, days=7):
    """Return True if a completed A-B-A still affects the current price window.

    observations: [(timezone-aware datetime, exact per-unit price), ...].
    Consecutive identical values form a run; A-B-B-A is also A-B-A.
    The reversal must occur within seven days (last A to first returning A).
    Mark *high-price observations involved in the reversal* as disputed for
    discount eligibility only. No claim is made about which price is correct.
    The return run contributes its first observation, not every later stable
    repetition, so a genuinely sustained price can eventually qualify.
    """
    if not observations:
        return False
    cutoff = now - timedelta(days=days)
    runs = []
    for at, value in sorted(observations, key=lambda point: point[0]):
        if at > now or value <= 0:
            continue
        if runs and runs[-1]["value"] == value:
            runs[-1]["times"].append(at)
        else:
            runs.append({"value": value, "times": [at]})

    for first, middle, last in zip(runs, runs[1:], runs[2:]):
        if first["value"] != last["value"] or first["value"] == middle["value"]:
            continue
        if last["times"][0] - first["times"][-1] > timedelta(days=days):
            continue
        high = max(first["value"], middle["value"])
        # Historical high-price evidence inside the current seven-day window
        # may be disputed even when the reversal's first A is outside it.
        candidates = []
        if first["value"] == high:
            candidates.extend(first["times"])
        if middle["value"] == high:
            candidates.extend(middle["times"])
        if last["value"] == high:
            candidates.append(last["times"][0])
        if any(cutoff <= at <= now for at in candidates):
            return True
    return False
