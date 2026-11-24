"""Block untrustworthy seven-day discounts after an exact or near A-B-A reversal.

Fourteen days determine price instability; seven days determine the deal
baseline. Read both windows together to prevent old disputed highs from
reappearing as legitimate discounts when the first A leaves the window.
"""
from __future__ import annotations

from datetime import timedelta

REVERSAL_DAYS = 14
RETURN_TOLERANCE_PERCENT = 15
MIN_NEAR_REVERSAL_SWING_PERCENT = 20


def is_price_reversal(a, b, c):
    """Exact ABA keeps its original rule; near ABA needs a substantial swing.

    Compare unrounded verified unit prices. Multiplication avoids division
    and preserves exact boundaries for Fraction-valued observations.
    """
    if min(a, b, c) <= 0:
        return False
    if a == c:
        return a != b
    return (
        100 * abs(c - a) <= RETURN_TOLERANCE_PERCENT * a
        and (b > max(a, c) or b < min(a, c))
        and 100 * abs(b - a) >= MIN_NEAR_REVERSAL_SWING_PERCENT * a
        and 100 * abs(b - c) >= MIN_NEAR_REVERSAL_SWING_PERCENT * c
    )


def has_disputed_high_in_week(observations, now, days=7, reversal_days=REVERSAL_DAYS):
    """Block a complete reversal within 14 days or a still-disputed weekly high.

    Prices are exact verified unit costs. Repeated equal prices form one run.
    No price is labelled incorrect or deleted. A reversal must complete within
    reversal_days, measured from the last initial A to the first returning price.
    A complete recent reversal blocks the SKU even if the high is no longer
    a candidate; older reversals also block while their high is in the week.
    """
    cutoff = now - timedelta(days=days)
    reversal_cutoff = now - timedelta(days=reversal_days)
    maximum_span = timedelta(days=reversal_days)
    runs = []
    for at, value in sorted(observations, key=lambda point: point[0]):
        if at > now or value <= 0:
            continue
        if runs and runs[-1]["value"] == value:
            runs[-1]["times"].append(at)
        else:
            runs.append({"value": value, "times": [at]})

    for first, middle, last in zip(runs, runs[1:], runs[2:]):
        if not is_price_reversal(first["value"], middle["value"], last["value"]):
            continue
        initial = first["times"][-1]
        returned = last["times"][0]
        if returned - initial > maximum_span:
            continue
        if initial >= reversal_cutoff:
            return True
        # A high middle run is disputed; for a low middle run both
        # near-equal high endpoints can be disputed weekly references.
        if middle["value"] > first["value"]:
            candidates = middle["times"]
        else:
            candidates = first["times"] + [returned]
        if any(cutoff <= at <= now for at in candidates):
            return True
    return False
