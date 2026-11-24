"""Block untrustworthy seven-day discounts after an exact A-B-A reversal.

Fourteen days determine price instability; seven days determine the deal
baseline. Read both windows together to prevent old disputed highs from
reappearing as legitimate discounts when the first A leaves the window.
"""
from __future__ import annotations

from datetime import timedelta

REVERSAL_DAYS = 14


def has_disputed_high_in_week(observations, now, days=7, reversal_days=REVERSAL_DAYS):
    """Block a complete reversal within 14 days or a still-disputed weekly high.

    Prices are exact verified unit costs. Repeated equal prices form one run.
    No price is labelled incorrect or deleted. A reversal must complete within
    reversal_days, measured from the last initial A to the first returning A.
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
        if first["value"] != last["value"] or first["value"] == middle["value"]:
            continue
        initial = first["times"][-1]
        returned = last["times"][0]
        if returned - initial > maximum_span:
            continue
        if initial >= reversal_cutoff:
            return True
        high = max(first["value"], middle["value"])
        candidates = []
        if first["value"] == high:
            candidates.extend(first["times"])
        if middle["value"] == high:
            candidates.extend(middle["times"])
        if last["value"] == high:
            candidates.append(returned)
        if any(cutoff <= at <= now for at in candidates):
            return True
    return False
