"""The FoodRadar price-change rule: compare two consecutive valid unit prices.

The caller verifies that prices are from the same store and product and from
observations during business hours. Promotion labels never gate a price drop.
"""
from __future__ import annotations

from math import isfinite


def compare_effective_prices(previous: float | None, current: float | None) -> dict | None:
    """Signed change (current minus previous); no result without a valid baseline."""
    try:
        prior = round(float(previous), 2)
        now = round(float(current), 2)
    except (ValueError, TypeError, OverflowError):
        return None
    if not isfinite(prior) or not isfinite(now) or prior <= 0 or now <= 0:
        return None
    change = round(now - prior, 2)
    savings = round(max(0.0, -change), 2)
    return {
        "previous": prior,
        "current": now,
        "change": change,
        "direction": "down" if change < 0 else "up" if change > 0 else "flat",
        "savings": savings,
        "discount_pct": round(savings / prior * 100, 2) if savings else 0.0,
    }
