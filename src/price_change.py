"""The sole FoodRadar price-change decision and arithmetic engine.

Callers supply the previous and current *verified* effective per-item prices.
Identity matching and in-business-hours observation validation happen upstream.
No presentation, SQL, JS or export path should calculate price drops independently.
"""
from __future__ import annotations

from math import isfinite

LARGE_DROP_PCT = 30.0


class PriceChange:
    """Stateless comparison; safe for normalized DB, ETL, tests and repair tools."""

    @staticmethod
    def compare(previous: float | None, current: float | None) -> dict | None:
        try:
            prior = round(float(previous), 2)
            now = round(float(current), 2)
        except (ValueError, TypeError, OverflowError):
            return None
        if not isfinite(prior) or not isfinite(now) or prior <= 0 or now <= 0:
            return None

        delta = round(now - prior, 2)
        change_pct = round(delta / prior * 100, 2)
        savings = round(max(0.0, -delta), 2)
        drop_pct = round(savings / prior * 100, 2) if savings else 0.0
        direction = "down" if delta < 0 else "up" if delta > 0 else "flat"
        return {
            "previous": prior,
            "current": now,
            "change": delta,
            "change_percent": change_pct,
            "direction": direction,
            "is_price_drop": direction == "down",
            "is_price_deal": direction == "down" and drop_pct >= LARGE_DROP_PCT,
            "savings": savings,
            "discount_pct": drop_pct,
        }

    @staticmethod
    def qualifies(comparison: dict | None, min_percent: float = 0.0,
                  min_savings: float = 0.0) -> bool:
        """Use the same rule for every price-drop list, regardless of source."""
        return bool(
            comparison and comparison["is_price_drop"]
            and comparison["discount_pct"] >= min_percent
            and comparison["savings"] >= min_savings
        )

    @staticmethod
    def persisted(comparison: dict | None) -> dict:
        """Stable DB fields consumed by the packed website without recalculation."""
        return {
            "reference_price": comparison["previous"] if comparison else None,
            "price_direction": comparison["direction"] if comparison else "unknown",
            "is_price_drop": int(comparison["is_price_drop"]) if comparison else 0,
            "price_change_amount": comparison["change"] if comparison else 0.0,
            "price_change_percent": comparison["change_percent"] if comparison else 0.0,
            "discount_amount": comparison["savings"] if comparison else 0.0,
            "discount_pct": comparison["discount_pct"] if comparison else 0.0,
            "is_price_deal": int(comparison["is_price_deal"]) if comparison else 0,
        }


# Existing Python integrations retain one authoritative implementation.
compare_effective_prices = PriceChange.compare
