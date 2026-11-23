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
    def seven_day_high(observations, current_time, current_price, days=7):
        """Compare current unit cost with maximum valid previous N-day unit cost."""
        from datetime import datetime, timedelta
        try:
            now = datetime.fromisoformat(str(current_time))
            current = round(float(current_price), 2)
            if not isfinite(current) or current <= 0: return None
        except (ValueError, TypeError, OverflowError):
            return None
        cutoff = now - timedelta(days=days)
        unique = {}
        for point in observations:
            # Older history rows contain [time, unit]; new HF RAW rows add
            # [original_list_price, promo_type, quantity]. The comparison
            # ALWAYS reads the same unit-cost field in position 1.
            try:
                stamp, price = point[0], point[1]
                when = datetime.fromisoformat(str(stamp))
                value = round(float(price), 2)
                if cutoff <= when < now and isfinite(value) and value > 0:
                    unique[str(stamp)] = value
            except (ValueError, TypeError, OverflowError):
                continue
        if not unique: return None
        high = max(unique.values())
        return {"reference": high, "observations": len(unique),
                "comparison": PriceChange.compare(high, current)}

    @staticmethod
    def persisted_seven_day(week: dict | None, min_percent: float = LARGE_DROP_PCT,
                            min_savings: float = 0.0) -> dict:
        """Main deal-list fields: previous SEVEN DAYS' highest valid unit price.

        Previous-crawl reference_price/discount_pct remain separate movement
        diagnostics. Never replace seven_day_* with the last observed price,
        the median, a menu list price, or an unverified archived quote.
        """
        comparison = week.get("comparison") if week else None
        dropped = bool(comparison and comparison["is_price_drop"])
        return {
            "seven_day_reference_price": week["reference"] if week else None,
            "seven_day_discount_pct": comparison["discount_pct"] if dropped else 0.0,
            "seven_day_discount_amount": comparison["savings"] if dropped else 0.0,
            "seven_day_is_price_deal": int(PriceChange.qualifies(
                comparison, min_percent, min_savings)),
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
