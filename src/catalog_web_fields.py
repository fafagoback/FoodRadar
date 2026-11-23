"""Small web-facing projection of native Uber Eats catalog items.

HF Raw always stores the unchanged HTTP response. This module keeps only
fields needed by FoodRadar web product cards and diagnostics; it is never
used to reconstruct or overwrite the original response.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from verified_prices import _thumbnail_prices


def _positive_int_cents(value):
    if type(value) not in (int, float) or value <= 0:
        return None
    try:
        n = int(Decimal(str(value)).to_integral_value(rounding=ROUND_HALF_UP))
        return n if n > 0 else None
    except (ArithmeticError, ValueError):
        return None


def _url(value):
    if isinstance(value, str) and len(value) <= 2048 and value.startswith(("https://", "http://")):
        return value
    return None


def _uuid(value):
    return value if isinstance(value, str) and 0 < len(value) < 128 else None


def _text(value):
    return value.strip()[:200] if isinstance(value, str) and value.strip() else None


def web_catalog_fields(item):
    """Project an item's unique UI/availability/promotion facts (no raw JSON copy)."""
    if not isinstance(item, dict):
        return {}
    current_cents = _positive_int_cents(item.get("price"))
    previous, displayed = _thumbnail_prices(item)
    list_price = (next(iter(previous))
                  if current_cents and len(previous) == 1 and len(displayed) == 1
                  and displayed == {current_cents} and next(iter(previous)) > current_cents
                  else None)
    level = item.get("itemLevelPromotion") or {}
    cart = level.get("multiSKUFlatPromotion") or {}
    promo = item.get("promoInfo") or {}
    analytics = item.get("catalogItemAnalyticsData") or {}
    availability = item.get("itemAvailabilityState")
    result = {
        "image_url": _url(item.get("imageUrl")),
        "display_original_price_cents": list_price,
        "native_promo_type": _text(analytics.get("promoType")),
        "promotion_uuid": _uuid(promo.get("promotionUUID")),
        "cart_campaign_uuid": _uuid(cart.get("promotionUuid")),
        "cart_promo_label": _text((cart.get("itemPromotionTag") or {}).get("text")),
        "is_available": item.get("isAvailable") if type(item.get("isAvailable")) is bool else None,
        "is_sold_out": item.get("isSoldOut") if type(item.get("isSoldOut")) is bool else None,
        "availability_state": _text(availability) if isinstance(availability, str) else None,
        "section_uuid": _uuid(item.get("sectionUuid")),
        "subsection_uuid": _uuid(item.get("subsectionUuid")),
    }
    return {k: v for k, v in result.items() if v is not None}
