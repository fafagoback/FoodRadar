"""Evidence-based product promotion detection from Uber's native catalog metadata.

Only unambiguous item-level fields may change unit-price calculations.
Unknown types remain in the full ApiRaw response, not guessed to be BOGO.
"""
import re

PROMOTION_FIELDS = (
    "promotion", "promotions", "promotionInfo", "promotionDetails",
    "itemPromotion", "itemPromotions", "offerInfo", "offer", "offers",
    "discountInfo", "discount", "discounts", "benefit", "benefits",
)
TYPE_FIELDS = ("promotionType", "offerType", "discountType", "benefitType", "type", "kind", "code")
BUY_FIELDS = ("buyQuantity", "buyQty", "buyCount", "purchaseQuantity", "requiredQuantity")
FREE_FIELDS = ("freeQuantity", "freeQty", "getQuantity", "rewardQuantity", "rewardCount")


def _quantity(obj, keys, default=1):
    for key in keys:
        value = obj.get(key)
        try:
            number = int(value)
        except (ValueError, TypeError, OverflowError):
            continue
        if 1 <= number <= 9:
            return number
    return default


def _candidates(value, depth=0):
    if depth > 2:
        return
    if isinstance(value, dict):
        yield value
        for key in PROMOTION_FIELDS:
            if key in value:
                yield from _candidates(value[key], depth + 1)
    elif isinstance(value, (list, tuple)):
        for entry in value:
            yield from _candidates(entry, depth + 1)


def native_item_promo(item):
    """(type, item count) for verified structured types; otherwise None."""
    if not isinstance(item, dict):
        return None
    candidates = [item]
    for key in PROMOTION_FIELDS:
        candidates.extend(_candidates(item.get(key)))
    for candidate in candidates:
        raw_type = next((candidate.get(k) for k in TYPE_FIELDS if isinstance(candidate.get(k), str)), None)
        if not raw_type:
            continue
        kind = re.sub(r"[^A-Z0-9]", "", raw_type.upper())
        match = re.fullmatch(r"BUY([1-9])GET([1-9])(?:FREE)?", kind)
        if match:
            buy, free = map(int, match.groups())
            return f"買{buy}送{free}", buy + free
        if kind in {"BOGO", "BUYONEGETONE", "BUYONEGETONEFREE", "BUYGETFREE", "BUYXGETYFREE"}:
            buy = _quantity(candidate, BUY_FIELDS)
            free = _quantity(candidate, FREE_FIELDS)
            return f"買{buy}送{free}", buy + free
        if kind in {"SECONDITEMHALFPRICE", "SECONDHALFPRICE", "BUYONEGETONEHALFOFF"}:
            return "第2件半價", 1
    return None


def native_promotion_field_names(item):
    """Audit newly observed field names without speculating about their meaning."""
    if not isinstance(item, dict):
        return []
    return sorted(k for k in item if re.search(r"promo|offer|discount|benefit|reward|deal|bogo", k, re.I))
