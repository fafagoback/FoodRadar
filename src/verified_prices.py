"""FoodRadar V2: only native same-SKU buy-X-get-Y and direct item markdowns.

Quotes are derived exclusively from numeric getStoreV1 fields. Unknown / multi-SKU
promotions are not converted into imaginary discounts. Source hashes allow audits
against immutable HF ApiRaw snapshots.
"""
from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, ROUND_HALF_UP

SCHEMA_VERSION = 2
AUTHORIZED_KINDS = frozenset(("NONE", "SAME_SKU_BXGY", "DIRECT_DISCOUNT"))
MONEY_RE = re.compile(r"(?:NT\$|TWD\s*|\$)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)", re.I)


def _money_cents(text):
    if not isinstance(text, str):
        return None
    matches = MONEY_RE.findall(text)
    if len(matches) != 1:
        return None
    try:
        return int((Decimal(matches[0].replace(",", "")) * 100).to_integral_exact())
    except (ValueError, ArithmeticError):
        return None


def _thumbnail_prices(item):
    """Read exact UI numeric current and STRIKE_THROUGH old price, never % badges."""
    old_prices, new_prices = set(), set()
    for block in item.get("itemThumbnailElements") or []:
        elements = (((block.get("payload") or {}).get("labelPayload") or {})
                    .get("label") or {}).get("richTextElements") or []
        for element in elements:
            txt = element.get("text") or {}
            value = _money_cents((txt.get("text") or {}).get("text"))
            if value is None:
                continue
            if "STRIKE_THROUGH" in (txt.get("predefinedDecorations") or []):
                old_prices.add(value)
            else:
                new_prices.add(value)
    return old_prices, new_prices


def _invalid(kind, item, reason, source_hash):
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "UNVERIFIED",
        "kind": kind,
        "menu_price_cents": item.get("price"),
        "reason": reason,
        "source_hash": source_hash,
    }


def quote_from_native_item(item):
    """Parse one getStoreV1 catalog item without heuristics or inferred discounts."""
    if not isinstance(item, dict):
        return None
    source_hash = hashlib.sha256(
        json.dumps(item, sort_keys=True, ensure_ascii=False,
                   separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    price = item.get("price")
    if type(price) is not int or price <= 0 or price > 1_000_000_000:
        return _invalid("UNKNOWN", item, "INVALID_MENU_PRICE", source_hash)

    analytics = item.get("catalogItemAnalyticsData") or {}
    category = analytics.get("promoType")
    native = item.get("itemPromotion") or {}
    level = item.get("itemLevelPromotion") or {}

    if category == "BOGO":
        values = native.get("buyXGetYItemPromotion") or {}
        buy = values.get("buyQuantity")
        free = values.get("getQuantity")
        if (native.get("type") != "buyXGetYItemPromotion"
                or type(buy) is not int or type(free) is not int
                or buy <= 0 or free <= 0 or buy > 100 or free > 100
                or level):
            return _invalid("SAME_SKU_BXGY", item, "INVALID_NATIVE_SAME_SKU_OFFER", source_hash)
        kind = "SAME_SKU_BXGY"
        original, payable, received = price * (buy + free), price * buy, buy + free
    elif category in ("DISCOUNTED_ITEM", "CATEGORY_DISCOUNT"):
        if native or level:
            return _invalid("DIRECT_DISCOUNT", item, "OVERLAPPING_PROMOTION", source_hash)
        old, current = _thumbnail_prices(item)
        if len(old) != 1 or len(current) != 1 or current != {price}:
            return _invalid("DIRECT_DISCOUNT", item, "MISSING_OR_CONFLICTING_PRICE_LABELS", source_hash)
        original = next(iter(old))
        if original <= price:
            return _invalid("DIRECT_DISCOUNT", item, "INVALID_STRIKETHROUGH_PRICE", source_hash)
        kind, buy, free, received, payable = "DIRECT_DISCOUNT", 1, 0, 1, price
    elif category in (None, "", "MISSING", "NONE"):
        # An unknown active campaign may hide a cart discount: do not label as NONE.
        if native or level or item.get("promoInfo"):
            return _invalid("UNKNOWN", item, "UNSUPPORTED_UNTYPED_PROMOTION", source_hash)
        # Store-written marketing in a title is not certified by Uber's promotion
        # engine, but it means we cannot certify no offer either.
        title = str(item.get("title") or "")
        if re.search(r"買\s*[一二三四五六七八九\d]+\s*送|buy\s*\d+\s*get", title, re.I):
            return _invalid("UNKNOWN", item, "UNVERIFIED_TITLE_PROMOTION", source_hash)
        kind, buy, free, received = "NONE", 1, 0, 1
        original = payable = price
    else:
        return _invalid("UNKNOWN", item, "UNSUPPORTED_PROMOTION_TYPE", source_hash)

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "VERIFIED_MENU",
        "kind": kind,
        "menu_price_cents": price,
        "buy_quantity": buy,
        "free_quantity": free,
        "received_quantity": received,
        "original_total_cents": original,
        "payable_total_cents": payable,
        "source_hash": source_hash,
    }


def quote_from_converted_item(item):
    """Reject legacy Json inferred by title/section rather than raw native API."""
    q = item.get("verified_price") if isinstance(item, dict) else None
    if not isinstance(q, dict) or q.get("schema_version") != SCHEMA_VERSION:
        return None
    if q.get("status") != "VERIFIED_MENU" or q.get("kind") not in AUTHORIZED_KINDS:
        return None
    original, payable, quantity = (q.get("original_total_cents"),
                                   q.get("payable_total_cents"),
                                   q.get("received_quantity"))
    if (type(original) is not int or type(payable) is not int
            or type(quantity) is not int or original <= 0
            or payable < 0 or payable > original or quantity <= 0
            or not re.fullmatch(r"[0-9a-f]{64}", str(q.get("source_hash") or ""))):
        return None
    return q


def quote_numbers(quote):
    """Returns exact Decimal values; round only when formatting in the UI."""
    original = Decimal(quote["original_total_cents"])
    payable = Decimal(quote["payable_total_cents"])
    received = Decimal(quote["received_quantity"])
    return {
        "effective_unit_price": payable / (received * 100),
        "discount_rate": (original - payable) * 100 / original,
    }


def reconcile_duplicate_skus(doc):
    """Never prefer one carousel's price/promo when duplicate SKU terms conflict."""
    grouped = {}
    for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
        for item in section.get("hasMenuItem") or []:
            sku = item.get("identifier")
            if not sku:
                continue
            quote = item.get("verified_price")
            signature = None
            if isinstance(quote, dict):
                signature = (
                    quote.get("status"), quote.get("kind"),
                    quote.get("menu_price_cents"),
                    quote.get("original_total_cents"),
                    quote.get("payable_total_cents"),
                    quote.get("received_quantity"),
                )
            grouped.setdefault(sku, []).append((signature, item))
    for entries in grouped.values():
        if len({repr(signature) for signature, _ in entries}) <= 1:
            continue
        for _, item in entries:
            item["verified_price"] = {
                "schema_version": 2, "status": "UNVERIFIED",
                "kind": "UNKNOWN", "reason": "CONFLICTING_DUPLICATE_SKU"
            }
    return doc


def ui_percent(value):
    return Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
