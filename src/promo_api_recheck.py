"""Same-run, API-only verification of abrupt promotion disappearance.

This module never calls a browser, never guesses an earlier price, and never
rewrites an original HTTP response.  A failed second request is UNAVAILABLE,
not evidence that the first response was wrong.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

from verified_prices import quote_from_native_item

TW = timezone(timedelta(hours=8))
PROMOTIONS = {"BOGO", "DISCOUNTED_ITEM", "CATEGORY_DISCOUNT"}
QUARANTINE_STATUSES = frozenset({"CONFLICT", "UNAVAILABLE"})


def _catalog_items(groups):
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        payload = group.get("payload") or {}
        standard = payload.get("standardItemsPayload") or {}
        for item in standard.get("catalogItems") or []:
            if isinstance(item, dict):
                yield item


def _signature(item):
    sku = item.get("uuid") or item.get("itemUuid") or item.get("id")
    if not isinstance(sku, str) or not sku:
        return None
    raw_price = item.get("price")
    if type(raw_price) not in (int, float) or not 0 < raw_price < 1_000_000_000:
        return None
    price_cents = int(Decimal(str(raw_price)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    analytics = item.get("catalogItemAnalyticsData") or {}
    kind = str(analytics.get("promoType") or "NONE")
    native = item.get("itemPromotion") or {}
    bogo = native.get("buyXGetYItemPromotion") or {}
    buy = bogo.get("buyQuantity") if kind == "BOGO" else None
    free = bogo.get("getQuantity") if kind == "BOGO" else None
    # Compare the SAME original and payable price evidence the ETL uses.
    # A direct discount with a different strike-through original price must
    # not pass merely because the current payable amount is unchanged.
    verified = quote_from_native_item(item)
    verified_status = (verified or {}).get("status", "UNVERIFIED")
    original_cents = (verified or {}).get("original_total_cents")
    payable_cents = (verified or {}).get("payable_total_cents")
    received_qty = (verified or {}).get("received_quantity")
    validation_reason = (verified or {}).get("reason") if verified_status != "VERIFIED_MENU" else None
    if kind == "BOGO":
        rank = 3
    elif kind in ("DISCOUNTED_ITEM", "CATEGORY_DISCOUNT"):
        rank = 2
    elif kind not in ("NONE", "MISSING", ""):
        rank = 1
    else:
        rank = 0
    # Include native price/promotion fields. The raw bodies themselves remain
    # the authoritative, immutable evidence in HF.
    return sku, (
        price_cents, kind, buy, free,
        verified_status, original_cents, payable_cents, validation_reason,
        received_qty,
    ), rank


def read_capture_view(raw_root: Path, store: dict) -> dict:
    """Read actual saved getStoreV1 and getCatalogPresentationV2 bodies.

    Prefer the full category API over the smaller homepage preview, as the
    existing ingestion adapter does.  Never infer promotional offers from titles.
    """
    if store.get("success") is not True or store.get("inactive"):
        raise ValueError("No successfully captured active store to verify")
    base_path = store.get("response_path")
    if not base_path:
        raise ValueError("Missing store API response path")
    base = json.loads((raw_root / base_path).read_bytes())
    base_data = base.get("data")
    if not isinstance(base_data, dict):
        raise ValueError("Invalid saved store API data")
    native = {}
    for sections in (base_data.get("catalogSectionsMap") or {}).values():
        for item in _catalog_items(sections):
            sig = _signature(item)
            if sig is None:
                continue
            sku, offer, rank = sig
            old = native.get(sku)
            if old is None or rank > old[1]:
                native[sku] = (offer, rank)
    catalog = {}
    for page in store.get("catalog_responses") or []:
        body = json.loads((raw_root / page["path"]).read_bytes())
        data = body.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("catalog"), (list, type(None))):
            raise ValueError("Invalid saved native catalog page")
        for item in _catalog_items(data.get("catalog") or []):
            sig = _signature(item)
            if sig is None:
                continue
            sku, offer, rank = sig
            old = catalog.get(sku)
            if old is None or rank > old[1]:
                catalog[sku] = (offer, rank)
    # V2 is the primary full-catalog source; homepage-only SKUs are retained.
    selected = {sku: val[0] for sku, val in native.items()}
    selected.update({sku: val[0] for sku, val in catalog.items()})
    if not selected:
        raise ValueError("Saved API catalog contains no valid priced items")
    counts = {"total_skus": len(selected), "bogo": 0,
              "direct_discount": 0, "other_promo": 0}
    for price, kind, buy, free, *_ in selected.values():
        if kind == "BOGO":
            counts["bogo"] += 1
        elif kind in ("DISCOUNTED_ITEM", "CATEGORY_DISCOUNT"):
            counts["direct_discount"] += 1
        elif kind not in ("NONE", "MISSING", ""):
            counts["other_promo"] += 1
    counts["signature_sha256"] = hashlib.sha256(
        json.dumps(sorted(selected.items()), separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()
    # Retain ONLY native-verified promoted SKUs. This compact historic witness
    # lets the next run catch one vanished BOGO without requiring a full Raw
    # archive download or changing the seven-day price formula.
    watch = {}
    for sku, evidence in selected.items():
        _, kind, _, _, status, _, payable, _, received = evidence
        if (kind in PROMOTIONS and status == "VERIFIED_MENU"
                and type(payable) is int and payable > 0
                and type(received) is int and received > 0):
            watch[sku] = [payable, received]
    return {"stats": counts, "items": selected, "promotion_watch": watch}


def trigger_reason(previous: dict | None, current: dict,
                   prior_batch: str = "", now: datetime | None = None,
                   *, previous_watch: dict | None = None,
                   current_items: dict | None = None) -> str | None:
    """Trigger only a repeat *check*, not an automatic error classification."""
    now = now or datetime.now(TW)
    if previous and previous.get("_recheck_required") is True:
        # Last run could not confirm the API result; do not allow a later
        # run to silently treat that unconfirmed state as an accepted baseline.
        return "previous_api_recheck_inconclusive"
    if previous:
        try:
            previous_at = datetime.strptime(prior_batch, "%Y%m%d%H%M%S").replace(tzinfo=TW)
            if timedelta(0) <= now - previous_at <= timedelta(hours=72):
                # The store's overall deal count might barely change even
                # while one previously promoted SKU suddenly doubles its
                # effective unit price. Check that specific SKU first.
                if isinstance(previous_watch, dict) and isinstance(current_items, dict):
                    for sku, old in previous_watch.items():
                        if sku not in current_items:
                            continue  # Missing product is not a verified price increase.
                        if (not isinstance(old, (list, tuple)) or len(old) != 2
                                or type(old[0]) is not int or type(old[1]) is not int
                                or old[0] <= 0 or old[1] <= 0):
                            continue
                        new = current_items[sku]
                        if not isinstance(new, (list, tuple)) or len(new) < 9:
                            return "previously_promoted_sku_incomplete"
                        if new[4] != "VERIFIED_MENU":
                            return "previously_promoted_sku_unverified"
                        payable, qty = new[6], new[8]
                        if (type(payable) is not int or type(qty) is not int
                                or payable <= 0 or qty <= 0):
                            return "previously_promoted_sku_invalid_price"
                        if payable * old[1] > old[0] * qty:
                            return "previously_promoted_sku_price_increase"
                if current["total_skus"] >= 100:
                    for field in ("bogo", "direct_discount"):
                        before = int(previous.get(field) or 0)
                        after = int(current.get(field) or 0)
                        if before >= 20 and after * 5 <= before:
                            return f"{field}_drop_{before}_to_{after}"
        except (ValueError, TypeError, KeyError):
            pass
    # First rollout or missing recent baseline: a large, apparently promotion-
    # free catalog is unusual enough to justify one independent API retry.
    if (current["total_skus"] >= 1200 and current["bogo"] == 0
            and current["direct_discount"] == 0):
        return "large_catalog_zero_item_promotions"
    return None


def compare_views(first: dict, second: dict) -> dict:
    a, b = first["items"], second["items"]
    overlap = set(a) & set(b)
    mismatch = sorted(sku for sku in overlap if a[sku] != b[sku])
    coverage = len(overlap) / max(len(a), len(b), 1)
    # A partial API response is an unavailable/incomplete cross-check, not a
    # contradiction proving the first capture was wrong. Never call 90% good
    # enough: a missing 10% can contain the entire promoted category.
    missing = sorted(set(a) - set(b))
    added = sorted(set(b) - set(a))
    if missing or added:
        status = "UNAVAILABLE"
    elif mismatch:
        status = "CONFLICT"
    else:
        status = "MATCHED"
    return {
        "status": status,
        "first_skus": len(a), "second_skus": len(b),
        "common_skus": len(overlap),
        "common_coverage_pct": round(100 * coverage, 2),
        "missing_in_second": len(missing),
        "new_in_second": len(added),
        "missing_uuid_samples": missing[:12],
        "new_uuid_samples": added[:12],
        "price_or_promo_mismatches": len(mismatch),
        "mismatch_uuid_samples": mismatch[:12],
    }


def verify_suspicious_store(store_item: dict, raw_root: Path, original: dict,
                            previous: dict | None, previous_batch: str = "",
                            fetch_again=None, now: datetime | None = None,
                            *, previous_watch: dict | None = None) -> dict:
    """Capture again immediately, using fetch_store_raw's fresh HTTP Session.

    The second original HTTP record set is stored under raw/promo-recheck/...
    and included in the normal immutable SHA-256 archive.
    """
    if original.get("success") is not True or original.get("inactive"):
        return original
    try:
        first = read_capture_view(raw_root, original)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        # Never convert a locally unreadable first record into a claim that
        # the upstream API incorrectly priced the products.
        reason = type(exc).__name__ + ": " + str(exc)[:180]
        original["promotion_stats_error"] = reason
        # A successfully fetched but unreadable native menu is NOT a confirmed
        # valid price observation. Retain the prior DB state and original Raw.
        original["promo_recheck"] = {
            "status": "UNAVAILABLE", "trigger": "initial_native_parse_unavailable",
            "error": reason, "previous_stats": previous,
            "previous_batch": previous_batch,
            "previous_watch": previous_watch if isinstance(previous_watch, dict) else {},
        }
        return original
    original["promotion_stats"] = first["stats"]
    original["promotion_watch"] = first["promotion_watch"]
    reason = trigger_reason(
        previous, first["stats"], previous_batch, now,
        previous_watch=previous_watch, current_items=first["items"])
    if not reason:
        return original
    if fetch_again is None:
        from raw_capture import fetch_store_raw
        fetch_again = fetch_store_raw
    sid = original["store_uuid"]
    retry_root = raw_root / "promo-recheck" / sid
    retry_info = {"status": "UNAVAILABLE", "trigger": reason,
                  "previous_batch": previous_batch,
                  "previous_stats": previous, "first_stats": first["stats"],
                  "raw_prefix": f"promo-recheck/{sid}/"}
    try:
        repeated = fetch_again(store_item, retry_root)
        if repeated.get("success") is True and repeated.get("store_uuid") != sid:
            retry_info["error"] = "Recheck returned different store UUID"
        elif repeated.get("success") is not True or repeated.get("inactive"):
            retry_info["error"] = str(repeated.get("error") or "API retry returned no valid store")[:250]
        else:
            second = read_capture_view(retry_root, repeated)
            retry_info["second_stats"] = second["stats"]
            retry_info["comparison"] = compare_views(first, second)
            retry_info["status"] = retry_info["comparison"]["status"]
            retry_info["second_response_path"] = retry_info["raw_prefix"] + repeated["response_path"]
            retry_info["second_catalog_response_paths"] = [
                retry_info["raw_prefix"] + page["path"]
                for page in repeated.get("catalog_responses") or []
            ]
            retry_info["second_catalog_pages"] = len(retry_info["second_catalog_response_paths"])
    except (OSError, ValueError, TypeError, KeyError, RuntimeError, json.JSONDecodeError) as exc:
        retry_info["error"] = type(exc).__name__ + ": " + str(exc)[:250]
    if retry_info["status"] in QUARANTINE_STATUSES:
        # Keep the last trusted watch for the next batch; the unconfirmed
        # first capture cannot become tomorrow's baseline.
        retry_info["previous_watch"] = (
            previous_watch if isinstance(previous_watch, dict) else {})
    original["promo_recheck"] = retry_info
    log_record = {
        k: v for k, v in retry_info.items() if k != "previous_watch"
    }
    if "previous_watch" in retry_info:
        log_record["previous_watch_count"] = len(retry_info["previous_watch"])
    print("FOODRADAR_PROMO_API_RECHECK=" + json.dumps({
        "store_uuid": sid, **log_record,
    }, ensure_ascii=False), flush=True)
    return original
