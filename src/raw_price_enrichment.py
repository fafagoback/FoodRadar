"""Attach native getStoreV1 item evidence when replaying immutable HF ApiRaw archives.

Legacy Json-only archives are deliberately NOT treated as certified promotions.
"""
from __future__ import annotations
import copy
from verified_prices import quote_from_native_item


def enrich_converted_with_native(converted_doc, api_wrapper):
    doc = copy.deepcopy(converted_doc)
    native = (api_wrapper or {}).get("data") or {}
    observed = {}
    for sections in (native.get("catalogSectionsMap") or {}).values():
        if not isinstance(sections, list):
            continue
        for section in sections:
            items = ((section.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or []
            for item in items:
                sku = item.get("uuid") or item.get("itemUuid") or item.get("id")
                if not sku:
                    continue
                q = quote_from_native_item(item)
                signature = (q.get("status"), q.get("kind"),
                             q.get("menu_price_cents"), q.get("original_total_cents"),
                             q.get("payable_total_cents"), q.get("received_quantity"))
                if sku in observed and observed[sku][0] != signature:
                    # Conflicting product appearances: never choose arbitrary section order.
                    observed[sku] = (None, {
                        "schema_version": 2, "status": "UNVERIFIED",
                        "kind": "UNKNOWN", "reason": "CONFLICTING_DUPLICATE_SKU",
                    })
                elif sku not in observed:
                    observed[sku] = (signature, q)
    for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
        for item in section.get("hasMenuItem") or []:
            sku = item.get("identifier")
            if sku in observed:
                item["verified_price"] = copy.deepcopy(observed[sku][1])
    return doc
