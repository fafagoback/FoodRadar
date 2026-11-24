"""Independent recheck of suspicious SKU evidence without re-crawling unrelated pages.

Only replay exact native requests actually observed in the same first capture.
Whole-store anomalies, missing request provenance and moved products still
require the existing full-catalog independent check; never bless incomplete
or inconsistent evidence.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
import requests

from menu_core import attach_delivery_location, global_limiter
from catalog_complete import CATALOG_API, _catalog_entries, _check_rate_limit, _UpstreamRateLimit
from promo_api_recheck import _catalog_items, _signature, trigger_reason

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
           "Accept": "*/*", "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
           "x-csrf-token": "x", "Content-Type": "application/json"}


def suspicious_skus(previous, first, previous_batch, now, previous_watch):
    if not isinstance(previous_watch, dict):
        return []
    # Never downgrade a whole-store or previously inconclusive check.
    if trigger_reason(previous, first["stats"], previous_batch, now):
        return []
    return sorted(sid for sid, old in previous_watch.items()
                  if trigger_reason(previous, first["stats"], previous_batch, now,
                     previous_watch={sid: old}, current_items=first["items"]))


def signatures(groups, wanted):
    result = {}
    for item in _catalog_items(groups):
        sig = _signature(item)
        if sig is None or sig[0] not in wanted:
            continue
        sku, value, _ = sig
        result.setdefault(sku, []).append(value)
    return {sku: sorted(values, key=repr) for sku, values in result.items()}


def base_signatures(body, wanted):
    data = body.get("data")
    if not isinstance(data, dict) or not (data.get("title") or data.get("catalogSectionsMap")):
        raise ValueError("Invalid native store preview")
    return signatures([group for groups in (data.get("catalogSectionsMap") or {}).values()
                       for group in groups], wanted)


def plan_target_pages(raw_root, original, targets):
    wanted = set(targets)
    if not wanted or len(wanted) > 20:
        return None
    base = json.loads((raw_root / original["response_path"]).read_bytes())
    initial_base = base_signatures(base, wanted)
    pages = original.get("catalog_responses") or []
    matched = set()
    selected = []
    for page in pages:
        body = json.loads((raw_root / page["path"]).read_bytes())
        data, groups = _catalog_entries(body)
        items = signatures(groups, wanted)
        if not items:
            continue
        payload = page.get("request_payload")
        if not isinstance(payload, dict):
            return None
        filters = payload.get("storeFilters") or {}
        paging = payload.get("pagingInfo") or {}
        if (filters.get("storeUuid") != original["store_uuid"]
                or paging.get("enabled") is not True or paging.get("offset") != page.get("offset")
                or type(paging.get("offset")) is not int or not 0 <= paging["offset"] < 1000
                or payload.get("source") != "NV_L2_CATALOG"):
            return None
        if page.get("subsection_uuid"):
            if (filters.get("subsectionUuids") != [page["subsection_uuid"]]
                    or filters.get("sectionUuids") is not None):
                return None
        elif (filters.get("sectionUuids") != [page.get("section_uuid")]
              or filters.get("subsectionUuids") is not None):
            return None
        selected.append((payload, items, data["pagingInfo"]))
        matched.update(items)
    # A homepage-only SKU in a full paginated catalog has no independently
    # proven exact query location; keep the old full verification in that case.
    if (pages and matched != wanted) or (not pages and set(initial_base) != wanted):
        return None
    if pages and len(selected) * 2 >= len(pages):
        return None
    return initial_base, selected


def fetch_body(session, url, payload, folder, tag):
    from raw_capture import capture_response
    retries = max(3, min(12, int(os.getenv("FOODRADAR_CATALOG_429_RETRIES", "6"))))
    for attempt in range(1, retries + 1):
        try:
            global_limiter.wait()
            response = session.post(url, json=payload, headers=HEADERS, timeout=30)
            path = f"{tag}-attempt-{attempt:02d}-status-{response.status_code}.body"
            capture_response(folder, path, response)
            body = response.json() if response.status_code == 200 else None
            _check_rate_limit(response, body, attempt)
            if response.status_code != 200 or not isinstance(body, dict) or body.get("status") == "failure":
                raise ValueError("Independent native response unavailable")
            global_limiter.record_success()
            return path, body
        except (requests.RequestException, ValueError) as exc:
            limit = retries if isinstance(exc, _UpstreamRateLimit) else 3
            if attempt >= limit:
                raise RuntimeError("Targeted promotion request failed") from exc
            if not isinstance(exc, _UpstreamRateLimit):
                time.sleep(attempt)


def targeted_recheck(item, raw_root, original, targets, retry_root,
                     session_factory=requests.Session, fetch=fetch_body):
    """Return exact SKU proof, None if full recheck is necessary."""
    plan = plan_target_pages(raw_root, original, targets)
    if plan is None:
        return None
    expected_base, pages = plan
    sid = original["store_uuid"]
    wanted = set(targets)
    prefix = f"promo-recheck/{sid}/"
    with session_factory() as session:
        if not attach_delivery_location(session, item):
            raise RuntimeError("Independent promotion check lacks delivery coordinate")
        base_path, base = fetch(session, "https://www.ubereats.com/api/getStoreV1",
                                {"storeUuid": sid, "sfNuggetCount": 24}, retry_root, "target-store")
        actual_base = base_signatures(base, wanted)
        if set(actual_base) != set(expected_base):
            return None  # SKU moved or appeared in preview, require full proof.
        mismatch = {sku for sku in actual_base if actual_base[sku] != expected_base[sku]}
        paths = []
        covered = set(actual_base)
        for index, (payload, expected, old_paging) in enumerate(pages):
            path, body = fetch(session, CATALOG_API, payload, retry_root,
                               f"target-catalog/page-{index:04d}")
            data, groups = _catalog_entries(body)
            actual = signatures(groups, wanted)
            if set(actual) != set(expected) or data["pagingInfo"] != old_paging:
                return None  # Changed pagination is not a contradiction.
            paths.append(prefix + path)
            covered.update(actual)
            mismatch.update(sku for sku in actual if actual[sku] != expected[sku])
        if covered != wanted:
            return None
        status = "CONFLICT" if mismatch else "MATCHED"
        return {"status": status, "mode": "targeted_sku", "target_skus": sorted(wanted),
                "second_response_path": prefix + base_path,
                "second_catalog_response_paths": paths, "second_catalog_pages": len(paths),
                "comparison": {"status": status, "checked_skus": len(wanted),
                               "price_or_promo_mismatches": len(mismatch),
                               "mismatch_uuid_samples": sorted(mismatch)[:12],
                               "checked_pages": len(paths)}}
