"""Capture all native Uber Eats catalog pages; never replace or forge the HTTP Raw bodies."""
from __future__ import annotations

import json
import time
from pathlib import Path
import requests

CATALOG_API = "https://www.ubereats.com/api/getCatalogPresentationV2"
MAX_CATALOG_PAGES = 250


def _section_ids(data):
    ids = []
    seen = set()
    for entries in (data.get("aisles") or {}).values():
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            section = entry.get("catalogSectionUUID")
            if isinstance(section, str) and section and section not in seen:
                ids.append(section)
                seen.add(section)
    return ids


def _catalog_entries(body):
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict) or not isinstance(data.get("catalog"), list):
        raise ValueError("getCatalogPresentationV2 returned no native catalog")
    return data, data["catalog"]


def _item_count(entries):
    return sum(len(((entry.get("payload") or {}).get("standardItemsPayload") or {})
                   .get("catalogItems") or []) for entry in entries if isinstance(entry, dict))


def capture_catalog(session, store_uuid, base_data, root, headers, capture_response):
    """Return immutable page paths; if any page is missing, raise and reject this batch.

    Uses category IDs provided by the same getStoreV1 response. Uber's first
    catalog-presentation page often contains most sections, but pagingInfo.hasMore
    still requires subsequent offset requests to avoid losing long-tail SKUs.
    """
    section_uuids = _section_ids(base_data)
    if not section_uuids:
        paging = base_data.get("catalogSectionPagingInfo") or {}
        if paging.get("hasMore"):
            raise ValueError("Store reports more menu pages but exposes no category IDs")
        return {"catalog_responses": [], "catalog_complete": True, "catalog_item_occurrences": 0}

    responses, offset, page, seen_offsets = [], 0, 0, set()
    use_collection_type = False
    while page < MAX_CATALOG_PAGES:
        if offset in seen_offsets:
            raise ValueError(f"Catalog pagination loop at offset={offset}")
        seen_offsets.add(offset)
        filters = {"storeUuid": store_uuid, "sectionUuids": section_uuids, "subsectionUuids": None}
        if use_collection_type:
            filters["sectionTypes"] = ["COLLECTION"]
        payload = {"sortAndFilters": None, "storeFilters": filters,
                   "pagingInfo": {"enabled": True, "offset": offset},
                   "source": "NV_L2_CATALOG"}
        body = None
        for attempt in range(1, 4):
            try:
                resp = session.post(CATALOG_API, json=payload, headers=headers, timeout=30)
                relative = (f"catalog/{store_uuid}/page-{page:04d}-offset-{offset:06d}-"
                            f"attempt-{attempt:02d}-status-{resp.status_code}.body")
                capture_response(root, relative, resp)
                if resp.status_code != 200:
                    if attempt < 3:
                        time.sleep(attempt * 2)
                        continue
                    raise RuntimeError(f"Catalog HTTP {resp.status_code}")
                body = resp.json()
                data, entries = _catalog_entries(body)
                paging_info = data.get("pagingInfo")
                if not isinstance(paging_info, dict) or type(paging_info.get("hasMore")) is not bool:
                    raise ValueError("Catalog pagination metadata missing; refusal to truncate")
                n = _item_count(entries)
                # Some collection-store responses are empty without sectionTypes,
                # while others only work without it. Try the other form once.
                if page == 0 and n == 0 and not use_collection_type:
                    use_collection_type = True
                    filters["sectionTypes"] = ["COLLECTION"]
                    try:
                        alt = session.post(CATALOG_API, json=payload, headers=headers, timeout=30)
                        alt_relative = (f"catalog/{store_uuid}/page-0000-offset-000000-"
                                        f"collection-status-{alt.status_code}.body")
                        capture_response(root, alt_relative, alt)
                        if alt.status_code == 200:
                            alt_body = alt.json()
                            alt_data, alt_entries = _catalog_entries(alt_body)
                            if (_item_count(alt_entries) > 0
                                    and isinstance(alt_data.get("pagingInfo"), dict)):
                                body, data, entries = alt_body, alt_data, alt_entries
                                relative, n = alt_relative, _item_count(alt_entries)
                            else:
                                use_collection_type = False
                        else:
                            use_collection_type = False
                    except (requests.RequestException, ValueError):
                        use_collection_type = False
                responses.append({"path": relative, "offset": offset, "item_count": n})
                break
            except (requests.RequestException, ValueError) as exc:
                if attempt >= 3:
                    raise RuntimeError(f"Catalog page {page} at offset={offset} failed") from exc
                time.sleep(attempt)
        if body is None:
            raise ValueError("Catalog body missing")
        paging_info = data["pagingInfo"]
        page += 1
        if not paging_info["hasMore"]:
            return {"catalog_responses": responses, "catalog_complete": True,
                    "catalog_section_count": len(section_uuids),
                    "catalog_item_occurrences": sum(x["item_count"] for x in responses)}
        next_offset = paging_info.get("offset")
        if not isinstance(next_offset, int) or next_offset <= offset:
            raise ValueError(f"Catalog pagination stalled at {offset} -> {next_offset}")
        offset = next_offset
    raise ValueError(f"Catalog exceeded {MAX_CATALOG_PAGES} pages; refusing partial batch")


def merge_catalog_pages(native, raw_bodies):
    """Create an in-memory normalized view from separately archived HTTP page bodies.

    Original getStoreV1 and getCatalogPresentationV2 bodies remain byte-for-byte
    preserved in Hugging Face Raw. The ETL view is *not* written back into Raw.
    """
    if not raw_bodies:
        return native
    groups = []
    for page in raw_bodies:
        data, entries = _catalog_entries(page)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            payload = entry.get("payload") or {}
            standard = payload.get("standardItemsPayload") or {}
            items = standard.get("catalogItems") or []
            if not isinstance(items, list) or not items:
                continue
            groups.append(entry)
    if not groups:
        raise ValueError("Catalog pages yielded no products; cannot erase a menu")
    native = dict(native)
    original = native.get("catalogSectionsMap") or {}
    # Prefer the category API's complete, priced items. Keep original sections
    # too, because curated or featured items may not exist in regular aisles.
    native["catalogSectionsMap"] = {"v2_catalog": groups, **original}
    return native
