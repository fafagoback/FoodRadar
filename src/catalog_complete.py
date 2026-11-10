"""Capture all native Uber Eats catalog pages; never replace or forge the HTTP Raw bodies."""
from __future__ import annotations

import json
import time
from pathlib import Path
import requests
from menu_core import global_limiter

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
    if not isinstance(data, dict) or "catalog" not in data:
        raise ValueError("getCatalogPresentationV2 is missing its catalog field")
    paging = data.get("pagingInfo")
    if not isinstance(paging, dict) or type(paging.get("hasMore")) is not bool:
        raise ValueError("getCatalogPresentationV2 has invalid paging metadata")
    catalog = data["catalog"]
    # Uber legitimately returns catalog=null on empty *intermediate* and
    # terminal pages. The response is HTTP 200 with a valid pagingInfo object;
    # use hasMore/offset, not catalog existence, to determine completion.
    if catalog is None:
        catalog = []
    if not isinstance(catalog, list):
        raise ValueError("getCatalogPresentationV2 catalog has unsupported type")
    return data, catalog


def _item_count(entries):
    return sum(len(((entry.get("payload") or {}).get("standardItemsPayload") or {})
                   .get("catalogItems") or []) for entry in entries if isinstance(entry, dict))


def capture_catalog(session, store_uuid, base_data, root, headers, capture_response, store_attempt=1):
    """Capture every category separately, preserving original HTTP response bytes.

    A grouped sectionUuids request is NOT complete merely because its pagination
    reports hasMore=false: it can return only a small fraction of a large store.
    Each category needs its own independent offset=0..terminal traversal.
    """
    section_uuids = _section_ids(base_data)
    if not section_uuids:
        paging = base_data.get("catalogSectionPagingInfo") or {}
        if paging.get("hasMore"):
            raise ValueError("Store reports more menu pages but exposes no category IDs")
        return {"catalog_responses": [], "catalog_complete": True,
                "catalog_section_count": 0, "catalog_sections_finished": 0,
                "catalog_section_reports": [], "catalog_item_occurrences": 0}

    responses, section_reports = [], []
    for section_index, section_uuid in enumerate(section_uuids):
        offset, page, seen_offsets = 0, 0, set()
        use_collection_type = False
        category_occurrences = 0
        while page < MAX_CATALOG_PAGES:
            if offset in seen_offsets:
                raise ValueError(
                    f"Catalog paging loop in section {section_uuid} at offset={offset}")
            seen_offsets.add(offset)
            filters = {"storeUuid": store_uuid,
                       "sectionUuids": [section_uuid], "subsectionUuids": None}
            if use_collection_type:
                filters["sectionTypes"] = ["COLLECTION"]
            payload = {"sortAndFilters": None, "storeFilters": filters,
                       "pagingInfo": {"enabled": True, "offset": offset},
                       "source": "NV_L2_CATALOG"}
            body, data = None, None
            for attempt in range(1, 4):
                try:
                    global_limiter.wait()
                    resp = session.post(CATALOG_API, json=payload,
                                        headers=headers, timeout=30)
                    relative = (
                        f"catalog/{store_uuid}/store-attempt-{store_attempt:02d}-"
                        f"section-{section_index:03d}-{section_uuid}-"
                        f"page-{page:04d}-offset-{offset:06d}-"
                        f"attempt-{attempt:02d}-status-{resp.status_code}.body")
                    capture_response(root, relative, resp)
                    if resp.status_code != 200:
                        if resp.status_code == 429:
                            global_limiter.trigger_backoff(attempt * 8, "Catalog HTTP 429")
                        raise ValueError(f"Catalog HTTP {resp.status_code}")
                    body = resp.json()
                    data, entries = _catalog_entries(body)
                    n = _item_count(entries)
                    # Some collection categories need sectionTypes=COLLECTION.
                    # Persist both attempts; only the chosen body is in manifest.
                    if page == 0 and n == 0 and not use_collection_type:
                        filters["sectionTypes"] = ["COLLECTION"]
                        try:
                            global_limiter.wait()
                            alt = session.post(CATALOG_API, json=payload,
                                               headers=headers, timeout=30)
                            alt_relative = (
                                f"catalog/{store_uuid}/store-attempt-{store_attempt:02d}-"
                                f"section-{section_index:03d}-{section_uuid}-"
                                f"page-0000-offset-000000-collection-"
                                f"status-{alt.status_code}.body")
                            capture_response(root, alt_relative, alt)
                            if alt.status_code == 200:
                                alternative, alt_entries = _catalog_entries(alt.json())
                                if _item_count(alt_entries) > 0:
                                    body = {"data": alternative}
                                    data, entries = alternative, alt_entries
                                    relative, n = alt_relative, _item_count(alt_entries)
                                    use_collection_type = True
                                else:
                                    filters.pop("sectionTypes", None)
                            else:
                                filters.pop("sectionTypes", None)
                        except (requests.RequestException, ValueError):
                            filters.pop("sectionTypes", None)
                    responses.append({
                        "path": relative, "section_uuid": section_uuid,
                        "section_index": section_index, "page": page,
                        "offset": offset, "item_count": n})
                    category_occurrences += n
                    break
                except (requests.RequestException, ValueError) as exc:
                    if attempt >= 3:
                        raise RuntimeError(
                            f"Catalog section {section_uuid}, page {page}, "
                            f"offset {offset} failed") from exc
                    time.sleep(attempt)
            if data is None:
                raise ValueError(f"Catalog section {section_uuid} returned no data")
            paging = data["pagingInfo"]
            page += 1
            if paging["hasMore"] is False:
                section_reports.append({
                    "section_uuid": section_uuid,
                    "pages": page,
                    "item_occurrences": category_occurrences,
                    "terminal_has_more": False})
                break
            next_offset = paging.get("offset")
            if not isinstance(next_offset, int) or next_offset <= offset:
                raise ValueError(
                    f"Catalog section {section_uuid} offset stalled: "
                    f"{offset} -> {next_offset}")
            offset = next_offset
        else:
            raise ValueError(
                f"Catalog section {section_uuid} exceeded "
                f"{MAX_CATALOG_PAGES} pages")

    if len(section_reports) != len(section_uuids):
        raise ValueError("Not all catalog sections reached a terminal page")
    return {
        "catalog_responses": responses,
        "catalog_complete": True,
        "catalog_section_count": len(section_uuids),
        "catalog_sections_finished": len(section_reports),
        "catalog_section_reports": section_reports,
        "catalog_item_occurrences": sum(x["item_count"] for x in responses),
    }


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
