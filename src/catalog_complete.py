"""Capture all native Uber Eats catalog pages; never replace or forge the HTTP Raw bodies."""
from __future__ import annotations

import json
import time
from pathlib import Path
import requests
from menu_core import global_limiter

CATALOG_API = "https://www.ubereats.com/api/getCatalogPresentationV2"
MAX_CATALOG_PAGES = 250
DEEP_OFFSET_BOUNDARY = 1000


def is_recoverable_catalog_failure(error):
    """Known catalog partition failures eligible for an *explicitly marked*
    partial Raw snapshot. A failed store is never published as freshly
    verified and its prior data stays unchanged. Do not whitelist general
    network/timeouts, 403, invalid HTTP or unknown parsing failures.
    """
    message = str(error or "")
    return any(marker in message for marker in (
        "native code 500",
        "previously seen native parent SKUs",
        "native subsection UUIDs unavailable",
    ))



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


class _DeepOffsetRejected(ValueError):
    """Uber returned a verified application-level 500 for a deep page."""


class _UpstreamRateLimit(ValueError):
    """HTTP 429 or Uber's HTTP-200 application error code 429."""


def _check_rate_limit(response, body, attempt):
    data = body.get("data") if isinstance(body, dict) else None
    application_code = (
        str(data.get("code") or "") if isinstance(data, dict)
        and body.get("status") == "failure" else ""
    )
    if response.status_code == 429 or application_code == "429":
        # Uber often sends HTTP 200 + data.code=429. A short local retry alone
        # can keep every worker throttled; pause all threads instead.
        seconds = min(120, 15 * (2 ** (attempt - 1)))
        kind = "HTTP 429" if response.status_code == 429 else "application 429"
        global_limiter.trigger_backoff(seconds, f"Catalog {kind}")
        raise _UpstreamRateLimit(f"Catalog {kind} (retry {attempt})")


def _subsections_for(base_data, section_uuid):
    """Only accept real subsection UUIDs supplied by getStoreV1."""
    for section in base_data.get("sections") or []:
        if isinstance(section, dict) and section.get("uuid") == section_uuid:
            return list(dict.fromkeys(
                x for x in (section.get("subsectionUuids") or [])
                if isinstance(x, str) and x))
    return []


def _native_subsections_from_catalog(raw_bodies, parent_uuid):
    """Read real native subsection UUIDs from already captured parent HTTP bodies.

    getStoreV1.sections can omit late categories even while aisles exposes them.
    In that case, getCatalogPresentationV2's actual product groups and items
    carry their native subsection UUIDs. Never invent or cache identifiers.
    Do not treat the parent UUID itself as a leaf: Uber can return an empty
    response for subsectionUuids=[parent] despite parent items existing.
    """
    ids = []
    seen = set()
    for body in raw_bodies:
        _, groups = _catalog_entries(body)
        for group in groups:
            if not isinstance(group, dict):
                continue
            standard = ((group.get("payload") or {}).get("standardItemsPayload") or {})
            items = standard.get("catalogItems") or []
            for item in items:
                if not isinstance(item, dict):
                    continue
                native_id = item.get("subsectionUuid") or group.get("catalogSectionUUID")
                if (isinstance(native_id, str) and native_id
                        and native_id != parent_uuid and native_id not in seen):
                    ids.append(native_id)
                    seen.add(native_id)
    return ids


def _catalog_skus(raw_bodies):
    skus = set()
    for body in raw_bodies:
        _, entries = _catalog_entries(body)
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            items = (((entry.get("payload") or {})
                      .get("standardItemsPayload") or {}).get("catalogItems") or [])
            for item in items:
                if isinstance(item, dict):
                    sku = item.get("uuid") or item.get("itemUuid") or item.get("id")
                    if isinstance(sku, str) and sku:
                        skus.add(sku)
    return skus


def capture_catalog(session, store_uuid, base_data, root, headers, capture_response, store_attempt=1):
    """Fetch every native category; split deep failing categories by real subsections.

    Uber's getCatalogPresentationV2 can return HTTP 200 + status=failure /
    data.code=500 at offset>1000. This is not a terminal page. Instead of
    dropping items or claiming completion, fetch each of its native subsections
    to a terminal page, and verify that all earlier parent UUIDs are retained.
    Never fabricate a category, response, or a completed status.
    """
    section_uuids = _section_ids(base_data)
    if not section_uuids:
        paging = base_data.get("catalogSectionPagingInfo") or {}
        if paging.get("hasMore"):
            raise ValueError("Store reports more menu pages but exposes no category IDs")
        return {"catalog_responses": [], "catalog_complete": True,
                "catalog_section_count": 0, "catalog_sections_finished": 0,
                "catalog_section_reports": [], "catalog_item_occurrences": 0}

    def collect(section_index, section_uuid, subsection_uuid=None, stop_before_offset=None):
        records, bodies, seen_offsets = [], [], set()
        offset, page, use_collection_type = 0, 0, False
        while page < MAX_CATALOG_PAGES:
            # Never ask the upstream API for the known deep-offset boundary.
            # An unfinished partition is NOT complete until native children are
            # fetched to terminal pagination and audited against parent SKUs.
            if offset >= DEEP_OFFSET_BOUNDARY or (stop_before_offset is not None and offset >= stop_before_offset):
                return records, bodies, False
            if offset in seen_offsets:
                raise ValueError(f"Catalog loop at {section_uuid}/{subsection_uuid} {offset}")
            seen_offsets.add(offset)
            # Live Uber API probes (2026-11-11) verified that
            # sectionUuids=[parent] + subsectionUuids=[child] can yield zero
            # items, whereas subsectionUuids=[child] alone returns real SKUs.
            # Never constrain a genuine child with its parent simultaneously.
            filters = {"storeUuid": store_uuid,
                       "sectionUuids": ([section_uuid] if subsection_uuid is None else None),
                       "subsectionUuids": ([subsection_uuid] if subsection_uuid else None)}
            if use_collection_type:
                filters["sectionTypes"] = ["COLLECTION"]
            payload = {"sortAndFilters": None, "storeFilters": filters,
                       "pagingInfo": {"enabled": True, "offset": offset},
                       "source": "NV_L2_CATALOG"}
            body, data, n, relative = None, None, 0, None
            for attempt in range(1, 7):
                try:
                    global_limiter.wait()
                    resp = session.post(CATALOG_API, json=payload,
                                        headers=headers, timeout=30)
                    relative = (
                        f"catalog/{store_uuid}/store-attempt-{store_attempt:02d}-"
                        f"section-{section_index:03d}-{section_uuid}-"
                        f"sub-{subsection_uuid or 'none'}-"
                        f"page-{page:04d}-offset-{offset:06d}-"
                        f"attempt-{attempt:02d}-status-{resp.status_code}.body")
                    capture_response(root, relative, resp)
                    if resp.status_code != 200:
                        _check_rate_limit(resp, None, attempt)
                        raise ValueError(f"Catalog HTTP {resp.status_code}")
                    body = resp.json()
                    _check_rate_limit(resp, body, attempt)
                    failure = body.get("data") if isinstance(body, dict) else None
                    if (offset >= 1000 and isinstance(failure, dict)
                            and body.get("status") == "failure"
                            and str(failure.get("code") or "") == "500"):
                        raise _DeepOffsetRejected(
                            f"Catalog {section_uuid}/{subsection_uuid} "
                            f"offset {offset}: native code 500")
                    # Some native deep-offset failures provide no catalog and
                    # encode 500 as a string. Preserve the response and try
                    # verified native subsections instead of retrying a
                    # deterministic malformed parent page multiple times.
                    if (offset >= 1000 and isinstance(body, dict)
                            and body.get("status") == "failure"
                            and isinstance(failure, dict)
                            and "catalog" not in failure
                            and str(failure.get("code") or "") in ("500", "502", "503")):
                        raise _DeepOffsetRejected(
                            f"Catalog {section_uuid}/{subsection_uuid} "
                            f"offset {offset}: native code 500 (deep-page failure)")
                    data, entries = _catalog_entries(body)
                    global_limiter.record_success()
                    n = _item_count(entries)
                    if page == 0 and n == 0 and not use_collection_type:
                        filters["sectionTypes"] = ["COLLECTION"]
                        try:
                            global_limiter.wait()
                            alt = session.post(CATALOG_API, json=payload,
                                               headers=headers, timeout=30)
                            alt_relative = (
                                f"catalog/{store_uuid}/store-attempt-{store_attempt:02d}-"
                                f"section-{section_index:03d}-{section_uuid}-"
                                f"sub-{subsection_uuid or 'none'}-"
                                f"page-0000-offset-000000-collection-"
                                f"attempt-{attempt:02d}-status-{alt.status_code}.body")
                            capture_response(root, alt_relative, alt)
                            if alt.status_code == 200:
                                alt_body = alt.json()
                                _check_rate_limit(alt, alt_body, attempt)
                                alt_data, alt_entries = _catalog_entries(alt_body)
                                global_limiter.record_success()
                                if _item_count(alt_entries) > 0:
                                    body, data, entries = alt_body, alt_data, alt_entries
                                    relative, n = alt_relative, _item_count(alt_entries)
                                    use_collection_type = True
                                else:
                                    filters.pop("sectionTypes", None)
                            else:
                                _check_rate_limit(alt, None, attempt)
                                filters.pop("sectionTypes", None)
                        except _UpstreamRateLimit:
                            # Retry the entire page rather than silently accepting
                            # an unverified empty category when alternate mode is throttled.
                            raise
                        except (requests.RequestException, ValueError):
                            filters.pop("sectionTypes", None)
                    records.append({"path": relative, "section_uuid": section_uuid,
                                    "subsection_uuid": subsection_uuid,
                                    "section_index": section_index, "page": page,
                                    "offset": offset, "item_count": n})
                    bodies.append(body)
                    break
                except _DeepOffsetRejected:
                    # A verified deterministic error. Do not waste 3 identical retries.
                    raise
                except (requests.RequestException, ValueError) as exc:
                    # Keep normal transient errors at three tries, but give
                    # rate-limited requests enough global cooldown to recover.
                    retries = 6 if isinstance(exc, _UpstreamRateLimit) else 3
                    if attempt >= retries:
                        raise RuntimeError(
                            f"Catalog {section_uuid}/{subsection_uuid}, "
                            f"page {page}, offset {offset} failed: {exc}") from exc
                    if not isinstance(exc, _UpstreamRateLimit):
                        time.sleep(attempt)
            if data is None:
                raise ValueError(f"Catalog {section_uuid}/{subsection_uuid} missing body")
            paging = data["pagingInfo"]
            page += 1
            if paging["hasMore"] is False:
                return records, bodies, True
            nxt = paging.get("offset")
            if not isinstance(nxt, int) or nxt <= offset:
                raise ValueError(
                    f"Catalog {section_uuid}/{subsection_uuid} stalled: {offset} -> {nxt}")
            offset = nxt
        raise ValueError(
            f"Catalog {section_uuid}/{subsection_uuid} exceeded {MAX_CATALOG_PAGES} pages")

    def verified_partition(section_index, parent_uuid, subsection_uuid=None, visited=None):
        """Get all native pages without ever requesting offset >= 1000.

        Parent pages up to the boundary provide independent SKU coverage
        evidence. When the parent would cross the boundary, continue through
        *actual* native child UUIDs to terminal pages, preserving the parent
        preview as a validation set rather than substituting a partial menu.
        """
        visited = frozenset() if visited is None else visited
        current_uuid = subsection_uuid or parent_uuid
        if current_uuid in visited or len(visited) >= 8:
            raise RuntimeError(f"Catalog native subsection cycle at {current_uuid}")
        records, bodies, terminal = collect(
            section_index, parent_uuid, subsection_uuid,
            stop_before_offset=1000)
        if terminal:
            return records, bodies, "section" if subsection_uuid is None else "subsection", 0
        children = _subsections_for(base_data, current_uuid)
        source = "getStoreV1_sections"
        if not children:
            # An API response can expose only a subset of the store's section
            # descriptors. Real child UUIDs may still be available in items.
            children = _native_subsections_from_catalog(bodies, current_uuid)
            source = "catalog_native_item_subsections"
        if not children:
            raise RuntimeError(
                f"Catalog {current_uuid} reached deep-offset boundary; "
                "native subsection UUIDs unavailable, refusing partial menu")
        parent_skus = _catalog_skus(bodies)
        print("CATALOG_NATIVE_SUBSECTIONS=" + json.dumps({
            "store_uuid": store_uuid, "section_uuid": current_uuid,
            "source": source, "children": len(children),
            "known_parent_skus": len(parent_skus),
            "offset_1020_requested": False,
        }), flush=True)
        child_records, child_bodies = [], []
        for child in children:
            cr, cb, _, _ = verified_partition(
                section_index, parent_uuid, child, visited | {current_uuid})
            child_records.extend(cr)
            child_bodies.extend(cb)
        child_skus = _catalog_skus(child_bodies)
        missing = parent_skus - child_skus
        # getStoreV1 may list only a subset of real native children. If that
        # list fails to cover already observed parent SKUs, extract *additional*
        # actual UUIDs from the parent HTTP item payload. These are evidence,
        # not guessed or fabricated partition IDs.
        if missing and source == "getStoreV1_sections":
            extra = [
                uuid for uuid in _native_subsections_from_catalog(bodies, current_uuid)
                if uuid not in children and uuid not in visited and uuid != current_uuid
            ]
            if extra:
                print("CATALOG_NATIVE_CHILD_GAP_RECOVERY=" + json.dumps({
                    "store_uuid": store_uuid,
                    "section_uuid": current_uuid,
                    "metadata_child_count": len(children),
                    "item_native_extra_child_count": len(extra),
                    "missing_parent_skus_before": len(missing),
                }), flush=True)
                for child in extra:
                    cr, cb, _, _ = verified_partition(
                        section_index, parent_uuid, child, visited | {current_uuid})
                    child_records.extend(cr)
                    child_bodies.extend(cb)
                child_skus = _catalog_skus(child_bodies)
                missing = parent_skus - child_skus
                children = children + extra
                source = "getStoreV1_plus_catalog_native_item_subsections"
        if missing:
            raise RuntimeError(
                f"Subsections for {current_uuid} omitted {len(missing)} "
                f"previously seen native parent SKUs; refusing partial menu")
        if not child_records:
            raise RuntimeError(f"Catalog {current_uuid} has no verified child pages")
        print("CATALOG_SUBSECTION_RECOVERY=" + json.dumps({
            "store_uuid": store_uuid, "section_uuid": current_uuid,
            "subsections": len(children),
            "child_pages": len(child_records),
            "child_unique_skus": len(child_skus),
            "parent_observed_skus": len(parent_skus),
            "parent_missing_from_children": len(missing),
            "avoided_offset": 1020,
            "mode": "native_children_before_deep_offset",
            "child_uuid_source": source,
        }), flush=True)
        return child_records, child_bodies, "native_children_before_deep_offset", len(children)

    responses, section_reports = [], []
    for section_index, section_uuid in enumerate(section_uuids):
        records, bodies, mode, children_count = verified_partition(
            section_index, section_uuid)
        responses.extend(records)
        section_reports.append({
            "section_uuid": section_uuid,
            "pages": len(records),
            "item_occurrences": sum(x["item_count"] for x in records),
            "mode": mode,
            "subsections": children_count,
            "terminal_has_more": False})
    if len(section_reports) != len(section_uuids):
        raise ValueError("Not all catalog sections reached terminal pages")
    return {
        "catalog_responses": responses,
        "catalog_complete": True,
        "catalog_section_count": len(section_uuids),
        "catalog_sections_finished": len(section_reports),
        "catalog_section_reports": section_reports,
        "catalog_item_occurrences": sum(x["item_count"] for x in responses),
    }


class EmptyCatalogPagesError(ValueError):
    """Verified Raw pages contain no products; preserve the previous store menu."""


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
        raise EmptyCatalogPagesError("Catalog pages yielded no products; cannot erase a menu")
    native = dict(native)
    original = native.get("catalogSectionsMap") or {}
    # Prefer the category API's complete, priced items. Keep original sections
    # too, because curated or featured items may not exist in regular aisles.
    native["catalogSectionsMap"] = {"v2_catalog": groups, **original}
    return native
