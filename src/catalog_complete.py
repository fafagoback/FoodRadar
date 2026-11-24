"""Capture all native Uber Eats catalog pages; never replace or forge the HTTP Raw bodies."""
from __future__ import annotations

import json
import os
import time
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
import requests
from menu_core import global_limiter, response_retry_after

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
    if isinstance(body, dict) and body.get("status") == "failure":
        raise ValueError("getCatalogPresentationV2 returned native failure")
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
        global_limiter.trigger_backoff(seconds, f"Catalog {kind}", retry_after=response_retry_after(response))
        raise _UpstreamRateLimit(f"Catalog {kind} (retry {attempt})")


def _subsections_for(base_data, section_uuid):
    """Read actual child UUIDs in sections OR aisles, never guessed filters."""
    candidates = [
        item for item in (base_data.get("sections") or [])
        if isinstance(item, dict) and item.get("uuid") == section_uuid
    ]
    aisles = base_data.get("aisles") or {}
    if isinstance(aisles, dict):
        for entries in aisles.values():
            if isinstance(entries, list):
                candidates.extend(
                    item for item in entries
                    if isinstance(item, dict)
                    and item.get("catalogSectionUUID") == section_uuid
                )
    children = []
    for item in candidates:
        values = item.get("subsectionUuids") or []
        if not isinstance(values, list):
            continue
        for uuid in values:
            if (isinstance(uuid, str) and uuid
                    and uuid != section_uuid and uuid not in children):
                children.append(uuid)
    return children


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
                # A product can echo the parent UUID, while its actual native
                # child is carried by the enclosing group or payload. A parent
                # echo must not mask a usable child (nor be queried as a leaf).
                native_id = next((candidate for candidate in (
                    item.get("subsectionUuid"), group.get("catalogSectionUUID"),
                    standard.get("subsectionUuid"),
                ) if isinstance(candidate, str) and candidate
                     and candidate != parent_uuid), None)
                if native_id is not None and native_id not in seen:
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


def capture_catalog(session, store_uuid, base_data, root, headers, capture_response, store_attempt=1,
                    session_factory=None, section_workers=1):
    """Fetch every native category; split deep failing categories by real subsections.

    Uber's getCatalogPresentationV2 can return HTTP 200 + status=failure /
    data.code=500 at offset>1000. This is not a terminal page. Instead of
    dropping items or claiming completion, fetch each of its native subsections
    to a terminal page, and verify that all earlier parent UUIDs are retained.
    Never fabricate a category, response, or a completed status.
    """
    section_uuids = _section_ids(base_data)
    paging = base_data.get("catalogSectionPagingInfo") or {}
    if paging.get("hasMore") is True:
        raise ValueError("Store reports additional category pages; category enumeration unverified")
    if not section_uuids:
        return {"catalog_responses": [], "catalog_complete": True,
                "catalog_section_count": 0, "catalog_sections_finished": 0,
                "catalog_section_reports": [], "catalog_item_occurrences": 0}

    # A child-only request does not contain its parent's UUID. Within this
    # store/session, reuse exactly the same native request partition. Never
    # cache across stores, sessions, batches or promotion rechecks.
    partition_cache = {}
    reused_partitions = 0
    cache_lock = threading.Lock()
    local_session = threading.local()

    def collect(section_index, section_uuid, subsection_uuid=None, stop_before_offset=None):
        nonlocal reused_partitions
        key = (("child", subsection_uuid) if subsection_uuid is not None
               else ("parent", section_uuid), stop_before_offset)
        with cache_lock:
            future = partition_cache.get(key)
            owner = future is None
            if owner:
                future = Future()
                partition_cache[key] = future
            else:
                reused_partitions += 1
        if owner:
            try:
                future.set_result(collect_uncached(section_index, section_uuid,
                                                   subsection_uuid, stop_before_offset))
            except BaseException as exc:
                future.set_exception(exc)
                raise
        return future.result()

    def collect_uncached(section_index, section_uuid, subsection_uuid=None, stop_before_offset=None):
        active_session = getattr(local_session, 'value', session)
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
            rate_retries = max(6, min(12, int(os.getenv("FOODRADAR_CATALOG_429_RETRIES", "6"))))
            for attempt in range(1, rate_retries + 1):
                try:
                    global_limiter.wait()
                    resp = active_session.post(CATALOG_API, json=payload,
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
                    if page == 0 and n == 0 and not data["pagingInfo"]["hasMore"] and not use_collection_type:
                        filters["sectionTypes"] = ["COLLECTION"]
                        try:
                            global_limiter.wait()
                            alt = active_session.post(CATALOG_API, json=payload,
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
                                if _item_count(alt_entries) > 0 or alt_data["pagingInfo"]["hasMore"]:
                                    body, data, entries = alt_body, alt_data, alt_entries
                                    relative, n = alt_relative, _item_count(alt_entries)
                                    use_collection_type = True
                                else:
                                    filters.pop("sectionTypes", None)
                            else:
                                _check_rate_limit(alt, None, attempt)
                                raise ValueError(f"Empty catalog alternative HTTP {alt.status_code}")
                        except (requests.RequestException, ValueError):
                            # A failed alternate request is not proof of an empty
                            # category. Restore the original query before retry.
                            filters.pop("sectionTypes", None)
                            raise
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
                    retries = rate_retries if isinstance(exc, _UpstreamRateLimit) else 3
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
        metadata_children = _subsections_for(base_data, current_uuid)
        item_children = _native_subsections_from_catalog(bodies, current_uuid)
        children = list(dict.fromkeys(metadata_children + item_children))
        source = ("getStoreV1_plus_catalog_native_item_subsections"
                  if metadata_children and set(item_children) - set(metadata_children)
                  else "getStoreV1_sections" if metadata_children
                  else "catalog_native_item_subsections")
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
    response_paths, unique_skus = set(), set()
    def capture_section(pair):
        index, section = pair
        if session_factory is None or section_workers <= 1:
            return verified_partition(index, section)
        with session_factory() as independent:
            local_session.value = independent
            try:
                return verified_partition(index, section)
            finally:
                del local_session.value

    pairs = list(enumerate(section_uuids))
    # Different native sections can overlap network wait, never bypass shared
    # request pacing, and each pagination chain remains strictly sequential.
    if session_factory is not None and section_workers > 1 and len(pairs) >= 8:
        with ThreadPoolExecutor(max_workers=min(3, section_workers)) as executor:
            partitions = list(executor.map(capture_section, pairs))
    else:
        partitions = [verified_partition(i, s) for i, s in pairs]
    for (section_index, section_uuid), (records, bodies, mode, children_count) in zip(pairs, partitions):
        for record in records:
            if record["path"] not in response_paths:
                responses.append(record)
                response_paths.add(record["path"])
        section_skus = _catalog_skus(bodies)
        unique_skus.update(section_skus)
        section_reports.append({
            "section_uuid": section_uuid,
            "pages": len(records),
            "item_occurrences": sum(x["item_count"] for x in records),
            "unique_skus": len(section_skus),
            "coverage_evidence": ("observed_parent_subset_and_terminal_children"
                                  if children_count else "terminal_pagination"),
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
        "catalog_unique_skus": len(unique_skus),
        "catalog_reused_partitions": reused_partitions,
        # Terminal pagination and observed parent coverage do not prove
        # completeness against an independent upstream inventory.
        "catalog_independent_inventory_verified": False,
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
