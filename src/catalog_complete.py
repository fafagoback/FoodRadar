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

    def collect(section_index, section_uuid, subsection_uuid=None):
        records, bodies, seen_offsets = [], [], set()
        offset, page, use_collection_type = 0, 0, False
        while page < MAX_CATALOG_PAGES:
            if offset in seen_offsets:
                raise ValueError(f"Catalog loop at {section_uuid}/{subsection_uuid} {offset}")
            seen_offsets.add(offset)
            filters = {"storeUuid": store_uuid,
                       "sectionUuids": [section_uuid],
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
                    if (offset > 1000 and isinstance(failure, dict)
                            and body.get("status") == "failure"
                            and failure.get("code") == 500):
                        raise _DeepOffsetRejected(
                            f"Catalog {section_uuid}/{subsection_uuid} "
                            f"offset {offset}: native code 500")
                    data, entries = _catalog_entries(body)
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
                                f"status-{alt.status_code}.body")
                            capture_response(root, alt_relative, alt)
                            if alt.status_code == 200:
                                alt_body = alt.json()
                                _check_rate_limit(alt, alt_body, attempt)
                                alt_data, alt_entries = _catalog_entries(alt_body)
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
                return records, bodies
            nxt = paging.get("offset")
            if not isinstance(nxt, int) or nxt <= offset:
                raise ValueError(
                    f"Catalog {section_uuid}/{subsection_uuid} stalled: {offset} -> {nxt}")
            offset = nxt
        raise ValueError(
            f"Catalog {section_uuid}/{subsection_uuid} exceeded {MAX_CATALOG_PAGES} pages")

    responses, section_reports = [], []
    for section_index, section_uuid in enumerate(section_uuids):
        try:
            records, bodies = collect(section_index, section_uuid)
            mode = "section"
            children_count = 0
        except _DeepOffsetRejected as exc:
            # Read-only proven fallback. All 7 PX Mart native subsection IDs
            # were accepted by the API at offset=0 in live diagnostic.
            children = _subsections_for(base_data, section_uuid)
            if not children:
                raise RuntimeError(
                    f"{exc}; native subsection UUIDs unavailable, refusing partial menu") from exc
            # Preserve the already captured parent pages for consistency validation.
            # Raw HTTP files remain immutable on disk even if omitted from manifest.
            parent_bodies = []
            prefix = (f"catalog/{store_uuid}/store-attempt-{store_attempt:02d}-"
                      f"section-{section_index:03d}-{section_uuid}-sub-none-")
            for raw_path in root.rglob("*.body"):
                if str(raw_path.relative_to(root)).startswith(prefix):
                    try:
                        raw = json.loads(raw_path.read_bytes())
                        _catalog_entries(raw)
                        parent_bodies.append(raw)
                    except (ValueError, json.JSONDecodeError):
                        pass
            parent_skus = _catalog_skus(parent_bodies)
            records, bodies = [], []
            for child in children:
                child_records, child_bodies = collect(section_index, section_uuid, child)
                records.extend(child_records)
                bodies.extend(child_bodies)
            child_skus = _catalog_skus(bodies)
            missing = parent_skus - child_skus
            if missing:
                raise RuntimeError(
                    f"Subsections for {section_uuid} omitted {len(missing)} "
                    f"previously seen native parent SKUs; refusing partial menu")
            mode = "subsections_after_deep_offset_error"
            children_count = len(children)
            print("CATALOG_SUBSECTION_RECOVERY=" + json.dumps({
                "section_uuid": section_uuid,
                "subsections": children_count,
                "pages": len(records),
                "child_unique_skus": len(child_skus),
                "parent_observed_skus": len(parent_skus),
                "parent_missing_from_children": len(missing),
            }), flush=True)
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
