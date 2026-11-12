"""Live 1020 regression: CURRENT big supermarkets, not historical failures.

Read-only. Preflight current original category API at offset 0 and 960 and
require an actual nonempty offset-960 page with hasMore=true and next=1020.
Only then capture the entire store using production code. No HF/Turso writes.
A store that does not CURRENTLY qualify is NEVER counted as a successful test.
"""
from __future__ import annotations

import json
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import raw_capture
from catalog_complete import _section_ids, _catalog_entries, _item_count, _catalog_skus
from menu_core import attach_delivery_location, global_limiter

STORES = [
    {"name": "PX Mart Beitou Mingde", "uuid": "db0e33e1-2eb6-5ac6-954d-fdedda8fe7b4",
     "url": "https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA",
     "lat": 25.115, "lon": 121.520},
    {"name": "Carrefour Beitou Guangming", "uuid": "77acc2fb-3569-5338-a45b-775387611849",
     "url": "https://www.ubereats.com/tw/store/%E5%AE%B6%E6%A8%82%E7%A6%8F%E8%B6%85%E5%B8%82-%E5%8C%97%E6%8A%95%E5%85%89%E6%98%8E%E5%BA%97/d6zC-zVpUzikW3dTh2EYSQ",
     "lat": 25.134, "lon": 121.500},
    {"name": "Costco Beitou", "uuid": "a4e2201d-5fc7-5a10-a41a-dfe58975a7a7",
     "url": "https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%8C%97%E6%8A%95%E5%BA%97/pOIgHV_HWhCkGt_liXWnpw",
     "lat": 25.13184, "lon": 121.498632},
]
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
    "Accept": "*/*", "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
    "Content-Type": "application/json", "x-csrf-token": "x",
}


def native_request(session, route, payload):
    for attempt in range(1, 4):
        global_limiter.wait()
        response = session.post("https://www.ubereats.com/api/" + route,
                                json=payload, headers=HEADERS, timeout=30)
        body = response.json()
        code = str((body.get("data") or {}).get("code") or "") if isinstance(body, dict) else ""
        if response.status_code == 429 or (body.get("status") == "failure" and code == "429"):
            global_limiter.trigger_backoff(min(120, 15 * 2 ** (attempt - 1)), "Live 1020 supermarket 429")
            if attempt == 3:
                raise RuntimeError("Rate limited during current 1020 qualification")
            continue
        response.raise_for_status()
        global_limiter.record_success()
        return body
    raise RuntimeError("Could not obtain native API response")


def live_qualify(store):
    """Use the current actual API; do not infer from yesterday's catalog size."""
    session = requests.Session()
    try:
        if not attach_delivery_location(session, {"discovered_points": [{
                "lat": store["lat"], "lon": store["lon"], "county": "台北市"}]}):
            raise RuntimeError("Missing delivery location")
        native = native_request(session, "getStoreV1",
                                {"storeUuid": store["uuid"], "sfNuggetCount": 24})
        base = native.get("data") or {}
        sections = _section_ids(base)
        if not sections:
            raise RuntimeError("No native category list for this store")
        found = []
        for section in sections:
            responses = {}
            for offset in (0, 960):
                data = native_request(session, "getCatalogPresentationV2", {
                    "sortAndFilters": None,
                    "storeFilters": {"storeUuid": store["uuid"],
                                     "sectionUuids": [section],
                                     "subsectionUuids": None},
                    "pagingInfo": {"enabled": True, "offset": offset},
                    "source": "NV_L2_CATALOG"})
                try:
                    parsed, entries = _catalog_entries(data)
                except ValueError as exc:
                    print("FOODRADAR_LIVE_1020_PREFLIGHT_ERROR=" + json.dumps({
                        "store": store["name"], "section": section,
                        "offset": offset, "error": str(exc)}, ensure_ascii=False), flush=True)
                    responses = {}
                    break
                responses[offset] = {
                    "items": _item_count(entries),
                    "has_more": parsed["pagingInfo"]["hasMore"],
                    "next_offset": parsed["pagingInfo"].get("offset"),
                }
            qualified = (
                0 in responses and 960 in responses
                and responses[0]["items"] > 0
                and responses[960]["items"] > 0
                and responses[960]["has_more"] is True
                and isinstance(responses[960]["next_offset"], int)
                and responses[960]["next_offset"] >= 1020)
            if qualified:
                found.append(section)
                print("FOODRADAR_LIVE_1020_QUALIFIED=" + json.dumps({
                    "store": store["name"], "store_uuid": store["uuid"],
                    "section_uuid": section, "offset_960": responses[960],
                    "offset_0": responses[0],
                    "next_required_offset": responses[960]["next_offset"],
                    "verified_at_execution": True}, ensure_ascii=False), flush=True)
                # One CURRENT over-boundary category is enough to prove that this
                # large supermarket is an eligible difficult test case.
                break
        return found
    finally:
        session.close()


def capture(store, qualifying_sections, root):
    offsets = []
    original = raw_capture.capture_catalog
    store_root = root / store["uuid"]
    def monitored(session, store_uuid, base_data, folder, headers, capture_response,
                  store_attempt=1):
        saved_post = session.post
        def checked_post(api_url, **kwargs):
            if api_url.endswith("/getCatalogPresentationV2"):
                offset = kwargs.get("json", {}).get("pagingInfo", {}).get("offset")
                offsets.append(offset)
                if not isinstance(offset, int) or offset >= 1000:
                    raise AssertionError(f"Production issued forbidden deep-offset request: {offset}")
            return saved_post(api_url, **kwargs)
        session.post = checked_post
        try:
            return original(session, store_uuid, base_data, folder, headers,
                            capture_response, store_attempt)
        finally:
            session.post = saved_post
    row = {
        "store_uuid": store["uuid"], "store_url": store["url"],
        "discovered_points": [{"lat": store["lat"], "lon": store["lon"], "county": "台北市"}],
    }
    with patch.object(raw_capture, "capture_catalog", side_effect=monitored):
        result = raw_capture.fetch_store_raw(row, store_root, max_retries=1)
    report = result.get("catalog_section_reports") or []
    by_uuid = {r.get("section_uuid"): r for r in report}
    actual_sections = [
        {"section_uuid": uuid,
         "completed": uuid in by_uuid,
         "mode": by_uuid.get(uuid, {}).get("mode"),
         "subsections": by_uuid.get(uuid, {}).get("subsections"),
         "pages": by_uuid.get(uuid, {}).get("pages")}
        for uuid in qualifying_sections]
    pages = result.get("catalog_responses") or []
    bodies = [json.loads((store_root / p["path"]).read_bytes()) for p in pages]
    terminals = defaultdict(list)
    for rec, body in zip(pages, bodies):
        terminal = (body.get("data") or {}).get("pagingInfo") or {}
        terminals[(rec["section_uuid"], rec["subsection_uuid"])].append(
            (rec["offset"], terminal.get("hasMore")))
    terminal_ok = bool(terminals) and all(
        max(values, key=lambda x: x[0])[1] is False for values in terminals.values())
    qualified_recovered = all(
        r["completed"] and r["mode"] == "native_children_before_deep_offset"
        and isinstance(r["subsections"], int) and r["subsections"] > 0
        for r in actual_sections)
    passed = (
        result.get("success") is True
        and result.get("catalog_complete") is True
        and result.get("catalog_section_count") == result.get("catalog_sections_finished")
        and qualified_recovered and terminal_ok and 1020 not in offsets
        and all(isinstance(x, int) and x < 1000 for x in offsets)
        and bool(_catalog_skus(bodies)))
    evidence = {
        "store": store["name"], "store_uuid": store["uuid"],
        "CURRENT_offset_960_hasMore_next_1020": bool(qualifying_sections),
        "qualified_sections": actual_sections,
        "full_store_success": result.get("success") is True,
        "catalog_complete": result.get("catalog_complete"),
        "sections_finished": result.get("catalog_sections_finished"),
        "sections_total": result.get("catalog_section_count"),
        "catalog_pages": len(pages),
        "unique_skus": len(_catalog_skus(bodies)),
        "terminal_by_partition": terminal_ok,
        "production_catalog_requests": len(offsets),
        "max_production_offset": max(offsets, default=None),
        "offset_1020_requested_in_production": 1020 in offsets,
        "passed": passed, "error": result.get("error"),
    }
    print("FOODRADAR_BIGSTORE_1020_RESULT=" +
          json.dumps(evidence, ensure_ascii=False), flush=True)
    return evidence


def main():
    qualifying = []
    rejected = []
    # User-requested repeat test of a single CURRENTLY over-1020 grocery store.
    selected = STORES[:1]
    for store in selected:
        try:
            sections = live_qualify(store)
            if sections:
                qualifying.append((store, sections))
            else:
                rejected.append({"store": store["name"], "reason": "No CURRENT offset 960 -> 1020 section"})
        except Exception as exc:
            rejected.append({"store": store["name"], "reason": f"{type(exc).__name__}: {exc}"})
        if len(qualifying) >= 2:
            break
    results = []
    with tempfile.TemporaryDirectory(prefix="foodradar-live-supermarket-1020-") as folder:
        for store, sections in qualifying:
            results.append(capture(store, sections, Path(folder)))
    passed = bool(results) and all(r["passed"] for r in results)
    summary = {
        "passed": passed, "currently_qualified_stores": len(qualifying),
        "tested": len(results), "results": results, "not_qualified": rejected,
        "HF_write": False, "Turso_write": False}
    print("FOODRADAR_BIGSTORE_1020_SUMMARY=" +
          json.dumps(summary, ensure_ascii=False), flush=True)
    import os
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("### 現在超過 offset=1020 的大型賣場實測\n")
            f.write(f"結果：{'PASS' if passed else 'FAIL'}；目前符合條件 {len(qualifying)} 家；實際完整測試 {len(results)} 家\n")
            for r in results:
                f.write(f"- {r['store']}：商品 {r['unique_skus']}、分類 {r['sections_finished']}/{r['sections_total']}、"
                        f"分頁 {r['catalog_pages']}、最大 offset {r['max_production_offset']}、PASS={r['passed']}\n")
            for r in rejected:
                f.write(f"- 未入選：{r['store']}，原因：{r['reason']}\n")
    if not passed:
        raise SystemExit("CURRENTLY over-1020 large supermarket NOT verified; see failure report")


if __name__ == "__main__":
    main()
