"""Run *only two* formerly failing stores against the live Uber native API.

No HF/Turso writes. Assert that the production catalog code terminates all
selected categories, preserves original bodies and never requests offset >=1000.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import raw_capture
from catalog_complete import _catalog_skus, _native_subsections_from_catalog

CASES = [
    ("Watsons Tianbei", "0fd40e62-4a52-49a5-8620-0706906896e1",
     "https://www.ubereats.com/tw/store/watsons%E5%B1%88%E8%87%A3%E6%B0%8F-%E5%A4%A9%E5%8C%97%E9%96%80%E5%B8%82-s0454/D9QOYkpSSaWGIAcGkGiW4Q"),
    ("DaShu Wenlin", "2a2613cb-f67c-5774-935d-bb47511095d0",
     "https://www.ubereats.com/tw/store/%E5%A4%A7%E6%A8%B9%E8%97%A5%E5%B1%80-%E5%A3%AB%E6%9E%97%E6%96%87%E6%9E%97%E5%BA%97/KiYTy_Z8V3STXbtHURCV0A"),
]
if os.environ.get("FOODRADAR_LIVE_1020_PAIR") == "pets":
    CASES = [
        ("Pet Park Tianmu", "56b04e05-29cf-406f-9aaa-f5dfb23386a5",
         "https://www.ubereats.com/tw/store/%E5%AF%B5%E7%89%A9%E5%85%AC%E5%9C%92-%E5%A4%A9%E6%AF%8D%E5%BA%97/VrBOBSnPQG-aqvXfsjOGpQ"),
        ("Jinji Pet Tianmu", "0941d15a-f620-5970-b04a-b9edf626cc7f",
         "https://www.ubereats.com/tw/store/%E9%87%91%E5%90%89%E5%88%A9%E5%AF%B5%E7%89%A9%E7%B2%BE%E5%93%81%E5%A4%A9%E6%AF%8D%E5%BA%97/CUHRWvYgWXCwSrnt9ibMfw"),
    ]
elif os.environ.get("FOODRADAR_LIVE_1020_PAIR") == "jinji":
    CASES = [
        ("Jinji Pet Tianmu", "0941d15a-f620-5970-b04a-b9edf626cc7f",
         "https://www.ubereats.com/tw/store/%E9%87%91%E5%90%89%E5%88%A9%E5%AF%B5%E7%89%A9%E7%B2%BE%E5%93%81%E5%A4%A9%E6%AF%8D%E5%BA%97/CUHRWvYgWXCwSrnt9ibMfw"),
    ]

def main():
    original = raw_capture.capture_catalog
    summary = []
    with tempfile.TemporaryDirectory(prefix="foodradar-1020-verified-") as tmp:
        root = Path(tmp)
        for name, sid, url in CASES:
            store_root = root / sid
            offsets = []
            def tracked_catalog(session, store_uuid, base_data, folder, headers,
                                capture_response, store_attempt=1):
                saved_post = session.post
                def monitored_post(api_url, **kwargs):
                    if api_url.endswith("/getCatalogPresentationV2"):
                        offset = kwargs.get("json", {}).get("pagingInfo", {}).get("offset")
                        offsets.append(offset)
                        if not isinstance(offset, int) or offset >= 1000:
                            raise ValueError(
                                f"Forbidden catalog offset {offset}; "
                                "must use verified native children")
                    return saved_post(api_url, **kwargs)
                session.post = monitored_post
                try:
                    if os.environ.get("FOODRADAR_FORCE_MISSING_SECTION_METADATA") == "1":
                        # Simulate the real getStoreV1 omission observed on
                        # 2026-11-12. Raw HTTP bodies remain untouched.
                        base_data = {**base_data, "sections": []}
                    return original(session, store_uuid, base_data, folder,
                                    headers, capture_response, store_attempt)
                finally:
                    session.post = saved_post

            item = {"store_uuid": sid, "store_url": url,
                    "discovered_points": [{"lat": 25.123246847935267,
                                           "lon": 121.52951826298867,
                                           "county": "台北市"}]}
            with patch.object(raw_capture, "capture_catalog",
                              side_effect=tracked_catalog), \
                 patch("catalog_complete._native_subsections_from_catalog",
                       wraps=_native_subsections_from_catalog) as native_fallback:
                captured = raw_capture.fetch_store_raw(item, store_root, max_retries=1)
                native_fallback_calls = native_fallback.call_count
            page_refs = captured.get("catalog_responses") or []
            verified_pages = []
            terminal_by_partition = defaultdict(list)
            for page in page_refs:
                path = store_root / page["path"]
                body = json.loads(path.read_bytes())
                verified_pages.append(body)
                paging = body["data"]["pagingInfo"]
                terminal_by_partition[(page["section_uuid"],
                                       page["subsection_uuid"])].append(
                    (page["offset"], paging["hasMore"]))
            terminal_ok = bool(terminal_by_partition) and all(
                max(pages, key=lambda entry: entry[0])[1] is False
                for pages in terminal_by_partition.values())
            unique_skus = len(_catalog_skus(verified_pages))
            record = {"store_name": name, "store_uuid": sid,
                      "success": captured.get("success") is True,
                      "catalog_complete": captured.get("catalog_complete") is True,
                      "sections_finished": captured.get("catalog_sections_finished"),
                      "sections_total": captured.get("catalog_section_count"),
                      "catalog_pages": len(page_refs), "unique_skus": unique_skus,
                      "terminal_by_partition": terminal_ok,
                      "catalog_calls": len(offsets),
                      "max_requested_offset": max(offsets, default=None),
                      "offset_1020_requested": 1020 in offsets,
                      "error": captured.get("error"),
                      "missing_section_metadata_simulated":
                          os.environ.get("FOODRADAR_FORCE_MISSING_SECTION_METADATA") == "1",
                      "native_item_fallback_calls": native_fallback_calls}
            print("FOODRADAR_LIVE_1020_STORE=" +
                  json.dumps(record, ensure_ascii=False), flush=True)
            summary.append(record)
    passed = all(
        r["success"] and r["catalog_complete"] and r["terminal_by_partition"]
        and r["unique_skus"] > 0 and not r["offset_1020_requested"]
        and r["catalog_pages"] > 0
        and r["sections_finished"] == r["sections_total"]
        and (r["max_requested_offset"] is not None and
             r["max_requested_offset"] < 1000)
        and (not r["missing_section_metadata_simulated"]
             or r["native_item_fallback_calls"] > 0)
        for r in summary
    )
    report = {"passed": passed, "tested": len(summary), "stores": summary,
              "hf_write": False, "db_write": False}
    print("FOODRADAR_LIVE_1020_SUMMARY=" +
          json.dumps(report, ensure_ascii=False), flush=True)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("### FoodRadar 原生商品 API 單店實測\\n")
            f.write(f"整體結果：{'PASS' if passed else 'FAIL'}；測試店家：{len(summary)}\\n")
            for item in summary:
                f.write(f"- {item['store_name']}：商品 {item['unique_skus']} 件、有效分頁 {item['catalog_pages']}、"
                        f"分類 {item['sections_finished']}/{item['sections_total']}、"
                        f"最大 offset {item['max_requested_offset']}、"
                        f"成功 {item['success']}\\n")
    if not passed:
        raise SystemExit("Live store catalog not complete: inspect per-store errors")
    print("Both live stores complete without requesting offset 1020", flush=True)


if __name__ == "__main__":
    main()
