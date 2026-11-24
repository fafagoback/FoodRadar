"""Read-only two-store live diagnostic; never updates production or claims search completeness."""
import base64
import copy
import json
import os
import re
import sys
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from menu_core import attach_delivery_location
from catalog_complete import (_catalog_entries, _catalog_skus, _section_ids,
                              _subsections_for, _native_subsections_from_catalog,
                              capture_catalog)
from raw_capture import capture_response
from verified_prices import quote_from_native_item

CASES = [
    ("0fd40e62-4a52-49a5-8620-0706906896e1", "637d42bd-facb-589f-8aa7-068af8552717"),
    ("773cda1c-4917-416a-8527-91a078e1c6ef", "5a1e3e94-c9ad-5869-9edc-20d0e9c77c91"),
]
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
           "Accept": "*/*", "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8",
           "Content-Type": "application/json", "x-csrf-token": "x"}
BASE = "https://www.ubereats.com/api/"
class StopProbe(Exception):
    pass

def log(tag, value):
    print(tag + "=" + json.dumps(value, ensure_ascii=False, default=str), flush=True)

def items(groups):
    return [i for g in groups or [] if isinstance(g, dict)
            for i in ((g.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems", [])
            if isinstance(i, dict) and isinstance(i.get("uuid"), str)]

def map_items(data):
    return [i for groups in (data.get("catalogSectionsMap") or {}).values()
            if isinstance(groups, list) for i in items(groups)]

def explicit_children(obj):
    """Only real identifiers from explicit native subsection fields."""
    found = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k.lower() == "subsectionuuid" and isinstance(v, str):
                found.add(v)
            elif k.lower() == "subsectionuuids" and isinstance(v, list):
                found.update(x for x in v if isinstance(x, str))
            found.update(explicit_children(v))
    elif isinstance(obj, list):
        for v in obj:
            found.update(explicit_children(v))
    return {x for x in found if re.fullmatch(r"[0-9a-fA-F-]{36}", x)}

def main(index):
    sid, parent = CASES[index]
    out = ROOT / "diagnostic-results" / str(index)
    out.mkdir(parents=True, exist_ok=True)
    cfg = json.loads((ROOT / "config.json").read_text())
    report = {"store_uuid": sid, "failed_section_uuid": parent, "methods": {},
              "full_store_verified": False, "requests": 0}
    session = requests.Session()
    assert attach_delivery_location(session, {"discovered_points": [
        {"lat": cfg["latitude"], "lon": cfg["longitude"], "county": "台北市"}]})
    def call(api, payload, label):
        if report["requests"] >= 750:
            raise StopProbe("diagnostic request budget reached")
        report["requests"] += 1
        response = session.post(BASE + api, json=payload, headers=HEADERS, timeout=20)
        p = out / (f"{report['requests']:04d}-{label}.json")
        p.write_bytes(response.content)
        if response.status_code in (403, 429):
            raise StopProbe(f"upstream HTTP {response.status_code}; stop without bypass")
        try:
            body = response.json()
        except ValueError:
            body = {}
        data = body.get("data") or {} if isinstance(body, dict) else {}
        data = data if isinstance(data, dict) else {}
        if str(data.get("code")) in ("403", "429"):
            raise StopProbe("upstream application access/rate limit; stop")
        return response, body, data
    def catalog(segmented, offset, child=None, collection=False):
        filters = {"storeUuid": sid, "sectionUuids": None if child else [parent],
                   "subsectionUuids": [child] if child else None}
        if segmented:
            filters["shouldReturnSegmentedControlData"] = True
        if collection:
            filters["sectionTypes"] = ["COLLECTION"]
        return call("getCatalogPresentationV2", {"storeFilters": filters,
                    "pagingInfo": {"enabled": True, "offset": offset},
                    "sortAndFilters": None, "source": "NV_L2_CATALOG"},
                    "segmented" if segmented else "baseline")
    try:
        r, body, base = call("getStoreV1", {"storeUuid": sid, "sfNuggetCount": 24}, "store")
        if r.status_code != 200 or body.get("status") == "failure" or not base.get("title"):
            raise StopProbe("store unavailable: HTTP " + str(r.status_code))
        report["name"] = base.get("title")
        report["sections"] = _section_ids(base)
        report["preview_skus"] = len({i["uuid"] for i in map_items(base)})
        report["parent_currently_listed"] = parent in report["sections"]
        log("STORE_META", {k: v for k, v in report.items() if k != "methods"})
        native_children = set(_subsections_for(base, parent))
        samples = {}
        known = {}
        for segmented in (False, True):
            for offset in (0, 960):
                r, b, d = catalog(segmented, offset)
                label = ("segmented" if segmented else "baseline") + "_" + str(offset)
                try:
                    _, groups = _catalog_entries(b)
                    rows = items(groups)
                    ids = {i["uuid"] for i in rows}
                    paging = d["pagingInfo"]
                    for i in rows:
                        known[i["uuid"]] = i
                    if segmented:
                        native_children.update(_native_subsections_from_catalog([b], parent))
                        for k, v in d.items():
                            if "segment" in k.lower() or "subsection" in k.lower():
                                native_children.update(explicit_children(v))
                    samples[label] = {"http": r.status_code, "skus": len(ids),
                                      "hasMore": paging["hasMore"], "next_offset": paging.get("offset"),
                                      "data_keys": list(d), "application_code": d.get("code"),
                                      "segment_fields": [k for k in d if "segment" in k.lower()]}
                except (ValueError, TypeError, KeyError) as e:
                    samples[label] = {"http": r.status_code, "error": str(e),
                                      "application_code": d.get("code"), "data_keys": list(d)}
                log("PAGE_PROBE", {"name": report["name"], "variant": label, **samples[label]})
        native_children.discard(parent)
        report["methods"]["segmented"] = {"samples": samples, "explicit_child_ids": sorted(native_children)}
        log("SEGMENTED_RESULT", report["methods"]["segmented"])
        # Search is a diagnostic, never a certificate of full catalog coverage.
        queries = list(dict.fromkeys([str(i.get("title") or "").strip() for i in list(known.values())[:3]] +
                                     ["洗髮", "牙膏", "清潔", "保健"]))
        search_results = []
        for q in filter(None, queries):
            r, b, d = call("getInStoreSearchV1", {"diningMode": "DELIVERY",
                    "sectionUUIDs": None, "storeUUIDs": [sid], "userQuery": q,
                    "isGrocery": False, "entrypointContext": "IN_STORE_SEARCH"}, "search")
            rows = map_items(d)
            new = {i["uuid"] for i in rows} - set(known)
            common = [i for i in rows if i["uuid"] in known]
            comparisons = 0
            mismatches = 0
            for i in common:
                try:
                    a, z = quote_from_native_item(known[i["uuid"]]), quote_from_native_item(i)
                    if a is not None and z is not None:
                        comparisons += 1
                        mismatches += int(a != z)
                except (ValueError, TypeError, KeyError):
                    pass
            result = {"query": q, "http": r.status_code, "code": d.get("code"),
                      "skus": len({i["uuid"] for i in rows}), "outside_sample": len(new),
                      "common_sample": len(common), "quote_comparisons": comparisons,
                      "quote_mismatches": mismatches}
            search_results.append(result)
            log("SEARCH_RESULT", result)
        report["methods"]["search"] = search_results
        # Try the whole existing collector, with the native segmented flag.
        # The only additional child identifiers are explicit values from native responses.
        if report["parent_currently_listed"]:
            modified = copy.deepcopy(base)
            if native_children:
                modified.setdefault("sections", []).append(
                    {"uuid": parent, "subsectionUuids": sorted(native_children)})
            class Wrapped:
                def post(self, url, json, headers, timeout):
                    payload = copy.deepcopy(json)
                    payload["storeFilters"]["shouldReturnSegmentedControlData"] = True
                    r, _, _ = call("getCatalogPresentationV2", payload, "full")
                    return r
            try:
                raw = out / "full-catalog"
                result = capture_catalog(Wrapped(), sid, modified, raw, HEADERS, capture_response)
                bodies = [json.loads((raw / p["path"]).read_bytes()) for p in result["catalog_responses"]]
                skus = _catalog_skus(bodies)
                preview = {i["uuid"] for i in map_items(base)}
                missing = preview - skus
                # Terminal coverage and preview agreement are necessary, not proof of a
                # universal independent store item total.
                report["methods"]["full"] = {
                    "catalog_complete": result["catalog_complete"],
                    "sections_finished": result["catalog_sections_finished"],
                    "sections_total": result["catalog_section_count"],
                    "unique_catalog_skus": len(skus), "preview_skus_missing": len(missing),
                    "preview_union_skus": len(skus | preview), "pages": len(bodies),
                    "independent_total_available": False}
                report["full_store_verified"] = bool(result["catalog_complete"] and
                    result["catalog_sections_finished"] == result["catalog_section_count"] and
                    not missing and skus)
            except (ValueError, RuntimeError, requests.RequestException, StopProbe) as e:
                report["methods"]["full"] = {"catalog_complete": False, "error": str(e)}
        else:
            report["methods"]["full"] = {"catalog_complete": False,
                 "error": "historically failed section not currently in native store menu; cannot reproduce"}
    except (StopProbe, requests.RequestException, ValueError, TypeError, KeyError) as e:
        report["error"] = str(e)
    finally:
        session.close()
        (out / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf8")
        log("FINAL_DIAGNOSTIC", report)
        summary = os.getenv("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a", encoding="utf8") as f:
                f.write("### " + str(report.get("name", sid)) + "\n\n")
                f.write("- 完整採集驗證：" + str(report["full_store_verified"]) + "\n")
                f.write("- API 請求：" + str(report["requests"]) + "\n")
                f.write("\n" + "~~~json\n" + json.dumps(report, ensure_ascii=False, indent=2) + "\n~~~\n")
if __name__ == "__main__":
    main(int(sys.argv[1]))
