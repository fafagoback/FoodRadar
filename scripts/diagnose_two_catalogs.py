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
    sid, old_parent = CASES[index]
    out = ROOT / "diagnostic-results" / str(index)
    out.mkdir(parents=True, exist_ok=True)
    cfg = json.loads((ROOT / "config.json").read_text())
    report = {"store_uuid": sid, "old_failed_section": old_parent, "requests": 0,
              "sections": [], "search": [], "all_current_sections_complete": False}
    session = requests.Session()
    assert attach_delivery_location(session, {"discovered_points": [
        {"lat": cfg["latitude"], "lon": cfg["longitude"], "county": "台北市"}]})
    limit = 550
    def call(api, payload, label):
        if report["requests"] >= limit:
            raise StopProbe("request budget reached")
        # Sequential bounded read-only requests. Do not retry access refusals.
        import time
        time.sleep(0.30)
        report["requests"] += 1
        r = session.post(BASE + api, json=payload, headers=HEADERS, timeout=20)
        (out / (f"{report['requests']:04d}-{label}.json")).write_bytes(r.content)
        try:
            body = r.json()
        except ValueError:
            body = {}
        data = body.get("data") or {} if isinstance(body, dict) else {}
        data = data if isinstance(data, dict) else {}
        if r.status_code in (403, 429) or str(data.get("code")) in ("403", "429"):
            raise StopProbe("upstream access/rate limit; stop without retry")
        return r, body, data
    def catalog(parent, offset, *, segmented=False, collection=False, paging=True, page_size=None, child=None):
        filters = {"storeUuid": sid, "sectionUuids": None if child else [parent],
                   "subsectionUuids": [child] if child else None}
        if segmented: filters["shouldReturnSegmentedControlData"] = True
        if collection: filters["sectionTypes"] = ["COLLECTION"]
        pi = {"enabled": paging, "offset": offset}
        if page_size is not None: pi["pageSize"] = page_size
        r, b, d = call("getCatalogPresentationV2", {"storeFilters": filters,
                    "pagingInfo": pi, "sortAndFilters": None, "source": "NV_L2_CATALOG"},
                    "catalog")
        if r.status_code != 200 or b.get("status") == "failure":
            raise ValueError(f"HTTP {r.status_code}, app {d.get('code')}")
        _, groups = _catalog_entries(b)
        return b, d, items(groups)
    def walk(parent, **kwargs):
        offset, seen = 0, set()
        rows, bodies = {}, []
        for page in range(150):
            if offset >= 1000:
                return rows, bodies, False, f"offset {offset} at boundary"
            if offset in seen:
                return rows, bodies, False, f"offset {offset} repeats"
            seen.add(offset)
            try:
                b, d, rr = catalog(parent, offset, **kwargs)
                # Match production's COLLECTION fallback for an empty first page.
                if page == 0 and not rr and not kwargs.get("collection"):
                    alt = {**kwargs, "collection": True}
                    ab, ad, ar = catalog(parent, offset, **alt)
                    if ar:
                        b, d, rr = ab, ad, ar
                        kwargs = alt
                bodies.append(b)
                rows.update({i["uuid"]: i for i in rr})
                p = d["pagingInfo"]
                if p["hasMore"] is False:
                    return rows, bodies, True, ""
                nxt = p.get("offset")
                if type(nxt) is not int or nxt <= offset:
                    return rows, bodies, False, f"invalid offset {nxt}"
                offset = nxt
            except (requests.RequestException, ValueError, TypeError, KeyError) as e:
                return rows, bodies, False, str(e)
        return rows, bodies, False, "page budget reached"
    def describe_sample(parent, label, **kwargs):
        try:
            b, d, rr = catalog(parent, 0, **kwargs)
            p = d["pagingInfo"]
            return {"method": label, "skus": len({i["uuid"] for i in rr}),
                    "has_more": p["hasMore"], "next_offset": p.get("offset"),
                    "segment_fields": [k for k in d if "segment" in k.lower()],
                    "native_child_count": len(explicit_children(d) - {parent})}
        except (requests.RequestException, ValueError, TypeError, KeyError) as e:
            return {"method": label, "error": str(e)}
    all_rows = {}
    try:
        r, b, base = call("getStoreV1", {"storeUuid": sid, "sfNuggetCount": 24}, "store")
        if r.status_code != 200 or b.get("status") == "failure" or not base.get("title"):
            raise StopProbe("store unavailable")
        report["name"] = base["title"]
        sections = _section_ids(base)
        preview = {i["uuid"]: i for i in map_items(base)}
        report["expected_sections"] = len(sections)
        report["preview_skus"] = len(preview)
        report["old_parent_still_listed"] = old_parent in sections
        log("CURRENT_STORE", {k:v for k,v in report.items() if k not in ("sections","search")})
        for parent in sections:
            start = report["requests"]
            rows, bodies, terminal, reason = walk(parent)
            sr = {"uuid": parent, "baseline_skus": len(rows), "baseline_terminal": terminal,
                  "baseline_error": reason, "methods": [], "terminal": terminal}
            report["sections"].append(sr)
            # Retain original per-section evidence; only unresolved sections get extra tests.
            selected = rows
            if not terminal:
                sr["methods"] = [
                    describe_sample(parent, "segmented", segmented=True),
                    describe_sample(parent, "pageSize250", page_size=250),
                    describe_sample(parent, "unpaged", paging=False),
                    describe_sample(parent, "collection", collection=True)]
                # Discover the genuine native child IDs in both metadata and current pages.
                children = set(_subsections_for(base, parent))
                children.update(_native_subsections_from_catalog(bodies, parent))
                segrows, segbodies, segterminal, segreason = walk(parent, segmented=True)
                children.update(_native_subsections_from_catalog(segbodies, parent))
                for sb in segbodies:
                    d = sb.get("data") or {}
                    for k,v in d.items():
                        if "segment" in k.lower() or "subsection" in k.lower():
                            children.update(explicit_children(v))
                children.discard(parent)
                sr["segmented_walk"] = {"skus": len(segrows), "terminal": segterminal,
                                        "error": segreason, "missing_baseline": len(set(rows)-set(segrows))}
                sr["native_children"] = sorted(children)
                if segterminal and set(rows) <= set(segrows):
                    selected, sr["terminal"] = segrows, True
                    sr["accepted_method"] = "segmented"
                elif children:
                    # Test native partitions separately; require terminal pagination for each
                    # and coverage of every SKU already observed in the parent.
                    childrows, cr = {}, []
                    for child in sorted(children):
                        rr, bb, tt, ee = walk(parent, child=child, segmented=True)
                        childrows.update(rr)
                        cr.append({"uuid": child, "skus": len(rr), "terminal": tt, "error": ee})
                    missing = (set(rows)|set(segrows)) - set(childrows)
                    sr["child_walk"] = {"children": cr, "unique_skus": len(childrows),
                                        "missing_observed_parent": len(missing)}
                    if all(c["terminal"] for c in cr) and not missing and childrows:
                        selected, sr["terminal"] = childrows, True
                        sr["accepted_method"] = "native_children"
                # Do not use unpaged or search samples as proof of complete coverage.
            all_rows.update(selected)
            sr["selected_skus"] = len(selected)
            sr["requests"] = report["requests"] - start
            log("SECTION_RESULT", sr)
        report["all_current_sections_complete"] = bool(sections) and all(x["terminal"] for x in report["sections"])
        report["unique_catalog_skus"] = len(all_rows)
        report["preview_missing"] = len(set(preview)-set(all_rows))
        # Independently query titles actually present in the catalog and several broad
        # terms. Compare UUIDs, store price quotes, and search-only SKUs.
        titles = [str(i.get("title") or "").strip() for i in list(all_rows.values())[:4]]
        queries = list(dict.fromkeys(["洗髮","牙膏","清潔","保健"] + titles))
        search_only = set()
        for q in filter(None, queries):
            r,b,d = call("getInStoreSearchV1", {"diningMode":"DELIVERY","sectionUUIDs":None,
                  "storeUUIDs":[sid],"userQuery":q,"isGrocery":False,
                  "entrypointContext":"IN_STORE_SEARCH"}, "search")
            rr = {i["uuid"]:i for i in map_items(d)}
            common = set(rr)&set(all_rows)
            extra = set(rr)-set(all_rows)
            search_only.update(extra)
            good, mismatch, invalid = 0,0,0
            for k in common:
                try:
                    a,z = quote_from_native_item(rr[k]),quote_from_native_item(all_rows[k])
                    if a is None or z is None:
                        invalid += 1
                    elif a == z: good += 1
                    else: mismatch += 1
                except (ValueError,TypeError,KeyError):
                    invalid += 1
            sr = {"query":q,"http":r.status_code,"skus":len(rr),"overlap":len(common),
                  "search_only":len(extra),"matching_quotes":good,"mismatching_quotes":mismatch,
                  "invalid_quotes":invalid}
            report["search"].append(sr)
            log("SEARCH_AUDIT", sr)
        report["search_only_unique"] = len(search_only)
        report["all_current_sections_and_preview_verified"] = report["all_current_sections_complete"] and not report["preview_missing"]
        report["independent_total_available"] = False
    except (StopProbe, requests.RequestException, ValueError, TypeError, KeyError) as e:
        report["error"] = str(e)
    finally:
        session.close()
        (out/"report.json").write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf8")
        log("FINAL_DIAGNOSTIC",report)
        summary=os.getenv("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary,"a",encoding="utf8") as f:
                f.write("### "+str(report.get("name",sid))+"\n\n~~~json\n"+json.dumps(report,ensure_ascii=False,indent=2)+"\n~~~\n")
if __name__ == "__main__":
    main(int(sys.argv[1]))
