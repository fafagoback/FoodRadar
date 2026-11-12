"""One-off read-only coverage audit: two real stores, all *native* root categories.

Never request offset >= 1000. Measure whether other categories cover SKUs
already visible in the truncated category. Does not declare completeness from
overlap alone; always reports category terminal pagination evidence.
"""
import json
import sys
from pathlib import Path
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location, global_limiter
from catalog_complete import _section_ids

API="https://www.ubereats.com/api/"
HEAD={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
      "Accept":"*/*", "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
      "Content-Type":"application/json","x-csrf-token":"x"}
CASES=[
 ("Watsons Tianbei","0fd40e62-4a52-49a5-8620-0706906896e1",
  "1afa9c0f-13f3-524e-ae44-70e2fbefcd3e"),
 ("DaShu Wenlin","2a2613cb-f67c-5774-935d-bb47511095d0",
  "8d66d350-a0bb-5378-84f4-8163e50c7720"),
]

def category_items(payload):
    items={}
    for group in payload if isinstance(payload,list) else []:
        if not isinstance(group,dict):
            continue
        for item in (((group.get("payload") or {}).get("standardItemsPayload") or {})
                     .get("catalogItems") or []):
            if not isinstance(item,dict): continue
            sid=item.get("uuid") or item.get("itemUuid") or item.get("id")
            if not sid: continue
            promo=bool(item.get("itemLevelPromotion")) or bool(
                (item.get("catalogItemAnalyticsData") or {}).get("promoType"))
            items[sid]=bool(items.get(sid) or promo)
    return items

def section_titles(data):
    titles={}
    for values in (data.get("aisles") or {}).values():
        if not isinstance(values,list):continue
        for v in values:
            if not isinstance(v,dict):continue
            sid=v.get("catalogSectionUUID")
            display=((v.get("payload") or {}).get("categoryItemPayload") or {}).get("title")
            if sid:titles[sid]=display if isinstance(display,str) else ""
    return titles

def scan(session,store_uuid,section):
    items={}
    offsets=[]
    offset=0
    terminal=False
    error=None
    while len(offsets)<25:
        if offset>=1000:
            break
        payload={"sortAndFilters":None,
                 "storeFilters":{"storeUuid":store_uuid,"sectionUuids":[section],
                                 "subsectionUuids":None},
                 "pagingInfo":{"enabled":True,"offset":offset},
                 "source":"NV_L2_CATALOG"}
        try:
            global_limiter.wait()
            r=session.post(API+"getCatalogPresentationV2",json=payload,
                           headers=HEAD,timeout=30)
            if r.status_code==429:
                global_limiter.trigger_backoff(30,"live coverage probe HTTP 429")
                raise RuntimeError("HTTP 429")
            if r.status_code!=200:
                raise RuntimeError(f"HTTP {r.status_code}")
            envelope=r.json()
            data=envelope.get("data") if isinstance(envelope,dict) else {}
            if (not isinstance(data,dict)
                    or not isinstance(data.get("catalog"),(list,type(None)))
                    or not isinstance(data.get("pagingInfo"),dict)):
                raise ValueError(f"invalid catalog response, code={data.get('code')}")
            paging=data["pagingInfo"]
            if type(paging.get("hasMore")) is not bool:
                raise ValueError("missing hasMore")
            global_limiter.record_success()
            items.update(category_items(data.get("catalog")))
            offsets.append(offset)
            if not paging["hasMore"]:
                terminal=True
                break
            nxt=paging.get("offset")
            if not isinstance(nxt,int) or nxt<=offset:
                raise ValueError("invalid next offset")
            offset=nxt
        except Exception as exc:
            error=f"{type(exc).__name__}: {exc}"
            break
    return {"uuids":set(items),"promo":{k for k,v in items.items() if v},
            "offsets":offsets,"terminal":terminal,"error":error,
            "next_offset":offset if not terminal else None}

for name,store,parent in CASES:
    session=requests.Session()
    attach_delivery_location(session,{"discovered_points":[{"lat":25.123246847935267,
            "lon":121.52951826298867,"county":"台北市"}]})
    r=session.post(API+"getStoreV1",json={"storeUuid":store,"sfNuggetCount":24},
                   headers=HEAD,timeout=30)
    r.raise_for_status()
    base=r.json().get("data") or {}
    sections=_section_ids(base)
    titles=section_titles(base)
    results={}
    for sid in sections:
        result=scan(session,store,sid)
        results[sid]=result
        print("NATIVE_1020_CATEGORY_COVERAGE="+json.dumps({
            "store":name,"section":sid,"title":titles.get(sid,""),
            "terminal":result["terminal"],"unique_skus":len(result["uuids"]),
            "promo_skus":len(result["promo"]),"pages":len(result["offsets"]),
            "max_offset":max(result["offsets"],default=None),
            "next_offset":result["next_offset"],"error":result["error"],
        },ensure_ascii=False),flush=True)
    parent_skus=results[parent]["uuids"]
    all_others_terminal=set().union(*[
        result["uuids"] for sid,result in results.items()
        if sid!=parent and result["terminal"]]) if results else set()
    all_others_seen=set().union(*[
        result["uuids"] for sid,result in results.items()
        if sid!=parent]) if results else set()
    terminal_sections=sum(x["terminal"] for x in results.values())
    summary={
        "store":name,
        "parent_title":titles.get(parent),
        "parent_prefix_unique":len(parent_skus),
        "parent_prefix_covered_by_terminal_other_sections":len(parent_skus&all_others_terminal),
        "parent_prefix_missing_from_terminal_others":len(parent_skus-all_others_terminal),
        "parent_prefix_missing_from_all_other_observed":len(parent_skus-all_others_seen),
        "terminal_sections":terminal_sections,"total_sections":len(results),
        "truncated_section_titles":[titles.get(k) for k,v in results.items()
                                     if not v["terminal"]],
        "all_root_categories_reached_terminal":terminal_sections==len(results),
        "offset_1020_requested":any(1020 in r["offsets"] for r in results.values()),
    }
    print("NATIVE_1020_CROSS_CATEGORY_SUMMARY="+json.dumps(
        summary,ensure_ascii=False),flush=True)
    session.close()
