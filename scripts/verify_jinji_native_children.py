"""Read-only Jinji positive-case test for native item-derived subsections.

Jinji previously failed at 1020, then succeeded once but later lacked getStoreV1
sections. Validate that item data itself gives native UUIDs that can be fetched
to terminal pages, without ever requesting offset >=1000.
"""
import json
import sys
from pathlib import Path
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location,global_limiter
SID="0941d15a-f620-5970-b04a-b9edf626cc7f"
PARENTS=["113eea56-673a-507b-bc33-9d11f3a2183f",
         "5e5e0f23-5a2f-5f9a-8fce-7dbd05b3b349"]
HEAD={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
      "Accept":"*/*","Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
      "Content-Type":"application/json","x-csrf-token":"x"}
s=requests.Session()
assert attach_delivery_location(s,{"discovered_points":[{
    "lat":25.123246847935267,"lon":121.52951826298867,"county":"台北市"}]})
def request(section=None,subsection=None,offset=0):
    if not 0 <= offset < 1000:raise RuntimeError("forbidden offset >= 1000")
    payload={"sortAndFilters":None,"storeFilters":{
      "storeUuid":SID,"sectionUuids":[section] if section else None,
      "subsectionUuids":[subsection] if subsection else None},
      "pagingInfo":{"enabled":True,"offset":offset},"source":"NV_L2_CATALOG"}
    global_limiter.wait()
    r=s.post("https://www.ubereats.com/api/getCatalogPresentationV2",
             json=payload,headers=HEAD,timeout=30)
    if r.status_code==429:
        global_limiter.trigger_backoff(30,"native children probe 429")
        raise RuntimeError("429")
    r.raise_for_status()
    d=r.json().get("data") or {}
    p=d.get("pagingInfo")
    if not isinstance(d.get("catalog"),(list,type(None))) or not isinstance(p,dict):
        raise RuntimeError(f"Bad response code {d.get('code')}")
    return d
def scan(section=None,subsection=None):
    offsets=[];seen=set();groups={};terminal=False
    offset=0
    while len(offsets)<17 and offset<1000:
        d=request(section,subsection,offset)
        offsets.append(offset)
        for g in d.get("catalog") or []:
            native=(g.get("payload") or {}).get("standardItemsPayload") or {}
            for item in native.get("catalogItems") or []:
                sku=item.get("uuid") or item.get("itemUuid")
                if sku:
                    seen.add(sku)
                    sub=item.get("subsectionUuid") or g.get("catalogSectionUUID")
                    if sub:groups.setdefault(sub,set()).add(sku)
        pg=d["pagingInfo"]
        if pg.get("hasMore") is False:
            terminal=True;break
        nxt=pg.get("offset")
        if not isinstance(nxt,int) or nxt<=offset:raise RuntimeError("Paging stalled")
        offset=nxt
    return {"seen":seen,"groups":groups,"offsets":offsets,"terminal":terminal}
for parent in PARENTS:
    parent_result=scan(section=parent)
    groups=sorted(parent_result["groups"])
    child_results={}
    for sub in groups:
        result=scan(subsection=sub)
        child_results[sub]=result
        print("JINJI_CHILD_FILTER="+json.dumps({
          "parent":parent,"subsection":sub,"is_self":sub==parent,
          "seen_skus":len(result["seen"]),"terminal":result["terminal"],
          "pages":len(result["offsets"]),
          "max_offset":max(result["offsets"],default=None),
          "same_group_prefix_covered":len(result["seen"]&parent_result["groups"][sub]),
          "same_group_prefix_count":len(parent_result["groups"][sub]),
        },ensure_ascii=False),flush=True)
    combined=set().union(*(v["seen"] for v in child_results.values()))
    remaining=parent_result["seen"]-combined
    print("JINJI_CHILD_FILTER_SUMMARY="+json.dumps({
      "parent":parent,"parent_prefix_skus":len(parent_result["seen"]),
      "prefix_group_uuids":groups,
      "group_count":len(groups),
      "all_native_groups_terminal":all(x["terminal"] for x in child_results.values()),
      "missing_from_terminal_groups":len(remaining),
      "missing_sample":sorted(remaining)[:8],
      "max_requested_offset":max(
         (max(x["offsets"],default=0) for x in child_results.values()),default=0),
      "parent_ended":parent_result["terminal"],
    },ensure_ascii=False),flush=True)
s.close()
