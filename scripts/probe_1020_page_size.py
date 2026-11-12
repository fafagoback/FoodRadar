"""Probe native Uber page-size/preview alternatives for two real deep-offset failures.

Only this targeted diagnostic runs; no HF or database updates, no offset >=1000.
"""
import json
import sys
import time
from pathlib import Path
import requests

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location

BASE="https://www.ubereats.com/api/"
HEAD={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
      "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
      "Content-Type":"application/json","x-csrf-token":"x"}
CASES=[
  ("Watsons Tianbei","0fd40e62-4a52-49a5-8620-0706906896e1",
   "1afa9c0f-13f3-524e-ae44-70e2fbefcd3e"),
  ("DaShu Wenlin","2a2613cb-f67c-5774-935d-bb47511095d0",
   "8d66d350-a0bb-5378-84f4-8163e50c7720"),
]

def items_from_groups(groups):
    seen=set()
    for g in groups if isinstance(groups,list) else []:
        if isinstance(g,dict):
            for item in (((g.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or []):
                if isinstance(item,dict):
                    value=item.get("uuid") or item.get("itemUuid") or item.get("id")
                    if value: seen.add(value)
    return seen

def inspect_store(data):
    bysection = data.get("catalogSectionsMap") or {}
    counts={}
    for key, groups in bysection.items() if isinstance(bysection,dict) else []:
        counts[key]=len(items_from_groups(groups))
    return {"sections_map_count":len(counts),
            "sections_map_items":sum(counts.values()),
            "sections_map_unique":len(set().union(*[
                items_from_groups(x) for x in bysection.values()])) if counts else 0,
            "largest_sections":sorted(counts.items(),key=lambda x:-x[1])[:8],
            "nuggets_count":len(data.get("nuggets") or []),
            "aisles_count":sum(len(v) for v in (data.get("aisles") or {}).values() if isinstance(v,list)),
            "sections_count":len(data.get("sections") or []),
            "catalogPagingInfo":str(data.get("catalogSectionPagingInfo"))[:400]}

for name,sid,parent in CASES:
    s=requests.Session()
    attach_delivery_location(s,{"discovered_points":[{"lat":25.123246847935267,
      "lon":121.52951826298867,"county":"台北市"}]})
    for count in (24,100,500,2000):
        response=s.post(BASE+"getStoreV1",json={"storeUuid":sid,"sfNuggetCount":count},
                        headers=HEAD,timeout=30)
        obj=response.json()
        d=obj.get("data") if isinstance(obj,dict) else None
        info=inspect_store(d) if isinstance(d,dict) else {}
        print("STORE_PREVIEW_VARIANT="+json.dumps({
            "store":name,"sfNuggetCount":count,"http":response.status_code,
            "byte_length":len(response.content),"status":obj.get("status"),
            "error":str(d.get("message") or "")[:120] if isinstance(d,dict) else None,
            **info},ensure_ascii=False),flush=True)
        time.sleep(.25)
    variants=[
       ("default",{"enabled":True,"offset":0}),
       ("pageSize100",{"enabled":True,"offset":0,"pageSize":100}),
       ("pageSize250",{"enabled":True,"offset":0,"pageSize":250}),
       ("pageSize500",{"enabled":True,"offset":0,"pageSize":500}),
       ("pageSize2000",{"enabled":True,"offset":0,"pageSize":2000}),
       ("limit500",{"enabled":True,"offset":0,"limit":500}),
       ("count500",{"enabled":True,"offset":0,"count":500}),
       ("pageSize500_offset960",{"enabled":True,"offset":960,"pageSize":500}),
    ]
    for label,paging in variants:
        payload={"sortAndFilters":None,"storeFilters":{
            "storeUuid":sid,"sectionUuids":[parent],"subsectionUuids":None},
            "pagingInfo":paging,"source":"NV_L2_CATALOG"}
        try:
            r=s.post(BASE+"getCatalogPresentationV2",json=payload,
                     headers=HEAD,timeout=30)
            o=r.json()
            data=o.get("data") if isinstance(o,dict) else {}
            data=data if isinstance(data,dict) else {}
            items=items_from_groups(data.get("catalog"))
            p=data.get("pagingInfo") or {}
            print("CATALOG_PAGESIZE_VARIANT="+json.dumps({
                "name":name,"variant":label,"http":r.status_code,
                "status":o.get("status") if isinstance(o,dict) else None,
                "code":data.get("code"),"message":str(data.get("message") or "")[:120],
                "response_bytes":len(r.content),"item_count":len(items),
                "hasMore":p.get("hasMore"),"next_offset":p.get("offset"),
            },ensure_ascii=False),flush=True)
        except Exception as exc:
            print("CATALOG_PAGESIZE_ERROR="+json.dumps({
               "name":name,"variant":label,"error":repr(exc)[:180]}),flush=True)
        time.sleep(.23)
    s.close()
