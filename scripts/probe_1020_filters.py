"""Bounded read-only live discovery of native catalog partitions for two failed stores.

No offset >=1000 is requested, no HF/Turso writes, and all evidence comes
directly from Uber native responses with the same delivery coordinate.
"""
import json
import sys
import time
from pathlib import Path

import requests

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from menu_core import attach_delivery_location
from catalog_complete import _catalog_entries, _catalog_skus, _section_ids

BASE="https://www.ubereats.com/api/"
HEADERS={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
         "Accept":"*/*", "Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
         "Content-Type":"application/json", "x-csrf-token":"x"}
CASES=[
    ("watsons-tianbei", "0fd40e62-4a52-49a5-8620-0706906896e1",
     "1afa9c0f-13f3-524e-ae44-70e2fbefcd3e"),
    ("dashu-wenlin", "2a2613cb-f67c-5774-935d-bb47511095d0",
     "8d66d350-a0bb-5378-84f4-8163e50c7720"),
]

def describe(tag, resp):
    try:
        o=resp.json()
        d=o.get("data") if isinstance(o,dict) else {}
        d=d if isinstance(d,dict) else {}
        c=d.get("catalog")
        entries=c if isinstance(c,list) else []
        ids=sorted(_catalog_skus([o])) if "catalog" in d and isinstance(c,list) and isinstance(d.get("pagingInfo"),dict) else []
        p=d.get("pagingInfo") if isinstance(d.get("pagingInfo"),dict) else {}
        ret={"tag":tag,"http":resp.status_code,
             "status":o.get("status") if isinstance(o,dict) else None,
             "code":d.get("code"),"message":str(d.get("message") or "")[:80],
             "items":len(ids),"first_ids":ids[:4],
             "hasMore":p.get("hasMore"),"next_offset":p.get("offset"),
             "data_keys":list(d)[:25]}
        print("NATIVE_1020_VARIANT="+json.dumps(ret,ensure_ascii=False),flush=True)
        return set(ids)
    except Exception as exc:
        print("NATIVE_1020_VARIANT_ERROR="+json.dumps({
            "tag":tag, "error":f"{type(exc).__name__}: {exc}"},ensure_ascii=False),flush=True)
        return set()

for name,uuid,parent in CASES:
    session=requests.Session()
    assert attach_delivery_location(session,{"discovered_points":[{
        "lat":25.123246847935267, "lon":121.52951826298867, "county":"台北市"}]})
    r=session.post(BASE+"getStoreV1",json={"storeUuid":uuid,"sfNuggetCount":24},
                   headers=HEADERS,timeout=30)
    r.raise_for_status()
    o=r.json()
    data=o.get("data") or {}
    sections=data.get("sections") or []
    section=next((s for s in sections if isinstance(s,dict) and s.get("uuid")==parent),{})
    children=[x for x in (section.get("subsectionUuids") or []) if isinstance(x,str)]
    print("NATIVE_1020_STORE_META="+json.dumps({
        "name":name,"store":uuid,"base_keys":list(data)[:100],
        "section_ids":_section_ids(data)[:25],
        "sections_count":len(sections),
        "parent_section":section,
        "subsectionsMap_type":type(data.get("subsectionsMap")).__name__,
        "subsectionsMap_keys":list(data.get("subsectionsMap") or {})[:16]
            if isinstance(data.get("subsectionsMap"),dict) else None,
        "subsectionsMap_parent":str((data.get("subsectionsMap") or {}).get(parent))[:2500]
            if isinstance(data.get("subsectionsMap"),dict) else None,
        "sectionEntitiesMap_parent":str((data.get("sectionEntitiesMap") or {}).get(parent))[:2500]
            if isinstance(data.get("sectionEntitiesMap"),dict) else None,
        "catalogSectionPagingInfo":str(data.get("catalogSectionPagingInfo"))[:1600],
        "aisles_parent":str(data.get("aisles"))[:2600],
        "children":children,
    },ensure_ascii=False),flush=True)
    def call(tag,section_ids,sub_ids=None,types=None,enabled=True,offset=0,source="NV_L2_CATALOG"):
        assert 0<=offset<1000
        filt={"storeUuid":uuid,"sectionUuids":section_ids,"subsectionUuids":sub_ids}
        if types is not None: filt["sectionTypes"]=types
        payload={"sortAndFilters":None,"storeFilters":filt,
                 "pagingInfo":{"enabled":enabled,"offset":offset},"source":source}
        try:
            resp=session.post(BASE+"getCatalogPresentationV2",json=payload,
                              headers=HEADERS,timeout=30)
            out=describe(name+"/"+tag,resp)
        except requests.RequestException as exc:
            print("NATIVE_1020_NETWORK_ERROR="+json.dumps({
                "tag":name+"/"+tag,"error":str(exc)},ensure_ascii=False),flush=True)
            out=set()
        time.sleep(0.20)
        return out
    parent0=call("parent-0",[parent])
    parent360=call("parent-360",[parent],offset=360)
    parent960=call("parent-960",[parent],offset=960)
    for tag,section_ids,subs,types,enabled in [
        ("global-none",None,None,None,True),
        ("global-empty",[],None,None,True),
        ("collection",[parent],None,["COLLECTION"],True),
        ("category",[parent],None,["CATEGORY"],True),
        ("parent-unpaged",[parent],None,None,False),
        ("global-unpaged",None,None,None,False),
        ("as-subsection",None,[parent],None,True),
        ("parent-sub-empty",[parent],[],None,True),
    ]:
        call(tag,section_ids,subs,types,enabled)
    print("NATIVE_1020_PARENT_EVIDENCE="+json.dumps({
        "name":name, "parent_sample_skus":len(parent0|parent360|parent960),
        "children":len(children)},ensure_ascii=False),flush=True)
    for index, child in enumerate(children[:4]):
        sid=child[:8]
        call("child-only-"+sid,None,[child])
        call("child-as-section-"+sid,[child],None)
        call("parent-plus-child-"+sid,[parent],[child])
    session.close()
