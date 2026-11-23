"""Read-only live probe for native Uber catalog >1000 offset failure.

Only native API results. No HF/Turso writes. Capture actual section metadata
and compare alternative API query shapes before modifying production parsing.
"""
import json
import sys
import time
from pathlib import Path

import requests
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from menu_core import attach_delivery_location

BASE="https://www.ubereats.com/api/"
HEADERS={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
         "Accept":"*/*","Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
         "x-csrf-token":"x","Content-Type":"application/json"}
STORES=[
    ("2a2613cb-f67c-5774-935d-bb47511095d0", "8d66d350-a0bb-5378-84f4-8163e50c7720"),
    ("0941d15a-f620-5970-b04a-b9edf626cc7f", "5e5e0f23-5a2f-5f9a-8fce-7dbd05b3b349"),
    ("0fd40e62-4a52-49a5-8620-0706906896e1", "1afa9c0f-13f3-524e-ae44-70e2fbefcd3e"),
]

def inspect_response(tag, r):
    try:
        o=r.json()
        data=o.get("data") if isinstance(o,dict) else None
        data=data if isinstance(data,dict) else {}
        cat=data.get("catalog")
        entries=cat if isinstance(cat,list) else []
        skus=[]
        for e in entries:
            for item in (((e.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or []):
                if isinstance(item,dict):
                    sku=item.get("uuid") or item.get("itemUuid") or item.get("id")
                    if sku: skus.append(sku)
        p=data.get("pagingInfo") if isinstance(data.get("pagingInfo"),dict) else {}
        print("CATALOG_ALTERNATIVE_PROBE="+json.dumps({
            "tag":tag,"http":r.status_code,"status":o.get("status") if isinstance(o,dict) else None,
            "code":data.get("code"),"message":str(data.get("message") or "")[:85],
            "page_entries":len(entries),"items":len(skus),"unique_items":len(set(skus)),
            "hasMore":p.get("hasMore"),"next_offset":p.get("offset"),
            "catalog_type":type(cat).__name__
        },ensure_ascii=False),flush=True)
        return set(skus)
    except Exception as e:
        print("CATALOG_ALTERNATIVE_PROBE="+json.dumps({"tag":tag,"error":repr(e)[:240]}),flush=True)
        return set()

for uuid, parent in STORES:
    sess=requests.Session()
    attach_delivery_location(sess,{"discovered_points":[{"lat":25.123246847935267,"lon":121.52951826298867,"county":"台北市"}]})
    r=sess.post(BASE+"getStoreV1",json={"storeUuid":uuid,"sfNuggetCount":24},headers=HEADERS,timeout=30)
    r.raise_for_status()
    root=(r.json().get("data") or {})
    section=next((z for z in (root.get("sections") or []) if isinstance(z,dict) and z.get("uuid")==parent),{})
    children=[x for x in (section.get("subsectionUuids") or []) if isinstance(x,str)]
    print("CATALOG_SECTION_METADATA="+json.dumps({
        "store":uuid,"parent":parent,"parent_keys":list(section)[:15],
        "subsections":len(children),"aisle_keys":list(root.get("aisles") or {})[:10],
        "paging_keys":list(root.get("catalogSectionPagingInfo") or {})[:15],
    },ensure_ascii=False),flush=True)
    modes=[
        ("normal", [parent], None, None),
        ("collection", [parent], None, ["COLLECTION"]),
        ("category", [parent], None, ["CATEGORY"]),
        ("no-section", None, None, None),
        ("section-empty", [], None, None),
    ]
    if children:
        modes.extend([
            ("child-under-parent",[parent],[children[0]],None),
            ("child-section",[children[0]],None,None),
            ("child-only",None,[children[0]],None),
        ])
    for mode,sections,subs,types in modes:
        for offset in (0,1020) if mode in ("normal","collection","category","no-section","section-empty") else (0,):
            filters={"storeUuid":uuid,"sectionUuids":sections,"subsectionUuids":subs}
            if types: filters["sectionTypes"]=types
            payload={"sortAndFilters":None,"storeFilters":filters,
                     "pagingInfo":{"enabled":True,"offset":offset},"source":"NV_L2_CATALOG"}
            try:
                res=sess.post(BASE+"getCatalogPresentationV2",json=payload,headers=HEADERS,timeout=30)
                inspect_response(f"{uuid[:8]}/{mode}/{offset}",res)
            except requests.RequestException as e:
                print("CATALOG_ALTERNATIVE_PROBE="+json.dumps({
                    "tag":f"{uuid[:8]}/{mode}/{offset}","transport_error":repr(e)[:220]}),flush=True)
            time.sleep(0.22)
    sess.close()
