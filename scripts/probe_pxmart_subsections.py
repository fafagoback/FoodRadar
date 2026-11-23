"""Find viable native Uber category subdivision to bypass the verified offset>1000 cap.

No browser, screenshots, synthetic data or TinyFish. Probe real subsection UUIDs
from getStoreV1.sections and compare returned SKU identity at offset=0.
"""
import json
import sys
import requests
sys.path.insert(0,"src")
from menu_core import attach_delivery_location,normalize_store_uuid
from catalog_complete import _item_count

URL="https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"
SID=normalize_store_uuid("",URL)
TARGET="71498aa4-69d8-58ec-87cf-8d9ffd75d6e0"
HEADERS={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
         "Accept-Language":"zh-TW,zh;q=0.9","Content-Type":"application/json","x-csrf-token":"x"}
s=requests.Session()
assert attach_delivery_location(s,{"discovered_points":[{"lat":25.115,"lon":121.520,"county":"台北市"}]})
base=s.post("https://www.ubereats.com/api/getStoreV1",headers=HEADERS,json={"storeUuid":SID,"sfNuggetCount":24},timeout=30)
base.raise_for_status()
data=(base.json().get("data") or {})
sections=[x for x in data.get("sections") or [] if isinstance(x,dict)]
matches=[x for x in sections if x.get("uuid")==TARGET]
children=list(dict.fromkeys((matches[0].get("subsectionUuids") or []) if matches else []))
print("PX_SECTION_CHILDREN="+json.dumps({"matches":matches,"number_children":len(children)},ensure_ascii=False),flush=True)
def call(tag, sections, subs, offset=0):
    filters={"storeUuid":SID,"sectionUuids":sections,"subsectionUuids":subs}
    payload={"sortAndFilters":None,"storeFilters":filters,
             "pagingInfo":{"enabled":True,"offset":offset},"source":"NV_L2_CATALOG"}
    try:
        r=s.post("https://www.ubereats.com/api/getCatalogPresentationV2",headers=HEADERS,json=payload,timeout=30)
        o=r.json()
        d=o.get("data") if isinstance(o,dict) else None
        p=d.get("pagingInfo") if isinstance(d,dict) else None
        catalog=d.get("catalog") if isinstance(d,dict) else None
        items=[]
        for group in catalog if isinstance(catalog,list) else []:
            native=((group.get("payload") or {}).get("standardItemsPayload") or {})
            for item in native.get("catalogItems") or []:
                if isinstance(item,dict) and item.get("uuid"):
                    items.append(str(item["uuid"]))
        out={"tag":tag,"http":r.status_code,
             "top_status":o.get("status") if isinstance(o,dict) else None,
             "code":d.get("code") if isinstance(d,dict) else None,
             "message":d.get("message") if isinstance(d,dict) else None,
             "offset":offset,"sections":sections,"subsections":subs,
             "count":len(items),"unique":len(set(items)),
             "first_ids":items[:3],"hasMore":p.get("hasMore") if isinstance(p,dict) else None,
             "next_offset":p.get("offset") if isinstance(p,dict) else None}
        print("PX_SUB_PROBE="+json.dumps(out,ensure_ascii=False),flush=True)
        return set(items)
    except Exception as e:
        print("PX_SUB_PROBE_ERROR="+json.dumps({"tag":tag,"error":str(e)[:250]},ensure_ascii=False),flush=True)
        return set()
original=call("baseline",[TARGET],None,0)
for idx,child in enumerate(children):
    call("child_under_parent_"+str(idx),[TARGET],[child])
    if idx<3:
        call("child_as_section_"+str(idx),[child],None)
        call("child_only_"+str(idx),[],[child])
if len(children)>1:
    call("first_2_children",[TARGET],children[:2])
