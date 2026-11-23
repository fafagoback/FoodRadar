"""Diagnose PX Mart API offset cap using only native API, no assumptions.

Runs on GitHub Actions. Prints direct Uber error payload and native section/subsection
metadata; probes bounded alternative pagination and genuine subsection filters.
"""
import json
import sys
import requests
sys.path.insert(0,"src")
from menu_core import attach_delivery_location,normalize_store_uuid
from catalog_complete import _section_ids,_item_count
URL="https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"
SID=normalize_store_uuid("",URL)
TARGET="71498aa4-69d8-58ec-87cf-8d9ffd75d6e0"
HEADERS={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
         "Accept-Language":"zh-TW,zh;q=0.9","Content-Type":"application/json","x-csrf-token":"x"}
s=requests.Session()
assert attach_delivery_location(s,{"discovered_points":[{"lat":25.115,"lon":121.520,"county":"台北市"}]})
response=s.post("https://www.ubereats.com/api/getStoreV1",headers=HEADERS,json={"storeUuid":SID,"sfNuggetCount":24},timeout=30)
response.raise_for_status()
base=response.json().get("data") or {}
def compact(x,n=1600):
    return json.dumps(x,ensure_ascii=False,default=str)[:n]
parts=base.get("subsectionsMap") or {}
entities=base.get("sectionEntitiesMap") or {}
print("PX_META="+compact({"subsections_type":type(parts).__name__,"subsections_keys":list(parts)[:35] if isinstance(parts,dict) else None,
    "target_subsections":parts.get(TARGET) if isinstance(parts,dict) else None,
    "entities_type":type(entities).__name__,"entities_keys":list(entities)[:40] if isinstance(entities,dict) else None,
    "target_entities":entities.get(TARGET) if isinstance(entities,dict) else None,
    "categories":base.get("categories"),"sections":str(base.get("sections"))[:1000]},5500),flush=True)
for key in list(parts)[:4] if isinstance(parts,dict) else []:
    print("PX_SUBSECTION_SAMPLE="+compact({"key":key,"value":parts[key]},3000),flush=True)
# Actual subsection IDs may be nested in store data. Discover UUIDs explicitly
# without fabricating them. Only use if they are included in native metadata.
import re
candidate_subsections=[]
subsections_text=compact(parts,150000)
for match in re.finditer(r"[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}",subsections_text):
    sid=match.group()
    if sid not in candidate_subsections and sid!=TARGET:
        candidate_subsections.append(sid)
print("PX_SUBSECTION_IDS="+compact(candidate_subsections[:25],1500),flush=True)
def query(offset,section_ids=None,sub_ids=None,section_types=None,enabled=True,source="NV_L2_CATALOG"):
    sf={"storeUuid":SID,"sectionUuids":[TARGET] if section_ids is None else section_ids,
        "subsectionUuids":sub_ids}
    if section_types:sf["sectionTypes"]=section_types
    payload={"sortAndFilters":None,"storeFilters":sf,
             "pagingInfo":{"enabled":enabled,"offset":offset},"source":source}
    try:
        r=s.post("https://www.ubereats.com/api/getCatalogPresentationV2",headers=HEADERS,json=payload,timeout=30)
        obj=r.json()
        d=obj.get("data") if isinstance(obj,dict) else {}
        paging=d.get("pagingInfo") if isinstance(d,dict) else None
        cat=d.get("catalog") if isinstance(d,dict) else None
        ret={"http":r.status_code,"top_status":obj.get("status") if isinstance(obj,dict) else None,
             "offset":offset,"sections":sf["sectionUuids"],"subs":sub_ids,"enabled":enabled,"source":source,
             "items":_item_count(cat) if isinstance(cat,list) else None,
             "hasMore":paging.get("hasMore") if isinstance(paging,dict) else None,
             "next_offset":paging.get("offset") if isinstance(paging,dict) else None,
             "error_message":d.get("message") if isinstance(d,dict) else None,
             "error_code":d.get("code") if isinstance(d,dict) else None,
             "error_meta":d.get("meta") if isinstance(d,dict) else None,
             "data_keys":list(d) if isinstance(d,dict) else None}
        print("PX_VARIANT="+compact(ret,1900),flush=True)
    except Exception as e:
        print("PX_VARIANT_ERROR="+compact({"offset":offset,"error":type(e).__name__+":"+str(e)[:200]}),flush=True)
# Locate threshold with controlled sparse offsets.
for off in [960, 999,1000,1001,1019,1020,1021,1100]:
    query(off)
# Query section in alternate native forms, not synthetic SKU data.
query(1020,section_ids=[],sub_ids=None)
query(1020,section_ids=[TARGET],sub_ids=[],enabled=False)
query(1020,section_ids=[TARGET],sub_ids=None,enabled=False)
for sid in candidate_subsections[:3]:
    query(0,section_ids=[TARGET],sub_ids=[sid])
    query(1020,section_ids=[TARGET],sub_ids=[sid])
