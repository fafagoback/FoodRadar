"""Probe actual native subsection IDs from returned item / catalog metadata.

Only one previously proven 1020-failing merchant, no request to offset 1020,
and no HF/Turso writes.
"""
import json
import sys
from collections import defaultdict
from pathlib import Path
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location,global_limiter

SID="0941d15a-f620-5970-b04a-b9edf626cc7f"
PARTS=["113eea56-673a-507b-bc33-9d11f3a2183f",
       "5e5e0f23-5a2f-5f9a-8fce-7dbd05b3b349"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
     "Accept":"*/*","Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
     "Content-Type":"application/json","x-csrf-token":"x"}
s=requests.Session()
attach_delivery_location(s,{"discovered_points":[{
    "lat":25.123246847935267,"lon":121.52951826298867,"county":"台北市"}]})
for part in PARTS:
    by_subsection=defaultdict(set)
    group_subsection=defaultdict(set)
    unseen=0
    for offset in (0, 60, 300, 660, 960):
        assert offset<1000
        global_limiter.wait()
        r=s.post("https://www.ubereats.com/api/getCatalogPresentationV2",
                 json={"sortAndFilters":None,
                       "storeFilters":{"storeUuid":SID,"sectionUuids":[part],
                                       "subsectionUuids":None},
                       "pagingInfo":{"enabled":True,"offset":offset},
                       "source":"NV_L2_CATALOG"},headers=HDR,timeout=30)
        r.raise_for_status()
        resp=r.json()
        data=resp.get("data") or {}
        groups=data.get("catalog") or []
        items=0
        native_fields=set()
        for g in groups if isinstance(groups,list) else []:
            native=(g.get("payload") or {}).get("standardItemsPayload") or {}
            native_fields.update(native.keys())
            entry_subsection=g.get("catalogSectionUUID") or native.get("subsectionUuid")
            for item in native.get("catalogItems") or []:
                if not isinstance(item,dict):continue
                item_id=item.get("uuid") or item.get("itemUuid")
                subsection=item.get("subsectionUuid")
                if item_id:
                    items+=1
                    if isinstance(subsection,str):
                        by_subsection[subsection].add(item_id)
                    else:
                        unseen+=1
                    if isinstance(entry_subsection,str):
                        group_subsection[entry_subsection].add(item_id)
        print("JINJI_ITEM_SUBSECTION_PAGE="+json.dumps({
            "part":part,"offset":offset,"http":r.status_code,
            "items":items,"item_subsections":len(by_subsection),
            "group_subsections":len(group_subsection),
            "missing_item_subsection":unseen,
            "native_keys":sorted(native_fields),
            "hasMore":(data.get("pagingInfo") or {}).get("hasMore"),
        },ensure_ascii=False),flush=True)
    print("JINJI_ITEM_SUBSECTION_REPORT="+json.dumps({
        "part":part,
        "item_subsection_ids":{k:len(v) for k,v in by_subsection.items()},
        "group_subsection_ids":{k:len(v) for k,v in group_subsection.items()},
        "items_without_subsection":unseen,
    },ensure_ascii=False),flush=True)
s.close()
