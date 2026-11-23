"""Read-only check of getStoreV1 native subdivision metadata instability.

Jinji is proven to have failed at offset 1020 in old Actions #397.
The same store then succeeded with 2 native children (run 43134440335)
and failed on rerun because children were absent (run 43134788915).
Only getStoreV1 is queried, no other stores or publishing.
"""
import json
import sys
import time
from pathlib import Path
import requests
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from menu_core import attach_delivery_location,global_limiter
SID="0941d15a-f620-5970-b04a-b9edf626cc7f"
PARENTS=["113eea56-673a-507b-bc33-9d11f3a2183f",
         "5e5e0f23-5a2f-5f9a-8fce-7dbd05b3b349"]
HDR={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Safari/537.36",
     "Accept":"*/*","Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8",
     "Content-Type":"application/json","x-csrf-token":"x"}
sess=requests.Session()
assert attach_delivery_location(sess,{"discovered_points":[{"lat":25.123246847935267,
    "lon":121.52951826298867,"county":"台北市"}]})
sets={}
for attempt in range(1,5):
    global_limiter.wait()
    r=sess.post("https://www.ubereats.com/api/getStoreV1",
                json={"storeUuid":SID,"sfNuggetCount":24},
                headers=HDR,timeout=30)
    if r.status_code==429:
        global_limiter.trigger_backoff(30,"probe metadata 429")
        raise RuntimeError("HTTP 429")
    r.raise_for_status()
    obj=r.json()
    d=obj.get("data") or {}
    sections=d.get("sections") or []
    record={}
    for parent in PARENTS:
        item=next((s for s in sections
                   if isinstance(s,dict) and s.get("uuid")==parent),None)
        children=list(dict.fromkeys((item or {}).get("subsectionUuids") or []))
        record[parent]={"present":item is not None,"children":children}
        sets.setdefault(parent,[]).append(children)
    print("JINJI_NATIVE_METADATA="+json.dumps({
        "attempt":attempt,"store_uuid":SID,"status":obj.get("status"),
        "sections_count":len(sections),"aisles_count":sum(
            len(x) for x in (d.get("aisles") or {}).values() if isinstance(x,list)),
        "parents":record,
    },ensure_ascii=False),flush=True)
    time.sleep(.5)
print("JINJI_METADATA_SUMMARY="+json.dumps({
    "attempts":4,
    "union_children":{parent:sorted(set().union(*map(set,items))) for parent,items in sets.items()},
    "missing_children_attempts":{parent:sum(not item for item in items)
                                  for parent,items in sets.items()}
},ensure_ascii=False),flush=True)
sess.close()
