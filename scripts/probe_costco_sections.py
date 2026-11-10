"""Diagnose Uber's category API: batch request vs one-category-at-a-time requests.

Only requests public Uber store/category responses. Read-only; no DB publishing.
"""
import json
import tempfile
from pathlib import Path
import requests
from sys import path
path.insert(0,"src")
path.insert(0,"scripts")
from daily_triple_audit import HEADERS
from menu_core import attach_delivery_location, normalize_store_uuid
from catalog_complete import _section_ids, capture_catalog, _catalog_entries, merge_catalog_pages

URL="https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%8C%97%E6%8A%95%E5%BA%97/pOIgHV_HWhCkGt_liXWnpw"
LAT,LON=25.13184,121.498632
sid=normalize_store_uuid("",URL)
session=requests.Session()
assert attach_delivery_location(session,{"discovered_points":[{"lat":LAT,"lon":LON,"county":"台北市"}]})
response=session.post("https://www.ubereats.com/api/getStoreV1",headers=HEADERS,json={"storeUuid":sid,"sfNuggetCount":24},timeout=30)
response.raise_for_status()
base=response.json()["data"]
ids=_section_ids(base)
print("PROBE_AISLES="+json.dumps({"store_uuid":sid,"ids":ids,"aisle_keys":list((base.get("aisles") or {}).keys()),"store_base_sections":list((base.get("catalogSectionsMap") or {}).keys()),"first_aisle":str(next(iter((base.get("aisles") or {}).values()),[]))[:1500]},ensure_ascii=False),flush=True)
with tempfile.TemporaryDirectory() as tmp:
  root=Path(tmp)
  def save(root_,name,resp):
    out=root_/name
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_bytes(resp.content)
  def catalog_items(data):
    rows={}
    for sections in (data.get("catalogSectionsMap") or {}).values():
      if not isinstance(sections,list):continue
      for section in sections:
        for item in (((section.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or []):
          if not isinstance(item,dict): continue
          key=item.get("uuid") or item.get("itemUuid") or item.get("id")
          if key:rows[key]=item.get("title")
    return rows
  aggregate={}
  for i,uid in enumerate(ids):
    label=str(uid)[:45]
    trial={"aisles":{"cat":[{"catalogSectionUUID":uid}]},"catalogSectionsMap":{}}
    folder=root/str(i)
    folder.mkdir(exist_ok=True)
    try:
      got=capture_catalog(session,sid,trial,folder,HEADERS,save)
      raw=[json.loads((folder / p["path"]).read_bytes()) for p in got["catalog_responses"]]
      if got["catalog_item_occurrences"]:
        merged=merge_catalog_pages(trial,raw)
        items=catalog_items(merged)
      else:items={}
      new=set(items)-set(aggregate)
      aggregate.update(items)
      print("PROBE_SECTION="+json.dumps({"index":i,"section":label,"pages":len(raw),"occurrences":got["catalog_item_occurrences"],"distinct_items":len(items),"new_items":len(new),"cumulative_items":len(aggregate),"samples":list(items.values())[:3]},ensure_ascii=False),flush=True)
    except Exception as e:
      print("PROBE_SECTION_ERROR="+json.dumps({"index":i,"section":label,"error":str(e)[:350]},ensure_ascii=False),flush=True)
  print("PROBE_TOTAL="+json.dumps({"sections":len(ids),"aggregate_items":len(aggregate),"sample":list(aggregate.values())[:15]},ensure_ascii=False),flush=True)
