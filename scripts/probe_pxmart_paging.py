"""Read-only targeted diagnosis of PX Mart Beitou Mingde paging at offset 1020."""
import json
import requests
import sys
sys.path.insert(0,"src")
from menu_core import attach_delivery_location, normalize_store_uuid
from catalog_complete import _section_ids, _item_count
URL="https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA"
SID=normalize_store_uuid("",URL)
TARGET="71498aa4-69d8-58ec-87cf-8d9ffd75d6e0"
headers={"User-Agent":"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
         "Accept-Language":"zh-TW,zh;q=0.9","Content-Type":"application/json","x-csrf-token":"x"}
session=requests.Session()
attach_delivery_location(session,{"discovered_points":[{"lat":25.115,"lon":121.520,"county":"台北市"}]})
base=session.post("https://www.ubereats.com/api/getStoreV1",json={"storeUuid":SID,"sfNuggetCount":24},headers=headers,timeout=30)
data=(base.json().get("data") or {})
sections=[x for entries in (data.get("aisles") or {}).values() for x in entries if isinstance(x,dict)]
matched=[x for x in sections if x.get("catalogSectionUUID")==TARGET]
print("PX_BASE="+json.dumps({"status":base.status_code,"total_sections":len(_section_ids(data)),"target":matched,"section_paging":data.get("catalogSectionPagingInfo"),"keys":list(data)},ensure_ascii=False)[:8000],flush=True)
for offset in (900,960,1020,1080):
    for sectype in (None,["COLLECTION"]):
        filters={"storeUuid":SID,"sectionUuids":[TARGET],"subsectionUuids":None}
        if sectype:filters["sectionTypes"]=sectype
        payload={"sortAndFilters":None,"storeFilters":filters,
                 "pagingInfo":{"enabled":True,"offset":offset},"source":"NV_L2_CATALOG"}
        try:
            res=session.post("https://www.ubereats.com/api/getCatalogPresentationV2",headers=headers,json=payload,timeout=30)
            ob=res.json()
            d=ob.get("data") if isinstance(ob,dict) else None
            cat=d.get("catalog") if isinstance(d,dict) else None
            print("PX_PAGING_PROBE="+json.dumps({
                "offset":offset,"sectionTypes":sectype,"http":res.status_code,
                "top_keys":list(ob) if isinstance(ob,dict) else None,
                "data_keys":list(d) if isinstance(d,dict) else None,
                "hasMore":(d.get("pagingInfo") or {}).get("hasMore") if isinstance(d,dict) else None,
                "next_offset":(d.get("pagingInfo") or {}).get("offset") if isinstance(d,dict) else None,
                "items":_item_count(cat) if isinstance(cat,list) else None,
                "error":str(ob.get("error") or ob.get("message") or "")[:400] if isinstance(ob,dict) else "",
                "body_head":res.text[:230] if res.status_code!=200 else "",
            },ensure_ascii=False),flush=True)
        except Exception as exc:
            print("PX_PAGING_ERROR="+json.dumps({"offset":offset,"type":type(exc).__name__,"error":str(exc)[:250]},ensure_ascii=False),flush=True)
