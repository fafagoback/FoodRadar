"""One-off live audit: Uber Eats 優市北投店, native category pagination.
Read-only: does not write HF, Turso, or production snapshots.
"""
import sys,json,re,time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
import requests
from menu_core import attach_delivery_location

UUID="d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"
STORE={"store_uuid":UUID,"store_url":"https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg",
"discovered_points":[{"lat":25.123246847935267,"lon":121.52951826298867,"county":"台北市"}]}
sess=requests.Session()
assert attach_delivery_location(sess,STORE)
headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
"Accept-Language":"zh-TW,zh;q=0.9,en-US;q=0.8","Content-Type":"application/json","x-csrf-token":"x","x-uber-client-gitref":"x"}
def log(tag,val):print(tag+"="+json.dumps(val,ensure_ascii=False,default=str),flush=True)
def native_items(payload):
    data=(payload or {}).get("data") or {}
    sections=data.get("catalog") or []
    result=[]
    for sec in sections:
        if not isinstance(sec,dict):continue
        result.extend(((sec.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or [])
    return result
def describe(resp):
    try:
        body=resp.json()
        data=body.get("data") or {}
        result=native_items(body)
        return {"status":resp.status_code,"outer_status":body.get("status"),"data_keys":list(data)[:35],
        "meta":data.get("meta"),"catalog_entries":len(data.get("catalog") or []),"items":len(result),
        "brand":[{"name":x.get("title"),"price":x.get("price"),"promoType":(x.get("catalogItemAnalyticsData") or {}).get("promoType")} for x in result if "haagen" in str(x.get("title")).lower() or "häagen" in str(x.get("title")).lower() or "哈根達斯" in str(x.get("title"))][:25],
        "samples":[x.get("title") for x in result[:5]],"error":data.get("message")}
    except Exception as e:return {"status":resp.status_code,"error":str(e),"text":resp.text[:130]}
url="https://www.ubereats.com/api/getStoreV1"
r=sess.post(url,json={"storeUuid":UUID,"sfNuggetCount":24},headers=headers,timeout=25)
r.raise_for_status()
data=r.json()["data"]
catalog=data.get("catalogSectionsMap") or {}
existing=[it for secs in catalog.values() for s in secs for it in (((s.get("payload") or {}).get("standardItemsPayload") or {}).get("catalogItems") or [])]
aisles=data.get("aisles") or {}
catlist=[x for vals in aisles.values() if isinstance(vals,list) for x in vals if isinstance(x,dict)]
log("LIVE_INITIAL",{"status":r.status_code,"total_items":len(existing),"brand":sum("哈根達斯" in str(x.get("title")) or "Häagen" in str(x.get("title")) for x in existing),"aisles":len(catlist),"paging":data.get("catalogSectionPagingInfo")})
related=[x for x in catlist if any(k in json.dumps(x,ensure_ascii=False).lower() for k in ("哈根達斯","冰品","haagen","優惠"))]
log("LIVE_AISLES",[{"title":((x.get("payload") or {}).get("categoryItemPayload") or {}).get("title"),"uuid":x.get("catalogSectionUUID"),"type":x.get("type")} for x in related][:20])
targets=related[:3]
if not any("哈根達斯" in json.dumps(x,ensure_ascii=False) for x in targets):
    targets=([x for x in catlist if "哈根達斯" in json.dumps(x,ensure_ascii=False)]+targets)[:3]
if not targets:
    raise RuntimeError("No category ids to request")
for target in targets:
    section=target.get("catalogSectionUUID")
    title=((target.get("payload") or {}).get("categoryItemPayload") or {}).get("title")
    if not section:
        continue
    for offset in (0,):
      for with_type in (True,False):
        store_filters={"storeUuid":UUID,"sectionUuids":[section],"subsectionUuids":None}
        if with_type:store_filters["sectionTypes"]=["COLLECTION"]
        payload={"sortAndFilters":None,"storeFilters":store_filters,"pagingInfo":{"enabled":True,"offset":offset},"source":"NV_L2_CATALOG"}
        for endpoint in ("https://www.ubereats.com/api/getCatalogPresentationV2","https://www.ubereats.com/_p/api/getCatalogPresentationV2"):
          try:
            res=sess.post(endpoint,json=payload,headers=headers,timeout=30)
            info=describe(res)
            log("LIVE_CATEGORY",{"title":title,"sectionUuid":section,"withType":with_type,"offset":offset,"endpoint":endpoint,**info})
            if info.get("items",0)>0:
                if (info.get("meta") or {}).get("hasMore"):
                    p2=dict(payload,pagingInfo={"enabled":True,"offset":len(native_items(res.json()))})
                    response2=sess.post(endpoint,json=p2,headers=headers,timeout=30)
                    log("LIVE_NEXT_PAGE",{"title":title,"sectionUuid":section,"offset":p2["pagingInfo"]["offset"],**describe(response2)})
                break
          except Exception as exc:
            log("LIVE_CATEGORY_ERROR",{"title":title,"endpoint":endpoint,"error":str(exc)})
        if info.get("items",0)>0:
            break

sections=[x.get("catalogSectionUUID") for x in catlist if x.get("catalogSectionUUID")]
for test_sections in [sections[:2],sections]:
  if not test_sections:continue
  p={"sortAndFilters":None,"storeFilters":{"storeUuid":UUID,"sectionUuids":test_sections,"subsectionUuids":None},"pagingInfo":{"enabled":True,"offset":0},"source":"NV_L2_CATALOG"}
  response=sess.post("https://www.ubereats.com/api/getCatalogPresentationV2",json=p,headers=headers,timeout=40)
  result=native_items(response.json())
  pg=(response.json().get("data") or {}).get("pagingInfo")
  log("LIVE_MULTISECTION",{"sections_requested":len(test_sections),"http":response.status_code,"catalog_sections":len((response.json().get("data") or {}).get("catalog") or []),"total_items":len(result),"unique_sku":len({i.get("uuid") or i.get("itemUuid") or i.get("title") for i in result}),"pagingInfo":pg,
       "haagen":sum("Häagen" in str(i.get("title")) for i in result)})
  if len(test_sections)==len(sections):
    base_uuids={x.get("uuid") for x in result}
    for off in [60,120,180]:
      p["pagingInfo"]["offset"]=off
      resp2=sess.post("https://www.ubereats.com/api/getCatalogPresentationV2",json=p,headers=headers,timeout=40)
      more=native_items(resp2.json())
      ids={x.get("uuid") for x in more}
      log("LIVE_MULTI_PAGE",{"offset":off,"status":resp2.status_code,"items":len(more),"unique_sku":len(ids),"new_sku_vs_first":len(ids-base_uuids),"page_meta":(resp2.json().get("data") or {}).get("pagingInfo"),"first5":[x.get("title") for x in more[:5]]})
  matches=[i for i in result if "Häagen" in str(i.get("title"))][:4]
  for i in matches:
    log("LIVE_SKU_FIELDS",{"name":i.get("title"),"keys":list(i.keys()),"price":i.get("price"),"analytics":i.get("catalogItemAnalyticsData"),"thumbnail":i.get("itemThumbnailElements"),"itemPromotion":i.get("itemPromotion"),"itemLevelPromotion":i.get("itemLevelPromotion"),"promoInfo":i.get("promoInfo"),"titleBadge":i.get("titleBadge")})


# Exercise the *production* capture/ETL path on this live store; no HF writes.
import tempfile,tarfile,hashlib
from raw_capture import fetch_store_raw, documents_from_hf_raw
with tempfile.TemporaryDirectory() as work:
  root=Path(work)/"raw"
  root.mkdir()
  result=fetch_store_raw(STORE,root)
  log("LIVE_PRODUCTION_CAPTURE",{"success":result.get("success"),"error":result.get("error"),
      "sections":result.get("catalog_section_count"),"pages":len(result.get("catalog_responses") or []),
      "item_occurrences":result.get("catalog_item_occurrences")})
  if not result.get("success"):raise RuntimeError("Fixed crawler did not capture the whole store")
  batch="20261109093000"
  tarpath=Path(work)/"raw.tar.gz"
  responses={}
  with tarfile.open(tarpath,"w:gz") as archive:
    for p in sorted(root.rglob("*.body")):
      name=p.relative_to(root).as_posix()
      responses[name]=hashlib.sha256(p.read_bytes()).hexdigest()
      archive.add(p,arcname=name)
  manifest={"format":"foodradar-immutable-http-body-v1","batch_id":batch,
     "complete":True,"stores":[result],"responses":responses}
  with tarfile.open(tarpath) as archive:
    doc=documents_from_hf_raw(archive,manifest)[0]
  items=[i for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []
           for i in section.get("hasMenuItem") or []]
  brand=[{"name":i.get("name"),"price":(i.get("offers") or {}).get("price"),
          "proof_status":(i.get("verified_price") or {}).get("status"),
          "proof_type":(i.get("verified_price") or {}).get("kind")}
        for i in items if "Häagen" in i.get("name","") or "哈根達斯" in i.get("name","")]
  from collections import Counter
  reason_counts=Counter(((i.get("verified_price") or {}).get("reason") or (i.get("verified_price") or {}).get("status")) for i in items if "Häagen" in i.get("name",""))
  log("LIVE_PRICE_REJECTION_REASONS",dict(reason_counts))
  from verified_prices import quote_from_native_item
  raw_reasons=Counter()
  raw_examples=[]
  for response in result.get("catalog_responses") or []:
    original=json.loads((root/response["path"]).read_text())
    for it in native_items(original):
      if "Häagen" not in str(it.get("title")):continue
      proof=quote_from_native_item(it) or {}
      raw_reasons[(proof.get("status"),proof.get("reason"),proof.get("kind"))]+=1
      if len(raw_examples)<8:raw_examples.append({"title":it.get("title"),"price":it.get("price"),"proof":proof})
  log("LIVE_RAW_PRICE_PROOFS",{"types":[{"signature":k,"count":v} for k,v in raw_reasons.items()],"examples":raw_examples})
  log("LIVE_ETL_VERIFICATION",{"total_item_occurrences":len(items),
      "brand_item_occurrences":len(brand),"unique_brand_skus":len({
      i.get("identifier") for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []
      for i in section.get("hasMenuItem") or [] if "Häagen" in i.get("name","")}),
      "brand_proof_verified":sum(x["proof_status"]=="VERIFIED_MENU" for x in brand),
      "brand_items":brand[:25]})
  assert len(brand)>=10, "Crawler still missing Haagen"
  assert sum(x["proof_status"]=="VERIFIED_MENU" for x in brand)>=10, "Native item markdown is still not verified"

print("LIVE_AUDIT_COMPLETE",flush=True)
