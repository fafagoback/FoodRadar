"""Audit original immutable HTTP response bytes for an abnormal FoodRadar price.

No business rules are inferred: prints exact native Uber Eats numeric item price
before and after transformation, verifies manifest/archive/body SHA-256.
"""
import hashlib
import json
import os
import tarfile
from pathlib import Path
from huggingface_hub import HfApi,hf_hub_download

REPO="hub-google/FoodRadar"
TARGET_SID="c98b81a0-e46e-4f08-be3d-9e84815e0b9c"
TARGET_PID="44e7fcbe-8ed5-52a7-98b7-5357c86889f5"
DAYS=("20261117",)  # Target the one anomalous native HTTP capture first.

def audit():
  token=os.environ.get("HF_TOKEN")
  api=HfApi(token=token)
  files=api.list_repo_files(REPO,repo_type="dataset")
  paths=[p for p in files if p.startswith("Raw/") and p.endswith("/manifest.json")
         and p.split("/")[1][:8] in DAYS]
  print("RAW_MANIFEST_CANDIDATES="+json.dumps(paths),flush=True)
  found=0
  for manifest_path in sorted(paths):
    batch=manifest_path.split("/")[1]
    mp=hf_hub_download(REPO,manifest_path,repo_type="dataset",token=token)
    meta=json.loads(Path(mp).read_bytes())
    stores=[s for s in meta.get("stores",[]) if s.get("store_uuid")==TARGET_SID
        or TARGET_SID in s.get("store_url","")]
    # Exclude irrelevant large archives, but show coverage for each day.
    if not stores:
      print("RAW_TARGET_STORE_ABSENT="+batch,flush=True)
      continue
    print("RAW_STORE_BATCH="+json.dumps({
      "batch":batch,"store_count":len(stores),"complete":meta.get("complete"),
      "raw_archive":meta.get("archive_path"),"manifest_sha":hashlib.sha256(Path(mp).read_bytes()).hexdigest(),
      "store_records":[{"store_uuid":s.get("store_uuid"),"success":s.get("success"),
        "response_path":s.get("response_path"),"catalog_pages":s.get("catalog_responses"),
        "catalog_complete":s.get("catalog_complete"),"captured_at":s.get("captured_at")}
        for s in stores]},ensure_ascii=False),flush=True)
    if not any(s.get("success") for s in stores):
      continue
    raw=hf_hub_download(REPO,meta["archive_path"],repo_type="dataset",token=token)
    archive_sha=hashlib.sha256(Path(raw).read_bytes()).hexdigest()
    if archive_sha!=meta["archive_sha256"]:raise ValueError("RAW_ARCHIVE_HASH_MISMATCH: "+batch)
    with tarfile.open(raw,"r:gz") as tar:
      for s in stores:
        if not s.get("success"):continue
        paths_for_store=[s.get("response_path")]+[x["path"] for x in s.get("catalog_responses") or []]
        for path in paths_for_store:
          if not path:continue
          f=tar.extractfile(path)
          if f is None:raise ValueError("Missing original response "+path)
          body=f.read()
          sha=hashlib.sha256(body).hexdigest()
          if sha!=meta["responses"][path]:raise ValueError("ORIGINAL_HTTP_SHA_MISMATCH: "+path)
          envelope=json.loads(body)
          native=envelope.get("data",{})
          section_count=0
          items=[]
          # getStoreV1 keeps sections in catalogSectionsMap; getCatalogPresentationV2
          # uses data.catalog (separate HTTP archive bodies).
          source_sections=([native.get("catalog") or []] if path.startswith("catalog/")
                           else (native.get("catalogSectionsMap") or {}).values())
          for section_list in source_sections:
            if not isinstance(section_list,list):continue
            for sec in section_list:
              section_count+=1
              payload=sec.get("payload",{}).get("standardItemsPayload",{})
              section=(payload.get("title") or {}).get("text")
              for it in payload.get("catalogItems") or []:
                uuid=it.get("uuid") or it.get("itemUuid") or it.get("id")
                title=str(it.get("title") or "")
                if uuid==TARGET_PID or ("牛肩里肌火鍋肉片" in title):
                  items.append({
                    "uuid":uuid,"title":title,"section":section,
                    "native_price_cents":it.get("price"),
                    "native_price_twd":(it.get("price")/100 if isinstance(it.get("price"),(float,int)) else None),
                    "itemDescription":str(it.get("itemDescription") or "")[:220],
                    "catalogItemAnalyticsData":it.get("catalogItemAnalyticsData"),
                    "itemPromotion":it.get("itemPromotion"),
                    "itemLevelPromotion":it.get("itemLevelPromotion"),
                    "itemThumbnailElements":it.get("itemThumbnailElements"),
                    "original_http_body_sha256":sha,
                    "native_item_sha256":hashlib.sha256(json.dumps(
                       it,sort_keys=True,ensure_ascii=False,separators=(",",":")).encode()).hexdigest(),
                  })
          if items:
            found+=1
            print("RAW_ORIGINAL_ITEM_EVIDENCE="+json.dumps({
              "batch":batch,"capture":s.get("captured_at"),"store_uuid":s.get("store_uuid"),
              "raw_path":path,"archive_sha256":archive_sha,"body_sha256":sha,
              "isOpen":native.get("isOpen"),"store_title":native.get("title"),
              "records":items
            },ensure_ascii=False),flush=True)
          else:
            print("RAW_ITEM_ABSENT="+json.dumps({
              "batch":batch,"raw_path":path,"sections":section_count},ensure_ascii=False),flush=True)
  print("ORIGINAL_PRICE_EVIDENCE_COUNT="+str(found),flush=True)

if __name__=="__main__":audit()
