"""Read-only provenance inspection of suspicious FoodRadar menu prices."""
import hashlib
import json
import os
from pathlib import Path

import zstandard as zstd
from huggingface_hub import hf_hub_download

REPO="hub-google/FoodRadar"
PREFIX="StaticCatalog"
STORE_FRAGMENT="北投行義"
PRODUCT_FRAGMENT="美國牛肩里肌火鍋肉片"

def load(token,filename):
    p=hf_hub_download(repo_id=REPO,repo_type="dataset",filename=PREFIX+"/"+filename,token=token)
    data=Path(p).read_bytes()
    if filename.endswith(".zst"):
        sha=Path(filename).stem
        assert hashlib.sha256(data).hexdigest()==sha
        data=zstd.ZstdDecompressor().decompress(data)
    return json.loads(data)

def inspect():
    token=os.environ.get("HF_TOKEN")
    manifest=load(token,"current.json")
    stores=load(token,"blobs/"+manifest["files"]["stores"]+".zst")
    product_index=load(token,"blobs/"+manifest["files"]["catalog"]+".zst")
    names={s["store_uuid"]:s["name"] for s in stores}
    matches=[]
    for index,row in enumerate(product_index):
        name=names.get(row[0],"")
        if STORE_FRAGMENT in name and PRODUCT_FRAGMENT in str(row[16]):
            matches.append((index,row,name))
    print("SUSPECT_MATCHES_COUNT="+str(len(matches)),flush=True)
    for ix,row,store_name in matches[:8]:
        h=manifest["files"]["products"][ix//manifest["chunk_rows"]]
        card=load(token,"blobs/"+h+".zst")[ix%manifest["chunk_rows"]]
        bid=hashlib.sha256(row[0].encode()).digest()[0]%manifest["history_buckets"]
        histsha=manifest["files"]["history"].get(str(bid))
        hist=load(token,"blobs/"+histsha+".zst").get(row[0]+"\x1f"+row[1],[]) if histsha else []
        events=[]
        for evt in hist:
            try:state=json.loads(evt.get("new_state") or evt.get("old_state") or "{}")
            except (ValueError,TypeError):continue
            events.append({"at":evt.get("event_time"),"type":evt.get("event_type"),
                 "price":state.get("price"),"effective_price":state.get("effective_price"),
                 "promo":state.get("promo_type"),"quantity":state.get("quantity"),
                 "price_valid":state.get("price_valid"),"menu_quote_valid":state.get("menu_quote_valid"),
                 "unit_only":state.get("effective_unit_only")})
        result={"store_name":store_name,"product_name":card.get("product_name"),
                "store_uuid":row[0],"product_uuid":row[1],
                "last_price":card.get("price"),"effective_price":card.get("current_price"),
                "reference_price":card.get("seven_day_reference_price"),
                "discount_pct":card.get("discount_pct"),
                "verified":card.get("price_evidence_status"),
                "is_deal":card.get("is_verified_delivery_deal"),
                "history":events[-40:]}
        print("SUSPECT_PRICE_PROVENANCE="+json.dumps(result,ensure_ascii=False),flush=True)
    return len(matches)

if __name__=="__main__":
    if not inspect():
        raise SystemExit("TARGET_NOT_FOUND")
