"""Read-only audit of a real HF archive's native-to-converted identity join.

No database writes, no guessed promotions, no raw customer/store payload logs.
"""
import collections
import json
import os
import sys
import tarfile
from pathlib import Path

from huggingface_hub import hf_hub_download

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from archive_pairing import archived_documents_with_native
from verified_prices import quote_from_converted_item

repo="hub-google/UberEat"
prefix="FoodRadar"
token=os.getenv("HF_TOKEN")
latest=json.loads(Path(hf_hub_download(repo,f"{prefix}/latest.json",
                  repo_type="dataset",token=token)).read_text(encoding="utf-8"))
batch=latest["batch_id"]
archive=hf_hub_download(repo,latest["archive_path"],repo_type="dataset",token=token)
with tarfile.open(archive,"r:gz") as tar:
    members={m.name:m for m in tar.getmembers() if m.isfile() and m.name.endswith(".json")}
    docs,pairing=archived_documents_with_native(tar,members,batch,latest)
    codes=collections.Counter()
    uncertified=collections.Counter()
    with_proof=0
    for doc in docs:
        for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
            for item in section.get("hasMenuItem") or []:
                q=item.get("verified_price")
                if q is not None:
                    with_proof+=1
                    if quote_from_converted_item(item) is not None:
                        codes[q["kind"]]+=1
                    else:
                        uncertified[q.get("reason","INVALID_PROOF")]+=1
    result={"pairing":pairing,"native_item_occurrences":with_proof,
            "verified_types":dict(codes),
            "unverified_reasons":dict(uncertified.most_common(16))}
    print("HF_V2_LIVE_RAW_AUDIT="+json.dumps(result,ensure_ascii=False),flush=True)
    if pairing["raw_files"]<1 or pairing["native_matched"]!=pairing["raw_files"]:
        raise SystemExit("HF raw pair coverage failed")
    if not codes.get("NONE"):
        raise SystemExit("No verified native standard price parsed; cannot ship")
    if not codes.get("SAME_SKU_BXGY"):
        raise SystemExit("No native same-SKU buy/get parsed; cannot ship")
