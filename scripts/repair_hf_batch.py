"""Backup, verify, then delete only an explicitly identified HF Raw batch."""
import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tarfile
from pathlib import Path
from huggingface_hub import HfApi, CommitOperationDelete, hf_hub_download

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from raw_capture import documents_from_hf_raw

REPO = "hub-google/FoodRadar"

def paths(batch):
    if not re.fullmatch(r"\d{14}", batch):
        raise ValueError("Invalid Raw batch ID")
    return [f"Raw/{batch}/manifest.json", f"Raw/{batch}/capture.tar.gz"]

def digest(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()

def present(api, batch):
    files = set(api.list_repo_files(repo_id=REPO, repo_type="dataset"))
    return [p in files for p in paths(batch)]

def matching_anomaly(docs):
    found = []
    for doc in docs:
        store = str(doc.get("name") or "")
        if "北投" not in store:
            continue
        for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
            for item in section.get("hasMenuItem") or []:
                name = re.sub(r"\s+", "", str(item.get("name") or "")).lower()
                if not all(x in name for x in ("ariel", "抗菌洗衣精", "800g", "除蟎")):
                    continue
                try:
                    price = float((item.get("offers") or {}).get("price") or 0)
                except (ValueError, TypeError):
                    continue
                if abs(price - 229.0) <= 0.001:
                    found.append({"store": store, "product": item.get("name"),
                                  "price": price, "at": doc.get("captured_at")})
    return found

def stage(api, batch, backup):
    backup.mkdir(parents=True, exist_ok=True)
    found = present(api, batch)
    if found == [False, False]:
        report = {"batch": batch, "status": "already_deleted"}
    elif found != [True, True]:
        raise RuntimeError("HF Raw pair incomplete")
    else:
        a,b = paths(batch)
        token = os.environ["HF_TOKEN"]
        mb = Path(hf_hub_download(REPO,a,repo_type="dataset",token=token)).read_bytes()
        manifest = json.loads(mb)
        raw = Path(hf_hub_download(REPO,b,repo_type="dataset",token=token))
        if (manifest.get("batch_id") != batch
                or manifest.get("archive_path") != b
                or manifest.get("archive_sha256") != digest(raw)):
            raise RuntimeError("HF source integrity check failed")
        with tarfile.open(raw,"r:gz") as archive:
            docs = documents_from_hf_raw(archive,manifest,quarantined_store_uuids=[])
        evidence = matching_anomaly(docs)
        if not evidence:
            # Capture actual parsed names/prices before touching HF. A visible
            # price can originate from a different batch or a normalized quote.
            near = []
            for doc in docs:
                shop = str(doc.get("name") or "")
                for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
                    for item in section.get("hasMenuItem") or []:
                        name = str(item.get("name") or "")
                        if ("ariel" in name.lower() or "洗衣精" in name) and len(near) < 60:
                            near.append({"store": shop, "name": name,
                                         "price": (item.get("offers") or {}).get("price"),
                                         "captured_at": doc.get("captured_at")})
            print("FOODRADAR_RAW_REPAIR_DIAGNOSTIC=" + json.dumps({
                "batch": batch, "parsed_stores": len(docs),
                "beitou_stores": [str(d.get("name") or "") for d in docs
                                  if "北投" in str(d.get("name") or "")][:30],
                "matching_products": near,
            }, ensure_ascii=False), flush=True)
            raise RuntimeError("Target ARIEL NT$229 source evidence not found; no deletion")
        (backup/"manifest.json").write_bytes(mb)
        shutil.copyfile(raw,backup/"capture.tar.gz")
        report = {"batch":batch,"status":"backed_up",
                  "manifest_sha256":hashlib.sha256(mb).hexdigest(),
                  "archive_sha256":digest(raw),"evidence":evidence[:10]}
    (backup/"verification.json").write_text(
        json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
    print("FOODRADAR_PRICE_REPAIR_STAGE="+json.dumps(report,ensure_ascii=False),flush=True)

def delete(api,batch,backup):
    report=json.loads((backup/"verification.json").read_text(encoding="utf-8"))
    if report.get("batch") != batch:
        raise RuntimeError("Wrong backup batch")
    found=present(api,batch)
    if found == [False,False]:
        print("FOODRADAR_PRICE_REPAIR_ALREADY_DELETED="+batch,flush=True)
        return
    if (found != [True,True] or report.get("status") != "backed_up"
            or digest(backup/"capture.tar.gz") != report.get("archive_sha256")
            or hashlib.sha256((backup/"manifest.json").read_bytes()).hexdigest()
                != report.get("manifest_sha256")):
        raise RuntimeError("Source/backup mismatch; refusing deletion")
    api.create_commit(repo_id=REPO,repo_type="dataset",
                      operations=[CommitOperationDelete(path_in_repo=p) for p in paths(batch)],
                      commit_message="Remove anomalous verified Raw "+batch)
    if present(api,batch) != [False,False]:
        raise RuntimeError("HF delete not verified")
    print("FOODRADAR_PRICE_REPAIR_DELETED="+batch,flush=True)

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("action",choices=["stage","delete"])
    parser.add_argument("--batch",required=True)
    parser.add_argument("--backup-dir",type=Path,required=True)
    args=parser.parse_args()
    paths(args.batch)
    api=HfApi(token=os.environ["HF_TOKEN"])
    (stage if args.action=="stage" else delete)(api,args.batch,args.backup_dir)
