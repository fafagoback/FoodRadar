"""Two-phase HF release: immutable SQLite first, manifest pointer last.

No UE requests. No production writes unless --upload is explicitly passed.
Do not use before the HF browser Range/CORS and speed gates pass.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile

def sha256(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as f:
        for chunk in iter(lambda:f.read(1<<20),b""):
            h.update(chunk)
    return h.hexdigest()

def candidate_manifest(dbfile):
    file=Path(dbfile).resolve()
    with sqlite3.connect(file.as_uri()+"?mode=ro",uri=True) as db:
        data={k:json.loads(v) for k,v in
              db.execute("SELECT key,value FROM metadata")}
        if data.get("schema_version")!=1 or not data.get("batch"):
            raise ValueError("Not a verified relational SQLite v1 candidate")
        if (db.execute("PRAGMA quick_check").fetchone()[0]!="ok"
            or list(db.execute("PRAGMA foreign_key_check"))):
            raise ValueError("SQLite integrity check failed")
        tables=("stores","products","store_products",
                "price_terms","price_observations")
        sizes={t:db.execute("SELECT COUNT(*) FROM "+t).fetchone()[0]
               for t in tables}
        expected=data.get("counts") or {}
        if (sizes["stores"]!=expected.get("stores")
            or sizes["store_products"]!=expected.get("store_products")
            or sizes["price_observations"]!=expected.get("observations")):
            raise ValueError("Relational candidate counts differ from metadata")
    batch=data["batch"]
    if not (type(batch) is str and batch.isdigit() and len(batch)==14):
        raise ValueError("Unsafe batch identifier")
    return {"format":"foodradar-relational-v1","batch":batch,
            "path":f"DB/versions/{batch}/catalog.db",
            "size":file.stat().st_size,"sha256":sha256(file),
            "schema_version":1,"counts":sizes,
            "retention_days":data.get("retention_days")}

def publish(api,repo,dbfile,meta):
    from huggingface_hub import CommitOperationAdd,hf_hub_download
    from huggingface_hub.errors import EntryNotFoundError
    if repo not in ("hub-google/FoodRadar","hub-google/Foodradar-TW"):
        raise ValueError("Refusing unknown HF repository")
    previous=None
    try:
        location=hf_hub_download(repo,filename="DB/current.json",
            repo_type="dataset",token=os.environ["HF_TOKEN"])
        previous=json.loads(Path(location).read_text(encoding="utf-8"))
    except EntryNotFoundError:
        pass
    if previous and previous.get("batch","")>meta["batch"]:
        raise RuntimeError("Newer HF database already published")
    if previous and previous.get("batch")==meta["batch"]:
        if previous.get("sha256")!=meta["sha256"]:
            raise RuntimeError("Batch already exists with different checksum")
        return {"already_current":True,**meta}
    api.create_commit(repo_id=repo,repo_type="dataset",
        operations=[CommitOperationAdd(
            path_in_repo=meta["path"],path_or_fileobj=str(dbfile))],
        commit_message="Stage verified relational SQLite "+meta["batch"])
    remote=next(iter(api.get_paths_info(
        repo,[meta["path"]],repo_type="dataset")))
    if remote.path!=meta["path"] or remote.size!=meta["size"]:
        raise RuntimeError("Remote SQLite size mismatch; pointer not updated")
    with tempfile.TemporaryDirectory() as folder:
        pointer=Path(folder)/"current.json"
        pointer.write_text(json.dumps(meta,ensure_ascii=False,
            sort_keys=True,indent=2),encoding="utf-8")
        api.create_commit(repo_id=repo,repo_type="dataset",
            operations=[CommitOperationAdd(
                path_in_repo="DB/current.json",
                path_or_fileobj=str(pointer))],
            commit_message="Activate verified relational SQLite "+meta["batch"])
    return {"already_current":False,**meta}

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--file",required=True)
    parser.add_argument("--repo",required=True)
    parser.add_argument("--upload",action="store_true")
    args=parser.parse_args()
    meta=candidate_manifest(args.file)
    if not args.upload:
        print("DRY_RUN_ONLY="+json.dumps(meta,ensure_ascii=False))
    else:
        from huggingface_hub import HfApi
        api=HfApi(token=os.environ["HF_TOKEN"])
        print("HF_SQLITE_PUBLISH="+json.dumps(
            publish(api,args.repo,args.file,meta),ensure_ascii=False))
