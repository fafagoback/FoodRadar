"""Publish a verified FR SQLite overlay to HF *preview* only.

No UE calls. Does not touch Raw, State/current or the live DB/current.json.
Versioned files are immutable; preview pointer is switched only after upload
and remote byte size verification. Production activation is a separate gate.
"""
from __future__ import annotations
import argparse,hashlib,json,os,sqlite3,tempfile
from pathlib import Path

REPO="hub-google/FoodRadar"
POINTER="DB/preview.json"

def digest(path):
    h=hashlib.sha256()
    with Path(path).open("rb") as fp:
        for chunk in iter(lambda:fp.read(1024*1024),b""):h.update(chunk)
    return h.hexdigest()

def validate(path):
    p=Path(path).resolve()
    if not p.is_file() or p.stat().st_size==0:raise ValueError("Missing candidate DB")
    with sqlite3.connect(p.as_uri()+"?mode=ro",uri=True) as db:
        data={k:json.loads(v) for k,v in db.execute(
             "SELECT key,value FROM relational_meta")}
        if data.get("format")!="foodradar-relational-overlay-v7":
            raise ValueError("Invalid overlay schema")
        if db.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
            raise ValueError("Corrupted SQLite")
        if list(db.execute("PRAGMA foreign_key_check")):
            raise ValueError("Invalid relational links")
        counts=data.get("counts",{})
        for table,label in [
            ("relational_stores","stores"),("relational_products","definitions"),
            ("relational_store_products","links"),("relational_prices","prices")]:
            if db.execute("SELECT COUNT(*) FROM "+table).fetchone()[0]!=counts.get(label):
                raise ValueError("SQLite inventory mismatch: "+table)
        if db.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]!=counts["stores"]:
            raise ValueError("Original compressed history was lost")
        if db.execute("SELECT COUNT(*) FROM shop_state").fetchone()[0]!=counts["stores"]:
            raise ValueError("Original compressed store records were lost")
        page_size=db.execute("PRAGMA page_size").fetchone()[0]
        if page_size not in (1024,2048,4096,8192):
            raise ValueError("Unexpected SQLite page size")
    batch=data["batch"]
    if not(isinstance(batch,str) and batch.isdigit() and len(batch)==14):
        raise ValueError("Unsafe batch name")
    sha=digest(p)
    return {"format":"foodradar-relational-overlay-v7",
            "batch":batch,"schema_version":7,"page_size":page_size,
            "path":f"DB/previews/{batch}/{sha[:16]}/catalog.db",
            "size":p.stat().st_size,"sha256":sha,
            "counts":counts}

def publish(path,api,meta):
    from huggingface_hub import CommitOperationAdd
    if api.repo_info(repo_id=REPO,repo_type="dataset").private:
        raise ValueError("HF dataset must be public for direct website reads")
    info=list(api.get_paths_info(REPO,[meta["path"]],repo_type="dataset"))
    if info:
        if info[0].size!=meta["size"]:
            raise ValueError("Previously uploaded preview has a different size")
    else:
        api.create_commit(repo_id=REPO,repo_type="dataset",
            operations=[CommitOperationAdd(
                path_in_repo=meta["path"],path_or_fileobj=str(path))],
            commit_message="Stage FR verified relational SQLite v7 preview")
    info=list(api.get_paths_info(REPO,[meta["path"]],repo_type="dataset"))
    if len(info)!=1 or info[0].size!=meta["size"]:
        raise ValueError("Remote DB size verification failed")
    with tempfile.TemporaryDirectory() as temp:
        pointer=Path(temp)/"preview.json"
        pointer.write_text(json.dumps(meta,ensure_ascii=False,sort_keys=True,indent=2),
            encoding="utf-8")
        api.create_commit(repo_id=REPO,repo_type="dataset",
            operations=[CommitOperationAdd(
                path_in_repo=POINTER,path_or_fileobj=str(pointer))],
            commit_message="Point FR SQLite preview to verified immutable batch")
    return meta

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--file",required=True)
    p.add_argument("--publish",action="store_true")
    args=p.parse_args()
    result=validate(args.file)
    if args.publish:
        from huggingface_hub import HfApi
        result=publish(args.file,HfApi(token=os.environ["HF_TOKEN"]),result)
    print(("FR_SQLITE_PREVIEW_PUBLISHED=" if args.publish else
           "FR_SQLITE_PREVIEW_DRY_RUN=")+json.dumps(result,ensure_ascii=False),flush=True)
