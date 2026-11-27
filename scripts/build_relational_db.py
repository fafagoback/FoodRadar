"""Build candidate SQLite from an already-restored, verified HF checkpoint.

No UE requests, no HF uploads and no Pages deployment.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from compact_direct import open_collection
from relational_catalog import build

def digest(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for block in iter(lambda:f.read(1024*1024),b""):
            h.update(block)
    return h.hexdigest()

def load_checkpoint(folder):
    meta=json.loads((folder/"manifest.json").read_text(encoding="utf-8"))
    if meta.get("format")!="foodradar-checkpoint-v4":
        raise ValueError("Only verified v4 compact checkpoints are supported")
    parts=meta.get("parts") or [{"path":"serving.compact.db",
                                "sha256":meta.get("compact_sha256")}]
    paths=[]
    for part in parts:
        rel=Path(part["path"])
        if rel.is_absolute() or ".." in rel.parts or (
            rel.name!="serving.compact.db" and
            (len(rel.parts)!=2 or rel.parts[0]!="shards" or rel.suffix!=".db")
        ):
            raise ValueError("Unsafe compact checkpoint path")
        path=folder/rel
        if digest(path)!=part["sha256"]:
            raise ValueError("Compact checkpoint SHA verification failed")
        paths.append(path)
    collection=open_collection(paths)
    if collection.meta.get("latest_batch")!=meta["batch_id"] or (
        collection.stats()!={k:meta[k] for k in ("stores","products","observations")}
    ):
        collection.close()
        raise ValueError("Checkpoint inventory/batch mismatch")
    return collection,meta

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--checkpoint",default="data/state-checkpoint")
    parser.add_argument("--output",required=True)
    parser.add_argument("--report")
    args=parser.parse_args()
    state,meta=load_checkpoint(Path(args.checkpoint))
    try:
        result=build(state.shops(),args.output,meta["batch_id"])
        if (result["stores"]!=meta["stores"] or result["store_products"]!=meta["products"]\n            or result["observations"]+result.get("expired_observations_ignored",0)!=meta["observations"]):
            Path(args.output).unlink(missing_ok=True)
            raise RuntimeError("Source inventory differs from relational candidate")
        result["sha256"]=digest(args.output)
        result["checkpoint_batch"]=meta["batch_id"]
        if args.report:
            Path(args.report).write_text(
                json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
        print("FOODRADAR_RELATIONAL_CANDIDATE="+json.dumps(
            result,ensure_ascii=False),flush=True)
    finally:
        state.close()
if __name__=="__main__":main()
