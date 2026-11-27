"""Remove only unreferenced, immutable HF FR SQLite preview files.

Current/preview pointers are validated against the exact repository snapshot.
The 60-day incremental compact State/current/ is NOT removed because the DB
writer still requires it. Reclaim commit/LFS history via hf_retention.cleanup.
"""
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from pathlib import Path
from huggingface_hub import HfApi, CommitOperationDelete

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from hf_retention import REPO,config,snapshot,read

CANDIDATE=re.compile(r"DB/previews/\d{14}/[0-9a-f]{16}/catalog\.db")

def referenced(files,reader):
    if "DB/current.json" not in files:
        raise ValueError("No verified live DB/current.json; refuse deletion")
    protected=set()
    for pointer in ("DB/current.json","DB/preview.json"):
        if pointer not in files:
            continue
        row=json.loads(reader(pointer))
        path=row.get("path")
        if (row.get("format")!="foodradar-relational-overlay-v9" or
            not isinstance(path,str) or not CANDIDATE.fullmatch(path) or
            path not in files or
            not isinstance(row.get("size"),int) or row["size"]<=0):
            raise ValueError("Invalid or missing HF SQLite pointer: "+pointer)
        protected.add(path)
    return protected

def obsolete(files, protected):
    return sorted(path for path in files
                  if CANDIDATE.fullmatch(path) and path not in protected)

def prune(api,*,dry=False):
    if REPO!="hub-google/FoodRadar":
        raise ValueError("Only FR SQL previews may be pruned here")
    config()
    revision,files=snapshot(api)
    keep=referenced(files,lambda name:read(name,revision))
    targets=obsolete(files,keep)
    size_by_path={}
    for i in range(0,len(targets),100):
        group=targets[i:i+100]
        for row in api.get_paths_info(REPO,group,repo_type="dataset",revision=revision):
            size_by_path[row.path]=row.size
    if len(size_by_path)!=len(targets):
        raise ValueError("Unable to verify obsolete SQLite object sizes")
    report={"protected":sorted(keep),"stale_versions":len(targets),
            "stale_file_bytes":sum(size_by_path.values()),"mode":"dry-run" if dry else "apply"}
    if dry or not targets:
        print("FR_HF_SQLITE_PRUNE="+json.dumps(report,ensure_ascii=False),flush=True)
        return report
    # Exactly one optimistic lease; never delete files from a newer release.
    api.create_commit(REPO,repo_type="dataset",revision="main",parent_commit=revision,
        operations=[CommitOperationDelete(path_in_repo=p) for p in targets],
        commit_message="Prune only unreferenced FoodRadar SQL serving previews")
    new_rev,after=snapshot(api)
    if any(p in after for p in targets):
        raise RuntimeError("Unused SQL preview remains after cleanup")
    after_keep=referenced(after,lambda name:read(name,new_rev))
    if after_keep!=keep:
        raise RuntimeError("Active SQL pointer changed during cleanup")
    report["deleted"]=len(targets)
    print("FR_HF_SQLITE_PRUNE="+json.dumps(report,ensure_ascii=False),flush=True)
    return report

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--apply",action="store_true")
    args=parser.parse_args()
    prune(HfApi(token=os.environ["HF_TOKEN"]),dry=not args.apply)
