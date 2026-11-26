"""Explicitly requalify one previously rejected Raw batch without recapture."""
import argparse
import hashlib
import json
import os
import re
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from release_policy import validate_partial_manifest
from huggingface_hub import HfApi, CommitOperationAdd, hf_hub_download


def recover(config,batch):
    if not re.fullmatch(r"[0-9]{14}",batch):
        raise ValueError("Invalid original Raw batch timestamp")
    repo=config["hf_repo_id"]
    folder=config["hf_folder"].strip("/")
    prefix=f"{folder}/{batch}"
    path=prefix+"/manifest.json"
    archive_path=prefix+"/capture.tar.gz"
    token=os.environ["HF_TOKEN"]
    api=HfApi(token=token)
    revision=api.repo_info(repo,repo_type="dataset").sha
    def read(name):
        return Path(hf_hub_download(repo,name,repo_type="dataset",revision=revision,token=token)).read_bytes()
    original=read(path)
    meta=json.loads(original)
    if (meta.get("format")!="foodradar-immutable-http-body-v1" or meta.get("batch_id")!=batch
            or meta.get("complete") is not False or meta.get("archive_path")!=archive_path
            or not isinstance(meta.get("responses"),dict)
            or len(meta["responses"])!=meta.get("captured_count")
            or not re.fullmatch(r"[a-f0-9]{64}",str(meta.get("archive_sha256")))):
        raise ValueError("Original Raw manifest is invalid")
    passed,failed=validate_partial_manifest(meta)
    entries=api.get_paths_info(repo,[archive_path],repo_type="dataset",revision=revision,expand=True)
    if len(entries)!=1:
        raise ValueError("Archived Raw is missing")
    lfs=getattr(entries[0],"lfs",None)
    digest=lfs.get("sha256") if isinstance(lfs,dict) else getattr(lfs,"sha256",None)
    if digest!=meta["archive_sha256"]:
        raise ValueError("Archived Raw remote SHA256 does not match original manifest")
    if not meta.get("partial_replayable"):
        # Preserve original manifest as audit evidence. Never touch capture.tar.gz.
        meta["partial_replayable"]=True
        meta["release_policy"]="total_store_failure_rate_le_5pct"
        meta["previous_manifest_sha256"]=hashlib.sha256(original).hexdigest()
        payload=json.dumps(meta,ensure_ascii=False,separators=(",",":")).encode()
        ops=[CommitOperationAdd(path_in_repo=prefix+"/manifest-before-policy.json",path_or_fileobj=original),
             CommitOperationAdd(path_in_repo=path,path_or_fileobj=payload)]
        pointer=f"{folder}/latest-partial.json"
        if not api.file_exists(repo,pointer,repo_type="dataset",revision=revision) or str(json.loads(read(pointer)).get("batch_id",""))<=batch:
            ops.append(CommitOperationAdd(path_in_repo=pointer,path_or_fileobj=payload))
        api.create_commit(repo_id=repo,repo_type="dataset",operations=ops,
            parent_commit=revision,commit_message=f"Requalify {batch}: {len(failed)}/{len(meta['stores'])} failed, preserve original Raw")
    print("FOODRADAR_ARCHIVED_RAW_REQUALIFIED="+json.dumps({
        "batch":batch,"verified_stores":len(passed),"failed_stores":len(failed),
        "failed_rate_pct":100*len(failed)/len(meta["stores"]),"archive_sha256":digest,
        "archive_bytes":entries[0].size,"reused_raw":True}),flush=True)

if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("batch")
    args=parser.parse_args()
    recover(json.loads((ROOT/"config.json").read_text()),args.batch)
