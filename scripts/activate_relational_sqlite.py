"""Promote already-browser-verified public SQLite to DB/current.json (no UE calls)."""
from __future__ import annotations
import json
import os
import re
import tempfile
from pathlib import Path
from huggingface_hub import HfApi, hf_hub_download, CommitOperationAdd

REPO="hub-google/FoodRadar"
def promote():
    token=os.environ["HF_TOKEN"]
    api=HfApi(token=token)
    manifest_path=Path(hf_hub_download(REPO,"DB/preview.json",repo_type="dataset",token=token))
    meta=json.loads(manifest_path.read_text(encoding="utf-8"))
    if (meta.get("format")!="foodradar-relational-overlay-v9" or
        not re.fullmatch(r"DB/previews/\d{14}/[0-9a-f]{16}/catalog\.db",meta.get("path",""))):
        raise ValueError("Unverified SQLite candidate")
    info=list(api.get_paths_info(REPO,[meta["path"]],repo_type="dataset"))
    if len(info)!=1 or info[0].size!=meta["size"]:
        raise ValueError("Remote SQLite not fully uploaded")
    existing=None
    if api.file_exists(REPO,"DB/current.json",repo_type="dataset"):
        previous=Path(hf_hub_download(REPO,"DB/current.json",repo_type="dataset",
                                      token=token,force_download=True))
        existing=json.loads(previous.read_text(encoding="utf-8"))
        if existing.get("format")!="foodradar-relational-overlay-v9":
            raise ValueError("Cannot replace unrelated production DB pointer")
        if str(existing.get("batch",""))>str(meta["batch"]):
            raise ValueError("Do not downgrade the verified production batch")
    with tempfile.TemporaryDirectory() as tmp:
        path=Path(tmp)/"current.json"
        path.write_text(json.dumps(meta,ensure_ascii=False,sort_keys=True,indent=2),
                        encoding="utf-8")
        api.create_commit(repo_id=REPO,repo_type="dataset",
            operations=[CommitOperationAdd(path_in_repo="DB/current.json",path_or_fileobj=str(path))],
            commit_message="Activate browser-verified FoodRadar single SQLite serving DB")
    print("FR_SQLITE_PRODUCTION_POINTER="+json.dumps(
        {"path":meta["path"],"batch":meta["batch"],"size":meta["size"]}),flush=True)

if __name__=="__main__":promote()
