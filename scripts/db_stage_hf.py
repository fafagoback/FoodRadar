"""Stage 16: idempotent HF Raw upload, independent of DB worker stages."""
from __future__ import annotations
import argparse,json,os,sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))

def verified_raw(config,batch):
    from huggingface_hub import HfApi,hf_hub_download
    repo=config["hf_repo_id"]
    folder=config["hf_folder"].strip("/")
    api=HfApi(token=os.environ["HF_TOKEN"])
    manifest_path=f"{folder}/{batch}/manifest.json"
    if not api.file_exists(repo,manifest_path,repo_type="dataset"):
        return None
    meta=json.loads(Path(hf_hub_download(repo,manifest_path,repo_type="dataset",
                          token=os.environ["HF_TOKEN"])).read_text(encoding="utf-8"))
    from pipeline import raw_manifest_should_replay
    if not raw_manifest_should_replay(meta,manifest_path,folder):
        raise RuntimeError("Raw is archived but not safe for DB replay")
    if meta["batch_id"]!=batch:
        raise RuntimeError("HF Raw batch identity mismatch")
    entries=api.get_paths_info(repo,[meta["archive_path"]],
                               repo_type="dataset",expand=True)
    if len(entries)!=1:
        raise RuntimeError("Raw manifest references missing archive")
    lfs=getattr(entries[0],"lfs",None)
    checksum=lfs.get("sha256") if isinstance(lfs,dict) else getattr(lfs,"sha256",None)
    if not checksum or checksum!=meta["archive_sha256"]:
        raise RuntimeError("HF Raw archive lacks matching LFS SHA256 metadata")
    return {"batch":batch,"bytes":entries[0].size,
            "stores":len(meta["stores"]),"sha256":checksum}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-run-id",required=True)
    parser.add_argument("--batch-id",required=True)
    parser.add_argument("--allow-upload",action="store_true")
    args=parser.parse_args()
    if not args.source_run_id.isdecimal() or len(args.batch_id)!=14 or not args.batch_id.isdecimal():
        raise ValueError("Invalid source run or immutable batch ID")
    config=json.loads((ROOT/"config.json").read_text(encoding="utf-8"))
    result=verified_raw(config,args.batch_id)
    if result is None:
        if not args.allow_upload:
            raise RuntimeError("Offline verify requires existing HF Raw")
        from stream_ingest import run
        # Reuse the existing ZIP streaming merger; no UE requests.
        run(args.source_run_id)
        result=verified_raw(config,args.batch_id)
        if result is None:
            raise RuntimeError("Stream ingest did not publish requested batch")
    print("FOODRADAR_DB_STAGE_HF_16="+json.dumps(result),flush=True)

if __name__=="__main__":
    main()
