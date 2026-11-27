"""17: verify the store-level outputs, then update the one canonical compact DB.

An invalid or absent prior checkpoint NEVER silently discards previous price
history. This job sends no UE requests and does not re-download the HF Raw body.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import zstandard as zstd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from db_task_pool import DBTaskPool
from raw_io import DocumentSpool
from state_checkpoint import (restore_direct,new_direct,manifest_digest,
                              direct_contract_digest,save_direct,CACHE)
from compact_direct import MAX_BYTES
from pipeline import raw_manifest_should_replay

def read_verified_documents(result_folder, results):
    completed={r["store_uuid"]:r for r in results if r["status"]=="completed"}
    failed=[r["store_uuid"] for r in results if r["status"]=="failed"]
    consumed=set()
    spool=DocumentSpool()
    for path in sorted(result_folder.glob("db-worker-*/documents.jsonl.zst")):
        artifact=path.parent.name
        with path.open("rb") as compressed:
            with zstd.ZstdDecompressor().stream_reader(compressed) as reader:
                with io.TextIOWrapper(reader,encoding="utf-8") as lines:
                    for line in lines:
                        payload=line.encode("utf-8")
                        row=json.loads(line)
                        sid=row["store_uuid"]
                        expected=completed.get(sid)
                        if (expected is None or sid in consumed
                                or expected["result"]["artifact"]!=artifact
                                or hashlib.sha256(payload).hexdigest()
                                   != expected["result"]["sha256"]
                                or row["document"].get("store_uuid")!=sid):
                            raise ValueError("Duplicate, missing, or changed DB output")
                        consumed.add(sid)
                        spool.append(row["document"])
    if consumed!=set(completed):
        raise RuntimeError(f"DB worker output gap: {len(set(completed)-consumed)} stores")
    return spool,failed


def raw_records(config):
    from huggingface_hub import HfApi,hf_hub_download
    from concurrent.futures import ThreadPoolExecutor
    folder=config["hf_folder"].strip("/")
    api=HfApi(token=os.environ["HF_TOKEN"])
    paths=sorted(p for p in api.list_repo_files(repo_id=config["hf_repo_id"],repo_type="dataset")
                 if p.startswith(folder+"/") and p.endswith("/manifest.json"))
    def read(path):
        file=Path(hf_hub_download(config["hf_repo_id"],path,
                                  repo_type="dataset",token=os.environ["HF_TOKEN"]))
        data=file.read_bytes()
        return path,json.loads(data),hashlib.sha256(data).hexdigest()
    with ThreadPoolExecutor(max_workers=8) as ex:
        return list(ex.map(read,paths))


def build(batch,source_run,source_count,folder,*,pool=None,apply=True):
    pool=pool or DBTaskPool()
    status=pool.status()
    if any(int(status.get(k,0)) for k in ("pending","running","local_done")):
        raise RuntimeError("DB pool has unfinished tasks; cannot merge")
    completed=int(status.get("completed",0))
    failed=int(status.get("failed",0))
    if completed+failed!=source_count or completed==0:
        raise RuntimeError("DB task roster differs from the verified crawl")
    if failed*20>source_count:
        raise RuntimeError("Store failure rate exceeds 5%; preserve last DB")
    results=list(pool.results())
    if len(results)!=source_count:raise RuntimeError("Incomplete task result registry")
    spool,failed_ids=read_verified_documents(folder,results)
    if len(spool)!=completed or len(failed_ids)!=failed:
        raise RuntimeError("Verified DB document count mismatch")
    print("FOODRADAR_DB_STAGE_17_VERIFIED="+json.dumps({
        "batch":batch,"source_run":source_run,
        "verified_stores":completed,"failed_stores":failed}),flush=True)
    if not apply:
        spool.close()
        return
    config=json.loads((ROOT/"config.json").read_text(encoding="utf-8"))
    records=raw_records(config)
    usable=[(p,m,h) for p,m,h in records
            if raw_manifest_should_replay(m,p,config["hf_folder"].strip("/"))]
    if batch not in [m["batch_id"] for _,m,_ in usable]:
        raise ValueError("DB batch has no verified HF raw provenance")
    restored=restore_direct(records,config)
    if restored:
        state,previous,offset=restored
        if previous>=batch:
            raise RuntimeError("DB batch already applied, refusing overwrite")
        if usable[offset:][-1][1]["batch_id"]!=batch or len(usable[offset:])!=1:
            raise RuntimeError("Must apply historical batches in order")
    else:
        if len(usable)!=1 or usable[0][1]["batch_id"]!=batch:
            raise RuntimeError(
                "Existing HF history has no compatible durable DB checkpoint. "
                "A sequential historical backfill is required; do not erase price history.")
        state=new_direct()
    try:
        state.apply_batch(spool,batch,failed_ids)
        state.refresh_weekly(batch)
        state.flush()
        # Keep serving_state's current integrity and compact-size checks.
        if any(p.path.stat().st_size>=MAX_BYTES for p in state.parts):
            state=state.rebuild_partitions(CACHE)
        index={"repo_id":config["hf_repo_id"],"folder":config["hf_folder"].strip("/"),
               "batch_id":batch,"manifest_count":len(records),
               "verified_batches":len(state.batch_rows()),
               "manifest_digest":manifest_digest(records),
               "contract_digest":direct_contract_digest()}
        out=ROOT/"data"/"db-stage-checkpoint"
        out.mkdir(parents=True,exist_ok=True)
        path=out/"raw-manifest-index.json"
        path.write_text(json.dumps(index),encoding="utf-8")
        save_direct(state,path)
        print("FOODRADAR_DB_STAGE_17_CHECKPOINT="+json.dumps({
            "batch":batch,**state.metrics()}),flush=True)
    finally:
        state.close()
        spool.close()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-run-id",required=True)
    parser.add_argument("--batch-id",required=True)
    parser.add_argument("--verify-only",action="store_true")
    args=parser.parse_args()
    plan=json.loads((ROOT/"data"/"db-stage-plan.json").read_text(encoding="utf-8"))
    if plan["source_run_id"]!=args.source_run_id:
        raise ValueError("Source run differs from stage 00")
    result_folder=ROOT/"data"/"db-artifacts"
    build(args.batch_id,args.source_run_id,plan["stores"],
          result_folder,apply=not args.verify_only)


if __name__=="__main__":
    main()
