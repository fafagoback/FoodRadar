"""01..15: store-only CPU decode of existing verified crawler artifacts.

No UE requests. One shared Supabase pool; claim one shop at a time, refill
immediately. DB history remains owned by the eventual single writer.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import zstandard as zstd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from db_task_pool import DBTaskPool
from raw_capture import _decode_verified_store
from menu_core import canonical_store_uuid


def verified_response(raw_dir, relative, expected):
    path=(raw_dir/relative).resolve()
    if not path.is_relative_to(raw_dir.resolve()) or not path.is_file():
        raise RuntimeError("Unsafe or missing source response")
    body=path.read_bytes()
    if hashlib.sha256(body).hexdigest()!=expected:
        raise RuntimeError("Crawler response SHA256 mismatch")
    return body


def decode_task(folder, claim, batch):
    task=json.loads((folder/"task.json").read_text(encoding="utf-8"))
    source=claim["source"]
    sid=claim["store_uuid"]
    if (task["store_uuid"]!=sid
        or task["source_sha"]!=source["source_sha"]
        or task["epoch"]!=source["epoch"]):
        raise RuntimeError("Crawl task provenance does not match verified SB pool")
    if folder.name!=source["task_folder"]:
        raise RuntimeError("Artifact task folder mismatch")
    store=task["store"]
    if not store.get("success"):
        return None,"crawl_failed"
    if store.get("catalog_complete") is False:
        return None,"catalog_incomplete"
    recheck=store.get("promo_recheck") or {}
    if recheck.get("status") in ("CONFLICT","UNAVAILABLE"):
        return None,"promo_unverified"
    raw=folder/"raw"
    hashes=task["responses"]
    # Every response is checked once; only required menu bytes are kept in RAM.
    menu_paths=set()
    if not store.get("inactive"):
        menu_paths.add(store["response_path"])
        menu_paths.update(row["path"] for row in store.get("catalog_responses") or [])
    bodies={}
    for name,expected in hashes.items():
        body_bytes=verified_response(raw,name,expected)
        if name in menu_paths:
            bodies[name]=body_bytes
    if not menu_paths:
        body=None
        pages=[]
    else:
        body=bodies[store["response_path"]]
        pages=[bodies[row["path"]] for row in store.get("catalog_responses") or []]
    canonical=canonical_store_uuid(store["store_uuid"],store["store_url"])
    if canonical!=sid:
        raise RuntimeError("Canonical store identity changed")
    try:
        doc=_decode_verified_store(store,canonical,batch,body,pages)
    except (TypeError,ValueError,KeyError) as exc:
        return None,"invalid_native_menu:"+type(exc).__name__
    if doc is None:
        return None,"empty_catalog"
    return doc,None


def get_artifact(source, root, source_run):
    name=source["artifact"]
    if not name.startswith("fr-worker-") or "/" in name or ".." in name:
        raise RuntimeError("Invalid crawler artifact name")
    folder=root/name
    if not (folder/"batch.json").is_file():
        folder.mkdir(parents=True,exist_ok=True)
        subprocess.run(["gh","run","download",str(source_run),
                        "--repo",os.environ["GITHUB_REPOSITORY"],
                        "--name",name,"--dir",str(folder)],check=True)
    manifest=json.loads((folder/"batch.json").read_text(encoding="utf-8"))
    if manifest["source_sha"]!=source["source_sha"] or manifest["epoch"]!=source["epoch"]:
        raise RuntimeError("Crawler batch artifact provenance mismatch")
    return folder


def work(index,source_run,batch,plan,pool=None):
    pool=pool or DBTaskPool()
    root=ROOT/"data"/"db-source-cache"
    root.mkdir(parents=True,exist_ok=True)
    output=ROOT/"data"/f"db-worker-{index:02d}"
    output.mkdir(parents=True,exist_ok=False)
    wave=int(os.environ.get("FR_DB_WAVE","0"))
    artifact_name=(f"db-worker-{os.environ['GITHUB_RUN_ID']}-{index:02d}" +
                   (f"-w{wave:02d}" if wave else ""))
    sources=plan["artifacts"]
    preferred=sources[(index-1)%len(sources)]
    finished=[]
    failures=0
    stale=[]
    running=True
    def heartbeat():
        heartbeat_pool=DBTaskPool()
        while running:
            time.sleep(30)
            if not running:break
            try:
                heartbeat_pool.heartbeat(index)
            except Exception as exc:
                stale.append(str(exc))
                break
    pulse=threading.Thread(target=heartbeat,daemon=True)
    pulse.start()
    out_path=output/"documents.jsonl.zst"
    with out_path.open("wb") as compressed:
        with zstd.ZstdCompressor(level=3).stream_writer(compressed,closefd=False) as stream:
            while True:
                if stale:raise RuntimeError("Worker heartbeat lost: "+stale[0])
                response=pool.claim(index,preferred)
                claim=response.get("claimed")
                if claim is None:
                    # No ready store means this machine must first upload its
                    # artifact. Waiting for its own unfinalized leases deadlocks.
                    if "ready" not in response:
                        raise RuntimeError("DB pool did not report claim readiness")
                    if int(response["ready"]) == 0:
                        break
                    # A task may be row-locked by a competing claim briefly.
                    time.sleep(1)
                    continue
                source=claim["source"]
                preferred=source["artifact"]
                folder=get_artifact(source,root,source_run)
                task_folder=folder/"tasks"/source["task_folder"]
                if not task_folder.is_dir():
                    raise RuntimeError("Store is missing from its declared artifact")
                doc,issue=decode_task(task_folder,claim,batch)
                token=claim["lease_token"]
                sid=claim["store_uuid"]
                if issue:
                    pool.fail(index,sid,token,issue)
                    failures+=1
                    continue
                payload=json.dumps({"store_uuid":sid,"document":doc},
                                   ensure_ascii=False,separators=(",",":")).encode()+b"\n"
                stream.write(payload)
                finished.append({"store_uuid":sid,"token":token,
                                 "sha256":hashlib.sha256(payload).hexdigest()})
    running=False
    (output/"finalize.json").write_text(json.dumps({
        "worker":index,"artifact":artifact_name,"stores":finished,
        "source_run":str(source_run),"batch":batch},ensure_ascii=False),
        encoding="utf-8")
    print("FOODRADAR_DB_WORKER="+json.dumps({
        "worker":index,"processed":len(finished),"failed":failures,
        "artifact":artifact_name}),flush=True)
    if stale:raise RuntimeError("Worker heartbeat lost: "+stale[0])


def finalize(index):
    file=ROOT/"data"/f"db-worker-{index:02d}"/"finalize.json"
    data=json.loads(file.read_text(encoding="utf-8"))
    if data["worker"]!=index:raise ValueError("Incorrect worker result")
    count=DBTaskPool().finalize_batch(index,data["artifact"],data["stores"])
    if count!=len(data["stores"]):
        raise RuntimeError("DB worker finalize did not confirm every shop")
    print(f"FOODRADAR_DB_WORKER_FINALIZED={index}:{count}",flush=True)


def heartbeat_until_done(index, done_file):
    """Keep completed-but-not-uploaded tasks leased across Actions steps."""
    pool=DBTaskPool()
    marker=Path(done_file)
    while not marker.exists():
        pool.heartbeat(index)
        for _ in range(25):
            if marker.exists():
                return
            time.sleep(1)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("mode",choices=("worker","finalize","heartbeat"))
    parser.add_argument("--done-file")
    parser.add_argument("--index",type=int,required=True)
    parser.add_argument("--source-run-id")
    parser.add_argument("--batch-id")
    args=parser.parse_args()
    if args.index<1:raise ValueError("DB runner indexes begin at 01")
    if args.mode=="heartbeat":
        if not args.done_file:
            raise ValueError("Heartbeat requires a completion marker")
        heartbeat_until_done(args.index,args.done_file)
        return
    if args.mode=="finalize":
        finalize(args.index)
        return
    if not args.source_run_id or not args.source_run_id.isdecimal():
        raise ValueError("Verified source run ID required")
    if not args.batch_id or len(args.batch_id)!=14 or not args.batch_id.isdecimal():
        raise ValueError("14-digit source batch ID required")
    plan=json.loads((ROOT/"data"/"db-stage-plan.json").read_text(encoding="utf-8"))
    if plan["source_run_id"]!=args.source_run_id:
        raise RuntimeError("Plan and source run differ")
    work(args.index,args.source_run_id,args.batch_id,plan)


if __name__=="__main__":
    main()
