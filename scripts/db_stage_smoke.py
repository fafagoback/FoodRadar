"""Offline live-artifact smoke check: decode an existing crawl response, zero UE calls."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
import urllib.request
import tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from db_stage_worker import decode_task,get_artifact


def list_artifacts(run_id,repo,token):
    req=urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts?per_page=100",
        headers={"Authorization":"Bearer "+token,
                 "Accept":"application/vnd.github+json"})
    with urllib.request.urlopen(req,timeout=30) as response:
        return [a["name"] for a in json.load(response)["artifacts"]
                if a["name"].startswith("fr-worker-") and not a["expired"]]


def smoke(run_id,batch,repo,token):
    artifacts=sorted(list_artifacts(run_id,repo,token))
    if not artifacts:raise RuntimeError("No available original crawl artifact")
    name=artifacts[0]
    base=ROOT/"data"/"db-smoke-source"
    # The only download is an original completed crawler artifact.
    # No HTTP call is made to Uber Eats.
    folder=get_artifact({"artifact":name,
                          "source_sha":os.environ["SOURCE_SHA"],
                          "epoch":name.removeprefix("fr-worker-").split("-",1)[0]},
                         base,run_id)
    roster=json.loads((folder/"batch.json").read_text(encoding="utf-8"))
    decoded=0;attempts=0;failures=[]
    for sid,token in sorted(roster["records"].items()):
        attempts+=1
        source={"artifact":name,"source_sha":roster["source_sha"],
                "epoch":roster["epoch"],
                "task_folder":f"fr-task-{roster['epoch']}-{sid}-{token}"}
        claim={"store_uuid":sid,"source":source}
        try:
            doc,reason=decode_task(folder/"tasks"/source["task_folder"],claim,batch)
        except Exception as exc:
            raise RuntimeError("Store artifact decode crashed "+sid) from exc
        if doc is not None:
            if doc["batch_id"]!=batch:
                raise RuntimeError("Wrong decoded batch")
            # Real one-shop lifecycle, including native price evidence,
            # compact encoding and weekly ABA, without HF writes.
            from state_checkpoint import new_direct
            with tempfile.TemporaryDirectory(prefix="frt-db-one-shop-") as temp:
                state=new_direct(Path(temp)/"checkpoint")
                try:
                    counts=state.apply_batch([doc],batch)
                    weekly=state.refresh_weekly(batch)
                    evidence=state.stats()
                    if evidence["stores"]!=1:
                        raise RuntimeError("Real one-shop DB did not retain store")
                    if state.parts[0].conn.execute("PRAGMA quick_check").fetchone()[0]!="ok":
                        raise RuntimeError("One-shop compact SQLite failed quick_check")
                    print("FOODRADAR_DB_REAL_SHOP_LIFECYCLE="+json.dumps({
                        "store":sid,"products":evidence["products"],
                        "observations":evidence["observations"],
                        "weekly":weekly,"counts":counts}),flush=True)
                finally:
                    state.close()
            decoded+=1
            break
        failures.append(reason)
        if attempts>=25:break
    if not decoded:
        raise RuntimeError(f"Could not decode a valid original store: {failures}")
    print("FOODRADAR_DB_SMOKE="+json.dumps({
        "source_run":run_id,"batch":batch,"artifact":name,
        "attempts":attempts,"valid_documents":decoded}),flush=True)


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-run-id",required=True)
    parser.add_argument("--batch-id",required=True)
    parser.add_argument("--source-sha",required=True)
    args=parser.parse_args()
    os.environ["SOURCE_SHA"]=args.source_sha
    smoke(args.source_run_id,args.batch_id,
          os.environ["GITHUB_REPOSITORY"],os.environ["GH_TOKEN"])
