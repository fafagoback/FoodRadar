"""00 dispatcher for the DB-only phase. Never sends Uber Eats requests."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from db_task_pool import DBTaskPool


def github_get(path, token, repo):
    request=urllib.request.Request(
        f"https://api.github.com/repos/{repo}/{path}",
        headers={"Authorization":"Bearer "+token,
                 "Accept":"application/vnd.github+json",
                 "X-GitHub-Api-Version":"2022-11-28"})
    with urllib.request.urlopen(request,timeout=25) as response:
        return json.load(response)


def source_artifacts(source_run, token, repo):
    run=github_get(f"actions/runs/{source_run}",token,repo)
    if run.get("status")!="completed" or run.get("head_branch")!="main":
        raise RuntimeError("Source crawl must be completed on main")
    # The overall source run may fail AFTER crawling in its old DB publisher.
    # Its 15 worker artifacts must all be durable and individually successful.
    artifacts=[]
    page=1
    while True:
        response=github_get(
            f"actions/runs/{source_run}/artifacts?per_page=100&page={page}",
            token,repo)
        chunk=response.get("artifacts",[])
        artifacts.extend(a for a in chunk
                         if a["name"].startswith("fr-worker-") and not a["expired"])
        if len(chunk)<100:break
        page+=1
    if not artifacts:
        raise RuntimeError("No durable worker artifacts in this crawl")
    for a in artifacts:
        if a.get("size_in_bytes",0)<1000:
            raise RuntimeError("Unexpectedly small source artifact")
    return run,sorted(a["name"] for a in artifacts)


def plan(source_run_id, pool, token, repo):
    run,artifacts=source_artifacts(source_run_id,token,repo)
    # New run only; never clear live leases from another run.
    pool.purge()
    result=pool.seed_from_crawl(source_run_id,run.get("run_attempt",1))
    count=int(result["source_stores"])
    if count<=0 or int(result["inserted"])!=count:
        raise RuntimeError("Source crawl task roster did not seed in full")
    state=pool.status()
    if int(state.get("pending",0))!=count:
        raise RuntimeError("DB task pool count differs from verified crawl")
    output={"source_run_id":str(source_run_id),"source_attempt":run.get("run_attempt",1),
            "source_sha":run["head_sha"],"stores":count,"artifacts":artifacts}
    return output


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-run-id",required=True)
    args=parser.parse_args()
    token=os.environ["GH_TOKEN"]
    repo=os.environ["GITHUB_REPOSITORY"]
    if not args.source_run_id.isdecimal():
        raise ValueError("Numeric source run required")
    output=plan(args.source_run_id,DBTaskPool(),token,repo)
    path=ROOT/"data"/"db-stage-plan.json"
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding="utf-8")
    print("FOODRADAR_DB_STAGE_00="+json.dumps({
      "source_run_id":output["source_run_id"],"stores":output["stores"],
      "source_artifacts":len(output["artifacts"])}),flush=True)


if __name__=="__main__":
    main()
