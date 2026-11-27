"""00 dispatcher for the DB-only phase. Never sends Uber Eats requests."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path
from datetime import datetime,timedelta,timezone

TAIPEI=timezone(timedelta(hours=8))

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
    same_run=(os.environ.get("FR_DB_SAME_RUN")=="1"
              and str(run.get("id"))==str(source_run)
              and os.environ.get("GITHUB_RUN_ID")==str(source_run)
              and os.environ.get("GITHUB_SHA")==run.get("head_sha")
              and run.get("status")=="in_progress")
    if run.get("head_branch")!="main" or not (run.get("status")=="completed" or same_run):
        raise RuntimeError("Source crawl must be completed on main, or verified same-run stage 16")
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
    if run.get("conclusion")!="success" and not same_run:
        raise RuntimeError("Only a successful crawl or verified same-run stage 16 can seed DB")
    # A runner with zero assigned shops still uploads a valid batch.json-only
    # Artifact (normally ~900 bytes). Size is not an integrity signal.
    # Full batch manifest, response hashes and shop roster are checked by
    # get_artifact()/decode_task() and the fenced SB source receipt.
    if len({a["name"] for a in artifacts})!=len(artifacts):
        raise RuntimeError("Duplicate source worker artifact")
    for a in artifacts:
        if int(a.get("size_in_bytes") or 0)<=0:
            raise RuntimeError("Empty source artifact")
    return run,sorted(a["name"] for a in artifacts)


def assert_batch_provenance(run,batch_id):
    """A Raw batch cannot predate its source crawl or postdate its Action.
    Reject mislabeling the previous day's data before seeding any store.
    """
    if len(batch_id)!=14 or not batch_id.isdecimal():
        raise ValueError("Raw batch must be an immutable 14-digit timestamp")
    if run.get("path","").split("@")[0]!=".github/workflows/crawl.yml":
        raise ValueError("Source must be the original crawler workflow")
    when=datetime.strptime(batch_id,"%Y%m%d%H%M%S").replace(tzinfo=TAIPEI)
    began=datetime.fromisoformat(run["created_at"].replace("Z","+00:00"))
    finished=(datetime.now(timezone.utc) if run.get("status")=="in_progress" else
              datetime.fromisoformat(run["updated_at"].replace("Z","+00:00")))
    if not began<=when<=finished:
        raise ValueError(
            f"Raw batch {batch_id} is OUTSIDE source Action time window. "
            "Do not label one crawl as another batch.")
    return batch_id


def plan(source_run_id, batch_id, pool, token, repo):
    run,artifacts=source_artifacts(source_run_id,token,repo)
    assert_batch_provenance(run,batch_id)
    receipt=None
    if run.get("status")=="in_progress":
        from db_stage_source import validate_receipt
        receipt=validate_receipt(json.loads((ROOT/"data/fr-db-source.json").read_text(encoding="utf-8")),
                                 source_run_id,run["head_sha"])
        if receipt["batch_id"]!=batch_id:
            raise RuntimeError("Same-run Raw receipt is not the requested batch")
    # New run only; never clear live leases from another run.
    pool.purge()
    result=pool.seed_from_crawl(source_run_id,run.get("run_attempt",1))
    count=int(result["source_stores"])
    if receipt is not None and receipt["stores"]!=count:
        raise RuntimeError("Same-run stage 16 receipt store roster mismatch")
    if count<=0 or int(result["inserted"])!=count:
        raise RuntimeError("Source crawl task roster did not seed in full")
    state=pool.status()
    if int(state.get("pending",0))!=count:
        raise RuntimeError("DB task pool count differs from verified crawl")
    output={"source_run_id":str(source_run_id),"source_attempt":run.get("run_attempt",1),
            "source_sha":run["head_sha"],"batch_id":batch_id,
            "stores":count,"artifacts":artifacts}
    return output


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--source-run-id",required=True)
    parser.add_argument("--batch-id",required=True)
    args=parser.parse_args()
    token=os.environ["GH_TOKEN"]
    repo=os.environ["GITHUB_REPOSITORY"]
    if not args.source_run_id.isdecimal():
        raise ValueError("Numeric source run required")
    output=plan(args.source_run_id,args.batch_id,DBTaskPool(),token,repo)
    path=ROOT/"data"/"db-stage-plan.json"
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding="utf-8")
    print("FOODRADAR_DB_STAGE_00="+json.dumps({
      "source_run_id":output["source_run_id"],"batch_id":output["batch_id"],
      "stores":output["stores"],
      "source_artifacts":len(output["artifacts"])}),flush=True)


if __name__=="__main__":
    main()
