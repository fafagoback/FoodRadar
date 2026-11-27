"""Delete completed runs of obsolete FR workflows absent from main.

Only the three production paths are kept. The script refuses to touch a
workflow whose file still exists, any active run, or a current production Run.
Requires GitHub Actions token with actions:write.
"""
from __future__ import annotations
import json
import os
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

REPO = os.environ["GITHUB_REPOSITORY"]
TOKEN = os.environ["GH_TOKEN"]
BASE = "https://api.github.com/repos/" + REPO
KEEP = {".github/workflows/crawl.yml",
        ".github/workflows/pages.yml",
        ".github/workflows/fr-retention.yml"}
ACTIVE = {"queued", "in_progress", "pending", "waiting", "requested"}

def api(path, method="GET", *, may_404=False):
    req=Request(BASE + path,
                headers={"Authorization":"Bearer " + TOKEN,
                         "Accept":"application/vnd.github+json",
                         "X-GitHub-Api-Version":"2022-11-28"},
                method=method)
    for attempt in range(5):
        try:
            with urlopen(req,timeout=40) as response:
                data=response.read()
                return json.loads(data) if data else None
        except HTTPError as e:
            if e.code==404 and may_404:return None
            if e.code not in (403,429,500,502,503,504) or attempt==4:raise
            time.sleep(2**attempt)
    raise RuntimeError("Failed GitHub API: " + path)

def pages(path,key):
    page=1
    while True:
        sep="&" if "?" in path else "?"
        v=api(f"{path}{sep}per_page=100&page={page}")
        part=v[key]
        yield from part
        if len(part)<100:return
        page+=1

def prune():
    workflows=list(pages("/actions/workflows","workflows"))
    deleted=[];skipped=[];disabled=[];obsolete=[]
    for w in workflows:
        path=w["path"].split("@",1)[0]
        if path in KEEP:
            continue
        if not path.startswith(".github/workflows/"):
            skipped.append({"name":w["name"],"reason":"outside workflow directory"});continue
        if api("/contents/"+path,may_404=True) is not None:
            # A real workflow in main is never silently removed by log cleanup.
            skipped.append({"name":w["name"],"reason":"source file still exists"});continue
        runs=list(pages(f"/actions/workflows/{w['id']}/runs","workflow_runs"))
        if any(run["status"] in ACTIVE for run in runs):
            skipped.append({"name":w["name"],"reason":"active run exists"});continue
        obsolete.append({"name":w["name"],"id":w["id"],"runs":len(runs)})
        for run in runs:
            if run["status"]!="completed":
                raise RuntimeError("Refuse deletion of unfinished run")
            api(f"/actions/runs/{run['id']}","DELETE")
            deleted.append({"id":run["id"],"name":w["name"]})
            print("FR_LEGACY_RUN_DELETED="+str(run["id"])+" "+w["name"],flush=True)
        if w["state"]=="active":
            # The workflow endpoint may vanish immediately when its last Run
            # is deleted. That 404 is a successful retired-workflow outcome.
            api(f"/actions/workflows/{w['id']}/disable","PUT",may_404=True)
            disabled.append(w["name"])
    report={"allowed":sorted(KEEP),"obsolete":obsolete,
            "deleted_runs":len(deleted),"disabled_workflows":disabled,"skipped":skipped}
    print("FR_LEGACY_WORKFLOW_PRUNE="+json.dumps(report,ensure_ascii=False),flush=True)
    return report

if __name__=="__main__":
    prune()
