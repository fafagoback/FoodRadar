"""Plan further physical jobs only for unfinished, durably progressing tasks."""
import json,os,re
from pathlib import Path

def plan(status,names,epoch,previous):
    remaining=int(status["remaining"])
    sections=int(status.get("sections_remaining",0))
    if remaining<0 or sections<0:raise ValueError("Invalid queue counters")
    useful={n for n in names if re.match(r"^fr-(?:task|plan|section|page|small)-"+re.escape(epoch)+r"-",n)}
    progress=int(status["completed"])+int(status.get("sections_completed",0))+len(useful)
    if sections and not remaining:
        raise RuntimeError("Category tasks remain without a live store task")
    if remaining and previous>=0 and progress<=previous:
        raise RuntimeError("No durable progress in prior wave; refusing repeated blind retries")
    recovery=list(range(15)) if remaining else []
    return {"matrix":{"include":[{"shard":i,"worker":f"{i+1:02d}"} for i in recovery]},
            "missing_count":len(recovery),"progress":progress,"remaining":remaining}

def main():
    root=Path(os.environ["RUNNER_TEMP"])
    status=json.loads((root/"fr-queue-status.json").read_text())
    r=plan(status,(root/"fr-artifacts").read_text().splitlines(),
           os.environ["EPOCH"],int(os.environ["PREVIOUS_PROGRESS"]))
    if os.environ.get("FR_BATCH_RECOVERY") == "1":
        r["matrix"]["include"] = r["matrix"]["include"][:min(15,r["remaining"])]
        r["missing_count"] = len(r["matrix"]["include"])
    with open(os.environ["GITHUB_OUTPUT"],"a") as f:
        for k in ("matrix","missing_count","progress"):
            f.write(k+"="+json.dumps(r[k],separators=(",",":"))+"\n")
    print("FOODRADAR_WAVE_PLAN="+json.dumps(r),flush=True)

if __name__=="__main__":main()
