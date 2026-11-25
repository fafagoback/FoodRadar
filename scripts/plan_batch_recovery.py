"""Lightweight post-matrix gate: no HTTP body download or dependencies."""
import json
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/"src"))
from crawl_queue import SharedGitQueue

def main():
    q=SharedGitQueue(Path(__file__).resolve().parents[1],os.environ["GITHUB_RUN_ID"],
                     int(os.environ.get("GITHUB_RUN_ATTEMPT","1")))
    state=q.read()
    if (state["run_id"] != os.environ["GITHUB_RUN_ID"] or
            state["source_sha"] != os.environ["GITHUB_SHA"] or
            state["epoch"] != os.environ["EPOCH"]):
        raise ValueError("Task pool identity mismatch")
    remaining=len(state["pending"])+len(state["leased"])+len(state.get("local_done", {}))
    if state.get("catalog_groups"):
        raise ValueError("Unexpected legacy category checkpoint")
    if remaining:
        q.reset_terminated_wave()
    count=min(15,remaining)
    plan={"matrix":{"include":[{"shard":i,"worker":f"{i+1:02d}"} for i in range(count)]},
          "missing_count":count,"progress":len(state["completed"]),"remaining":remaining}
    with open(os.environ["GITHUB_OUTPUT"],"a") as f:
        for key in ("matrix","missing_count","progress"):
            f.write(key+"="+json.dumps(plan[key],separators=(",",":"))+"\n")
    print("FOODRADAR_WAVE_PLAN="+json.dumps(plan),flush=True)

if __name__=="__main__":
    main()
