"""Plan recovery from sealed HF Raw only; no crawler or UE network."""
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from huggingface_hub import HfApi
from hf_retention import config, snapshot, all_records, check_prefix, read, STATE, FOLDER
from pipeline import raw_manifest_should_replay
from state_checkpoint import compatible_direct_contract

def main():
    config()
    api = HfApi(token=os.environ["HF_TOKEN"])
    rev, files = snapshot(api)
    records = all_records(files, rev)
    prefix = 0
    if STATE + "manifest.json" in files:
        meta = json.loads(read(STATE + "manifest.json", rev))
        prefix = check_prefix(meta, records)
        if not compatible_direct_contract(meta):
            raise ValueError("Existing durable DB has an incompatible contract; preserve it and review migration before recovery")
    if not records:
        raise ValueError("No sealed Raw manifests")
    pending = [m["batch_id"] for p,m,_ in records[prefix:] if raw_manifest_should_replay(m,p,FOLDER)]
    if not pending and not prefix:
        raise ValueError("No replayable Raw")
    print("FOODRADAR_OFFLINE_PLAN=" + json.dumps({"pending":pending, "prefix":prefix, "source_revision":rev}), flush=True)
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as out:
        out.write("slots=" + json.dumps(list(range(1, len(pending)+1)) or [0]) + "\n")
        out.write("pending=" + str(len(pending)) + "\n")

if __name__ == "__main__":
    main()
