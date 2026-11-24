"""Staging-only integration fixture: never requests a real catalog or uploads HF."""
import hashlib
import json
import os
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
import dynamic_crawl as dc
from crawl_queue import GitQueue, initial_state
from distributed_crawl import merge_shards

CFG = json.loads((ROOT / "config.json").read_text())
DISCOVERY = ROOT / "data/discovery/discovery.json"


def init():
    body = DISCOVERY.parent / "raw/feed/fixture.body"
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_bytes(b'{"data":{"feedItems":[]}}')
    ids = [str(uuid.UUID(int=i + 1)) for i in range(60)]
    stores = [{"store_uuid": sid, "store_url": "https://example.invalid/store/fixture/" + sid}
              for sid in ids]
    document = {"format": "foodradar-shared-feed-v1", "assignment_mode": "dynamic",
                "owner_count": 15, "ownership": None, "run_id": os.environ["GITHUB_RUN_ID"],
                "source_sha": os.environ["GITHUB_SHA"], "latitude": CFG["latitude"],
                "longitude": CFG["longitude"], "feed_pages": 1, "stores": stores,
                "previous_promotion_stats": {}, "raw_checksums": {
                    "feed/fixture.body": hashlib.sha256(body.read_bytes()).hexdigest()},
                "fixture_start": time.time() + 35}
    DISCOVERY.write_text(json.dumps(document), encoding="utf8")
    shared, digest, ids = dc.shared_identity(DISCOVERY)
    dc.runtime_queue().initialize(initial_state(shared["run_id"], digest[:16],
                                                shared["source_sha"], ids))
    with open(os.environ["GITHUB_OUTPUT"], "a") as f:
        f.write("epoch=" + digest[:16] + "\n")


def worker(index):
    # Vary work duration, with no historic estimates and no real network.
    def capture(item, raw, **kwargs):
        sid = item["store_uuid"]
        time.sleep(.25 + (int(uuid.UUID(sid)) % 5) * .2)
        relative = "stores/" + sid + "-status-410.body"
        file = raw / relative
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(b'{"status":"failure","data":{"code":"410"}}')
        return {"store_uuid": sid, "store_url": item["store_url"], "success": True,
                "inactive": True, "response_path": relative, "captured_at": "2026-11-25T00:00:00+08:00"}
    dc.public_ip = lambda: None
    dc.HEARTBEAT_SECONDS = 5
    q = GitQueue(ROOT, os.environ["GITHUB_RUN_ID"],
                 int(os.environ["GITHUB_RUN_ATTEMPT"]), lease_seconds=45)
    if index == 0:
        # Simulate power loss just after immutable artifact upload, before
        # queue completion. Other actual hosted runners must recover it.
        def crashed(sid, token, artifact_id, recovered=False):
            print("FR_FIXTURE_CRASH_AFTER_SAVED=" + sid, flush=True)
            os._exit(73)
        q.finish = crashed
    start = json.loads(DISCOVERY.read_text())["fixture_start"]
    time.sleep(max(0, start - time.time()))
    dc.capture_worker(CFG, index, DISCOVERY, queue=q, capture=capture)


def assemble():
    q = dc.runtime_queue()
    dc.assemble(CFG, DISCOVERY, ROOT/"data/tasks", ROOT/"data/worker-summaries",
                ROOT/"data/parts", queue=q)
    state = q.read()
    assert len(state["completed"]) == 60
    assert not state["pending"] and not state["leased"]
    assert any(x["worker"] == 0 for x in state["completed"].values()), "Crash fixture did not claim a task"
    assert len(list((ROOT/"data/tasks").glob("fr-task-*"))) == 60, "A persisted task was crawled again"
    report = merge_shards(CFG, ROOT/"data/parts", root=ROOT, upload_raw=False,
                          strict=True, discovery_file=DISCOVERY)
    assert report["complete"] and report["captured_stores"] == 60
    print("FR_HOSTED_MATRIX_EXACTLY_ONCE=60; persisted-result fault recovered; no HF write", flush=True)


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "init": init()
    elif command == "worker": worker(int(sys.argv[2]))
    elif command == "assemble": assemble()
    else: raise ValueError(command)
