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
from crawl_queue import SharedGitQueue, initial_state
from distributed_crawl import merge_shards

CFG = json.loads((ROOT / "config.json").read_text())
DISCOVERY = ROOT / "data/discovery/discovery.json"


def init():
    body = DISCOVERY.parent / "raw/feed/fixture.body"
    body.parent.mkdir(parents=True, exist_ok=True)
    body.write_bytes(b'{"data":{"feedItems":[]}}')
    count = 1 if os.getenv('FR_FIXTURE_KIND') == 'partition' else 60
    ids = [str(uuid.UUID(int=i + 1)) for i in range(count)]
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


def partition_worker(index):
    """Real GitHub runners + artifact runtime; only upstream HTTP is synthetic."""
    from unittest.mock import patch
    from test_catalog_complete import Response, sample_item
    from catalog_complete import CATALOG_API
    from raw_capture import fetch_store_raw
    from catalog_broker import MIN_SECTIONS

    class Session:
        def close(self): pass
        def post(self,url,json,headers,timeout):
            if url.endswith("/getStoreV1"):
                return Response({"status":"success","data":{"title":"Fixture","aisles":{"x":[
                    {"catalogSectionUUID":str(i)} for i in range(12)]}}})
            assert url == CATALOG_API
            category=json["storeFilters"]["sectionUuids"][0]
            offset=json["pagingInfo"]["offset"]
            assert offset in (0,60)
            return Response({"status":"success","data":{"fixture_worker":index,
                "catalog":[{"payload":{"standardItemsPayload":{"catalogItems":[
                    sample_item(category+"-"+str(offset),"Fixture",10000)]}}}],
                "pagingInfo":{"hasMore":offset==0,"offset":60 if offset==0 else 120}}})
    class Transport:
        def session(self): return Session()
        def close(self): pass
    q=SharedGitQueue(ROOT,os.environ["GITHUB_RUN_ID"],int(os.environ["GITHUB_RUN_ATTEMPT"]),lease_seconds=90)
    dc.HEARTBEAT_SECONDS=15
    dc.public_ip=lambda:None
    start=json.loads(DISCOVERY.read_text())["fixture_start"]
    time.sleep(max(0,start-time.time()))
    if index:
        deadline=time.time()+150
        while not q.read().get("catalog_groups"):
            if time.time()>deadline:raise RuntimeError("Crash owner never created native category group")
            time.sleep(1)
    def storage(op,name,folder):
        result=dc.artifact(op,name,folder)
        if index==0 and op=="upload" and name.startswith("fr-page-"):
            print("FR_FIXTURE_CRASH_AFTER_SAVED_PAGE="+name,flush=True)
            os._exit(73)
        return result
    with patch("dynamic_crawl.TransportPool",Transport),patch("raw_capture.attach_delivery_location",return_value=True),patch("catalog_broker.attach_delivery_location",return_value=True):
        dc.capture_worker(CFG,index,DISCOVERY,queue=q,storage=storage,capture=fetch_store_raw,
            verify=lambda item,raw,result,*args,**kwargs:result)


def worker(index):
    if os.getenv("FR_FIXTURE_KIND")=="partition":
        return partition_worker(index)
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
    dc.HEARTBEAT_SECONDS = 15
    q = SharedGitQueue(ROOT, os.environ["GITHUB_RUN_ID"],
                 int(os.environ["GITHUB_RUN_ATTEMPT"]), lease_seconds=90)
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
    count = 1 if os.getenv("FR_FIXTURE_KIND")=="partition" else 60
    assert len(state["completed"]) == count
    assert not state["pending"] and not state["leased"]
    if count==60:
        assert any(x["worker"] == 0 for x in state["completed"].values()), "Crash fixture did not claim a task"
    assert len(list((ROOT/"data/tasks").glob("fr-task-*"))) == count, "A persisted task was crawled again"
    report = merge_shards(CFG, ROOT/"data/parts", root=ROOT, upload_raw=False,
                          strict=True, discovery_file=DISCOVERY)
    assert report["complete"] and report["captured_stores"] == count
    if count==1:
        group=next(iter(state["catalog_groups"].values()))
        assert len(group["completed"])==12 and not group["pending"] and not group["leased"]
        workers={v["worker"] for v in group["completed"].values()}
        assert len(workers)>=2 and 0 not in workers, workers
        folder=next((ROOT/"data/tasks").glob("fr-task-*"))
        result=json.loads((folder/"task.json").read_text())["store"]
        assert result["catalog_complete"] and result["catalog_section_count"]==12
        assert len(result["catalog_responses"])==24
        assert sum(json.loads((folder/"raw"/p["path"]).read_bytes())["data"].get("fixture_worker")==0 for p in result["catalog_responses"])==1
        print("FR_HOSTED_PARTITIONS="+json.dumps({"sections":12,"pages":24,"workers":sorted(workers),"crashed_page_reused":True,"hf_write":False}),flush=True)
    else:
        print("FR_HOSTED_MATRIX_EXACTLY_ONCE=60; persisted-result fault recovered; no HF write", flush=True)


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "init": init()
    elif command == "worker": worker(int(sys.argv[2]))
    elif command == "assemble": assemble()
    else: raise ValueError(command)
