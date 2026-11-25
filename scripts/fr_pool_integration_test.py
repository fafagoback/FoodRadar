"""Exercise the production SB adapter with synthetic jobs, no Uber/HF traffic."""
import concurrent.futures
import json
import os
import sys
import threading
import time
import uuid
from crawl_queue import initial_state
from sb_task_pool import SupabaseQueue

RUN=os.environ["GITHUB_RUN_ID"]
ATTEMPT=int(os.environ["GITHUB_RUN_ATTEMPT"])
IDS=[str(uuid.uuid5(uuid.NAMESPACE_URL,"foodradar-test-"+str(i))) for i in range(477)]
HEAVY=set(IDS[:12])

def queue():
    return SupabaseQueue(".",RUN,ATTEMPT)

def main():
    q=queue()
    if "--cleanup-only" in sys.argv:
        print("FR_SB_PRODUCTION_PURGE="+str(q.purge()),flush=True)
        return
    # No real tasks may coexist with this test.
    q.initialize(initial_state(RUN,"test-epoch",os.environ["GITHUB_SHA"],IDS,
                  {sid:600000 if sid in HEAVY else 400 for sid in IDS}))
    seen=set()
    lock=threading.Lock()
    try:
        # Requeue a simulated dead worker's unpersisted job.
        rows,_=q.exchange("0-"+uuid.uuid4().hex,0,1,{}, {})
        assert len(rows)==1 and q.reset_terminated_wave()==1
        started=time.monotonic()
        def worker(index):
            wq=queue()
            owner=f"{index}-{uuid.uuid4().hex}"
            stores={}; sections={}; done_s={}; done_c={}; lat=[]; last=0
            while True:
                before=time.monotonic()
                jobs,drained=wq.exchange(owner,index,2,done_s,done_c)
                lat.append(time.monotonic()-before)
                done_s={}; done_c={}
                for kind,sid,key,lease in jobs:
                    with lock:
                        identity=(kind,sid,key)
                        if identity in seen:
                            raise RuntimeError("duplicate task "+str(identity))
                        seen.add(identity)
                    if kind=="store":
                        if sid in HEAVY:
                            wq.register_catalog(sid,lease["token"],[f"native-{i}" for i in range(29)])
                        stores[sid]=lease["token"]; done_s[sid]=lease["token"]
                    else:
                        sections.setdefault(sid,{})[key]=lease["token"]
                        done_c.setdefault(sid,{})[key]=lease["token"]
                    last=time.monotonic()-started
                if not jobs and drained:
                    break
                if not jobs:
                    time.sleep(.3)
                if time.monotonic()-started>180:
                    raise RuntimeError("production pool timeout")
            count=wq.finalize_batch(owner,stores,sections,1000+index,"synthetic-worker-"+str(index))
            return {"worker":index+1,"stores":count[0],"sections":count[1],"last_task_s":round(last,2),
                    "lat":lat,**wq.metrics()}
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            results=list(executor.map(worker,range(15)))
        state=q.read()
        assert not state["pending"] and not state["leased"] and not state["local_done"]
        assert len(state["completed"])==477
        assert len(state["catalog_groups"])==12
        assert sum(len(g["completed"]) for g in state["catalog_groups"].values())==348
        assert len(seen)==825
        latency=sorted(t for r in results for t in r.pop("lat"))
        metrics={"stores":477,"sections":348,"workers":results,
                 "elapsed_s":round(time.monotonic()-started,2),
                 "rpc_p95_ms":round(latency[int((len(latency)-1)*.95)]*1000,1),
                 "http_429":sum(r["http_429"] for r in results),
                 "duplicate_claims":0,"cross_worker_sections":sum(
                  row["worker"]!=state["completed"][sid]["worker"]
                  for sid,g in state["catalog_groups"].items() for row in g["completed"].values())}
        print("FR_SB_PRODUCTION_RESULT="+json.dumps(metrics),flush=True)
        assert metrics["http_429"]==0 and metrics["cross_worker_sections"]>0
        assert q.cleanup()==825
        print("FR_SB_PRODUCTION_CLEANUP=825",flush=True)
    finally:
        print("FR_SB_PRODUCTION_PURGE="+str(q.purge()),flush=True)

if __name__=="__main__":
    main()
