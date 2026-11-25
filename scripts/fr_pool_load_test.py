"""Bounded synthetic benchmark: 15 concurrent clients, real Edge/RPC/SQL, no Uber."""
import concurrent.futures
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

ENDPOINT = "https://unapiwajmgqqtdkixaab.supabase.co/functions/v1/fr-task-pool-benchmark"
AUD = "foodradar:task-pool-benchmark"
CLIENTS = 15
DURATION = 12

def oidc():
    url = os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"]
    url += ("&" if "?" in url else "?") + urllib.parse.urlencode({"audience": AUD})
    with urllib.request.urlopen(urllib.request.Request(url, headers={
            "Authorization": "Bearer " + os.environ["ACTIONS_ID_TOKEN_REQUEST_TOKEN"]}), timeout=15) as r:
        return json.load(r)["value"]

def call(token, op, **kw):
    req = urllib.request.Request(ENDPOINT, data=json.dumps({"op":op, **kw}).encode(),
        headers={"X-FR-Workflow-Token":token, "Content-Type":"application/json"})
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.load(r)

def pct(values, p):
    if not values:
        return None
    values = sorted(values)
    return round(values[min(len(values)-1, int((len(values)-1)*p))]*1000, 1)

def stage(token, rate, seen):
    stop = threading.Event()
    lock = threading.Lock()
    start = time.monotonic()+0.5
    end = start+DURATION
    errors = []
    duplicates = []
    def worker(w):
        latencies = []
        previous = []
        completed = 0
        interval = CLIENTS/rate
        due = start+(w-1)/rate
        while not stop.is_set():
            delay = due-time.monotonic()
            if delay > 0:
                stop.wait(delay)
            if stop.is_set() or time.monotonic() >= end:
                break
            t = time.monotonic()
            try:
                result = call(token,"exchange",worker=w,finished=previous,slots=1)
                if result["finished"] != len(previous) or len(result["claimed"]) != 1:
                    raise RuntimeError("invalid claim/completion")
                row = result["claimed"][0]
                with lock:
                    if row["id"] in seen:
                        duplicates.append(row["id"])
                        stop.set()
                    seen.add(row["id"])
                completed += len(previous)
                previous = [{"id":row["id"],"token":row["lease_token"]}]
                latencies.append(time.monotonic()-t)
            except Exception as exc:
                with lock:
                    errors.append(getattr(exc,"code",type(exc).__name__))
                    stop.set()
                break
            # No request bursts or catch-up spikes after a slow response.
            due = max(due+interval, time.monotonic())
        if previous and not stop.is_set():
            try:
                result = call(token,"exchange",worker=w,finished=previous,slots=0)
                if result["finished"] != len(previous) or result["claimed"]:
                    raise RuntimeError("invalid final completion")
                completed += len(previous)
            except Exception as exc:
                with lock:
                    errors.append(getattr(exc,"code",type(exc).__name__))
                    stop.set()
        return latencies, completed
    with concurrent.futures.ThreadPoolExecutor(max_workers=CLIENTS) as pool:
        results = list(pool.map(worker,range(1,CLIENTS+1)))
    elapsed = max(0.001,time.monotonic()-start)
    lat = [v for series,_ in results for v in series]
    metric = {"target_rps":rate,"clients":CLIENTS,"duration_s":round(elapsed,2),
        "success":len(lat),"actual_rps":round(len(lat)/elapsed,2),
        "completed":sum(n for _,n in results),"p50_ms":pct(lat,.5),
        "p95_ms":pct(lat,.95),"p99_ms":pct(lat,.99),"max_ms":pct(lat,1),
        "http_429":errors.count(429),"errors":errors,"duplicate_claims":len(duplicates)}
    print("FOODRADAR_SB_BENCHMARK="+json.dumps(metric),flush=True)
    return metric

def main():
    token = oidc()
    if "--cleanup-only" in sys.argv:
        print("FOODRADAR_SB_CLEANUP="+json.dumps(call(token,"cleanup")))
        stats=call(token,"stats")
        if stats["total"] != 0:
            raise RuntimeError("benchmark rows remain")
        return
    seen=set()
    results=[]
    try:
        seed=call(token,"seed")
        if seed["seeded"] != 5000:
            raise RuntimeError("unexpected seed count")
        for rate in (15,30,60,150):
            metric=stage(token,rate,seen)
            results.append(metric)
            if metric["errors"] or metric["duplicate_claims"] or not metric["success"]:
                break
            # Protect other services sharing the free project.
            if metric["p95_ms"] > 1500:
                print("STOP: p95 exceeds 1500ms; no higher load")
                break
        stats=call(token,"stats")
        print("FOODRADAR_SB_STATS="+json.dumps(stats))
        valid=(not any(x["errors"] or x["duplicate_claims"] for x in results)
               and stats["running"]==0 and stats["local_done"]==len(seen)
               and stats["total"]==5000)
        print("FOODRADAR_SB_RESULT="+json.dumps({"valid":valid,"stages":results,"stats":stats}))
        if not valid:
            raise RuntimeError("benchmark consistency or transport failure")
    finally:
        print("FOODRADAR_SB_CLEANUP="+json.dumps(call(oidc(),"cleanup")))
        if call(oidc(),"stats")["total"] != 0:
            raise RuntimeError("benchmark cleanup incomplete")

if __name__=="__main__":
    main()
