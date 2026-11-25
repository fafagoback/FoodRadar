"""All store responses remain local until one final upload per worker."""
from __future__ import annotations
import argparse
import json
import os
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from adaptive_threads import AdaptiveThreads, DEFAULT_THREADS, MAX_THREADS, valid_threads
from crawl_queue import artifact_name
from crawl_transport import TransportPool
from distributed_crawl import ROOT, load_shared_discovery, public_ip
from dynamic_crawl import runtime_queue, shared_identity, verify_queue, body_checksums, validate_task
from menu_core import global_limiter
from promo_api_recheck import verify_suspicious_store
from raw_capture import fetch_store_raw

def batch_name(state, index, wave):
    return f"fr-worker-{state['epoch']}-{index}-w{wave:02d}"

def capture_worker(config, index, discovery_file, queue=None, root=ROOT,
                   capture=fetch_store_raw, verify=verify_suspicious_store):
    started = time.monotonic()
    shared, digest, ids = shared_identity(discovery_file)
    queue = queue or runtime_queue()
    state = queue.read()
    verify_queue(state, shared, digest, ids)
    if state.get("catalog_groups"):
        raise ValueError("Legacy page checkpoint queue")
    base = root / "data" / f"worker-batch-{index}"
    tasks = base / "tasks"
    tasks.mkdir(parents=True, exist_ok=False)
    discovered, _, _, owners = load_shared_discovery(discovery_file, config, base, -1)
    if owners is not None:
        raise ValueError("Static ownership is not allowed")
    baseline = shared.get("previous_promotion_stats") or {}
    wave = int(os.getenv("FR_CRAWL_WAVE", "0"))
    owner = f"{index}-{uuid.uuid4().hex}"
    initial = int(os.getenv("FOODRADAR_INITIAL_THREADS", str(DEFAULT_THREADS)))
    if not valid_threads(initial):
        raise ValueError("Invalid initial threads")
    ctrl = AdaptiveThreads(initial, window_seconds=10, minimum_successes=4)
    ctrl.reset(time.monotonic(), global_limiter.snapshot())
    transport = TransportPool()
    pending, active, finished = deque(), {}, {}
    drained, claim_future = False, None
    claim_seconds, idle_seconds = 0.0, 0.0
    last_saturated = False

    def claim(count):
        t = time.monotonic()
        rows = queue.claim(owner, index, count)
        return rows, time.monotonic() - t

    def one(sid, lease):
        folder = tasks / artifact_name(state, sid, lease)
        raw = folder / "raw"
        raw.mkdir(parents=True)
        item = discovered[sid]
        t = time.monotonic()
        result = capture(item, raw, session_factory=transport.session)
        if result.get("success") is True and not result.get("inactive"):
            prior = baseline.get(sid) or {}
            try:
                result = verify(item, raw, result, prior.get("stats"), prior.get("batch", ""),
                                previous_watch=prior.get("promotion_watch"))
            except Exception as exc:
                result["promo_recheck"] = {"status": "UNAVAILABLE", "trigger": "verifier_exception",
                    "error": str(exc)[:180], "previous_stats": prior.get("stats"),
                    "previous_batch": prior.get("batch", ""),
                    "previous_watch": prior.get("promotion_watch") or {}}
        result["crawl_elapsed_ms"] = max(1, round((time.monotonic()-t)*1000))
        checks = body_checksums(raw)
        result["crawl_api_requests"] = len(checks)
        doc = {"run_id": state["run_id"], "source_sha": state["source_sha"],
               "epoch": state["epoch"], "store_uuid": sid, "token": lease["token"],
               "worker": index, "store": result, "responses": checks}
        (folder / "task.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf8")
        validate_task(folder, state, sid, lease)
        return sid, lease["token"]

    try:
        # Coordinate claims asynchronously, never block API threads on Git.
        # No mid-run upload, heartbeat, expiry recovery, or polling other
        # workers' leases. Recovery only begins after the matrix terminates.
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool, ThreadPoolExecutor(max_workers=1) as coordinator:
            while True:
                now = time.monotonic()
                snap = global_limiter.snapshot()
                if snap["rate_limit_events"] > ctrl.baseline["rate_limit_events"]:
                    ctrl.observe(now, snap, False)
                    last_saturated = False
                if claim_future is not None and claim_future.done():
                    rows, elapsed = claim_future.result()
                    claim_seconds += elapsed
                    claim_future = None
                    pending.extend(rows)
                    if not rows:
                        drained = True
                if snap["cooldown_remaining_seconds"] <= 0:
                    while pending and len(active) < ctrl.target:
                        sid, lease = pending.popleft()
                        active[pool.submit(one, sid, lease)] = sid
                    if not drained and claim_future is None and len(pending) < max(1, min(2, ctrl.target)):
                        slots = max(1, min(4, ctrl.target - len(active) + 2))
                        claim_future = coordinator.submit(claim, slots)
                if not active and not pending and drained:
                    break
                saturated = len(active) >= ctrl.target and (bool(pending) or not drained)
                if saturated and not last_saturated:
                    ctrl.reset(time.monotonic(), global_limiter.snapshot())
                if saturated:
                    ctrl.observe(time.monotonic(), global_limiter.snapshot(), True)
                last_saturated = saturated
                waiting = set(active)
                if claim_future is not None:
                    waiting.add(claim_future)
                if not waiting:
                    time.sleep(.15)
                    continue
                idle_start = time.monotonic() if not active else None
                done, _ = wait(waiting, timeout=.25, return_when=FIRST_COMPLETED)
                if idle_start is not None:
                    idle_seconds += time.monotonic() - idle_start
                for future in done:
                    if future not in active:
                        continue
                    sid, token = future.result()
                    del active[future]
                    if sid in finished:
                        raise ValueError("Duplicate local store")
                    finished[sid] = token
    finally:
        transport.close()
    manifest = {"run_id": state["run_id"], "source_sha": state["source_sha"],
                "epoch": state["epoch"], "owner": owner, "worker": index,
                "wave": wave, "name": batch_name(state, index, wave), "records": finished}
    (base / "batch.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf8")
    summary = {"run_id": state["run_id"], "source_sha": state["source_sha"],
               "epoch": state["epoch"], "shard_index": index, "public_ip": public_ip(),
               "thread_learning": ctrl.report(), "api": global_limiter.snapshot(),
               "completed": len(finished), "wave": wave, "artifact_uploads": 1,
               "budget_exhausted": False,
               "timing": {"worker_seconds": round(time.monotonic()-started, 2),
                          "claim_seconds": round(claim_seconds, 2),
                          "idle_queue_seconds": round(idle_seconds, 2)}}
    (base / "summary.json").write_text(json.dumps(summary), encoding="utf8")
    print("FOODRADAR_DYNAMIC_WORKER=" + json.dumps(summary), flush=True)
    return summary

def finalize(index, artifact_id, discovery_file, queue=None, root=ROOT):
    shared, digest, ids = shared_identity(discovery_file)
    q = queue or runtime_queue()
    state = q.read()
    verify_queue(state, shared, digest, ids)
    base = root / "data" / f"worker-batch-{index}"
    doc = json.loads((base / "batch.json").read_text())
    wave = int(os.getenv("FR_CRAWL_WAVE", "0"))
    for k, v in {"run_id": state["run_id"], "source_sha": state["source_sha"],
                 "epoch": state["epoch"], "worker": index, "wave": wave,
                 "name": batch_name(state, index, wave)}.items():
        if doc.get(k) != v:
            raise ValueError("Worker batch identity mismatch: " + k)
    if not isinstance(doc["records"], dict) or not set(doc["records"]) <= set(ids):
        raise ValueError("Invalid worker batch roster")
    if {p.name for p in (base/"tasks").iterdir()} != {
        artifact_name(state, sid, {"token": token}) for sid, token in doc["records"].items()
    }:
        raise ValueError("Worker batch task folders do not match manifest")
    t = time.monotonic()
    count = q.finish_batch(doc["owner"], doc["records"], artifact_id, doc["name"])
    print("FOODRADAR_BATCH_FINALIZED=" + json.dumps({"worker": index, "wave": wave,
          "stores": count, "artifact_id": artifact_id, "queue_commit_seconds": round(time.monotonic()-t, 2)}), flush=True)

def reset_terminated_wave(discovery_file, queue=None):
    q = queue or runtime_queue()
    shared, digest, ids = shared_identity(discovery_file)
    verify_queue(q.read(), shared, digest, ids)
    print("FOODRADAR_REQUEUED_TERMINATED_WAVE=" + str(q.reset_terminated_wave()), flush=True)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["worker", "finalize", "recover"])
    p.add_argument("--index", type=int, default=0)
    p.add_argument("--artifact-id", type=int)
    p.add_argument("--discovery-file", type=Path, default=ROOT/"data/discovery/discovery.json")
    a = p.parse_args()
    if a.command == "recover":
        reset_terminated_wave(a.discovery_file)
    elif a.command == "finalize":
        if not a.artifact_id or a.artifact_id <= 0:
            p.error("finalize requires a successful artifact upload ID")
        finalize(a.index, a.artifact_id, a.discovery_file)
    else:
        capture_worker(json.loads((ROOT/"config.json").read_text()), a.index, a.discovery_file)

if __name__ == "__main__":
    main()
