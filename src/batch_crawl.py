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
from catalog_complete import _section_ids, capture_catalog
from menu_core import attach_delivery_location, response_retry_after
from raw_capture import capture_response
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
    active, finished, uncommitted = {}, {}, {}
    section_finished, uncommitted_sections = {}, {}
    claim_future, sync_future, status_future = None, None, None
    next_claim_at, next_status_at, last_flush_at = 0.0, 0.0, 0.0
    claim_seconds, idle_seconds, local_commit_seconds = 0.0, 0.0, 0.0
    last_saturated = False

    def claim(count):
        t = time.monotonic()
        rows = queue.claim(owner, index, count)
        return rows, time.monotonic() - t

    def one(kind, sid, key, lease):
        item = discovered[sid]
        section = kind == "section"
        folder = ((base / "sections" / (sid + "-" + key + "-" + lease["token"]))
                  if section else tasks / artifact_name(state, sid, lease))
        raw = folder / "raw"
        raw.mkdir(parents=True)
        t = time.monotonic()

        if section:
            expected_uuid = lease["section_uuid"]
            def only_section(store, root, relative, data, headers, attempt):
                section_ids = _section_ids(data)
                if expected_uuid not in section_ids:
                    raise ValueError("Native catalog category identity changed")
                section_index = section_ids.index(expected_uuid)
                session = transport.session()
                try:
                    if not attach_delivery_location(session, item):
                        raise ValueError("Missing delivery location for section worker")
                    def refresh_native():
                        global_limiter.wait()
                        response = session.post(
                            "https://www.ubereats.com/api/getStoreV1",
                            json={"storeUuid": sid, "sfNuggetCount": 24},
                            headers=headers, timeout=12)
                        capture_response(root, "refresh/native-" + str(section_index)
                                         + "-status-" + str(response.status_code) + ".body", response)
                        if response.status_code == 429:
                            global_limiter.trigger_backoff(30, "Section refresh HTTP 429",
                                                           retry_after=response_retry_after(response))
                        if response.status_code != 200:
                            raise RuntimeError("Section refresh HTTP " + str(response.status_code))
                        body = response.json()
                        data = body.get("data")
                        if (body.get("status") == "failure" or not isinstance(data, dict)
                                or set(_section_ids(data)) != set(section_ids)):
                            raise RuntimeError("Native catalog category metadata changed")
                        global_limiter.record_success()
                        return data
                    return capture_catalog(
                        session, sid, data, root, headers, capture_response,
                        store_attempt=attempt, section_indices=[section_index],
                        refresh_native_metadata=refresh_native)
                finally:
                    session.close()
            result = capture(item, raw, session_factory=transport.session,
                             catalog_runner=only_section)
            checks = body_checksums(raw)
            doc = {"run_id":state["run_id"], "source_sha":state["source_sha"],
                   "epoch":state["epoch"], "store_uuid":sid, "token":lease["token"],
                   "worker":index, "section_index":int(key),
                   "section_uuid":expected_uuid, "result":result, "responses":checks}
            (folder / "section.json").write_text(json.dumps(doc, ensure_ascii=False),
                                                 encoding="utf8")
            if (result.get("store_uuid") != sid or
                    result.get("catalog_section_count", 0) not in (0, 1)):
                raise ValueError("Invalid native section result")
            return kind, sid, key, lease["token"]

        def dispatch(store, root, relative, data, headers, attempt):
            sections = _section_ids(data)
            estimate = shared.get("workload_weights", {}).get(sid, 400)
            if capture is not fetch_store_raw:
                return None
            if len(sections) < 16 and not (len(sections) >= 8 and estimate >= 20000):
                return None
            queue.register_catalog(sid, lease["token"], sections)
            print("FOODRADAR_CATEGORY_SPLIT=" + json.dumps({
                "store_uuid": sid, "sections": len(sections), "estimated_work": estimate,
                "worker":index}), flush=True)
            return {"catalog_responses":[], "catalog_complete":False,
                    "catalog_section_count":len(sections),
                    "catalog_sections_finished":0, "catalog_item_occurrences":0,
                    "catalog_deferred":True}

        result = capture(item, raw, session_factory=transport.session,
                         catalog_runner=dispatch) if capture is fetch_store_raw else capture(
                             item, raw, session_factory=transport.session)
        if (result.get("success") is True and not result.get("inactive")
                and not result.get("catalog_deferred")):
            prior = baseline.get(sid) or {}
            try:
                result = verify(item, raw, result, prior.get("stats"), prior.get("batch", ""),
                                previous_watch=prior.get("promotion_watch"))
            except Exception as exc:
                result["promo_recheck"] = {"status":"UNAVAILABLE", "trigger":"verifier_exception",
                    "error":str(exc)[:180], "previous_stats":prior.get("stats"),
                    "previous_batch":prior.get("batch", ""),
                    "previous_watch":prior.get("promotion_watch") or {}}
        result["crawl_elapsed_ms"] = max(1, round((time.monotonic()-t)*1000))
        checks = body_checksums(raw)
        result["crawl_api_requests"] = len(checks)
        doc = {"run_id":state["run_id"], "source_sha":state["source_sha"],
               "epoch":state["epoch"], "store_uuid":sid, "token":lease["token"],
               "worker":index, "store":result, "responses":checks}
        (folder / "task.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf8")
        validate_task(folder, state, sid, lease)
        return kind, sid, key, lease["token"]

    try:
        # Claim no more than the available execution slots. Store bodies remain
        # local until a single final worker artifact upload.
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
                    if not rows:
                        next_claim_at = now + 2.0
                    for sid, lease in rows:
                        if len(active) >= ctrl.target:
                            raise ValueError("Claimed more than free execution slots")
                        active[pool.submit(one, sid, lease)] = sid
                if sync_future is not None and sync_future.done():
                    amount, elapsed = sync_future.result()
                    local_commit_seconds += elapsed
                    sync_future = None
                if status_future is not None and status_future.done():
                    state_now = status_future.result()
                    status_future = None
                    if state_now["pending"]:
                        next_claim_at = now
                    if (not active and not uncommitted and sync_future is None
                            and claim_future is None and not state_now["pending"]
                            and not state_now["leased"]
                            and (len(state_now.get("local_done", {}))
                                 + len(state_now["completed"]) == len(ids))):
                        break
                if sync_future is None and uncommitted and (
                        len(uncommitted) >= 3 or not active or now - last_flush_at >= 3):
                    staged = dict(uncommitted)
                    uncommitted.clear()
                    last_flush_at = now
                    def sync(rows=staged):
                        begin = time.monotonic()
                        n = queue.mark_local_batch(owner, rows)
                        return n, time.monotonic() - begin
                    sync_future = coordinator.submit(sync)
                if (snap["cooldown_remaining_seconds"] <= 0
                        and claim_future is None and now >= next_claim_at):
                    free_slots = max(0, ctrl.target - len(active))
                    if free_slots and not uncommitted:
                        claim_future = coordinator.submit(claim, free_slots)
                if (not active and not uncommitted and claim_future is None
                        and sync_future is None and status_future is None
                        and now >= next_status_at):
                    status_future = coordinator.submit(queue.read)
                    next_status_at = now + 2.0
                saturated = len(active) >= ctrl.target
                if saturated and not last_saturated:
                    ctrl.reset(time.monotonic(), global_limiter.snapshot())
                if saturated:
                    ctrl.observe(time.monotonic(), global_limiter.snapshot(), True)
                last_saturated = saturated
                waiting = set(active)
                for future in (claim_future, sync_future, status_future):
                    if future is not None:
                        waiting.add(future)
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
                    uncommitted[sid] = token
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
                          "idle_queue_seconds": round(idle_seconds, 2),
                           "local_commit_seconds": round(local_commit_seconds, 2)}}
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
