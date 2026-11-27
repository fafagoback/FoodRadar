"""All store responses remain local until one final upload per worker."""
from __future__ import annotations
import argparse
import json
import os
import time
import uuid
from collections import Counter, deque
import re
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from adaptive_threads import AdaptiveThreads, DEFAULT_THREADS, valid_threads
from crawl_queue import artifact_name
from crawl_transport import TransportPool
from distributed_crawl import ROOT, load_shared_discovery, public_ip
from dynamic_crawl import runtime_queue, shared_identity, verify_queue, body_checksums, validate_task
from menu_core import global_limiter
from raw_capture import fetch_store_raw

def section_refresh_name(section_index, attempt, sequence, status):
    return (f"refresh/native-{section_index}-attempt-{attempt}"
            f"-sequence-{sequence}-status-{status}.body")


def assert_supported_catalog_groups(state):
    """Late-starting worker must join an already active native section pool."""
    if any(group.get("format") != "batch-section-v2"
           for group in state.get("catalog_groups", {}).values()):
        raise ValueError("Unsupported legacy page checkpoint queue")


def should_split_catalog(sections_count, elapsed_ms, native_children=0):
    """Size, not category count, determines whether a store should fan out."""
    return sections_count >= 1 and (elapsed_ms >= 60000 or sections_count >= 40
                                    or native_children >= 8)


def batch_name(state, index, wave):
    return f"fr-worker-{state['epoch']}-{index}-w{wave:02d}"

def raw_response_category(path):
    """Classify immutable responses without fetching anything again."""
    if path.startswith("catalog/"):
        if "-collection-" in path:
            return "catalog_collection_probe"
        return "catalog_page"
    if path.startswith("stores/"):
        if "-metadata-refresh-" in path:
            return "store_metadata_refresh"
        return "store_metadata"
    return "other_raw"


def count_raw_http_responses(paths):
    """Count captured bodies; socket failures may lack a body."""
    counts = Counter()
    for path in paths:
        counts[raw_response_category(path)] += 1
        match = re.search(r"-attempt-(\d+)-status-\d+\.body$", path)
        if match and int(match.group(1)) > 1:
            counts["retry_response"] += 1
    return counts


def capture_worker(config, index, discovery_file, queue=None, root=ROOT,
                   capture=fetch_store_raw):
    started = time.monotonic()
    shared, digest, ids = shared_identity(discovery_file)
    queue = queue or runtime_queue()
    state = queue.read()
    verify_queue(state, shared, digest, ids)
    if state.get("catalog_groups"):
        raise ValueError("Store-only workers cannot consume a legacy split-catalog run")
    base = root / "data" / f"worker-batch-{index}"
    tasks = base / "tasks"
    tasks.mkdir(parents=True, exist_ok=False)
    discovered, _, _, owners = load_shared_discovery(discovery_file, config, base, -1)
    if owners is not None:
        raise ValueError("Static ownership is not allowed")
    wave = int(os.getenv("FR_CRAWL_WAVE", "0"))
    owner = f"{index}-{uuid.uuid4().hex}"
    # Every store slot is independent. Finished slots immediately take new stores.
    # Production must use the run's value fetched from Supabase.
    if os.getenv("FR_QUEUE_BACKEND") == "sb":
        initial = int(os.environ["FOODRADAR_INITIAL_THREADS"])
    else:
        initial = int(os.getenv("FOODRADAR_INITIAL_THREADS", str(DEFAULT_THREADS)))
    if not valid_threads(initial):
        raise ValueError("Invalid initial thread count")
    ctrl = AdaptiveThreads(initial)
    current_snapshot = {**global_limiter.snapshot(), "completed_stores": 0}
    prior_path = root / "data" / "previous-worker-learning" / "summary.json"
    if wave > 0 and prior_path.is_file():
        from dynamic_crawl import validate_learning_summary
        prior_doc = json.loads(prior_path.read_text(encoding="utf-8"))
        ctrl.resume(validate_learning_summary(prior_doc, state, index, wave - 1),
                    time.monotonic(), current_snapshot)
        print("FOODRADAR_THREAD_LEARNING_RESUMED=" + json.dumps({
            "worker": index + 1, "wave": wave, "target": ctrl.target,
            "best": ctrl.best}), flush=True)
    else:
        ctrl.reset(time.monotonic(), current_snapshot)
    transport = TransportPool()
    active, finished, uncommitted = {}, {}, {}
    reserved = deque()
    section_finished, uncommitted_sections = {}, {}
    claim_future, sync_future, status_future = None, None, None
    next_claim_at, next_status_at, last_flush_at = 0.0, 0.0, 0.0
    claim_seconds, idle_seconds, local_commit_seconds = 0.0, 0.0, 0.0
    last_task_finished = None
    raw_http_counts = Counter()
    raw_http_lock = threading.Lock()

    def claim(count):
        t = time.monotonic()
        rows = queue.claim_work(owner, index, count)
        return rows, time.monotonic() - t

    def one(kind, sid, key, lease):
        if kind != "store" or key is not None:
            raise ValueError("Only whole-store tasks are accepted")
        item = discovered[sid]
        folder = tasks / artifact_name(state, sid, lease)
        raw = folder / "raw"
        raw.mkdir(parents=True)
        t = time.monotonic()
        # No catalog_runner, promote_store or defer_children: all menu pages
        # and native subcategories are processed locally by this worker.
        result = capture(item, raw, session_factory=transport.session)
        if result.get("catalog_deferred") or result.get("catalog_deferred_children"):
            raise ValueError("Whole-store capture must not defer categories")
        result["crawl_elapsed_ms"] = max(1, round((time.monotonic()-t)*1000))
        checks = body_checksums(raw)
        result["crawl_api_requests"] = len(checks)
        with raw_http_lock:
            raw_http_counts.update(count_raw_http_responses(checks))
        doc = {"run_id":state["run_id"], "source_sha":state["source_sha"],
               "epoch":state["epoch"], "store_uuid":sid, "token":lease["token"],
               "worker":index, "store":result, "responses":checks}
        (folder / "task.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf8")
        validate_task(folder, state, sid, lease)
        return kind, sid, key, lease["token"]

    try:
        if hasattr(queue,"exchange"):
            # Keep active HTTP work running while a single coordinator returns
            # completed leases and immediately claims exactly the free slots.
            pending_exchange=None
            last_exchange=0.0
            next_probe=0.0
            empty_delay=0.6
            drained=False
            with ThreadPoolExecutor(max_workers=max(1, len(ids))) as pool, ThreadPoolExecutor(max_workers=1) as coordinator:
                while True:
                    now=time.monotonic()
                    snap=global_limiter.snapshot()
                    ctrl.observe(now, {**snap, "completed_stores": len(finished)}, len(active) >= ctrl.target, len(active))
                    if pending_exchange is not None and pending_exchange.done():
                        jobs,drained,elapsed=pending_exchange.result()
                        claim_seconds+=elapsed
                        pending_exchange=None
                        reserved.extend(jobs)
                        if len(jobs)>12 or any(kind!="store" or key is not None for kind,sid,key,lease in jobs):
                            raise ValueError("Store-only worker received invalid work")
                        if jobs:
                            empty_delay=0.6
                            next_probe=now
                        else:
                            next_probe=now+empty_delay
                            empty_delay=min(2.5,empty_delay*1.4)
                    if snap["cooldown_remaining_seconds"]<=0:
                        while reserved and len(active) < ctrl.target:
                            kind,sid,key,lease=reserved.popleft()
                            active[pool.submit(one,kind,sid,key,lease)]=(kind,sid,key)
                    if drained and not active and not reserved and not uncommitted and not uncommitted_sections and pending_exchange is None:
                        break
                    # Report completed stores and refill every available slot independently.
                    # Supabase RPC accepts 0..12 claims per request; replenish again
                    # if more than 12 worker slots remain free.
                    free=max(0,min(12,ctrl.target-len(active)-len(reserved)))
                    if snap["cooldown_remaining_seconds"]>0:
                        free=0
                    done_count=len(uncommitted)+sum(map(len,uncommitted_sections.values()))
                    # No artificial delay when a task just finished. Backoff is
                    # only for an empty pool; heartbeat keeps long work leased.
                    if pending_exchange is None and (
                            (free and (done_count or now>=next_probe))
                            or done_count>=3 or (done_count and now-last_exchange>=1)
                            or now-last_exchange>=30):
                        stores=dict(uncommitted)
                        sections={sid:dict(keys) for sid,keys in uncommitted_sections.items()}
                        uncommitted.clear()
                        uncommitted_sections.clear()
                        last_exchange=now
                        def do_exchange(slots=free,stores=stores,sections=sections):
                            begin=time.monotonic()
                            jobs,done=queue.exchange(owner,index,slots,stores,sections)
                            return jobs,done,time.monotonic()-begin
                        pending_exchange=coordinator.submit(do_exchange)
                    ctrl.observe(time.monotonic(), {**global_limiter.snapshot(), "completed_stores": len(finished)}, len(active) >= ctrl.target, len(active))
                    waiting=set(active)
                    if pending_exchange is not None:
                        waiting.add(pending_exchange)
                    idle_start=time.monotonic() if not active else None
                    if waiting:
                        done,_=wait(waiting,timeout=.15,return_when=FIRST_COMPLETED)
                    else:
                        time.sleep(.1)
                        done=set()
                    if idle_start is not None:
                        idle_seconds+=time.monotonic()-idle_start
                    ctrl.observe(time.monotonic(), {**global_limiter.snapshot(), "completed_stores": len(finished)}, len(active) >= ctrl.target, len(active))
                    for future in done:
                        if future not in active:
                            continue
                        kind,sid,key,token=future.result()
                        last_task_finished=time.monotonic()
                        del active[future]
                        drained=False
                        if kind=="section":
                            if key in section_finished.get(sid,{}):
                                raise ValueError("Duplicate native category")
                            section_finished.setdefault(sid,{})[key]=token
                            uncommitted_sections.setdefault(sid,{})[key]=token
                        else:
                            if sid in finished:
                                raise ValueError("Duplicate local store")
                            finished[sid]=token
                            uncommitted[sid]=token
        else:
            # Claim no more than the available execution slots. Store bodies remain
            # local until a single final worker artifact upload.
            with ThreadPoolExecutor(max_workers=max(1, len(ids))) as pool, ThreadPoolExecutor(max_workers=1) as coordinator:
                while True:
                    now = time.monotonic()
                    snap = global_limiter.snapshot()
                    ctrl.observe(now, {**snap, "completed_stores": len(finished)}, len(active) >= ctrl.target, len(active))
                    if claim_future is not None and claim_future.done():
                        rows, elapsed = claim_future.result()
                        claim_seconds += elapsed
                        claim_future = None
                        if not rows:
                            next_claim_at = now + 2.0
                        reserved.extend(rows)
                    if sync_future is not None and sync_future.done():
                        amount, elapsed = sync_future.result()
                        local_commit_seconds += elapsed
                        sync_future = None
                    if snap["cooldown_remaining_seconds"] <= 0:
                        while reserved and len(active) < ctrl.target:
                            kind, sid, key, lease = reserved.popleft()
                            active[pool.submit(one, kind, sid, key, lease)] = (kind, sid, key)
                    if status_future is not None and status_future.done():
                        state_now = status_future.result()
                        status_future = None
                        if state_now["pending"]:
                            next_claim_at = now
                        groups = state_now.get("catalog_groups", {})
                        categories_done = all(
                            g.get("format") == "batch-section-v2"
                            and not g["pending"] and not g["leased"]
                            and len(g["local_done"]) + len(g["completed"]) == len(g["sections"])
                            for g in groups.values())
                        if (not active and not reserved and not uncommitted and not uncommitted_sections
                                and sync_future is None and claim_future is None
                                and not state_now["pending"] and not state_now["leased"]
                                and categories_done and
                                len(state_now.get("local_done", {}))
                                + len(state_now["completed"]) == len(ids)):
                            break
                    if sync_future is None and (uncommitted or uncommitted_sections) and (
                            len(uncommitted) + sum(map(len, uncommitted_sections.values())) >= 3
                            or not active or now - last_flush_at >= 3):
                        staged = dict(uncommitted)
                        staged_sections = {sid:dict(rows) for sid,rows in uncommitted_sections.items()}
                        uncommitted.clear()
                        uncommitted_sections.clear()
                        last_flush_at = now
                        def sync(rows=staged, sections=staged_sections):
                            begin = time.monotonic()
                            n = queue.mark_local_batch(owner, rows) if rows else 0
                            n += queue.mark_local_sections(owner, sections) if sections else 0
                            return n, time.monotonic() - begin
                        sync_future = coordinator.submit(sync)
                    if (snap["cooldown_remaining_seconds"] <= 0
                            and claim_future is None and now >= next_claim_at):
                        free_slots = max(0, ctrl.target - len(active) - len(reserved))
                        if free_slots and not reserved and not uncommitted and not uncommitted_sections and sync_future is None:
                            claim_future = coordinator.submit(claim, free_slots)
                    if (not active and not reserved and not uncommitted and not uncommitted_sections and claim_future is None
                            and sync_future is None and status_future is None
                            and now >= next_status_at):
                        status_future = coordinator.submit(queue.read)
                        next_status_at = now + 2.0
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
                    ctrl.observe(time.monotonic(), {**global_limiter.snapshot(), "completed_stores": len(finished)}, len(active) >= ctrl.target, len(active))
                    for future in done:
                        if future not in active:
                            continue
                        kind, sid, key, token = future.result()
                        last_task_finished = time.monotonic()
                        del active[future]
                        if kind == "section":
                            if key in section_finished.get(sid, {}):
                                raise ValueError("Duplicate native category")
                            section_finished.setdefault(sid, {})[key] = token
                            uncommitted_sections.setdefault(sid, {})[key] = token
                        else:
                            if sid in finished:
                                raise ValueError("Duplicate local store")
                            finished[sid] = token
                            uncommitted[sid] = token
    finally:
        transport.close()
    manifest = {"run_id": state["run_id"], "source_sha": state["source_sha"],
                "epoch": state["epoch"], "owner": owner, "worker": index,
                "wave": wave, "name": batch_name(state, index, wave), "records": finished,
                 "sections": section_finished}
    (base / "batch.json").write_text(json.dumps(manifest, sort_keys=True), encoding="utf8")
    counts = dict(sorted(raw_http_counts.items()))
    recorded = sum(v for k, v in counts.items() if k != "retry_response")
    summary = {"run_id": state["run_id"], "source_sha": state["source_sha"],
               "epoch": state["epoch"], "shard_index": index, "public_ip": public_ip(),
               "thread_learning": ctrl.report(), "api": global_limiter.snapshot(),
               "captured_raw_http_responses": recorded,
               "captured_raw_http_by_type": counts,
               "completed": len(finished), "sections_completed": sum(len(x) for x in section_finished.values()),
                "wave": wave, "artifact_uploads": 1,
               "budget_exhausted": False,
               "timing": {"worker_seconds": round(time.monotonic()-started, 2),
                          "claim_seconds": round(claim_seconds, 2),
                          "idle_queue_seconds": round(idle_seconds, 2),
                           "local_commit_seconds": round(local_commit_seconds, 2),
                           "last_task_finish_seconds": round((last_task_finished or started)-started, 2),
                           "barrier_wait_seconds": round(time.monotonic()-(last_task_finished or started),2)}}
    if hasattr(queue,"metrics"):
        summary["sb_queue"]=queue.metrics()
    (base / "summary.json").write_text(json.dumps(summary), encoding="utf8")
    print("FOODRADAR_DYNAMIC_WORKER=" + json.dumps(summary), flush=True)
    return summary

def finalize(index, artifact_id, discovery_file, queue=None, root=ROOT):
    shared, digest, ids = shared_identity(discovery_file)
    q = queue or runtime_queue()
    # The server validates the run-scoped owner and fencing token for each
    # finalized task; a worker does not need to read the entire shared pool.
    if hasattr(q, "finalize_batch"):
        state = {"run_id": shared["run_id"], "source_sha": shared["source_sha"],
                 "epoch": digest[:16]}
    else:
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
    sections = doc.get("sections", {})
    if sections:
        raise ValueError("Store-only batch must not contain category tasks")
    expected_folders = {sid + "-" + key + "-" + token
                        for sid, keys in sections.items() for key, token in keys.items()}
    actual_folders = {p.name for p in (base/"sections").iterdir()} if (base/"sections").exists() else set()
    if actual_folders != expected_folders:
        raise ValueError("Worker native section folders do not match manifest")
    t = time.monotonic()
    if hasattr(q,"finalize_batch"):
        count,section_count=q.finalize_batch(doc["owner"],doc["records"],sections,artifact_id,doc["name"])
    else:
        count = q.finish_batch(doc["owner"], doc["records"], artifact_id, doc["name"])
        section_count = q.finish_section_batch(doc["owner"], sections, artifact_id, doc["name"])
    print("FOODRADAR_BATCH_FINALIZED=" + json.dumps({"worker": index, "wave": wave,
          "stores": count, "sections": section_count, "artifact_id": artifact_id, "queue_commit_seconds": round(time.monotonic()-t, 2)}), flush=True)

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
