"""Fixed fifteen persistent workers sharing one run-scoped store task pool."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path

from adaptive_threads import AdaptiveThreads, DEFAULT_THREADS, MAX_THREADS, valid_threads
from crawl_queue import SharedGitQueue, HEARTBEAT_SECONDS, initial_state, artifact_name, validate_batch_group
from crawl_transport import TransportPool
from distributed_crawl import ROOT, load_shared_discovery, public_ip, _validate_references
from menu_core import global_limiter
from promo_api_recheck import verify_suspicious_store
from raw_capture import fetch_store_raw, CheckpointStorageError
from catalog_broker import CatalogBroker, validate_group
from artifact_budget import ArtifactBudget

HELPER = ROOT / ".github/actions/fr-dynamic-crawl/artifact.cjs"


def runtime_queue():
    return SharedGitQueue(ROOT, os.environ["GITHUB_RUN_ID"],
                    int(os.environ.get("GITHUB_RUN_ATTEMPT", "1")))


def shared_identity(path):
    raw = path.read_bytes()
    shared = json.loads(raw)
    digest = hashlib.sha256(raw).hexdigest()
    if (shared.get("assignment_mode") != "dynamic"
            or shared.get("owner_count") != 15 or shared.get("ownership") is not None
            or shared.get("run_id") != os.environ.get("GITHUB_RUN_ID")
            or shared.get("source_sha") != os.environ.get("GITHUB_SHA")):
        raise ValueError("Invalid dynamic discovery identity")
    ids = sorted(s["store_uuid"] for s in shared["stores"])
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("Invalid discovered roster")
    return shared, digest, ids


def verify_queue(state, shared, digest, ids):
    if (state["run_id"] != shared["run_id"] or state["source_sha"] != shared["source_sha"]
            or state["epoch"] != digest[:16] or state["tasks"] != ids):
        raise ValueError("Queue and original discovery disagree")


def artifact(*args):
    proc = subprocess.run(["node", str(HELPER), *map(str, args)],
                          text=True, capture_output=True, timeout=180)
    if proc.returncode:
        raise RuntimeError("Actions artifact operation failed: " + proc.stderr[-1000:])
    for line in reversed(proc.stdout.splitlines()):
        if line.startswith("FR_ARTIFACT_RESULT="):
            return json.loads(line.split("=", 1)[1])
    raise RuntimeError("Missing artifact operation result")


def body_checksums(raw):
    return {p.relative_to(raw).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(raw.rglob("*.body"))}


def validate_task(folder, state, sid, lease):
    folder = Path(folder)
    doc = json.loads((folder / "task.json").read_text(encoding="utf8"))
    for key, expected in (("run_id", state["run_id"]), ("source_sha", state["source_sha"]),
                          ("epoch", state["epoch"]), ("store_uuid", sid),
                          ("token", lease["token"]), ("worker", lease["worker"])):
        if doc.get(key) != expected:
            raise ValueError("Task artifact identity mismatch: " + key)
    raw = folder / "raw"
    checks = doc["responses"]
    for name in checks:
        p = Path(name)
        if p.is_absolute() or ".." in p.parts or p.suffix != ".body":
            raise ValueError("Unsafe task response path")
    if checks != body_checksums(raw):
        raise ValueError("Task HTTP body checksum mismatch")
    result = doc["store"]
    if result.get("store_uuid") != sid:
        raise ValueError("Task result has wrong store UUID")
    if result.get("catalog_deferred"):
        if (result.get("catalog_complete") is not False
                or result.get("catalog_responses")
                or result.get("response_path") not in checks):
            raise ValueError("Invalid deferred native catalog")
    else:
        _validate_references(result, set(checks))
    recheck = result.get("promo_recheck") or {}
    if recheck.get("status") == "MATCHED":
        paths = [recheck.get("second_response_path")] + recheck.get("second_catalog_response_paths", [])
        if any(p not in checks for p in paths):
            raise ValueError("Missing independent promotion evidence")
    return doc


def restore_worker_learning(storage,state,index,wave):
    if wave==0:return None
    for previous in range(wave-1,-1,-1):
        name=f"fr-worker-{state['epoch']}-{index}-w{previous:02d}"
        with tempfile.TemporaryDirectory() as folder:
            found=storage("recover",name,folder)
            if found is None:continue
            doc=json.loads((Path(folder)/"summary.json").read_text())
            for k,v in (("run_id",state["run_id"]),("source_sha",state["source_sha"]),
                        ("epoch",state["epoch"]),("shard_index",index),("wave",previous)):
                if doc.get(k)!=v:raise ValueError("Saved thread learning identity mismatch")
            return doc["thread_learning"]
    return None


def capture_worker(config, index, discovery_file, queue=None, storage=artifact,
                   root=ROOT, capture=fetch_store_raw, verify=verify_suspicious_store):
    shared, digest, ids = shared_identity(discovery_file)
    queue = queue or runtime_queue()
    state = queue.read()
    verify_queue(state, shared, digest, ids)
    base = root / "data" / ("dynamic-worker-" + str(index))
    base.mkdir(parents=True, exist_ok=False)
    discovered, _, _, owners = load_shared_discovery(discovery_file, config, base / "feed-check", -1)
    if owners is not None:
        raise ValueError("Preassigned stores are forbidden in dynamic worker")
    baseline = shared.get("previous_promotion_stats") or {}
    owner = f"{index}-{uuid.uuid4().hex}"
    initial = int(os.getenv("FOODRADAR_INITIAL_THREADS", str(DEFAULT_THREADS)))
    if not valid_threads(initial):
        raise ValueError("Invalid initial threads")
    ctrl = AdaptiveThreads(initial)
    ctrl.reset(time.monotonic(), global_limiter.snapshot())
    wave=int(os.getenv("FR_CRAWL_WAVE","0"))
    prior=restore_worker_learning(storage,state,index,wave)
    if prior is not None:
        ctrl.resume(prior,time.monotonic(),global_limiter.snapshot())
        print("FOODRADAR_THREADS_RESUMED="+json.dumps({"worker":index,"wave":wave,
            "target":ctrl.target,"optimal_threads":ctrl.best}),flush=True)
    transport = TransportPool()
    storage=ArtifactBudget(storage,int(os.getenv('FOODRADAR_ARTIFACT_BUDGET','450')))
    broker = (CatalogBroker(queue,state,discovered,owner,index,storage,transport.session)
              if capture is fetch_store_raw else None)
    stop = threading.Event()
    heartbeat_errors = []
    def heartbeat():
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                queue.renew(owner)
            except Exception as exc:
                heartbeat_errors.append(exc)
                stop.set()
    pulse = threading.Thread(target=heartbeat, daemon=True)
    pulse.start()
    captured_count = 0
    def one(sid, lease):
        folder = base / (sid + "-" + lease["token"])
        raw = folder / "raw"
        raw.mkdir(parents=True)
        item = discovered[sid]
        started = time.monotonic()
        try:
            if broker is not None:
                result = broker.resume(sid, raw, lease.get("attempt", 1))
                if result is None:
                    result = capture(item, raw, session_factory=transport.session,
                                     catalog_runner=broker.dispatch)
            else:
                result = capture(item, raw, session_factory=transport.session)
        except CheckpointStorageError:
            raise
        except Exception as exc:
            result = {"store_uuid": sid, "store_url": item["store_url"], "success": False,
                      "error": type(exc).__name__ + ": " + str(exc)}
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
        result["crawl_elapsed_ms"] = max(1, round((time.monotonic() - started) * 1000))
        checks = body_checksums(raw)
        result["crawl_api_requests"] = len(checks)
        doc = {"run_id": state["run_id"], "source_sha": state["source_sha"],
               "epoch": state["epoch"], "store_uuid": sid, "token": lease["token"],
               "worker": index, "store": result, "responses": checks}
        (folder / "task.json").write_text(json.dumps(doc, ensure_ascii=False), encoding="utf8")
        validate_task(folder, state, sid, lease)
        name = artifact_name(state, sid, lease)
        artifact_id = storage("upload", name, folder)["id"]
        if type(artifact_id) is not int or artifact_id <= 0:
            raise RuntimeError("Invalid saved result ID")
        return sid, lease, artifact_id, folder
    def recover(name):
        # Read and verify the already-persisted result before calling the task
        # completed. The artifact service distinguishes not found from failure.
        with tempfile.TemporaryDirectory() as temp:
            saved = storage("recover", name, temp)
            if saved is None:
                return None
            latest = queue.read()
            matches = [(sid, l) for sid, l in latest["leased"].items()
                       if artifact_name(latest, sid, l) == name]
            if not matches:
                # Another worker has already fenced/reconciled this result.
                return saved["id"]
            sid, lease = matches[0]
            validate_task(Path(temp), latest, sid, lease)
            return saved["id"]
    active = {}
    next_poll = 0.0
    has_pending = True
    try:
        with ThreadPoolExecutor(max_workers=MAX_THREADS) as pool:
            while True:
                if heartbeat_errors:
                    raise RuntimeError("Lease heartbeat failed") from heartbeat_errors[0]
                now = time.monotonic()
                snap = global_limiter.snapshot()
                if snap["rate_limit_events"] > ctrl.baseline["rate_limit_events"]:
                    ctrl.observe(now, snap, False)
                free = ctrl.target - len(active)
                if storage.exhausted and not active:
                    break
                if not storage.exhausted and free > 0 and snap["cooldown_remaining_seconds"] <= 0 and now >= next_poll:
                    claims = queue.claim(owner, index, free)
                    for sid, lease in claims:
                        active[pool.submit(one, sid, lease)] = (sid, lease)
                    has_pending = len(claims) == free
                    if not claims:
                        current = queue.read()
                        has_pending = bool(current["pending"])
                        if broker is not None:
                            helper = broker.claim_any(current)
                            if helper is not None:
                                active[pool.submit(broker.run_partition,helper)] = None
                                has_pending = True
                                next_poll = 0
                                continue
                        if not current["pending"] and not current["leased"] and not active:
                            break
                        if not current["pending"] and any(
                            v["expires"] <= time.time() for v in current["leased"].values()):
                            queue.recover_expired(recover)
                        next_poll = time.monotonic() + random.uniform(2, 5)
                    else:
                        next_poll = 0
                if active:
                    done, _ = wait(active, timeout=.25, return_when=FIRST_COMPLETED)
                    ctrl.observe(time.monotonic(), global_limiter.snapshot(),
                                 len(active) >= ctrl.target and has_pending)
                    for future in done:
                        task = active.pop(future)
                        try:
                            result=future.result()
                        except CheckpointStorageError:
                            if storage.exhausted:
                                # Only durable results are accepted. Unfinished leases
                                # are expired after all capture threads have joined.
                                continue
                            raise
                        if task is None:
                            next_poll = 0
                            continue
                        sid, lease, artifact_id, folder = result
                        if not queue.finish(sid, lease["token"], artifact_id):
                            # A stale worker never overwrites a recovered task.
                            print("FOODRADAR_STALE_TASK_RESULT_IGNORED=" + sid, flush=True)
                        else:
                            captured_count += 1
                        shutil.rmtree(folder)
                        next_poll = 0
                else:
                    time.sleep(.25)
    finally:
        stop.set()
        pulse.join(timeout=45)
        transport.close()
        if storage.exhausted:
            queue.expire_owner(owner)
    summary = {"run_id": state["run_id"], "source_sha": state["source_sha"],
               "epoch": state["epoch"], "shard_index": index, "public_ip": public_ip(),
               "thread_learning": ctrl.report(), "api": global_limiter.snapshot(),
               "completed": captured_count, "wave":int(os.getenv("FR_CRAWL_WAVE","0")),
               "artifact_uploads":storage.used,"budget_exhausted":storage.exhausted,
               "catalog_helpers": broker.metrics if broker else {}}
    output = root / "data" / ("worker-summary-" + str(index))
    output.mkdir(parents=True, exist_ok=True)
    (output / "summary.json").write_text(json.dumps(summary), encoding="utf8")
    print("FOODRADAR_DYNAMIC_WORKER=" + json.dumps(summary), flush=True)
    return summary


def worker_reports(summary_dir,state,index):
    base=f"fr-worker-{state['epoch']}-{index}"
    paths=[summary_dir/base/"summary.json"]+sorted(summary_dir.glob(base+"-w[0-9][0-9]/summary.json"))
    reports=[]
    for path in paths:
        if not path.exists():continue
        summary=json.loads(path.read_text())
        if any(summary.get(k)!=v for k,v in (("run_id",state["run_id"]),("source_sha",state["source_sha"]),
                ("epoch",state["epoch"]),("shard_index",index))):
            raise ValueError("Wrong worker summary")
        reports.append(summary)
    if not reports:return {}
    reports.sort(key=lambda r:r.get("wave",0))
    if len({r.get("wave",0) for r in reports})!=len(reports):
        raise ValueError("Duplicate logical worker wave")
    final=dict(reports[-1])
    learning=[r.get("thread_learning") or {} for r in reports]
    # A later 429 may have invalidated earlier learning.
    chosen=learning[-1]
    final["thread_learning"]={**chosen,"samples":sum((r.get("samples",[]) for r in learning),[])}
    api=dict(final.get("api") or {})
    for k in ("requests","successes","rate_limit_events"):
        api[k]=sum(r.get("api",{}).get(k,0) for r in reports)
    final["api"]=api
    final["wave_count"]=len(reports)
    final["artifact_uploads"]=sum(r.get("artifact_uploads",0) for r in reports)
    final["timing"]={k:round(sum(r.get("timing",{}).get(k,0) for r in reports),2)
                     for k in ("worker_seconds","claim_seconds","idle_queue_seconds",
                               "local_commit_seconds","last_task_finish_seconds","barrier_wait_seconds")}
    final["worker_ips"]=list(dict.fromkeys(r["public_ip"] for r in reports if r.get("public_ip")))
    return final


def assemble(config, discovery_file, task_dir, summary_dir, output, queue=None):
    shared, digest, ids = shared_identity(discovery_file)
    state = (queue or runtime_queue()).read()
    verify_queue(state, shared, digest, ids)
    if state["pending"] or state["leased"] or state.get("local_done") or set(state["completed"]) != set(ids):
        raise RuntimeError("Task pool incomplete; no HF/DB update")
    for sid, group in state.get("catalog_groups", {}).items():
        validate_batch_group(sid, group)
        if (group["pending"] or group["leased"] or group["local_done"]
                or len(group["completed"]) != len(group["sections"])):
            raise RuntimeError("Cross-machine category pool incomplete; no HF/DB update")
    owners = {sid: state["completed"][sid]["worker"] for sid in ids}
    by_worker = [[] for _ in range(15)]
    checks_by_worker = [{} for _ in range(15)]
    for index in range(15):
        (output / f"foodradar-raw-shard-{index}" / "raw").mkdir(parents=True, exist_ok=False)
    raw_zero = output / "foodradar-raw-shard-0" / "raw"
    load_shared_discovery(discovery_file, config, raw_zero, 0)
    checks_by_worker[0].update(body_checksums(raw_zero))
    for sid in ids:
        lease = state["completed"][sid]
        name = artifact_name(state, sid, lease)
        if lease.get("batch_name"):
            batch = lease["batch_name"]
            if not __import__("re").fullmatch(r"fr-worker-[0-9a-f]{16}-\d+-w\d{2}", batch):
                raise ValueError("Invalid accepted batch name")
            folder = summary_dir / batch / "tasks" / name
        else:
            folder = task_dir / name
        doc = validate_task(folder, state, sid, lease)
        worker = lease["worker"]
        raw = output / f"foodradar-raw-shard-{worker}" / "raw"
        for relative, checksum in doc["responses"].items():
            if relative in checks_by_worker[worker]:
                raise ValueError("Duplicate raw response path in task pool")
            dest = raw / relative
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(folder / "raw" / relative, dest)
            checks_by_worker[worker][relative] = checksum
        store_result = doc["store"]
        group = state.get("catalog_groups", {}).get(sid)
        if store_result.get("catalog_deferred"):
            if group is None:
                raise ValueError("Deferred store has no section task group")
            pages, reports, failures = [], [], []
            total_requests = store_result.get("crawl_api_requests", 0)
            elapsed = store_result.get("crawl_elapsed_ms", 0)
            helpers = set()
            for key in map(str, range(len(group["sections"]))):
                row = group["completed"][key]
                batch = row.get("batch_name")
                if not __import__("re").fullmatch(
                        r"fr-worker-[0-9a-f]{16}-[0-9]+-w[0-9]{2}", batch or ""):
                    raise ValueError("Invalid category result artifact")
                source_dir = (summary_dir / batch / "sections" /
                              (sid + "-" + key + "-" + row["token"]))
                section_doc = json.loads((source_dir/"section.json").read_text(encoding="utf8"))
                for field, value in (("run_id",state["run_id"]),
                        ("source_sha",state["source_sha"]),("epoch",state["epoch"]),
                        ("store_uuid",sid),("worker",row["worker"]),
                        ("token",row["token"]),("section_index",int(key)),
                        ("section_uuid",group["sections"][int(key)])):
                    if section_doc.get(field) != value:
                        raise ValueError("Wrong category provenance: " + field)
                checks = section_doc["responses"]
                if checks != body_checksums(source_dir/"raw"):
                    raise ValueError("Native category original HTTP SHA256 mismatch")
                sec = section_doc["result"]
                if sec.get("store_uuid") != sid:
                    raise ValueError("Category result belongs to another store")
                prefix = "cross-sections/" + sid + "/" + key + "/"
                for relative, checksum in checks.items():
                    dest_rel = prefix + relative
                    if dest_rel in checks_by_worker[worker]:
                        raise ValueError("Duplicate category response path")
                    dest = raw / dest_rel
                    dest.parent.mkdir(parents=True,exist_ok=True)
                    shutil.copyfile(source_dir/"raw"/relative, dest)
                    checks_by_worker[worker][dest_rel] = checksum
                helpers.add(row["worker"])
                total_requests += len(checks)
                elapsed += sec.get("crawl_elapsed_ms", 0)
                if sec.get("success") is not True or sec.get("catalog_complete") is not True:
                    failures.append(str(sec.get("error") or "Unverified native category"))
                    continue
                sec_reports = sec.get("catalog_section_reports") or []
                if (len(sec_reports) != 1 or
                        sec_reports[0].get("section_uuid") != group["sections"][int(key)] or
                        sec_reports[0].get("terminal_has_more") is not False):
                    raise ValueError("Native category terminal pagination unverified")
                for page in sec.get("catalog_responses", []):
                    if page["path"] not in checks:
                        raise ValueError("Missing original category HTTP response")
                    pages.append({**page, "path":prefix + page["path"]})
                reports.extend(sec_reports)
            store_result["crawl_api_requests"] = total_requests
            store_result["crawl_elapsed_ms"] = max(1, elapsed)
            store_result["catalog_worker_indices"] = sorted(helpers)
            if failures:
                store_result["success"] = False
                store_result["catalog_complete"] = False
                store_result["error"] = "; ".join(failures)
            else:
                if len(reports) != len(group["sections"]):
                    raise ValueError("Incomplete native catalog section roster")
                store_result["catalog_responses"] = pages
                store_result["catalog_section_reports"] = reports
                store_result["catalog_sections_finished"] = len(reports)
                store_result["catalog_complete"] = True
                store_result["catalog_item_occurrences"] = sum(p["item_count"] for p in pages)
                store_result["catalog_reused_partitions"] = 0
                store_result["catalog_independent_inventory_verified"] = False
                store_result["catalog_unique_skus"] = 0
            print("FOODRADAR_CROSS_WORKER_MERGE=" + json.dumps({
                "store_uuid":sid, "sections":len(group["sections"]),
                "workers":sorted(helpers), "pages":len(pages),
                "failed":len(failures)}),flush=True)
        elif group is not None:
            raise ValueError("Category task group without deferred store")
        by_worker[worker].append(store_result)
    for index in range(15):
        summary=worker_reports(summary_dir,state,index)
        stores = sorted(by_worker[index], key=lambda s: s["store_uuid"])
        if not summary:
            print("FOODRADAR_WORKER_METRICS_UNAVAILABLE=" + str(index), flush=True)
        meta = {"format": "foodradar-shard-v1", "run_id": state["run_id"],
                "source_sha": state["source_sha"], "shard_index": index, "shard_count": 15,
                "public_ip": summary.get("public_ip"), "feed_pages": shared["feed_pages"],
                "shared_discovery_digest": digest, "shared_owners": owners,
                "discovered_store_uuids": ids,
                "assigned_store_uuids": [s["store_uuid"] for s in stores],
                "stores": stores, "thread_learning": summary.get("thread_learning", {}),
                "api": summary.get("api", {}), "responses": checks_by_worker[index],
                "worker_waves":summary.get("wave_count",0),"worker_ips":summary.get("worker_ips",[]),
                 "artifact_uploads":summary.get("artifact_uploads",0),"timing":summary.get("timing",{})}
        (output / f"foodradar-raw-shard-{index}" / "shard.json").write_text(
            json.dumps(meta, ensure_ascii=False), encoding="utf8")
    print("FOODRADAR_DYNAMIC_POOL_COMPLETE=" + json.dumps({
        "stores": len(ids), "workers": 15, "per_worker": [len(x) for x in by_worker],
        "metrics_missing":sum(not worker_reports(summary_dir,state,i) for i in range(15))}),flush=True)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("command", choices=["init", "worker", "status", "assemble", "cleanup"])
    p.add_argument("--discovery-file", type=Path, default=ROOT / "data/discovery/discovery.json")
    p.add_argument("--index", type=int, default=0)
    p.add_argument("--task-dir", type=Path, default=ROOT / "data/tasks")
    p.add_argument("--summary-dir", type=Path, default=ROOT / "data/worker-summaries")
    p.add_argument("--output", type=Path, default=ROOT / "data/parts")
    args = p.parse_args()
    config = json.loads((ROOT / "config.json").read_text())
    q = runtime_queue()
    shared, digest, ids = shared_identity(args.discovery_file)
    if args.command == "init":
        q.initialize(initial_state(shared["run_id"], digest[:16], shared["source_sha"], ids,
                                   weights=shared.get("workload_weights") or {}))
    elif args.command == "status":
        state = q.read()
        verify_queue(state, shared, digest, ids)
        groups=state.get("catalog_groups",{}).values()
        print(json.dumps({"remaining": len(state["pending"]) + len(state["leased"]) + len(state.get("local_done", {})),
                          "completed": len(state["completed"]),
                          "sections_remaining":sum(len(g["pending"])+len(g["leased"]) for g in groups),
                          "sections_completed":sum(len(g["completed"]) for g in groups)}))
    elif args.command == "worker":
        capture_worker(config, args.index, args.discovery_file, queue=q)
    elif args.command == "cleanup":
        verify_queue(q.read(), shared, digest, ids)
        q.cleanup()
    else:
        assemble(config, args.discovery_file, args.task_dir, args.summary_dir, args.output, queue=q)


if __name__ == "__main__":
    main()
