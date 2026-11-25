"""GitHub schedules disjoint microbatches with at most 15 concurrent runners; a single merger verifies and publishes.

The original Feed, Store and Catalog HTTP bodies are not normalized or modified.
Each runner discovers the *same* single configured coordinate, then deterministically
owns only store UUIDs assigned to its shard. The merger checks the union of all
feed discoveries against exactly-once store assignments before uploading Raw to HF.
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import os
import shutil
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from feed_core import scan_single_point, extract_stores_from_feed
from discovery_coverage import get_points,validate_points,merge_store_points,COUNTIES,clean,discovery_scope
from menu_core import canonical_store_uuid, global_limiter
from raw_capture import archive_and_upload, fetch_store_raw
from catalog_complete import is_recoverable_catalog_failure
from promo_api_recheck import verify_suspicious_store
from adaptive_threads import run_adaptive, valid_threads, DEFAULT_THREADS, average_best
from crawl_batches import plan_batches, MAX_BATCHES, MAX_PARALLEL
from crawl_transport import TransportPool

ROOT = Path(__file__).resolve().parents[1]
TW = timezone(timedelta(hours=8))


def shard_for_uuid(store_uuid: str, count: int) -> int:
    if count < 1:
        raise ValueError("count must be >= 1")
    digest = hashlib.sha256(store_uuid.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % count


def weighted_owners(store_ids, count, historical_weights):
    """Deterministic LPT scheduling from previous native catalog page counts.

    Each UUID has exactly one owner. Without reliable history the existing
    SHA-256 ownership is preserved; no guesses based on names or categories.
    """
    ids = sorted(store_ids)
    if not historical_weights:
        return {sid: shard_for_uuid(sid, count) for sid in ids}
    loads = [0] * count
    owners = {}
    for sid in sorted(ids, key=lambda x: (-historical_weights.get(x, 400), x)):
        owner = min(range(count), key=lambda index: (loads[index], index))
        owners[sid] = owner
        loads[owner] += historical_weights.get(sid, 400)
    return owners


def workload_weight(store):
    """Milliseconds of estimated work, including independent promotion recheck."""
    measured = store.get("crawl_elapsed_ms")
    if type(measured) is int and measured > 0:
        return max(100, measured)
    pages = len(store.get("catalog_responses") or [])
    sections = int(store.get("catalog_section_count") or 0)
    recheck = store.get("promo_recheck") or {}
    second = int(recheck.get("second_catalog_pages") or 0)
    if recheck.get("status") and not second:
        second = pages
    return max(400, 400 * (1 + pages + second) + 100 * sections)


def latest_raw_workload_weights(config):
    """Prefer newest complete OR partial observation; never download Raw bodies."""
    if not os.environ.get("HF_TOKEN"):
        return {}
    from huggingface_hub import hf_hub_download
    candidates = []
    for pointer in ("latest.json", "latest-partial.json"):
        try:
            latest = Path(hf_hub_download(
                config["hf_repo_id"], config["hf_folder"].strip("/") + "/" + pointer,
                repo_type="dataset", token=os.environ["HF_TOKEN"]))
            manifest = json.loads(latest.read_text(encoding="utf-8"))
            if manifest.get("format") == "foodradar-immutable-http-body-v1":
                candidates.append(manifest)
        except Exception as exc:
            print("FOODRADAR_LOAD_POINTER_UNAVAILABLE=" + json.dumps({
                "pointer": pointer, "error_type": type(exc).__name__}), flush=True)
    weights = {}
    for manifest in sorted(candidates, key=lambda m: str(m.get("batch_id") or ""), reverse=True):
        for store in manifest.get("stores") or []:
            try:
                sid = canonical_store_uuid(store["store_uuid"], store["store_url"])
                if sid in weights:
                    continue
                if store.get("success") is True or store.get("crawl_elapsed_ms"):
                    weights[sid] = workload_weight(store)
            except (KeyError, ValueError, TypeError):
                continue
    print("FOODRADAR_PREVIOUS_LOAD_WEIGHTS=" + json.dumps({
        "stores": len(weights), "source_batches": sorted(
            str(m.get("batch_id") or "") for m in candidates)}), flush=True)
    return weights


def latest_raw_promotion_baseline(config):
    """Load tiny per-store promotion counts from the last immutable HF manifests.

    Never fetch full raw archives or a live shop just to obtain a baseline.
    Missing HF data disables historic comparison, not the whole crawler.
    """
    if not os.environ.get("HF_TOKEN"):
        return {}
    try:
        from huggingface_hub import hf_hub_download
        repo = config["hf_repo_id"]
        folder = config["hf_folder"].strip("/")
        candidates = []
        for pointer in ("latest.json", "latest-partial.json"):
            try:
                blob = Path(hf_hub_download(
                    repo, f"{folder}/{pointer}", repo_type="dataset",
                    token=os.environ["HF_TOKEN"]))
                manifest = json.loads(blob.read_text(encoding="utf8"))
                if manifest.get("format") == "foodradar-immutable-http-body-v1":
                    candidates.append(manifest)
            except Exception as exc:
                # A missing/corrupt optional pointer is not evidence of a
                # wrong product price and must never abort normal crawling.
                print('FOODRADAR_PROMO_BASELINE_POINTER_UNAVAILABLE=' + json.dumps({
                    'pointer': pointer, 'error_type': type(exc).__name__,
                }), flush=True)
                continue
        baseline = {}
        for manifest in sorted(candidates, key=lambda m: str(m.get("batch_id") or ""), reverse=True):
            batch = str(manifest.get("batch_id") or "")
            for store in manifest.get("stores") or []:
                if store.get("success") is not True:
                    continue
                sid = canonical_store_uuid(store["store_uuid"], store["store_url"])
                if sid in baseline:
                    continue
                recheck = store.get("promo_recheck") or {}
                if recheck.get("status") in ("CONFLICT", "UNAVAILABLE"):
                    # This observation was not released as a valid price.
                    # Never allow its counts to become the next trusted baseline.
                    prior_stats = recheck.get("previous_stats")
                    trusted = dict(prior_stats) if isinstance(prior_stats, dict) else {}
                    trusted["_recheck_required"] = True
                    baseline[sid] = {
                        "batch": str(recheck.get("previous_batch") or batch),
                        "stats": trusted,
                        "promotion_watch": (
                            recheck.get("previous_watch")
                            if isinstance(recheck.get("previous_watch"), dict)
                            else {}
                        ),
                    }
                    continue
                stats = store.get("promotion_stats")
                if not isinstance(stats, dict) or "total_skus" not in stats:
                    continue
                baseline[sid] = {
                    "batch": batch, "stats": stats,
                    "promotion_watch": (
                        store.get("promotion_watch")
                        if isinstance(store.get("promotion_watch"), dict)
                        else {}
                    ),
                }
        print("FOODRADAR_PREVIOUS_PROMOTION_BASELINE=" + json.dumps({
            "stores": len(baseline), "manifests": len(candidates)
        }), flush=True)
        return baseline
    except Exception as exc:
        print("FOODRADAR_PROMOTION_BASELINE_UNAVAILABLE=" + json.dumps({
            "reason": str(exc)[:160]
        }), flush=True)
        return {}


def public_ip() -> str | None:
    """Diagnostic only: GitHub does not guarantee unique egress IPs."""
    try:
        value = requests.get("https://api.ipify.org", timeout=8).text.strip()
        return str(ipaddress.ip_address(value))
    except (requests.RequestException, ValueError):
        return None


def discover(config: dict, raw: Path, index: int, coverage=None):
    plan=get_points(config)
    unique={};total_pages=0;reports=[]
    for point in plan:
        point_id,found,pages=scan_single_point(point,max_pages=config["max_pages"],
            raw_dir=raw/"feed"/f"shard-{index:02d}"/point["id"])
        if point_id!=point["id"] or not pages:
            raise RuntimeError("Incomplete feed discovery: "+point["id"])
        total_pages+=pages
        reports.append({"point_id":point_id,"pages":pages,"stores":len(found)})
        for item in found:
            store_uuid=canonical_store_uuid(item["store_uuid"],item["store_url"])
            try:store_uuid=str(uuid.UUID(store_uuid))
            except (TypeError,ValueError) as exc:raise ValueError("Invalid canonical store UUID in feed") from exc
            if store_uuid not in unique:
                unique[store_uuid]={**item,"store_uuid":store_uuid}
            else:merge_store_points(unique[store_uuid],item)
        print("FOODRADAR_POINT_COMPLETE="+json.dumps(reports[-1]),flush=True)
    if not unique or not total_pages:raise RuntimeError("Feed discovery returned no complete store list")
    if coverage is not None:coverage.update({"scope":discovery_scope(config),
           "plan":plan,"reports":reports})
    return unique,total_pages



def save_shared_discovery(config: dict, root: Path = ROOT, dynamic=False):
    """Discover once at the configured coordinate; all 15 crawl workers share this file."""
    folder = root / "data" / "discovery"
    if folder.exists():
        raise FileExistsError(f"Refusing to overwrite existing discovery: {folder}")
    raw = folder / "raw"
    raw.mkdir(parents=True)
    coverage={}
    stores, pages = discover(config, raw, index=0, coverage=coverage)
    bodies = {
        p.relative_to(raw).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(raw.rglob("*.body"))
    }
    previous_promotions = latest_raw_promotion_baseline(config)
    if dynamic:
        ownership, batch_count, estimates = None, 15, {}
        print("FOODRADAR_DYNAMIC_TASK_POOL=" + json.dumps({"workers": 15, "stores": len(stores)}), flush=True)
    else:
        weights = latest_raw_workload_weights(config)
        ownership, batch_count, estimates = plan_batches(stores, weights)
        loads = [0] * batch_count
        for sid, owner in ownership.items():
            loads[owner] += estimates[sid]
        print("FOODRADAR_WEIGHTED_SHARDS=" + json.dumps({
            "method": "fixed_15_weighted_workers", "max_parallel": MAX_PARALLEL, "batches": batch_count,
            "estimated_loads": loads, "stores": len(stores),
        }), flush=True)
    manifest = {
        "ownership": ownership, "owner_count": batch_count,
        "assignment_mode": "dynamic" if dynamic else "legacy-fixed",
        "workload_weights": estimates,
        "format": "foodradar-shared-feed-v1",
        "run_id": os.getenv("GITHUB_RUN_ID", "local"),
        "source_sha": os.getenv("GITHUB_SHA", "local"),
        "latitude": config["latitude"], "longitude": config["longitude"],
        "feed_pages": pages, "stores": [stores[sid] for sid in sorted(stores)],
        "previous_promotion_stats": previous_promotions,
        "raw_checksums": bodies, "discovery_coverage":coverage,
    }
    path = folder / "discovery.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf8")
    print("FOODRADAR_SHARED_FEED=" + json.dumps({
        "shops": len(stores), "feed_pages": pages, "raw_bodies": len(bodies),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }, ensure_ascii=False), flush=True)
    return manifest


def load_shared_discovery(path: Path, config: dict, raw: Path, shard: int):
    """Validate identical, checksum-verified original feed data in each worker."""
    manifest_bytes = path.read_bytes()
    manifest = json.loads(manifest_bytes)
    if (manifest.get("format") != "foodradar-shared-feed-v1"
            or manifest.get("run_id") != os.getenv("GITHUB_RUN_ID", "local")
            or manifest.get("source_sha") != os.getenv("GITHUB_SHA", "local")
            or manifest.get("latitude") != config["latitude"]
            or manifest.get("longitude") != config["longitude"]
            or not isinstance(manifest.get("stores"), list)
            or not manifest["stores"]
            or not isinstance(manifest.get("feed_pages"), int)
            or manifest["feed_pages"] < 1):
        raise ValueError("Shared feed manifest missing, incomplete or from another run")
    coverage=manifest.get("discovery_coverage")
    scope=discovery_scope(config)
    if coverage is None and scope!="single":raise ValueError("Missing multi-point coverage")
    if coverage is not None:
        points=validate_points(coverage["plan"])
        reports=coverage["reports"]
        if (coverage["scope"]!=scope or [r["point_id"] for r in reports]!=[p["id"] for p in points]
                or any(type(r["pages"]) is not int or r["pages"]<1 for r in reports)
                or sum(r["pages"] for r in reports)!=manifest["feed_pages"]):
            raise ValueError("Discovery plan did not finish all points")
        if scope=="taiwan" and ({clean(p["county"]) for p in points}!=COUNTIES
                  or len({(p["county"],p["town"]) for p in points})!=368):
            raise ValueError("National discovery plan is incomplete")
        if scope!="taiwan" and points!=get_points(config):raise ValueError("Discovery plan changed")
    feed_root = path.parent / "raw"
    checksums = manifest.get("raw_checksums")
    existing = {
        p.relative_to(feed_root).as_posix()
        for p in feed_root.rglob("*.body")
    }
    if not isinstance(checksums, dict) or not checksums or set(checksums) != existing:
        raise ValueError("Shared feed HTTP body paths do not match manifest")
    for name, checksum in checksums.items():
        p = feed_root / name
        if (len(checksum) != 64
                or hashlib.sha256(p.read_bytes()).hexdigest() != checksum):
            raise ValueError(f"Shared feed HTTP body checksum mismatch: {name}")
    stores = {}
    for store in manifest["stores"]:
        sid = canonical_store_uuid(store["store_uuid"], store["store_url"])
        if sid in stores or store["store_uuid"] != sid:
            raise ValueError("Duplicated or invalid canonical store in shared feed")
        stores[sid] = store
    ownership = manifest.get("ownership")
    if ownership is not None and (
            not isinstance(ownership, dict)
            or type(manifest.get("owner_count")) is not int
            or not 1 <= manifest["owner_count"] <= MAX_BATCHES
            or set(ownership) != set(stores)
            or any(type(index) is not int or not 0 <= index < manifest["owner_count"]
                   for index in ownership.values())):
        raise ValueError("Shared feed ownership map is missing or invalid")
    if coverage is not None:
        expected={p['id'] for p in points}
        if any(not s.get('discovered_points') or any(p['point_id'] not in expected for p in s['discovered_points']) for s in stores.values()):
            raise ValueError('Discovered store lacks original coordinate')
    if shard == 0:
        destination = raw / "feed"
        if destination.exists():
            raise FileExistsError("Discovery HTTP Raw already exists in shard zero")
        shutil.copytree(feed_root / "feed", destination)
    return stores, manifest["feed_pages"], hashlib.sha256(manifest_bytes).hexdigest(), ownership


def capture_shard(config: dict, index: int, count: int, root: Path = ROOT,
                  discovery_file: Path | None = None):
    if not 1 <= count <= MAX_BATCHES or not 0 <= index < count:
        raise ValueError("Invalid matrix batch count or index")
    if not 0 <= config["max_pages"] <= 100:
        raise ValueError("Invalid max_pages")
    path = root / "data" / f"shard-{index}"
    if path.exists():
        raise FileExistsError(f"Refusing to overwrite existing shard: {path}")
    raw = path / "raw"
    raw.mkdir(parents=True)
    ip = public_ip()
    print("FOODRADAR_SHARD_RUNNER=" + json.dumps({
        "shard": index, "count": count, "public_ip": ip,
        "github_run_id": os.getenv("GITHUB_RUN_ID", "local"),
    }), flush=True)
    if discovery_file is None:
        discovered, pages = discover(config, raw, index)
        discovery_digest, owners = None, None
    else:
        discovered, pages, discovery_digest, owners = load_shared_discovery(
            discovery_file, config, raw, index)
    assigned = sorted(
        sid for sid in discovered
        if (owners[sid] if owners is not None else shard_for_uuid(sid, count)) == index)
    baseline = {}
    if discovery_file is not None:
        shared = json.loads(discovery_file.read_text(encoding="utf8"))
        if owners is not None and shared.get("owner_count") != count:
            raise ValueError("Matrix count differs from verified discovery")
        weights = shared.get("workload_weights") or {}
        assigned.sort(key=lambda sid: (-weights.get(sid, 400), sid))
        baseline = shared.get("previous_promotion_stats") or {}
        if not isinstance(baseline, dict):
            raise ValueError("Invalid previous promotion baseline in shared discovery")
    captured = []
    transport = TransportPool()
    def capture_one(store_uuid):
        item = discovered[store_uuid]
        started = time.monotonic()
        try:
            result = fetch_store_raw(item, raw, session_factory=transport.session)
        except Exception as exc:
            result = {
                "store_uuid": store_uuid, "store_url": item["store_url"],
                "success": False, "error": f"{type(exc).__name__}: {exc}",
            }
        if result.get("success") is True and not result.get("inactive"):
            prior = baseline.get(store_uuid) or {}
            try:
                result = verify_suspicious_store(
                    item, raw, result, prior.get("stats"), prior.get("batch", ""),
                    previous_watch=prior.get("promotion_watch"))
            except Exception as exc:
                # A new diagnostic must never rewrite a successfully captured
                # HTTP observation into a claim that the crawler failed.
                result["promotion_stats_error"] = type(exc).__name__ + ": " + str(exc)[:180]
                # A broken verifier must NOT silently publish an unverified
                # first observation as a confirmed historical price.
                result["promo_recheck"] = {
                    "status": "UNAVAILABLE", "trigger": "verifier_exception",
                    "error": result["promotion_stats_error"],
                }
                print("FOODRADAR_PROMO_DIAGNOSTIC_UNAVAILABLE=" + json.dumps({
                    "store_uuid": store_uuid, "reason": result["promotion_stats_error"]
                }), flush=True)
        result["crawl_elapsed_ms"] = max(1, round((time.monotonic() - started) * 1000))
        # Process-wide request counters cannot be attributed to one thread.
        result["crawl_api_requests"] = (len(list((raw / "stores").glob(f"{store_uuid}-*.body")))
                                        + len(list((raw / "catalog" / store_uuid).rglob("*.body"))))
        return result

    def completed_one(result):
        captured.append(result)
        completed = len(captured)
        if completed % 10 == 0 or completed == len(assigned):
            print("FOODRADAR_SHARD_PROGRESS=" + json.dumps({
                "shard": index, "completed": completed, "assigned": len(assigned),
                "failed": sum(s.get("success") is not True for s in captured),
                "api": global_limiter.snapshot(),
            }, ensure_ascii=False), flush=True)
    initial = int(os.getenv("FOODRADAR_INITIAL_THREADS", str(DEFAULT_THREADS)))
    if not valid_threads(initial):
        raise ValueError("Invalid FOODRADAR_INITIAL_THREADS")
    try:
        thread_learning = run_adaptive(assigned, capture_one, global_limiter, initial, completed_one)
    finally:
        transport.close()
    captured.sort(key=lambda row: row["store_uuid"])
    print("FOODRADAR_THREAD_LEARNING=" + json.dumps(thread_learning), flush=True)
    checksums = {
        p.relative_to(raw).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(raw.rglob("*.body"))
    }
    metadata = {
        "format": "foodradar-shard-v1",
        "run_id": os.getenv("GITHUB_RUN_ID", "local"),
        "source_sha": os.getenv("GITHUB_SHA", "local"),
        "shard_index": index,
        "shard_count": count,
        "public_ip": ip,
        "feed_pages": pages,
        "shared_discovery_digest": discovery_digest,
        "shared_owners": owners,
        "discovered_store_uuids": sorted(discovered),
        "assigned_store_uuids": sorted(assigned),
        "stores": captured,
        "thread_learning": thread_learning,
        "responses": checksums,
        "api": global_limiter.snapshot(),
    }
    (path / "shard.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    print("FOODRADAR_SHARD_COMPLETE=" + json.dumps({
        "index": index, "discovered": len(discovered), "assigned": len(assigned),
        "success": sum(s.get("success") is True for s in captured),
        "failed": sum(s.get("success") is not True for s in captured),
        "responses": len(checksums),
        "public_ip": ip,
    }), flush=True)
    # A store-level failure is recorded in the artifact. Runner 16 decides
    # whether a limited, explicitly verified partial replay is safe.
    return metadata


def _validate_references(store: dict, paths: set[str]):
    if store.get("success") is not True:
        return
    response = store.get("response_path")
    if not isinstance(response, str) or response not in paths:
        raise ValueError(f"Missing original getStoreV1 body for {store.get('store_uuid')}")
    if store.get("inactive"):
        return
    if store.get("catalog_complete") is not True:
        raise ValueError(f"Store catalog not verified: {store.get('store_uuid')}")
    pages = store.get("catalog_responses") or []
    if not isinstance(pages, list):
        raise ValueError("Catalog page list is invalid")
    if any(not isinstance(p, dict) or p.get("path") not in paths for p in pages):
        raise ValueError(f"Missing original catalog page for {store.get('store_uuid')}")
    if store.get("catalog_section_count") != store.get("catalog_sections_finished"):
        raise ValueError(f"Missing catalog section for {store.get('store_uuid')}")



def _recover_missing_store_captures(missing, shard_paths, config, root, run_id):
    """Recover verified feed-discovery ownership gaps with real HTTP snapshots.

    Uber's getFeedV1 often gives different lists to concurrent workers.
    Load those immutable feed response bodies (not a guessed store list),
    find UUIDs discovered by another worker, and request their real menus.
    """
    wanted = set(missing)
    items = {}
    for raw_root, paths in shard_paths:
        for rel in paths:
            if not rel.startswith("feed/") or not rel.endswith(".body"):
                continue
            try:
                envelope = json.loads((raw_root / rel).read_bytes())
                native = envelope.get("data") if isinstance(envelope, dict) else None
                feed = native.get("feedItems") if isinstance(native, dict) else None
                if not isinstance(feed, list):
                    continue  # Earlier error attempt: use successful feed page.
                for store in extract_stores_from_feed(feed):
                    sid = canonical_store_uuid(
                        store.get("store_uuid", ""), store.get("store_url", ""))
                    if sid in wanted and sid not in items:
                        items[sid] = {
                            **store, "store_uuid": sid,
                            "discovered_points": [{
                                "point_id": "foodradar",
                                "lat": config["latitude"],
                                "lon": config["longitude"],
                                "county": "台北市",
                            }],
                        }
            except (ValueError, TypeError, KeyError, OSError) as exc:
                raise ValueError(f"Unparseable original feed HTTP body: {rel}") from exc
    unverified = wanted - set(items)
    if unverified:
        raise RuntimeError(
            f"Missing {len(unverified)} store identities in original feed Raw: "
            f"{sorted(unverified)[:12]}; refusing synthetic recovery")
    raw = root / "data" / f"owner-gap-{run_id}" / "raw"
    if raw.exists():
        raise FileExistsError(f"Refusing to overwrite owner gap capture {raw}")
    raw.mkdir(parents=True)
    results = {}
    for index, sid in enumerate(sorted(items), 1):
        item = items[sid]
        try:
            result = fetch_store_raw(item, raw)
        except Exception as exc:
            result = {"store_uuid": sid, "store_url": item["store_url"],
                      "success": False, "error": f"{type(exc).__name__}: {exc}"}
        results[sid] = result
        if index % 10 == 0 or index == len(items):
            print("FOODRADAR_OWNER_GAP_PROGRESS=" + json.dumps({
                "processed": index, "total": len(items),
                "successful": sum(r.get("success") is True for r in results.values()),
                "failed": sum(r.get("success") is not True for r in results.values()),
            }, ensure_ascii=False), flush=True)
    paths = sorted(p.relative_to(raw).as_posix() for p in raw.rglob("*.body"))
    available = set(paths)
    for result in results.values():
        _validate_references(result, available)
    return results, (raw, paths)


def merge_shards(config: dict, input_dir: Path, count: int = 15,
                 root: Path = ROOT, upload_raw: bool = True,
                 repair_missing: bool = False, strict: bool = False,
                 discovery_file: Path | None = None):
    if not 1 <= count <= MAX_BATCHES:
        raise ValueError("Invalid matrix batch count")
    metadata = []
    all_discovered = set()
    all_captured = {}
    run_ids, source_shas = set(), set()
    shard_paths = []
    total_responses = 0
    shared_digests = set()
    shared_rosters = []
    shared_ownership_maps = []
    for index in range(count):
        folder = input_dir / f"foodradar-raw-shard-{index}"
        manifest_path = folder / "shard.json"
        if not manifest_path.is_file():
            raise ValueError(f"Missing shard {index} manifest; refusing HF/DB update")
        m = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (m.get("format") != "foodradar-shard-v1"
                or m.get("shard_index") != index or m.get("shard_count") != count
                or not isinstance(m.get("feed_pages"), int)
                or m["feed_pages"] <= 0):
            raise ValueError(f"Invalid shard {index} metadata")
        run_ids.add(m.get("run_id"))
        source_shas.add(m.get("source_sha"))
        shared_digests.add(m.get("shared_discovery_digest"))
        if len(run_ids) != 1 or len(source_shas) != 1:
            raise ValueError("Shards belong to different runs or code revisions")
        found = m.get("discovered_store_uuids")
        assigned = m.get("assigned_store_uuids")
        stores = m.get("stores")
        checksums = m.get("responses")
        if (not isinstance(found, list) or not isinstance(assigned, list)
                or not isinstance(stores, list) or not isinstance(checksums, dict)
                or len(set(found)) != len(found) or len(set(assigned)) != len(assigned)):
            raise ValueError(f"Corrupt shard {index} assignment manifest")
        shared_rosters.append(found)
        owners = m.get("shared_owners")
        shared_ownership_maps.append(owners)
        if owners is not None and (not isinstance(owners, dict)
                or set(owners) != set(found)
                or any(type(v) is not int or not 0 <= v < count
                       for v in owners.values())):
            raise ValueError(f"Shard {index} invalid shared ownership map")
        expected = sorted(s for s in found
                          if (owners[s] if owners is not None
                              else shard_for_uuid(s, count)) == index)
        if expected != assigned:
            raise ValueError(f"Shard {index} did not follow deterministic store ownership")
        if (len(stores) != len(assigned) or
                {s.get("store_uuid") for s in stores} != set(assigned)):
            raise ValueError(f"Shard {index} has missing or duplicate store results")
        shard_raw = folder / "raw"
        actual_paths = {p.relative_to(shard_raw).as_posix()
                        for p in shard_raw.rglob("*.body")}
        if actual_paths != set(checksums):
            raise ValueError(f"Shard {index} original response list mismatch")
        for relative, digest in checksums.items():
            relative_path = Path(relative)
            if (relative_path.is_absolute() or ".." in relative_path.parts or
                    len(digest) != 64):
                raise ValueError(f"Unsafe response path in shard {index}")
            if hashlib.sha256((shard_raw / relative).read_bytes()).hexdigest() != digest:
                raise ValueError(f"Shard {index} original HTTP body checksum mismatch")
        for store in stores:
            _validate_references(store, actual_paths)
            sid = store["store_uuid"]
            if sid in all_captured or (owners[sid] if owners is not None
                                       else shard_for_uuid(sid, count)) != index:
                raise ValueError(f"Duplicate or misrouted store: {sid}")
            all_captured[sid] = store
        all_discovered.update(found)
        metadata.append(m)
        shard_paths.append((shard_raw, sorted(actual_paths)))
        total_responses += len(actual_paths)
    if any(x is not None for x in shared_digests):
        if (len(shared_digests) != 1
                or any(x != shared_rosters[0] for x in shared_rosters)
                or any(x != shared_ownership_maps[0] for x in shared_ownership_maps)):
            raise ValueError("Matrix workers used different discovery lists or checksums")
    if discovery_file is not None:
        expected_digest = hashlib.sha256(discovery_file.read_bytes()).hexdigest()
        if shared_digests != {expected_digest}:
            raise ValueError("Worker results do not match current discovery digest")
        expected = json.loads(discovery_file.read_bytes())
        if (run_ids != {expected.get("run_id")} or source_shas != {expected.get("source_sha")}
                or {s["store_uuid"] for s in expected["stores"]} != all_discovered
                or expected["owner_count"] != count
                or (expected.get("assignment_mode") != "dynamic"
                    and shared_ownership_maps[0] != expected["ownership"])
                or (expected.get("assignment_mode") == "dynamic"
                    and (expected.get("ownership") is not None
                         or not isinstance(shared_ownership_maps[0], dict)
                         or set(shared_ownership_maps[0]) != all_discovered))):
            raise ValueError("Worker results do not match current discovery assignment")
    owner_missing = sorted(all_discovered - set(all_captured))
    unexpected = sorted(set(all_captured) - all_discovered)
    recovered = {}
    if repair_missing and owner_missing and not unexpected:
        print("FOODRADAR_OWNER_GAP_PLAN=" + json.dumps({
            "missing": len(owner_missing),
            "evidence": "original_getFeedV1_saved_bodies",
        }), flush=True)
        recovered, extra_raw = _recover_missing_store_captures(
            owner_missing, shard_paths, config, root, next(iter(run_ids)))
        all_captured.update(recovered)
        shard_paths.append(extra_raw)
        total_responses += len(extra_raw[1])
    if not all_discovered or set(all_captured) != all_discovered:
        missing = sorted(all_discovered - set(all_captured))
        raise RuntimeError(
            f"Cross-runner feed coverage mismatch; missing={missing[:20]}, "
            f"unexpected={unexpected[:20]}; HF/DB unchanged")
    batch = datetime.now(TW).strftime("%Y%m%d%H%M%S")
    output = root / "data" / batch
    if output.exists():
        raise FileExistsError("Refusing to overwrite an existing Raw capture batch")
    raw = output / "raw"
    raw.mkdir(parents=True)
    copied = set()
    for shard_raw, paths in shard_paths:
        for relative in paths:
            if relative in copied:
                raise RuntimeError(f"Duplicate original HTTP response: {relative}")
            copied.add(relative)
            target = raw / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(shard_raw / relative, target)
    if len(copied) != total_responses:
        raise RuntimeError("Lost HTTP response body during shard merge")
    ip_list = [m.get("public_ip") for m in metadata]
    ip_unique = len(set(ip for ip in ip_list if ip))
    stores = [all_captured[sid] for sid in sorted(all_captured)]
    failures = [s for s in stores if s.get("success") is not True]
    successes = [s for s in stores if s.get("success") is True]
    partial_replayable = bool(
        not strict and failures and len(stores) >= 50 and len(successes) >= 50
        and len(failures) <= max(1, int(len(stores) * 0.05))
        and all(is_recoverable_catalog_failure(s.get("error"))
                for s in failures))
    complete = not failures
    report = {
        "batch": batch, "runners": count, "max_parallel_runners": min(count, MAX_PARALLEL),
        "learned_initial_threads": average_best([m.get("thread_learning") or {} for m in metadata]),
        "learning_samples": sum((m.get("thread_learning") or {}).get("optimal_threads") is not None for m in metadata),
        "feed_union": len(all_discovered),
        "captured_stores": len(stores), "verified_stores": len(successes),
        "owner_discovery_gap": len(owner_missing),
        "owner_gap_recaptured": len(recovered),
        "owner_gap_successful": sum(s.get("success") is True for s in recovered.values()),
        "failed_stores": len(failures), "raw_responses": total_responses,
        "runner_public_ips": ip_list, "unique_public_ips": ip_unique,
        "complete": complete, "partial_replayable": partial_replayable,
        "failed_store_details": [
            {"uuid": s["store_uuid"], "error": s.get("error")} for s in failures],
        "sum_api_requests": sum(m.get("api", {}).get("requests", 0) for m in metadata),
        "sum_429_events": sum(m.get("api", {}).get("rate_limit_events", 0) for m in metadata),
        "worker_artifact_uploads": sum(m.get("artifact_uploads",0) for m in metadata),
        "worker_timing": [{"worker":m["shard_index"]+1,"stores":len(m["stores"]),
                           "requests":m.get("api",{}).get("requests",0),
                           "uploads":m.get("artifact_uploads",0),**m.get("timing",{})} for m in metadata],
        "promo_api_rechecks": {
            state: sum((s.get("promo_recheck") or {}).get("status") == state for s in stores)
            for state in ("MATCHED", "CONFLICT", "UNAVAILABLE")
        },
    }
    (output / "shard-reconciliation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("FOODRADAR_15_RUNNER_RECONCILIATION=" +
          json.dumps(report, ensure_ascii=False), flush=True)
    if ip_unique < count:
        print("FOODRADAR_IP_DIAGNOSTIC=GitHub-hosted runners do not "
              "guarantee 15 distinct egress IPs", flush=True)
    if upload_raw:
        started = time.monotonic()
        archive_and_upload(raw, config, batch, stores,
                           max(m["feed_pages"] for m in metadata),
                           complete, partial_replayable=partial_replayable)
        report["hf_archive_upload_verify_seconds"] = round(time.monotonic() - started, 2)
        print("FOODRADAR_HF_TOTAL_SECONDS=" + str(report["hf_archive_upload_verify_seconds"]), flush=True)
        (output / "shard-reconciliation.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if failures and not partial_replayable:
        raise RuntimeError(
            f"{len(failures)} stores have unverified captures; "
            "Raw archived but HF release pointer and Turso remain unchanged")
    return report


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    worker = sub.add_parser("shard")
    worker.add_argument("--index", type=int, required=True)
    worker.add_argument("--count", type=int, default=15)
    worker.add_argument("--discovery-file", type=Path, default=None)
    discovery = sub.add_parser("discover")
    discovery.add_argument("--dynamic", action="store_true")
    merge = sub.add_parser("merge")
    merge.add_argument("--input-dir", type=Path, default=ROOT / "data" / "parts")
    merge.add_argument("--count", type=int, default=15)
    merge.add_argument("--upload", action="store_true")
    merge.add_argument("--repair-missing", action="store_true")
    merge.add_argument("--strict", action="store_true")
    merge.add_argument("--discovery-file", type=Path, default=None)
    args = parser.parse_args()
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    if args.command == "discover":
        save_shared_discovery(cfg, dynamic=args.dynamic)
    elif args.command == "shard":
        capture_shard(cfg, args.index, args.count, discovery_file=args.discovery_file)
    else:
        merge_shards(cfg, args.input_dir, args.count, upload_raw=args.upload,
                     repair_missing=args.repair_missing, strict=args.strict,
                     discovery_file=args.discovery_file)


if __name__ == "__main__":
    main()
