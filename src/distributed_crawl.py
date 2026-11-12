"""15 independent GitHub runners capture disjoint store partitions; runner 16 verifies and publishes.

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
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from feed_core import scan_single_point
from menu_core import canonical_store_uuid, global_limiter
from raw_capture import archive_and_upload, fetch_store_raw
from catalog_complete import is_recoverable_catalog_failure

ROOT = Path(__file__).resolve().parents[1]
TW = timezone(timedelta(hours=8))


def shard_for_uuid(store_uuid: str, count: int) -> int:
    if count < 1:
        raise ValueError("count must be >= 1")
    digest = hashlib.sha256(store_uuid.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % count


def public_ip() -> str | None:
    """Diagnostic only: GitHub does not guarantee unique egress IPs."""
    try:
        value = requests.get("https://api.ipify.org", timeout=8).text.strip()
        return str(ipaddress.ip_address(value))
    except (requests.RequestException, ValueError):
        return None


def discover(config: dict, raw: Path, index: int):
    point = {
        "id": "foodradar",
        "latitude": config["latitude"],
        "longitude": config["longitude"],
        "county": "台北市",
    }
    _, found, pages = scan_single_point(
        point, max_pages=config["max_pages"],
        raw_dir=raw / "feed" / f"shard-{index:02d}")
    if not found or not pages:
        raise RuntimeError("Feed discovery returned no complete store list")
    unique = {}
    for item in found:
        store_uuid = canonical_store_uuid(item["store_uuid"], item["store_url"])
        try:
            store_uuid = str(uuid.UUID(store_uuid))
        except (TypeError, ValueError) as exc:
            raise ValueError("Invalid canonical store UUID in feed") from exc
        unique.setdefault(store_uuid, {**item, "store_uuid": store_uuid})
    return unique, pages


def capture_shard(config: dict, index: int, count: int, root: Path = ROOT):
    if count != 15 or not 0 <= index < count:
        raise ValueError("Expected 15 shards with indices 0..14")
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
    discovered, pages = discover(config, raw, index)
    assigned = sorted(
        store_uuid for store_uuid in discovered
        if shard_for_uuid(store_uuid, count) == index)
    captured = []
    for completed, store_uuid in enumerate(assigned, 1):
        item = discovered[store_uuid]
        try:
            result = fetch_store_raw(item, raw)
        except Exception as exc:
            result = {
                "store_uuid": store_uuid, "store_url": item["store_url"],
                "success": False, "error": f"{type(exc).__name__}: {exc}",
            }
        captured.append(result)
        if completed % 10 == 0 or completed == len(assigned):
            print("FOODRADAR_SHARD_PROGRESS=" + json.dumps({
                "shard": index, "completed": completed, "assigned": len(assigned),
                "failed": sum(s.get("success") is not True for s in captured),
                "api": global_limiter.snapshot(),
            }, ensure_ascii=False), flush=True)
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
        "discovered_store_uuids": sorted(discovered),
        "assigned_store_uuids": assigned,
        "stores": captured,
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


def merge_shards(config: dict, input_dir: Path, count: int = 15,
                 root: Path = ROOT, upload_raw: bool = True):
    if count != 15:
        raise ValueError("Expected exactly 15 independent capture artifacts")
    metadata = []
    all_discovered = set()
    all_captured = {}
    run_ids, source_shas = set(), set()
    shard_paths = []
    total_responses = 0
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
        expected = sorted(s for s in found if shard_for_uuid(s, count) == index)
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
            if sid in all_captured or shard_for_uuid(sid, count) != index:
                raise ValueError(f"Duplicate or misrouted store: {sid}")
            all_captured[sid] = store
        all_discovered.update(found)
        metadata.append(m)
        shard_paths.append((shard_raw, sorted(actual_paths)))
        total_responses += len(actual_paths)
    if not all_discovered or set(all_captured) != all_discovered:
        missing = sorted(all_discovered - set(all_captured))
        unexpected = sorted(set(all_captured) - all_discovered)
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
        failures and len(stores) >= 50 and len(successes) >= 50
        and len(failures) <= max(1, int(len(stores) * 0.05))
        and all(is_recoverable_catalog_failure(s.get("error"))
                for s in failures))
    complete = not failures
    report = {
        "batch": batch, "runners": count, "feed_union": len(all_discovered),
        "captured_stores": len(stores), "verified_stores": len(successes),
        "failed_stores": len(failures), "raw_responses": total_responses,
        "runner_public_ips": ip_list, "unique_public_ips": ip_unique,
        "complete": complete, "partial_replayable": partial_replayable,
        "failed_store_details": [
            {"uuid": s["store_uuid"], "error": s.get("error")} for s in failures],
        "sum_api_requests": sum(m.get("api", {}).get("requests", 0) for m in metadata),
        "sum_429_events": sum(m.get("api", {}).get("rate_limit_events", 0) for m in metadata),
    }
    (output / "shard-reconciliation.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("FOODRADAR_15_RUNNER_RECONCILIATION=" +
          json.dumps(report, ensure_ascii=False), flush=True)
    if ip_unique < count:
        print("FOODRADAR_IP_DIAGNOSTIC=GitHub-hosted runners do not "
              "guarantee 15 distinct egress IPs", flush=True)
    if upload_raw:
        archive_and_upload(raw, config, batch, stores,
                           max(m["feed_pages"] for m in metadata),
                           complete, partial_replayable=partial_replayable)
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
    merge = sub.add_parser("merge")
    merge.add_argument("--input-dir", type=Path, default=ROOT / "data" / "parts")
    merge.add_argument("--count", type=int, default=15)
    merge.add_argument("--upload", action="store_true")
    args = parser.parse_args()
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    if args.command == "shard":
        capture_shard(cfg, args.index, args.count)
    else:
        merge_shards(cfg, args.input_dir, args.count, upload_raw=args.upload)


if __name__ == "__main__":
    main()
