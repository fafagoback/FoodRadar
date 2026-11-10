"""FoodRadar: capture original API response bodies, then upload Raw to HF.

Never convert menus, normalize price data, or build the serving database here.
The separate HF -> DB workflow owns all interpretation.
"""
import argparse
import json
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import monotonic
from datetime import datetime, timedelta, timezone
from pathlib import Path

from feed_core import scan_single_point
from menu_core import normalize_store_uuid, global_limiter
from raw_capture import fetch_store_raw, archive_and_upload

ROOT = Path(__file__).resolve().parents[1]


def distance_km(lat, lon, target_lat, target_lon):
    values = [float(v) for v in (lat, lon, target_lat, target_lon)]
    if not all(math.isfinite(v) for v in values):
        raise ValueError("non-finite coordinates")
    a, b, c, d = map(math.radians, values)
    h = math.sin((c-a)/2)**2 + math.cos(a)*math.cos(c)*math.sin((d-b)/2)**2
    return 6371.0088 * 2 * math.asin(min(1, math.sqrt(h)))


def crawl(config, upload_raw=False):
    if not 1 <= config["workers"] <= 5 or not 0 <= config["max_pages"] <= 100:
        raise ValueError("Invalid crawler worker or pagination configuration")
    batch = datetime.now(timezone(timedelta(hours=8))).strftime("%Y%m%d%H%M%S")
    output = ROOT / "data" / batch
    raw = output / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    point = dict(id="foodradar", latitude=config["latitude"],
                 longitude=config["longitude"], county="台北市")
    stores, pages = [], 0
    error = None
    try:
        _, discovered, pages = scan_single_point(
            point, max_pages=config["max_pages"], raw_dir=raw / "feed")
        if not discovered:
            raise RuntimeError("No stores returned by getFeedV1")
        # Deduplication is only used to avoid redundant HTTP requests.
        # The original feed pages are already saved byte-for-byte.
        unique = {}
        for discovered_store in discovered:
            key = normalize_store_uuid(
                discovered_store["store_uuid"], discovered_store["store_url"])
            unique.setdefault(key, discovered_store)
        # Report completed stores incrementally, without changing archive order
        # or treating partial captures as successful.
        items = list(unique.values())
        stores = [None] * len(items)
        started = monotonic()
        with ThreadPoolExecutor(max_workers=config["workers"]) as pool:
            pending = {
                pool.submit(fetch_store_raw, item, raw): index
                for index, item in enumerate(items)
            }
            failures_so_far = 0
            for completed, future in enumerate(as_completed(pending), start=1):
                index = pending[future]
                try:
                    stores[index] = future.result()
                except Exception as exc:
                    stores[index] = {
                        "store_uuid": items[index]["store_uuid"],
                        "store_url": items[index]["store_url"],
                        "success": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                if not stores[index].get("success"):
                    failures_so_far += 1
                if completed % 25 == 0 or completed == len(items):
                    duration = max(0.001, monotonic() - started)
                    print("RAW_CAPTURE_PROGRESS=" + json.dumps({
                        "batch": batch,
                        "completed_stores": completed,
                        "total_stores": len(items),
                        "failed_stores": failures_so_far,
                        "elapsed_seconds": round(duration, 1),
                        "stores_per_minute": round(completed * 60 / duration, 1),
                        "api": global_limiter.snapshot(),
                    }, ensure_ascii=False), flush=True)
        failures = [
            {"store_uuid": item.get("store_uuid"),
             "store_url": item.get("store_url"),
             "error": item.get("error", "Missing success flag")}
            for item in stores if not item.get("success")
        ]
        if failures:
            # Always print actionable diagnostics; the unmodified HTTP bodies
            # are separately archived, and an incomplete batch never publishes.
            print("RAW_STORE_FAILURES=" + json.dumps({
                "batch": batch, "discovered_stores": len(unique),
                "failed_count": len(failures), "failures": failures,
            }, ensure_ascii=False), flush=True)
            error = "One or more store API requests failed; original bodies preserved"
    except Exception as exc:
        error = str(exc)
    complete = bool(not error and pages and stores)
    if upload_raw and any(raw.rglob("*.body")):
        archive_and_upload(raw, config, batch, stores, pages, complete)
    # Keep final API pacing evidence even when a batch cannot be published.
    print("RAW_CAPTURE_API_METRICS=" + json.dumps(
        global_limiter.snapshot(), ensure_ascii=False), flush=True)
    if error:
        raise RuntimeError(error)
    if not complete:
        raise RuntimeError("No complete Raw snapshot; database is unchanged")
    print("RAW_CAPTURE_COMPLETE=" + json.dumps(
        {"batch": batch, "pages": pages, "stores": len(stores)},
        ensure_ascii=False), flush=True)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--check-config", action="store_true")
    args = parser.parse_args()
    config = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    if args.check_config:
        print(json.dumps(config, ensure_ascii=False, indent=2))
    else:
        crawl(config, upload_raw=args.upload)
