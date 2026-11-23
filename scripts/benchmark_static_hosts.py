#!/usr/bin/env python3
"""One-off FoodRadar host A/B: copy EXACT live Pages assets to HF and measure browser fetch.

Do not change web/ or production HF StaticCatalog. Artifacts are written only
under StaticCatalogBenchmark/<release>/ in the HF dataset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests
from huggingface_hub import CommitOperationAdd, HfApi

sys.path.insert(0, str(Path(__file__).resolve().parent))
from prepare_pages import referenced_blobs

PAGES_ROOT = "https://fafagoback.github.io/FoodRadar/"
PAGES_DATA = PAGES_ROOT + "data/"
HF_REPO = "hub-google/FoodRadarHostBenchmark"
HF_RESOLVE = f"https://huggingface.co/datasets/{HF_REPO}/resolve/main/"
HF_PREFIX = "StaticCatalogBenchmark"
STATE = threading.local()


def get(url: str) -> bytes:
    if not hasattr(STATE, "session"):
        STATE.session = requests.Session()
        STATE.session.headers.update({"User-Agent": "FoodRadar-static-host-benchmark/1.0"})
    last_error = None
    for attempt in range(5):
        try:
            r = STATE.session.get(url, timeout=(15, 90))
            r.raise_for_status()
            return r.content
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 12))
    raise RuntimeError(f"Download failed: {url}: {last_error}")


def publish(stage: Path, token: str, source_dir: Path | None = None) -> dict:
    if not token:
        raise ValueError("HF_TOKEN GitHub secret is required")
    stage.mkdir(parents=True, exist_ok=True)
    manifest_path = source_dir / "data" / "manifest.json" if source_dir else None
    manifest_bytes = manifest_path.read_bytes() if manifest_path else get(PAGES_DATA + "manifest.json")
    manifest = json.loads(manifest_bytes)
    if manifest.get("schema") != "foodradar-static-v2":
        raise ValueError(f"Live Pages must be V2; got {manifest.get('schema')}")
    hashes = sorted(referenced_blobs(manifest))
    geo_hashes = sorted({digest for table in ("products", "stores")
        for blobs_list in (manifest.get("geo") or {}).get(table, {}).values()
        for digest in blobs_list})
    release = str(manifest["batch"]) + "-" + str(manifest["data_revision"])[:12]
    prefix = f"{HF_PREFIX}/{release}"
    blobs = stage / "blobs"
    blobs.mkdir(exist_ok=True)
    (stage / "manifest.json").write_bytes(manifest_bytes)

    def one(digest: str) -> tuple[str, int]:
        archive_blob = source_dir / "data" / "blobs" / (digest + ".zst") if source_dir else None
        payload = archive_blob.read_bytes() if archive_blob else get(PAGES_DATA + "blobs/" + digest + ".zst")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != digest:
            raise ValueError(f"Pages SHA mismatch: {digest} != {actual}")
        path = blobs / (digest + ".zst")
        path.write_bytes(payload)
        return (digest, len(payload))

    with ThreadPoolExecutor(max_workers=14) as pool:
        sizes = dict(pool.map(one, hashes))
    total = sum(sizes.values())
    if total != manifest.get("bytes"):
        raise ValueError(f"Pages snapshot size mismatch: {total} vs {manifest.get('bytes')}")
    print("PAGES_SOURCE_VERIFIED=" + json.dumps(
        {"release": release, "blobs": len(hashes), "bytes": total}, ensure_ascii=False), flush=True)

    # The GitHub Pages delivery contains 17,427 individually addressable files.
    # Hugging Face rejects directories over 10,000 files, so distribute the
    # IDENTICAL bytes by the first hex digit of the existing SHA256 filename.
    # Upload the full folder in one high-level operation. HF handles
    # server-side batching and retries; never manually commit each shard.
    mirror = stage / "hf-mirror"
    mirror.mkdir(exist_ok=True)
    (mirror / "manifest.json").write_bytes(manifest_bytes)
    for digest in hashes:
        target = mirror / "blobs" / digest[0] / (digest + ".zst")
        target.parent.mkdir(parents=True, exist_ok=True)
        source = blobs / (digest + ".zst")
        try:
            os.link(source, target)
        except OSError:
            import shutil
            shutil.copy2(source, target)
    api = HfApi(token=token)
    api.create_repo(repo_id=HF_REPO, repo_type="dataset", private=False, exist_ok=True)
    print("HF_FOLDER_UPLOAD_START=" + json.dumps(
        {"repo": HF_REPO, "files": len(hashes) + 1, "bytes": total,
         "folder": prefix}, ensure_ascii=False), flush=True)
    api.upload_folder(
        repo_id=HF_REPO,
        repo_type="dataset",
        folder_path=str(mirror),
        path_in_repo=prefix,
        commit_message=f"Mirror exact FoodRadar V2 static assets {release} for host A/B",
        ignore_patterns=[".cache/**", ".git/**"],
    )
    print("HF_FOLDER_UPLOAD_COMPLETE=" + prefix, flush=True)
    hf_data = HF_RESOLVE + prefix + "/"
    hf_manifest = get(hf_data + "manifest.json")
    if hf_manifest != manifest_bytes:
        raise ValueError("HF published manifest bytes do not match GitHub Pages")
    sample = [hashes[0], hashes[len(hashes)//2], hashes[-1]]
    for h in sample:
        if hashlib.sha256(get(hf_data + "blobs/" + h[0] + "/" + h + ".zst")).hexdigest() != h:
            raise ValueError("HF published blob failed checksum: " + h)
    result = {"release": release, "data_revision": manifest["data_revision"],
              "bytes": total, "blobs": len(hashes), "hf_base": hf_data,
              "pages_base": PAGES_DATA, "files": sizes,
              "geo_hashes": geo_hashes}
    (stage / "published.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("HF_COPY_VERIFIED=" + json.dumps(
        {k: v for k, v in result.items() if k != "files"}, ensure_ascii=False), flush=True)
    return result


FETCH_JS = """async ({url}) => {
  const t0 = performance.now();
  try {
    const r = await fetch(url, {cache: 'no-store'});
    const buffer = await r.arrayBuffer();
    return {ok: r.ok, status: r.status, bytes: buffer.byteLength,
            ms: Math.round((performance.now() - t0) * 10) / 10,
            type: r.headers.get('content-type'), error: r.ok ? null : 'HTTP ' + r.status};
  } catch (e) {
    return {ok: false, status: 0, bytes: 0,
            ms: Math.round((performance.now() - t0) * 10) / 10,
            error: String(e)};
  }
}"""


def measure(published: dict, out: Path) -> dict:
    from playwright.sync_api import sync_playwright

    sizes = published["files"]
    sorted_files = sorted(sizes, key=lambda h: sizes[h])
    indexes = [0, 1, 2, len(sorted_files)//4, len(sorted_files)//2,
               (3*len(sorted_files))//4, len(sorted_files)-5,
               len(sorted_files)-4, len(sorted_files)-3,
               len(sorted_files)-2, len(sorted_files)-1]
    selected = set(sorted_files[i] for i in indexes)
    # Benchmark actual nearby-index shards, not only the old global search blobs.
    geo_hashes = published.get("geo_hashes") or []
    if geo_hashes:
        geo_sorted = sorted(geo_hashes, key=lambda h:sizes[h])
        geo_indexes = [0, len(geo_sorted)//4, len(geo_sorted)//2,
                       3*len(geo_sorted)//4, len(geo_sorted)-1]
        selected.update(geo_sorted[i] for i in geo_indexes)
    selected = sorted(selected)
    paths = ["manifest.json"] + ["blobs/" + h + ".zst" for h in selected]
    hosts = {"pages": published["pages_base"], "hf": published["hf_base"]}

    results = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = browser.new_page()
        page.goto(PAGES_ROOT, wait_until="domcontentloaded", timeout=90_000)
        for round_number in range(3):
            order = list(paths)
            random.Random(20261123 + round_number).shuffle(order)
            # Alternate host order to reduce the effect of temporary network swings.
            host_order = ["pages", "hf"] if round_number % 2 == 0 else ["hf", "pages"]
            for path in order:
                for host in host_order:
                    actual_path = ("blobs/" + path[6] + "/" + path[6:] if host == "hf" and path.startswith("blobs/") else path)
                    url = hosts[host] + actual_path + f"?abtest={round_number}-{time.time_ns()}"
                    measurement = page.evaluate(FETCH_JS, {"url": url})
                    expected = len(get(PAGES_DATA + "manifest.json")) if path == "manifest.json" else sizes[path[6:-4]]
                    if measurement["ok"] and measurement["bytes"] != expected:
                        measurement["ok"] = False
                        measurement["error"] = "Downloaded byte count differs from source"
                    results.append({"round": round_number+1, "host": host, "path": path,
                                    "is_geo": path.startswith("blobs/") and path[6:-4] in geo_hashes,
                                    "expected_bytes": expected, **measurement})
                    print("HOST_AB_SAMPLE=" + json.dumps(results[-1], ensure_ascii=False), flush=True)
        browser.close()

    summary = {}
    for host in hosts:
        rows = [r for r in results if r["host"] == host]
        successes = [r for r in rows if r["ok"]]
        failures = [r for r in rows if not r["ok"]]
        geo_successes = [r for r in successes if r["is_geo"]]
        summary[host] = {
            "success": len(successes), "failed": len(failures),
            "geo_success": len(geo_successes),
            "geo_median_ms": round(statistics.median([r["ms"] for r in geo_successes]), 1) if geo_successes else None,
            "median_ms": round(statistics.median([r["ms"] for r in successes]), 1) if successes else None,
            "mean_ms": round(statistics.mean([r["ms"] for r in successes]), 1) if successes else None,
            "median_kbps": round(statistics.median(
                [r["bytes"] * 1000 / (r["ms"] or 1) / 1024 for r in successes]), 1) if successes else None,
            "errors": [r["error"] for r in failures[:5]],
        }
    paired = []
    for path in paths:
        pr = [r["ms"] for r in results if r["host"] == "pages" and r["path"] == path and r["ok"]]
        hr = [r["ms"] for r in results if r["host"] == "hf" and r["path"] == path and r["ok"]]
        if len(pr) == 3 and len(hr) == 3:
            paired.append({"path": path, "bytes": (len(get(PAGES_DATA + "manifest.json"))
                          if path == "manifest.json" else sizes[path[6:-4]]),
                           "pages_median_ms": round(statistics.median(pr), 1),
                           "hf_median_ms": round(statistics.median(hr), 1)})
    report = {
        "measured_utc": datetime.now(timezone.utc).isoformat(),
        "method": "Real Chromium fetch from GitHub Actions runner, Pages page origin; 3 cold no-store rounds",
        "release": published["release"],
        "source_bytes": published["bytes"],
        "source_blobs": published["blobs"],
        "locations": hosts, "summary": summary, "paired": paired,
        "raw": results,
    }
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("HOST_AB_SUMMARY=" + json.dumps(
        {"release": report["release"], "summary": summary, "paired": paired},
        ensure_ascii=False), flush=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", type=Path, default=Path("hf-host-benchmark"))
    parser.add_argument("--mode", choices=("publish", "benchmark"), required=True)
    parser.add_argument("--source", type=Path, default=None, help="Extracted immutable Pages deployment site root")
    args = parser.parse_args()
    if args.mode == "publish":
        publish(args.stage, os.getenv("HF_TOKEN", ""), args.source)
    else:
        published = json.loads((args.stage / "published.json").read_text(encoding="utf-8"))
        measure(published, args.stage / "results.json")


if __name__ == "__main__":
    main()
