"""Assemble GitHub Pages from the last verified HF static pointer, never Turso."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from huggingface_hub import hf_hub_download


# The HF source is a verified V1 snapshot; convert it at deploy-time so old
# published data remains usable without fetching an all-products index in browsers.
# This conversion runs on GitHub Actions, never on a visitor's device.
from collections import defaultdict
import zstandard as zstd
from build_static_catalog import terms
from build_geo_index import augment_geo_index

INDEX_CHUNK_ROWS = 1024
BROWSE_CHUNK_ROWS = 8192
STORE_CHUNK_ROWS = 256


def _paged_manifest(manifest, data):
    files = dict(manifest["files"])
    blobs = data / "blobs"
    decompressor = zstd.ZstdDecompressor()
    compressor = zstd.ZstdCompressor(level=10, write_content_size=True)

    def read_hash(digest):
        return json.loads(decompressor.decompress((blobs/(digest+".zst")).read_bytes()))

    def write_hash(value):
        raw = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        compressed = compressor.compress(raw)
        digest = hashlib.sha256(compressed).hexdigest()
        target = blobs/(digest+".zst")
        if not target.exists():
            target.write_bytes(compressed)
        return digest

    def chunks(values, size):
        return [write_hash(values[i:i+size]) for i in range(0, len(values), size)]

    # V1 catalog/store blobs are read here only by the Actions runner.
    # Product card and verified history hashes remain untouched.
    minis = read_hash(files["catalog"])
    stores = read_hash(files["stores"])
    if len(minis) != manifest["counts"]["products"] or len(stores) != manifest["counts"]["stores"]:
        raise ValueError("Static source counts differ from catalog")
    lookup = defaultdict(dict)
    def bucket(value, count):
        digest=hashlib.sha256(str(value).encode("utf-8")).digest()
        return int.from_bytes(digest[:2],"big") % count if count>256 else digest[0] % count
    for idx, r in enumerate(minis):
        identity=str(r[0])+"\x1f"+str(r[1])
        lookup[bucket(identity, 4096)][identity] = idx

    def sort_key(r, sort, idx):
        if sort == "discount_desc": return (-float(r[4] or 0), -float(r[5] or 0), idx)
        if sort == "savings_desc": return (-float(r[5] or 0), -float(r[4] or 0), idx)
        if sort == "price_asc": return (float(r[6] or 0), -float(r[7] or 0), idx)
        if sort == "price_desc": return (-float(r[6] or 0), -float(r[7] or 0), idx)
        if sort == "promo_first": return (-int(r[9] or 0), -float(r[7] or 0), idx)
        if sort == "name_asc": return (str(r[16] or "").casefold(), idx)
        if sort == "new_desc": return (-int(r[8] or 0), -float(r[7] or 0), idx)
        return (-float(r[7] or 0), float(r[6] or 0), idx)

    groups = {
        "global": [i for i in range(len(minis))],
        "discounts": [i for i,r in enumerate(minis) if r[10]],
        "discounts30": [i for i,r in enumerate(minis) if r[10] and float(r[4] or 0) >= 30],
        "discounts20": [i for i,r in enumerate(minis) if r[10] and float(r[4] or 0) >= 20],
        "discounts40": [i for i,r in enumerate(minis) if r[10] and float(r[4] or 0) >= 40],
        "discounts50": [i for i,r in enumerate(minis) if r[10] and float(r[4] or 0) >= 50],
        "newProducts": [i for i,r in enumerate(minis) if r[8]],
        "promos": [i for i,r in enumerate(minis) if r[9]],
        "promosPlatform": [i for i,r in enumerate(minis) if r[9] and r[11]],
    }
    orders = {
        "global": ("rating_desc", "promo_first", "price_asc", "price_desc", "name_asc"),
        "discounts": ("discount_desc", "savings_desc", "price_asc", "price_desc"),
        "discounts30": ("discount_desc", "savings_desc", "price_asc", "price_desc"),
        "discounts20": ("discount_desc", "savings_desc", "price_asc", "price_desc"),
        "discounts40": ("discount_desc", "savings_desc", "price_asc", "price_desc"),
        "discounts50": ("discount_desc", "savings_desc", "price_asc", "price_desc"),
        "newProducts": ("new_desc",),
        "promos": ("price_asc", "price_desc", "rating_desc", "name_asc"),
        "promosPlatform": ("price_asc", "price_desc", "rating_desc", "name_asc"),
    }
    browse = {}
    for group, sorts in orders.items():
        for order in sorts:
            ordered = sorted(groups[group], key=lambda i:sort_key(minis[i], order, i))
            key = group+":"+order
            browse[key] = {"total":len(ordered), "chunks":chunks(ordered,BROWSE_CHUNK_ROWS)}
            # Compact per-city rank/select indexes reuse the global sort.
            # Stored positions are delta-encoded, never duplicate full product IDs.
            by_city = defaultdict(list)
            for position, product_id in enumerate(ordered):
                by_city[str(minis[product_id][2] or "")].append(position)
            for city, positions in by_city.items():
                if not city:
                    continue
                pieces = []
                for pos in range(0,len(positions),BROWSE_CHUNK_ROWS):
                    values = positions[pos:pos+BROWSE_CHUNK_ROWS]
                    deltas = [values[0]] + [
                        values[i]-values[i-1] for i in range(1,len(values))]
                    pieces.append(write_hash(deltas))
                browse[key+"@"+city] = {
                    "total":len(positions), "base":key, "positions":pieces}

    # Store browse is also paged, not a full national store directory download.
    active = [s for s in stores if s.get("status") == "active" and s.get("name")]
    baseline = str(manifest["metadata"].get("baseline_processed_at") or "")[:10]
    latest = str(manifest["metadata"].get("latest_processed_at") or "")[:10]
    cutoff = (datetime.fromisoformat(latest).date()-timedelta(days=7)).isoformat()
    recent = [s for s in active if str(s.get("first_seen") or "")[:10] > baseline
              and str(s.get("first_seen") or "")[:10] >= cutoff]
    store_browse = {}
    for group, values in (("all",active),("new",recent)):
        for order in ("rating_desc","reviews_desc","items_desc","name_asc"):
            def sk(s):
                rate = float(s.get("rating") or 0)
                reviews = int(s.get("review_count") or 0)
                count = int(s.get("menu_item_count") or 0)
                if order == "reviews_desc": return (-reviews,-rate,str(s.get("store_uuid")))
                if order == "items_desc": return (-count,str(s.get("store_uuid")))
                if order == "name_asc": return (str(s.get("name") or "").casefold(),str(s.get("store_uuid")))
                return (-rate,-reviews,str(s.get("store_uuid")))
            ordered = sorted(values,key=sk)
            store_browse[group+":"+order] = {
                "total": len(ordered), "chunks":chunks(ordered,STORE_CHUNK_ROWS)}
            by_city = defaultdict(list)
            for store in ordered:
                by_city[str(store.get("city") or "")].append(store)
            for city, city_stores in by_city.items():
                if city:
                    store_browse[group+":"+order+"@"+city] = {
                        "total":len(city_stores), "chunks":chunks(city_stores,STORE_CHUNK_ROWS)}

    # Split original 256-wide inverted-index buckets into bounded 4096-wide
    # buckets. Frequent terms use separately paged posting lists, not giant blobs.
    old_search = list(files["search"].values())
    new_search = defaultdict(dict)
    def wide_bucket(value):
        digest = hashlib.sha256(str(value).encode("utf-8")).digest()
        return int.from_bytes(digest[:2], "big") % 4096
    for sha in old_search:
        for term, matches in read_hash(sha).items():
            if len(matches) > 1024:
                by_city = defaultdict(list)
                for item_id in matches:
                    by_city[str(minis[item_id][2] or "")].append(item_id)
                record = {"count":len(matches), "cities":{
                    city:{"count":len(ids),"chunks":chunks(ids,BROWSE_CHUNK_ROWS)}
                    for city,ids in sorted(by_city.items())}}
            else:
                record = matches
            new_search[wide_bucket(term)][term] = record
    files["search"] = {str(k):write_hash(v) for k,v in sorted(new_search.items())}
    manifest["search_buckets"] = 4096

    # History used to share 128 very large buckets; redistribute by store ID.
    old_history = list(files["history"].values())
    new_history = defaultdict(dict)
    for sha in old_history:
        for compound, events in read_hash(sha).items():
            new_history[wide_bucket(compound)][compound] = events
    files["history"] = {str(k):write_hash(v) for k,v in sorted(new_history.items())}
    manifest["history_buckets"] = 4096

    # A store-name search must not scan every national store-view page.
    store_index = defaultdict(lambda: defaultdict(list))
    for index, store in enumerate(stores):
        if store.get("status") != "active" or not store.get("name"):
            continue
        text = " ".join([str(store.get(k) or "") for k in ("name","locality","address")])
        for term in sorted(terms(text)):
            store_index[wide_bucket(term)][term].append(index)
    files["store_search"] = {
        str(k):write_hash(v) for k,v in sorted(store_index.items())}
    files["store_catalog"] = chunks(stores,STORE_CHUNK_ROWS)

    old_catalog, old_stores = files["catalog"], files["stores"]
    files["catalog"] = chunks(minis, INDEX_CHUNK_ROWS)
    files["browse"] = browse
    files["store_browse"] = store_browse
    files["lookup"] = {str(k):write_hash(v) for k,v in sorted(lookup.items())}
    files.pop("stores",None)
    manifest = {**manifest, "schema":"foodradar-static-v2", "files":files,
                "index_chunk_rows":INDEX_CHUNK_ROWS,
                "browse_chunk_rows":BROWSE_CHUNK_ROWS, "lookup_buckets":4096,
                "store_chunk_rows":STORE_CHUNK_ROWS,"store_search_buckets":4096}
    references = set(files["catalog"] + files["products"] + files["store_catalog"] +
                     list(files["store_search"].values()) + list(files["search"].values()) +
                     list(files["history"].values()) + list(files["lookup"].values()))
    for entries in (browse,store_browse):
        references.update(h for entry in entries.values()
                          for h in entry.get("chunks",entry.get("positions",[])))
    for unused in (old_catalog,old_stores,*old_search,*old_history):
        if unused not in references:
            (blobs/(unused+".zst")).unlink()
    manifest["bytes"] = sum((blobs/(h+".zst")).stat().st_size for h in references)
    if manifest["bytes"] > 750 * 1024 * 1024:
        raise ValueError("Paged site exceeds static hosting size limit")
    for digest in references:
        if (blobs/(digest+".zst")).stat().st_size > 2 * 1024 * 1024:
            raise ValueError("Oversized search index shard: require narrower source sharding")
    return manifest


def referenced_blobs(manifest):
    files=manifest["files"]
    if manifest["schema"] == "foodradar-static-v1":
        return {files["stores"],files["catalog"],*files["products"],
                *files["search"].values(),*files["history"].values()}
    if manifest["schema"] != "foodradar-static-v2":
        raise ValueError("Unsupported FoodRadar static schema")
    refs=set(files["catalog"]+files["products"]+files["store_catalog"])
    for key in ("search","history","lookup","store_search"):
        refs.update(files[key].values())
    for group in ("browse","store_browse"):
        for entry in files[group].values():
            refs.update(entry.get("chunks",entry.get("positions",[])))
    for table in ("products", "stores"):
        for entries in (manifest.get("geo") or {}).get(table, {}).values():
            refs.update(entries)
    if not all(isinstance(h,str) and len(h)==64 for h in refs):
        raise ValueError("Invalid FoodRadar static blob reference")
    return refs

ROOT = Path(__file__).resolve().parents[1]
REPO_ID = "hub-google/FoodRadar"
PREFIX = "StaticCatalog"


def prepare(web_dir, source_dir=None, token=None, commit=None):
    web_dir = Path(web_dir)
    if source_dir is not None:
        source_dir = Path(source_dir)
        raw = (source_dir / "current.json").read_bytes()
    else:
        if not token:
            raise ValueError("HF_TOKEN required to prepare Pages")
        path = hf_hub_download(
            repo_id=REPO_ID, filename=PREFIX+"/current.json", repo_type="dataset",
            token=token, force_download=True)
        raw = Path(path).read_bytes()
    manifest = json.loads(raw)
    if (manifest.get("schema") not in ("foodradar-static-v1","foodradar-static-v2") or
            manifest.get("metadata", {}).get("application") != "FoodRadar"):
        raise ValueError("Invalid FoodRadar static manifest")
    batch = str(manifest.get("batch") or "")
    if len(batch) != 14 or not batch.isdigit() or not manifest.get("counts", {}).get("products"):
        raise ValueError("Static manifest has no verified source data")
    hashes = referenced_blobs(manifest)
    if not all(isinstance(h, str) and len(h) == 64 for h in hashes):
        raise ValueError("Invalid immutable static blob name")
    data = web_dir/"data"
    if data.exists():
        shutil.rmtree(data)
    target = data/"blobs"
    target.mkdir(parents=True)

    def download(digest):
        if source_dir is not None:
            src = source_dir/"blobs"/(digest+".zst")
        else:
            if manifest.get("hf_blob_layout") == "sha256-prefix-2":
                remote_path=f"{PREFIX}/v2/blobs/{digest[:2]}/{digest}.zst"
            else:
                remote_path=f"{PREFIX}/blobs/{digest}.zst"
            src = hf_hub_download(
                repo_id=REPO_ID, filename=remote_path,
                repo_type="dataset", token=token)
        payload = Path(src).read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("Static asset SHA256 verification failed: " + digest)
        (target/(digest+".zst")).write_bytes(payload)
        return len(payload)

    with ThreadPoolExecutor(max_workers=24) as executor:
        sizes = list(executor.map(download, sorted(hashes)))
    total = sum(sizes)
    if total != manifest.get("bytes") or total > 750*1024*1024:
        raise ValueError("Static artifact size differs from verified manifest")
    if manifest["schema"] == "foodradar-static-v1":
        manifest = _paged_manifest(manifest, data)
    manifest = augment_geo_index(manifest, data)
    (data/"manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),encoding="utf-8")
    commit = (commit or os.getenv("GITHUB_SHA") or "").strip()
    if not commit:
        raise ValueError("Pages release requires GITHUB_SHA")
    now = datetime.now(timezone(timedelta(hours=8)))
    version = batch+"-"+str(manifest["data_revision"])[:12]+"-"+commit[:12]
    release = {
        "version":version, "buildTime":now.strftime("%Y-%m-%d %H:%M:%S +08:00"),
        "batch":batch, "commit":commit, "data_revision":manifest["data_revision"]}
    (web_dir/"version.json").write_text(json.dumps(release, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    html = web_dir/"index.html"
    original = html.read_text(encoding="utf-8")
    if original.count("__FOODRADAR_RELEASE__") != 1:
        raise ValueError("Missing single Pages release placeholder")
    html.write_text(original.replace("__FOODRADAR_RELEASE__", version), encoding="utf-8")
    print("FOODRADAR_PAGES_STATIC_RELEASE="+json.dumps(
        {"batch":batch,"commit":commit,"blobs":len(referenced_blobs(manifest)),"bytes":manifest["bytes"],"source_bytes":total}, ensure_ascii=False), flush=True)
    return release

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging", default=None)
    parser.add_argument("--web-dir", default=str(ROOT/"web"))
    args = parser.parse_args()
    prepare(args.web_dir, args.staging, os.getenv("HF_TOKEN"))
