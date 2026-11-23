"""Read-only full validation of the published Hugging Face static catalog.

Validates content-addressed blob checksums and cross-references every searchable
product, without connecting to the retired Turso database.
"""
from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import zstandard as zstd
from huggingface_hub import hf_hub_download

REPO = "hub-google/FoodRadar"
PREFIX = "StaticCatalog"
MAX_BYTES = 750 * 1024 * 1024


def _json(raw):
    return json.loads(raw.decode("utf-8"))


def verify_live(token=None, repo=REPO):
    manifest_file = hf_hub_download(repo, PREFIX+"/current.json", repo_type="dataset",
                                    token=token, force_download=True)
    manifest = _json(Path(manifest_file).read_bytes())
    if manifest.get("schema") != "foodradar-static-v1":
        raise ValueError("Wrong static catalog schema")
    if manifest.get("metadata", {}).get("application") != "FoodRadar":
        raise ValueError("Refusing a release from a different application")
    if len(str(manifest.get("batch") or "")) != 14 or not str(manifest["batch"]).isdigit():
        raise ValueError("Invalid source batch")
    files = manifest["files"]
    hashes = {files["stores"], files["catalog"], *files["products"],
              *files["search"].values(), *files["history"].values()}
    def checked(digest):
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("Invalid blob digest")
        path = hf_hub_download(repo, PREFIX+"/blobs/"+digest+".zst",
                               repo_type="dataset", token=token)
        payload = Path(path).read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError("Failed compressed blob checksum: "+digest)
        return digest, len(payload), _json(zstd.ZstdDecompressor().decompress(payload))

    with ThreadPoolExecutor(max_workers=8) as workers:
        records = dict((digest,(size,body)) for digest,size,body in workers.map(checked, sorted(hashes)))
    total_bytes = sum(v[0] for v in records.values())
    if total_bytes != manifest.get("bytes") or total_bytes > MAX_BYTES:
        raise ValueError("Static release bytes mismatch or too large")
    stores = records[files["stores"]][1]
    rows = records[files["catalog"]][1]
    if len(stores) != manifest["counts"]["stores"]:
        raise ValueError("Store count differs from released manifest")
    if len(rows) != manifest["counts"]["products"]:
        raise ValueError("Indexed product count differs from released manifest")
    card_count = 0
    for sha in files["products"]:
        cards = records[sha][1]
        for card in cards:
            ref = rows[card_count]
            if (str(card.get("store_uuid")) != str(ref[0]) or
                    str(card.get("product_uuid")) != str(ref[1])):
                raise ValueError("Search row / product card UUID mismatch")
            card_count += 1
    if card_count != len(rows):
        raise ValueError("Unindexed or missing product cards")
    for sha in files["search"].values():
        for token, ids in records[sha][1].items():
            if not isinstance(token, str) or not isinstance(ids, list):
                raise ValueError("Invalid inverted index")
            if any(not isinstance(i, int) or i < 0 or i >= card_count for i in ids):
                raise ValueError("Inverted index contains invalid product id")
    if manifest["summary"].get("deals",0) > card_count:
        raise ValueError("Price drop summary exceeds published products")
    if manifest["summary"].get("promotions",0) > card_count:
        raise ValueError("Promotion summary exceeds published products")
    result = {
        "status":"verified", "batch":manifest["batch"],
        "data_revision":manifest["data_revision"], "stores":len(stores),
        "products":card_count, "blobs":len(hashes), "compressed_bytes":total_bytes,
        "search_buckets":len(files["search"]), "history_buckets":len(files["history"]),
    }
    return result


if __name__ == "__main__":
    result=verify_live(token=os.getenv("HF_TOKEN"))
    output=Path("audit-results/full-catalog-latest.json")
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,indent=2,ensure_ascii=False)+"\n",encoding="utf-8")
    print("FOODRADAR_STATIC_CATALOG_AUDIT="+json.dumps(result,ensure_ascii=False),flush=True)
