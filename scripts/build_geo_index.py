"""Deterministic sub-district spatial index for static FoodRadar V2.

The web client derives the same tile IDs from arbitrary coordinates and a radius.
Each product/store is indexed ONCE by its real location. Adjacent cells are
covered by the query bounding box and exact distance is checked in JS.
No city or township boundary is used as a search boundary.
"""
from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
import json
import math
from pathlib import Path

import zstandard as zstd

CELL_DEGREES = 0.025  # ~2.78 km north/south, ~2.5 km E/W in Taiwan.
MAX_COMPRESSED = 1024 * 1024
MAX_ROWS_PER_SHARD = 12000


def key(lat, lon):
    try:
        lat, lon = float(lat), float(lon)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    # Matching JS: Math.floor(lat / step).
    return f"{math.floor(lat / CELL_DEGREES)}:{math.floor(lon / CELL_DEGREES)}"


def augment_geo_index(manifest: dict, data_dir: Path) -> dict:
    """Add one-grid-owner location indexes to an already-built static V2."""
    if manifest["schema"] != "foodradar-static-v2":
        raise ValueError("Spatial indexing requires FoodRadar static V2")
    data_dir = Path(data_dir)
    blobs = data_dir / "blobs"
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor(level=10, write_content_size=True)

    def read(h):
        return json.loads(dctx.decompress((blobs / (h + ".zst")).read_bytes()))

    def write(rows):
        payload = json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        compressed = cctx.compress(payload)
        if len(compressed) > MAX_COMPRESSED:
            raise ValueError("Oversized spatial index; repartition into smaller shards")
        digest = sha256(compressed).hexdigest()
        (blobs / (digest + ".zst")).write_bytes(compressed)
        return digest

    products = defaultdict(list)
    stores = defaultdict(list)
    idx = 0
    for h in manifest["files"]["catalog"]:
        for record in read(h):
            cell = key(record[12], record[13])
            if cell is not None:
                # Product/store UUIDs are already in the immutable card blobs;
                # omit them from the nearby index to avoid copying 70+ bytes
                # of identity per product into every visitor's local search.
                products[cell].append([idx, record[2:]])
            idx += 1
    if idx != manifest["counts"]["products"]:
        raise ValueError("Spatial index does not cover the product catalog")

    for h in manifest["files"]["store_catalog"]:
        for record in read(h):
            cell = key(record.get("latitude"), record.get("longitude"))
            if cell is not None:
                stores[cell].append(record)

    def export(table):
        output = {}
        for cell, rows in sorted(table.items()):
            pieces = []
            for off in range(0, len(rows), MAX_ROWS_PER_SHARD):
                batch = rows[off:off + MAX_ROWS_PER_SHARD]
                # Ensure 1 MiB compressed maximum, even for unusually verbose rows.
                queue = [batch]
                while queue:
                    group = queue.pop()
                    raw = json.dumps(group, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                    compressed = cctx.compress(raw)
                    if len(compressed) > MAX_COMPRESSED:
                        if len(group) <= 1:
                            raise ValueError("Single oversized spatial index row")
                        half = len(group) // 2
                        queue.extend([group[half:], group[:half]])
                    else:
                        pieces.append(write(group))
            output[cell] = pieces
        return output

    geo = {
        "schema": "foodradar-geo-grid-v1",
        "cell_degrees": CELL_DEGREES,
        "products": export(products),
        "stores": export(stores),
    }
    old = manifest.get("geo", {})
    if old:
        raise ValueError("Refusing to overwrite an existing spatial index")
    manifest["geo"] = geo
    refs = set()
    for table in ("products", "stores"):
        for files in geo[table].values():
            refs.update(files)
    # The existing static files are still present and remain fully usable.
    extra = sum((blobs / (h + ".zst")).stat().st_size for h in refs
                if h not in _original_refs(manifest))
    manifest["bytes"] += extra
    print("FOODRADAR_GEO_INDEX=" + json.dumps({
        "cells_product": len(products), "cells_store": len(stores),
        "product_rows": sum(map(len, products.values())),
        "store_rows": sum(map(len, stores.values())),
        "geo_blobs": len(refs), "extra_bytes": extra,
    }, ensure_ascii=False), flush=True)
    return manifest


def _original_refs(manifest):
    files = manifest["files"]
    refs = set(files["catalog"] + files["products"] + files["store_catalog"])
    for kind in ("search", "history", "lookup", "store_search"):
        refs.update(files[kind].values())
    for kind in ("browse", "store_browse"):
        for group in files[kind].values():
            refs.update(group.get("chunks", group.get("positions", [])))
    return refs
