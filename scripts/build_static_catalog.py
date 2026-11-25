"""Compile FoodRadar's already-verified serving projection into immutable static blobs.

No online SQL, no reinterpreted promotions, and no duplicate product cards in
search indexes. Build from packed-serving.db (native V2 cards) and serving.db
(events / verified menu quote history); HF Raw remains the authority.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import shutil
import os
from time import perf_counter
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import zstandard as zstd

SCHEMA = "foodradar-static-v1"
CHUNK_ROWS = 128
BUCKETS = 256
HISTORY_BUCKETS = 128


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def normalize(text):
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(text or "")).casefold())


@lru_cache(maxsize=8192)
def terms(text):
    s = normalize(text)
    if not s:
        return set()
    out = {"u:" + char for char in s}
    out.update("b:" + s[i:i + 2] for i in range(len(s) - 1))
    out.update("t:" + s[i:i + 3] for i in range(len(s) - 2))
    return out


def bucket(key, count=BUCKETS):
    return hashlib.sha256(str(key).encode("utf-8")).digest()[0] % count


def write_blob(directory, compressor, data):
    original = _json(data)
    compressed = compressor.compress(original)
    digest = hashlib.sha256(compressed).hexdigest()
    path = directory / (digest + ".zst")
    if not path.exists():
        path.write_bytes(compressed)
    return digest


def _price_event(at, price, unit, promo, quantity, unit_only=False):
    state = {
        "price": float(unit if unit_only else price),
        "quantity": int(quantity or 1),
        "promo_type": promo if not unit_only else None,
        "effective_unit_only": bool(unit_only),
        "effective_price": float(unit),
        "price_evidence_status": "VERIFIED_MENU",
        "menu_quote_valid": True, "price_valid": False, "delivery_available": False,
    }
    return {"event_time": at, "event_type": "MENU_QUOTE", "new_state": json.dumps(
        state, ensure_ascii=False, separators=(",", ":"))}


class StaticBlobCache:
    """Reuse only blobs whose decoded bytes exactly equal the new input."""
    def __init__(self, directory):
        self.root=Path(directory)
        self.root.mkdir(parents=True,exist_ok=True)
        self.index=self.root/"index.json"
        try:
            value=json.loads(self.index.read_text())
            self.previous=value if isinstance(value,dict) else {}
        except (OSError,ValueError):
            self.previous={}
        self.current={}
        self.hits=0
        self.misses=0

    def write(self, directory, data):
        original=_json(data)
        key=hashlib.sha256(original).hexdigest()
        digest=self.previous.get(key)
        if isinstance(digest,str) and re.fullmatch(r"[0-9a-f]{64}",digest):
            cached=self.root/(digest+".zst")
            try:
                encoded=cached.read_bytes()
                if (hashlib.sha256(encoded).hexdigest()==digest and
                        zstd.ZstdDecompressor().decompress(encoded)==original):
                    self.hits+=1
                else:raise ValueError("Stale cached blob")
            except (OSError,ValueError,zstd.ZstdError):
                digest=None
        else:digest=None
        if digest is None:
            encoded=zstd.ZstdCompressor(level=10,write_content_size=True).compress(original)
            digest=hashlib.sha256(encoded).hexdigest()
            self.misses+=1
        target=directory/(digest+".zst")
        if not target.exists():target.write_bytes(encoded)
        cached=self.root/(digest+".zst")
        if not cached.exists() or cached.read_bytes()!=encoded:
            temp=cached.with_suffix(".tmp")
            temp.write_bytes(encoded);temp.replace(cached)
        self.current[key]=digest
        return digest

    def finish(self):
        temp=self.index.with_suffix(".tmp")
        temp.write_bytes(_json(self.current));temp.replace(self.index)
        retained=set(self.current.values())
        for path in self.root.glob("*.zst"):
            if path.stem not in retained:path.unlink()
        return {"reused_blobs":self.hits,"rebuilt_blobs":self.misses,
                "cache_bytes":sum(p.stat().st_size for p in self.root.glob("*.zst"))}


def build(packed_db, serving_db, output, *, direct=None, projection=None, cache_dir=None):
    from contextlib import nullcontext
    output=Path(output)
    if direct is None:
        packed_db,serving_db=Path(packed_db),Path(serving_db)
        if not packed_db.is_file() or not serving_db.is_file():
            raise FileNotFoundError("Both verified packed-serving.db and serving.db are required")
    elif projection is None:
        raise ValueError("Direct compact publication requires a verified card projection")
    output.mkdir(parents=True, exist_ok=True)
    blobs = output / "blobs"
    blobs.mkdir(exist_ok=True)
    compressor = zstd.ZstdCompressor(level=10, write_content_size=True)
    started=perf_counter();marks={}
    blob_cache=StaticBlobCache(cache_dir) if cache_dir is not None else None
    def emit(value):
        return blob_cache.write(blobs,value) if blob_cache is not None else write_blob(blobs,compressor,value)
    def mark(name,begin):
        marks[name]=round(perf_counter()-begin,3)
    with (sqlite3.connect(packed_db) if direct is None else nullcontext(None)) as packed, (sqlite3.connect(serving_db) if direct is None else nullcontext(None)) as source:
        if direct is None:
            packed.row_factory = sqlite3.Row
            source.row_factory = sqlite3.Row
            assert packed.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            assert source.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
            meta = {key: json.loads(value) for key, value in packed.execute("SELECT key,value FROM metadata")}
        else:
            if projection.validation_report is None:projection.validate()
            meta=projection.metadata
        if meta.get("application") != "FoodRadar" or meta.get("schema_version") != 2:
            raise ValueError("Refusing non-V2 or non-FoodRadar packed database")
        latest_batch = str(meta.get("latest_batch") or "")
        if len(latest_batch) != 14 or not latest_batch.isdigit():
            raise ValueError("Missing verified 14-digit source batch")
        actual_batch = ((direct.metadata().get("latest_batch"),) if direct is not None else
                        source.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone())
        if not actual_batch or actual_batch[0] != latest_batch:
            raise ValueError("Serving and packed database batches differ")
        batch_time = meta.get("latest_processed_at") or ""
        baseline = str(meta.get("baseline_processed_at") or "")[:10]
        try:
            cutoff = (datetime.fromisoformat(batch_time) - timedelta(days=7)).isoformat()
        except ValueError as exc:
            raise ValueError("Invalid verified latest_processed_at") from exc

        store_rows = projection.store_rows() if direct is not None else [dict(s) for s in packed.execute("SELECT * FROM store_directory ORDER BY store_uuid")]
        count_by_store = Counter()
        minis = []
        cards = []
        postings = defaultdict(lambda: defaultdict(list))
        history_keys = set()
        card_rows=(projection.rows() if direct is not None else packed.execute("""SELECT store_uuid,product_uuid,card,search_text,city,
                 category,discount_pct,savings,unit_price,rating,new_item,promo,
                 price_drop,platform_promo,latitude,longitude
                 FROM web_products ORDER BY store_uuid,product_uuid"""))
        for row in card_rows:
            card = json.loads(row["card"])
            sid, pid = str(row["store_uuid"]), str(row["product_uuid"])
            if card.get("store_uuid") != sid or card.get("product_uuid") != pid:
                raise ValueError("Packed card UUID mismatch")
            index = len(cards)
            cards.append(card)
            count_by_store[sid] += 1
            history_keys.add((sid, pid))
            searchable = normalize(row["search_text"])
            category = str(row["category"] or "")
            city = str(row["city"] or "")
            promo_type = str(card.get("promo_type") or "")
            minis.append([
                sid, pid, city, category,
                float(row["discount_pct"] or 0), float(row["savings"] or 0),
                float(row["unit_price"] or 0), float(row["rating"] or 0),
                int(row["new_item"] or 0), int(row["promo"] or 0),
                int(row["price_drop"] or 0), int(row["platform_promo"] or 0),
                row["latitude"], row["longitude"], searchable, promo_type, str(card.get("product_name") or "")
            ])
            for term in terms(searchable):
                postings[bucket(term)][term].append(index)
        if not cards or not store_rows:
            raise ValueError("Refusing to publish empty product or store catalog")
        for store in store_rows:
            store["menu_item_count"] = count_by_store[store["store_uuid"]]

        mark("card_and_search_index_seconds",started)
        t=perf_counter()
        product_hashes = [emit(cards[i:i+CHUNK_ROWS]) for i in range(0,len(cards),CHUNK_ROWS)]
        mark("product_chunks_seconds",t);t=perf_counter()
        search_hashes = {str(bid):emit(term_map) for bid,term_map in sorted(postings.items())}
        mark("search_compression_seconds",t);t=perf_counter()
        store_hash=emit(store_rows)
        catalog_hash=emit(minis)
        mark("store_and_catalog_seconds",t);t=perf_counter()

        # Canonical native quotes preserve the exact payable-price series.
        # The old generic events are redundant for all browser price graphs.
        history = defaultdict(dict)
        has_quotes = direct is not None or source.execute("""SELECT 1 FROM sqlite_master
             WHERE type IN ('table','view') AND name='product_price_observations'""").fetchone()
        if has_quotes:
            import sys
            sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
            from canonical_history import quote_points,points_from_rows
            if direct is not None:
                native_rows=((q[1],q[2],q[3],q[4],q[5],q[6],q[7],q[8],q[10])
                             for quotes in direct.quote_shops() for q in quotes)
                history_points=points_from_rows(native_rows)
            else:
                history_points=quote_points(source)
            for sid, pid, quote in history_points:
                if (sid,pid) not in history_keys:
                    continue
                at,unit,raw,promo,qty=quote
                bid=bucket(sid,HISTORY_BUCKETS)
                history[bid].setdefault(sid+"\x1f"+pid,[]).append(
                    _price_event(at,raw,unit,promo,qty))
        else:
            # Historical offline fixtures without V2 price observations still
            # test backwards compatibility with the original event archive.
            for row in source.execute("""SELECT store_uuid,product_uuid,event_time,event_type,
                   old_state,new_state FROM events WHERE product_uuid IS NOT NULL
                   ORDER BY store_uuid,product_uuid,event_time,id"""):
                key=(row["store_uuid"],row["product_uuid"])
                if key not in history_keys:
                    continue
                bid=bucket(key[0],HISTORY_BUCKETS)
                history[bid].setdefault(key[0]+"\x1f"+key[1],[]).append({
                    "event_time":row["event_time"],"event_type":row["event_type"],
                    "old_state":row["old_state"],"new_state":row["new_state"],
                })
            for row in source.execute("""SELECT store_uuid,product_uuid,menu_quote_history
                   FROM products WHERE menu_quote_history != '[]'"""):
                key=(row["store_uuid"],row["product_uuid"])
                if key not in history_keys:
                    continue
                bid=bucket(key[0],HISTORY_BUCKETS)
                compound=key[0]+"\x1f"+key[1]
                for quote in json.loads(row["menu_quote_history"] or "[]"):
                    if not isinstance(quote,list) or len(quote)<2:
                        continue
                    at,unit=quote[:2]
                    if not at or not isinstance(unit,(int,float)) or unit<=0:
                        continue
                    raw,promo,qty=(quote[2:5]+[None]*3)[:3]
                    evidence=(len(quote)>=5 and isinstance(raw,(int,float))
                              and raw>0 and bool(str(promo or "").strip()))
                    history[bid].setdefault(compound,[]).append(
                        _price_event(at,raw,unit,promo,qty,unit_only=not evidence))
        mark("history_projection_seconds",t);t=perf_counter()
        history_hashes={str(bid):emit(entries) for bid,entries in sorted(history.items())}
        mark("history_compression_seconds",t)
        new_stores = sum(1 for s in store_rows if s["status"] == "active" and
                         str(s.get("first_seen") or "") >= cutoff and
                         str(s.get("first_seen") or "")[:10] > baseline)
        summary = {
            "stores": new_stores,
            "newProducts": sum(row[8] for row in minis),
            "promotions": sum(row[9] for row in minis),
            "maxSavings": max((row[5] for row in minis if row[10]), default=0),
            "deals": sum(1 for row in minis if row[10] and row[4] >= 30),
        }
        manifest = {
            "schema": SCHEMA, "batch": latest_batch, "data_revision": meta["data_revision"],
            "metadata": meta, "summary": summary,
            "counts": {"stores": len(store_rows), "products": len(cards)},
            "chunk_rows": CHUNK_ROWS, "search_buckets": BUCKETS,
            "history_buckets": HISTORY_BUCKETS,
            "files": {"stores": store_hash, "catalog": catalog_hash,
                      "products": product_hashes, "search": search_hashes,
                      "history": history_hashes},
        }
        # All the individual blob names are the SHA256 of actual compressed bytes.
        refs = {store_hash, catalog_hash, *product_hashes, *search_hashes.values(), *history_hashes.values()}
        if set(p.stem for p in blobs.glob("*.zst")) != refs:
            raise ValueError("Unexpected extra or missing blobs in staging")
        total_bytes = sum((blobs / (digest + ".zst")).stat().st_size for digest in refs)
        if total_bytes > 750 * 1024 * 1024:
            raise ValueError("Static assets exceed safe GitHub Pages deployment budget")
        manifest["bytes"] = total_bytes
        (output / "current.json").write_bytes(_json(manifest))
        t=perf_counter();verify(output);mark("static_verification_seconds",t)
        if blob_cache is not None:marks.update(blob_cache.finish())
        marks["total_seconds"]=round(perf_counter()-started,3)
        print("FOODRADAR_STATIC_TIMING="+json.dumps(marks),flush=True)
        return manifest


def verify(directory):
    folder = Path(directory)
    manifest = json.loads((folder / "current.json").read_text(encoding="utf-8"))
    if manifest.get("schema") != SCHEMA:
        raise ValueError("Unsupported static schema")
    hashes = manifest["files"]
    values = [hashes["stores"], hashes["catalog"], *hashes["products"],
              *hashes["search"].values(), *hashes["history"].values()]
    dctx = zstd.ZstdDecompressor()
    for digest in set(values):
        data = (folder / "blobs" / (digest + ".zst")).read_bytes()
        if hashlib.sha256(data).hexdigest() != digest:
            raise ValueError("Static blob SHA256 mismatch: " + digest)
        dctx.decompress(data)
    if not manifest["counts"]["products"] or not manifest["counts"]["stores"]:
        raise ValueError("No serving records")
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("packed")
    parser.add_argument("serving")
    parser.add_argument("output")
    args = parser.parse_args()
    result = build(args.packed, args.serving, args.output)
    print("FOODRADAR_STATIC_BUILD=" + json.dumps({
        "batch": result["batch"], "counts": result["counts"], "bytes": result["bytes"],
        "blobs": len(list((Path(args.output) / "blobs").glob("*.zst"))),
        "revision": result["data_revision"],
    }, ensure_ascii=False), flush=True)
