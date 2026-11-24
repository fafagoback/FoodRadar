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


def build(packed_db, serving_db, output):
    packed_db, serving_db, output = Path(packed_db), Path(serving_db), Path(output)
    if not packed_db.is_file() or not serving_db.is_file():
        raise FileNotFoundError("Both verified packed-serving.db and serving.db are required")
    output.mkdir(parents=True, exist_ok=True)
    blobs = output / "blobs"
    blobs.mkdir(exist_ok=True)
    compressor = zstd.ZstdCompressor(level=10, write_content_size=True)
    with sqlite3.connect(packed_db) as packed, sqlite3.connect(serving_db) as source:
        packed.row_factory = sqlite3.Row
        source.row_factory = sqlite3.Row
        assert packed.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert source.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        meta = {key: json.loads(value) for key, value in packed.execute("SELECT key,value FROM metadata")}
        if meta.get("application") != "FoodRadar" or meta.get("schema_version") != 2:
            raise ValueError("Refusing non-V2 or non-FoodRadar packed database")
        latest_batch = str(meta.get("latest_batch") or "")
        if len(latest_batch) != 14 or not latest_batch.isdigit():
            raise ValueError("Missing verified 14-digit source batch")
        actual_batch = source.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
        if not actual_batch or actual_batch[0] != latest_batch:
            raise ValueError("Serving and packed database batches differ")
        batch_time = meta.get("latest_processed_at") or ""
        baseline = str(meta.get("baseline_processed_at") or "")[:10]
        try:
            cutoff = (datetime.fromisoformat(batch_time) - timedelta(days=7)).isoformat()
        except ValueError as exc:
            raise ValueError("Invalid verified latest_processed_at") from exc

        store_rows = [dict(s) for s in packed.execute("SELECT * FROM store_directory ORDER BY store_uuid")]
        count_by_store = Counter()
        minis = []
        cards = []
        postings = defaultdict(lambda: defaultdict(list))
        history_keys = set()
        for row in packed.execute("""SELECT store_uuid,product_uuid,card,search_text,city,
                 category,discount_pct,savings,unit_price,rating,new_item,promo,
                 price_drop,platform_promo,latitude,longitude
                 FROM web_products ORDER BY store_uuid,product_uuid"""):
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

        product_hashes = [
            write_blob(blobs, compressor, cards[i:i + CHUNK_ROWS])
            for i in range(0, len(cards), CHUNK_ROWS)
        ]
        search_hashes = {
            str(bid): write_blob(blobs, compressor, term_map)
            for bid, term_map in sorted(postings.items())
        }
        store_hash = write_blob(blobs, compressor, store_rows)
        catalog_hash = write_blob(blobs, compressor, minis)

        # Canonical native quotes preserve the exact payable-price series.
        # The old generic events are redundant for all browser price graphs.
        history = defaultdict(dict)
        has_quotes = source.execute("""SELECT 1 FROM sqlite_master
             WHERE type='table' AND name='product_price_observations'""").fetchone()
        if has_quotes:
            import sys
            sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
            from canonical_history import quote_points
            for sid, pid, quote in quote_points(source):
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
        history_hashes = {
            str(bid): write_blob(blobs, compressor, entries)
            for bid, entries in sorted(history.items())
        }
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
        verify(output)
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
