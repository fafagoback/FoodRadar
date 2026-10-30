"""Local-only prototype for losslessly packing the normalized serving database.

This intentionally has no Turso publishing code.  It stores canonical MessagePack
payloads compressed with Zstandard and a bucketed n-gram inverted index.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
import unicodedata
from collections import defaultdict
from pathlib import Path

import msgpack
import zstandard as zstd

TABLES = ("stores", "products", "events", "crawl_batches", "metadata")
SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE store_directory(
  store_id INTEGER PRIMARY KEY,store_uuid TEXT UNIQUE NOT NULL,name TEXT NOT NULL,
  city TEXT,locality TEXT,rating REAL,review_count INTEGER,chunk_count INTEGER NOT NULL,
  latitude REAL,longitude REAL,address TEXT,order_url TEXT,first_seen TEXT,last_seen TEXT,
  status TEXT,is_open INTEGER
);
CREATE TABLE store_bundles(
  store_id INTEGER NOT NULL,chunk_no INTEGER NOT NULL,codec TEXT NOT NULL,
  raw_bytes INTEGER NOT NULL,checksum TEXT NOT NULL,payload BLOB NOT NULL,
  PRIMARY KEY(store_id,chunk_no)
);
CREATE TABLE search_buckets(
  bucket_id INTEGER PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,
  checksum TEXT NOT NULL,payload BLOB NOT NULL
);
CREATE TABLE auxiliary_bundles(
  name TEXT PRIMARY KEY,codec TEXT NOT NULL,raw_bytes INTEGER NOT NULL,
  checksum TEXT NOT NULL,payload BLOB NOT NULL
);
CREATE TABLE web_products(
  store_uuid TEXT NOT NULL,product_uuid TEXT NOT NULL,
  card TEXT NOT NULL,checksum TEXT NOT NULL,search_text TEXT NOT NULL,
  city TEXT,category TEXT,discount_pct REAL,savings REAL,unit_price REAL,
  rating REAL,new_item INTEGER,promo INTEGER,price_drop INTEGER,platform_promo INTEGER,
  latitude REAL,longitude REAL,PRIMARY KEY(store_uuid,product_uuid)
);
CREATE INDEX idx_web_deals ON web_products(price_drop,discount_pct DESC,savings DESC);
CREATE INDEX idx_web_promos ON web_products(promo,unit_price);
CREATE INDEX idx_web_new ON web_products(new_item,rating DESC);
CREATE INDEX idx_web_rating ON web_products(rating DESC);
CREATE INDEX idx_web_city ON web_products(city);
"""


def columns(conn, table):
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def normalize(value):
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", str(value or "")).casefold())


def tokens(value):
    s = normalize(value)
    out = set()
    # Full normalized value preserves exact short-query lookup.  Character and
    # n-grams support Chinese substring intersections and fuzzy candidate recall.
    if s:
        out.add("w:" + s)
    out.update("u:" + c for c in s)
    out.update("b:" + s[i:i + 2] for i in range(max(0, len(s) - 1)))
    out.update("t:" + s[i:i + 3] for i in range(max(0, len(s) - 2)))
    return out


def query_tokens(value):
    s = normalize(value)
    if len(s) >= 3:
        return ["t:" + s[i:i + 3] for i in range(len(s) - 2)]
    if len(s) == 2:
        return ["b:" + s]
    return ["u:" + s] if s else []


def packed(value, compressor):
    raw = msgpack.packb(value, use_bin_type=True)
    return raw, compressor.compress(raw), hashlib.sha256(raw).hexdigest()


def row_array(row, cols):
    # msgpack preserves None/int/float/str/bytes storage classes.
    return [row[c] for c in cols]


def source_digest(conn, table, order):
    cols = columns(conn, table)
    h = hashlib.sha256()
    count = 0
    for row in conn.execute(f"SELECT * FROM {table} ORDER BY {order}"):
        h.update(msgpack.packb(list(row), use_bin_type=True))
        count += 1
    return count, h.hexdigest()


def build(source, output, bucket_count=1024, level=10, max_items=2048):
    started = time.perf_counter()
    src = sqlite3.connect(source)
    src.row_factory = sqlite3.Row
    out_path = Path(output)
    if out_path.exists():
        out_path.unlink()
    dst = sqlite3.connect(output)
    dst.executescript(SCHEMA)
    compressor = zstd.ZstdCompressor(level=level)
    postings = [defaultdict(list) for _ in range(bucket_count)]
    sc, pc, ec = (columns(src, t) for t in ("stores", "products", "events"))
    product_gid = 0
    max_raw = max_blob = chunks = 0

    # Bundles remain lossless archives of normalized current/events data. Only
    # active rows are added to the serving search postings below.
    # Append-only first_seen order prevents new identities from shifting existing IDs.
    stores = list(src.execute("SELECT * FROM stores ORDER BY first_seen,store_uuid"))
    baseline_time = src.execute('SELECT MIN(processed_at) FROM crawl_batches').fetchone()[0]
    latest_time = src.execute('SELECT MAX(processed_at) FROM crawl_batches').fetchone()[0]
    if not latest_time:
        raise ValueError("No completed crawl batch: cannot publish current prices")
    from datetime import datetime, timedelta
    cutoff = (datetime.fromisoformat(latest_time) - timedelta(days=7)).isoformat()
    active_stores = active_products = current_verified_products = 0
    active_comparable_products = active_platform_promotions = active_30pct_drops = 0
    seven_day_reference_products = 0
    archive_reference_products = archive_30pct_candidates = 0
    verified_history_days = sorted(set(
        str(t)[:10] for (t,) in src.execute(
            """SELECT processed_at FROM crawl_batches
               WHERE processed_at>=? AND processed_at<=? AND products_seen>0""",
            (cutoff,latest_time))))
    for store_id, store in enumerate(stores):
        sid = store["store_uuid"]
        products = list(src.execute("SELECT * FROM products WHERE store_uuid=? ORDER BY first_seen,product_uuid", (sid,)))
        events = list(src.execute("SELECT * FROM events WHERE store_uuid=? ORDER BY id", (sid,)))
        product_arrays = []
        store_terms = tokens(store["name"]) | tokens(store["city"]) | tokens(store["locality"]) | tokens(store["address"])
        for local_index, product in enumerate(products):
            if local_index >= (1 << 20):
                raise ValueError(f"store {sid} exceeds the packed 20-bit product index capacity")
            product_gid += 1
            arr = row_array(product, pc)
            product_arrays.append([(store_id << 20) | local_index, arr])
            # A closed shop or unavailable delivery cannot erase its last menu.
            searchable = (store["status"] == "active"
                          and product["status"] == "active"
                          and float(product["menu_quote_effective_price"] or product["effective_price"] or product["price"] or 0) > 0)
            current_menu = (store["last_seen"] == latest_time
                            and product["last_seen"] == latest_time
                            and int(store["is_open"] or 0) == 1
                            and int(product["is_open"] or 0) == 1)
            native_verified = product["price_evidence_status"] == "VERIFIED_MENU"
            platform_promo = native_verified and float(product["verified_discount_pct"] or 0) > 0
            current_menu = current_menu and native_verified
            current_verified = current_menu and int(product["delivery_available"] or 0) == 1
            terms = (store_terms | tokens(product["product_name"]) | tokens(product["category"]) | tokens(product["menu_quote_promo_type"] or product["promo_type"])) if searchable else set()
            price = float(product["menu_quote_effective_price"] or product["effective_price"] or product["price"] or 0)
            if searchable:
                active_products += 1
                if current_menu and float(product["seven_day_reference_price"] or 0) > 0:
                    active_comparable_products += 1
                if current_menu and float(product["seven_day_reference_price"] or 0) > 0:
                    seven_day_reference_products += 1
                if current_verified:
                    current_verified_products += 1
                if current_verified and platform_promo:
                    active_platform_promotions += 1
                if current_verified and int(product["seven_day_is_price_deal"] or 0) == 1:
                    active_30pct_drops += 1
                if current_verified and float(product["archive_reference_price"] or 0) > 0:
                    archive_reference_products += 1
                if current_verified and int(product["archive_is_price_deal"] or 0) == 1:
                    archive_30pct_candidates += 1
                    terms.add(f"f:archive_discount:{int(float(product['archive_discount_pct']) // 5)}")
                terms.add(f"f:price:{int(price // 50)}")
                terms.add("f:catalog")
                if (product['listing_started_at'][:10] > baseline_time[:10]
                        and product['listing_started_at'][:10] > store['first_seen'][:10]
                        and product['listing_started_at'] >= cutoff):
                    terms.add('f:new')
                if current_verified and (platform_promo
                        or float(product["seven_day_discount_pct"] or 0) > 0):
                    terms.add("f:promo")
                if current_verified and float(product["seven_day_discount_pct"] or 0) > 0:
                    terms.add(f"f:discount:{int(float(product['seven_day_discount_pct']) // 5)}")
            # Indexed, denormalized browsing cards. The archived bundles remain lossless;
            # browser searches no longer have to download/decompress entire stores.
            if searchable and int(store["is_open"] or 0) == 1 and int(product["is_open"] or 0) == 1 and native_verified:
                effective = float(product["menu_quote_effective_price"] or 0)
                if effective > 0:
                    raw_price = float(product["menu_quote_price"] or 0)
                    normal_price = float(product["price"] or 0)
                    week_reference = float(product["seven_day_reference_price"] or 0)
                    week_pct = float(product["seven_day_discount_pct"] or 0) if current_menu else 0
                    drop = (current_menu and week_pct > 0 and week_reference > effective)
                    ptype = product["menu_quote_promo_type"] or product["promo_type"] if raw_price > 0 else product["promo_type"]
                    platform = (float(product["verified_discount_pct"] or 0) > 0
                                and (re.fullmatch(r"買[0-9]+送[0-9]+",str(ptype or "")) is not None or ptype == "直接折價"))
                    new_item = (str(product["listing_started_at"] or "")[:10] > str(baseline_time)[:10]
                                and str(product["listing_started_at"] or "")[:10] > str(store["first_seen"] or "")[:10]
                                and str(product["listing_started_at"] or "") >= cutoff)
                    is_current_price = current_verified and normal_price > 0
                    card = {
                        "product_id": product["product_uuid"], "product_uuid": product["product_uuid"],
                        "store_id": sid, "store_uuid": sid, "store_name": store["name"],
                        "product_name": product["product_name"], "category_name": product["category"],
                        "description": product["description"],
                        "price": raw_price if raw_price > 0 else normal_price,
                        "quantity": int(product["menu_quote_quantity"] or 1) if raw_price > 0 else int(product["quantity"] or 1),
                        "promo_type": ptype, "eff_price": effective, "effective_price": effective,
                        "verified_discount_pct": float(product["verified_discount_pct"] or 0),
                        "price_evidence_status": "VERIFIED_MENU",
                        "order_action_url": product["order_url"] or store["order_url"],
                        "rating_value": store["rating"], "review_count": int(store["review_count"] or 0),
                        "locality": store["locality"], "street_address": store["address"],
                        "city": store["city"], "latitude": store["latitude"], "longitude": store["longitude"],
                        "crawled_time": product["last_seen"] or latest_time,
                        "first_seen": product["first_seen"],
                        "listing_started_at": product["listing_started_at"] or product["first_seen"],
                        "original_price": week_reference if drop else effective,
                        "reference_price": float(product["reference_price"]) if float(product["reference_price"] or 0) > 0 else None,
                        "seven_day_reference_price": week_reference if week_reference > 0 else None,
                        "is_new_price_drop": int(is_current_price and int(product["is_price_drop"] or 0) == 1),
                        "is_verified_delivery_deal": bool(drop and is_current_price),
                        "archive_reference_price": float(product["archive_reference_price"] or 0) if is_current_price else 0,
                        "archive_discount_pct": float(product["archive_discount_pct"] or 0) if is_current_price else 0,
                        "archive_savings_amount": float(product["archive_discount_amount"] or 0) if is_current_price else 0,
                        "archive_evidence_count": int(product["archive_evidence_count"] or 0) if is_current_price else 0,
                        "archive_is_price_deal": int(product["archive_is_price_deal"] or 0) if is_current_price else 0,
                        "current_price": effective, "price_direction": product["price_direction"] or "unknown",
                        "price_change_amount": float(product["price_change_amount"] or 0),
                        "price_change_percent": float(product["price_change_percent"] or 0),
                        "is_price_drop": int(drop), "discount_pct": week_pct if drop else 0,
                        "savings_amount": float(product["seven_day_discount_amount"] or 0) if drop else 0,
                        "is_price_deal": int(product["seven_day_is_price_deal"] or 0) if drop else 0,
                        "is_current_price_observation": bool(is_current_price),
                        "is_current_menu_observation": bool(current_menu),
                        "is_menu_catalog_available": True,
                        "delivery_available": bool(is_current_price) if current_menu else None,
                        "status": product["status"], "is_open": 1,
                    }
                    serialized = json.dumps(card,ensure_ascii=False,separators=(",",":"))
                    search_text = " ".join((normalize(store["name"]),normalize(product["product_name"]),
                                            normalize(product["category"]),normalize(store["locality"]),normalize(store["address"])))
                    dst.execute("""INSERT INTO web_products VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(
                        sid,product["product_uuid"],serialized,hashlib.sha256(serialized.encode()).hexdigest(),
                        search_text,store["city"],product["category"],week_pct if drop else 0,
                        float(product["seven_day_discount_amount"] or 0) if drop else 0,effective,
                        store["rating"],int(new_item),int((platform or drop) and is_current_price),
                        int(drop and is_current_price),int(platform and is_current_price),
                        store["latitude"],store["longitude"]))
            for term in sorted(terms):
                bid = int.from_bytes(hashlib.blake2s(term.encode(), digest_size=4).digest(), "big") % bucket_count
                # One integer identifies store and product without a row-per-product
                # lookup table (20 bits allows >1M products in one store).
                postings[bid][term].append((store_id << 20) | local_index)

        # Deterministic chunks keep pathological stores bounded. Store data is in
        # chunk zero; all product/event arrays remain present exactly once.
        pchunks = [product_arrays[i:i + max_items] for i in range(0, len(product_arrays), max_items)] or [[]]
        echunks = [[row_array(e, ec) for e in events[i:i + max_items]] for i in range(0, len(events), max_items)] or [[]]
        chunk_count = max(len(pchunks), len(echunks))
        if store['status'] == 'active':
            active_stores += 1
        dst.execute("INSERT INTO store_directory VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
            store_id, sid, store["name"], store["city"], store["locality"], store["rating"], store["review_count"], chunk_count,
            store['latitude'],store['longitude'],store['address'],store['order_url'],store['first_seen'],store['last_seen'],store['status'],store['is_open']))
        for chunk_no in range(chunk_count):
            value = {
                "v": 1, "store_columns": sc if chunk_no == 0 else None,
                "store": row_array(store, sc) if chunk_no == 0 else None,
                "product_columns": pc if chunk_no == 0 else None,
                "products": pchunks[chunk_no] if chunk_no < len(pchunks) else [],
                "event_columns": ec if chunk_no == 0 else None,
                "events": echunks[chunk_no] if chunk_no < len(echunks) else [],
            }
            raw, blob, digest = packed(value, compressor)
            dst.execute("INSERT INTO store_bundles VALUES(?,?,?,?,?,?)", (store_id, chunk_no, "msgpack+zstd", len(raw), digest, blob))
            max_raw, max_blob, chunks = max(max_raw, len(raw)), max(max_blob, len(blob)), chunks + 1
        if store_id % 1000 == 0:
            print(f"packed stores {store_id:,}/{len(stores):,}", flush=True)

    for bid, terms in enumerate(postings):
        raw, blob, digest = packed(dict(sorted(terms.items())), compressor)
        dst.execute("INSERT INTO search_buckets VALUES(?,?,?,?,?)", (bid, "msgpack+zstd", len(raw), digest, blob))

    auxiliary = {}
    for table in ("crawl_batches", "metadata"):
        auxiliary[table] = {"columns": columns(src, table), "rows": [list(r) for r in src.execute(f"SELECT * FROM {table} ORDER BY 1")]}
    raw, blob, digest = packed(auxiliary, compressor)
    dst.execute("INSERT INTO auxiliary_bundles VALUES(?,?,?,?,?)", ("normalized_auxiliary", "msgpack+zstd", len(raw), digest, blob))

    digests = {}
    store_order = '(SELECT first_seen FROM stores s WHERE s.store_uuid=products.store_uuid),store_uuid,first_seen,product_uuid'
    event_order = '(SELECT first_seen FROM stores s WHERE s.store_uuid=events.store_uuid),store_uuid,id'
    for table, order in (("stores", "first_seen,store_uuid"), ("products", store_order), ("events", event_order), ("crawl_batches", "batch_id"), ("metadata", "key")):
        count, digest = source_digest(src, table, order)
        digests[table] = {"count": count, "sha256": digest}
    source_meta = dict(src.execute("select key,value from metadata"))
    data_revision = hashlib.sha256(json.dumps(digests, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    info = {"definition_version": "foodradar-native-price-v2", "data_revision": data_revision, "source_revision": source_meta.get("source_revision", "local"),
            "raw_source": source_meta.get("raw_source", ""),
            "raw_snapshot_count": int(source_meta.get("raw_snapshot_count", 0)),
            "application": "FoodRadar", "schema_version": 2,
            "active_stores": active_stores, "active_products": active_products,
            "current_verified_products": current_verified_products,
            "active_comparable_products": active_comparable_products,
            "seven_day_reference_products": seven_day_reference_products,
            "archive_reference_products": archive_reference_products,
            "archive_30pct_candidates": archive_30pct_candidates,
            "seven_day_verified_dates": verified_history_days,
            "seven_day_history_incomplete": len(verified_history_days) < 3,
            "active_platform_promotions": active_platform_promotions,
            "active_30pct_drops": active_30pct_drops,
            "baseline_processed_at": baseline_time, "latest_processed_at": latest_time,
            "source_counts": {k: v['count'] for k,v in digests.items()},
            "latest_batch": source_meta.get("latest_batch", ""),
            "shard_id": source_meta.get("rebuild_shard_id"), "total_shards": source_meta.get("rebuild_total_shards"),
            "format": 1, "source": str(source), "buckets": bucket_count, "chunks": chunks,
            "products": product_gid, "max_raw_bundle": max_raw, "max_compressed_bundle": max_blob,
            "source_tables": digests}
    for key, value in info.items():
        dst.execute("INSERT INTO metadata VALUES(?,?)", (key, json.dumps(value, ensure_ascii=False, separators=(",", ":"))))
    dst.commit()
    dst.close(); src.close()
    info["database_bytes"] = out_path.stat().st_size
    info["build_seconds"] = round(time.perf_counter() - started, 3)
    info["packed_rows"] = chunks + len(stores) + bucket_count + 1 + len(info) - 2
    print(json.dumps(info, ensure_ascii=False, indent=2))


def unpack_blob(row, decompressor):
    raw = decompressor.decompress(row[0])
    if hashlib.sha256(raw).hexdigest() != row[1]:
        raise ValueError("bundle checksum mismatch")
    return msgpack.unpackb(raw, raw=False)


def verify(source, packed_db):
    started = time.perf_counter(); dec = zstd.ZstdDecompressor()
    src = sqlite3.connect(source); dst = sqlite3.connect(packed_db)
    expected = {r[0]: json.loads(r[1]) for r in dst.execute("SELECT key,value FROM metadata")}["source_tables"]
    hashes = {t: hashlib.sha256() for t in TABLES}; counts = defaultdict(int)
    for blob, checksum in dst.execute("SELECT payload,checksum FROM store_bundles ORDER BY store_id,chunk_no"):
        value = unpack_blob((blob, checksum), dec)
        if value["store"] is not None:
            hashes["stores"].update(msgpack.packb(value["store"], use_bin_type=True)); counts["stores"] += 1
        for _, row in value["products"]:
            hashes["products"].update(msgpack.packb(row, use_bin_type=True)); counts["products"] += 1
        for row in value["events"]:
            hashes["events"].update(msgpack.packb(row, use_bin_type=True)); counts["events"] += 1
    auxrow = dst.execute("SELECT payload,checksum FROM auxiliary_bundles WHERE name='normalized_auxiliary'").fetchone()
    aux = unpack_blob(auxrow, dec)
    for table in ("crawl_batches", "metadata"):
        for row in aux[table]["rows"]:
            hashes[table].update(msgpack.packb(row, use_bin_type=True)); counts[table] += 1
    result = {}
    for table in TABLES:
        result[table] = {"count": counts[table], "sha256": hashes[table].hexdigest(),
                         "expected": expected[table], "ok": counts[table] == expected[table]["count"] and hashes[table].hexdigest() == expected[table]["sha256"]}
    result["ok"] = all(result[t]["ok"] for t in TABLES)
    result["verify_seconds"] = round(time.perf_counter() - started, 3)
    src.close(); dst.close()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["ok"]: raise SystemExit(1)


def main():
    ap = argparse.ArgumentParser(); sub = ap.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build"); b.add_argument("source"); b.add_argument("output"); b.add_argument("--buckets", type=int, default=1024); b.add_argument("--level", type=int, default=10); b.add_argument("--max-items", type=int, default=2048)
    v = sub.add_parser("verify"); v.add_argument("source"); v.add_argument("packed")
    args = ap.parse_args()
    if args.cmd == "build": build(args.source, args.output, args.buckets, args.level, args.max_items)
    else: verify(args.source, args.packed)


if __name__ == "__main__": main()
