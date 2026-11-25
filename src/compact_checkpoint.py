"""Compact *incremental checkpoint*, preserving query semantics at restore time.

HF original HTTP archives remain immutable evidence. The checkpoint holds current
catalog, exact integer price observations, and last-seen lifecycle state. Derived
SHA hashes, bounded quote lists and full-text indexes are regenerated. Historical
raw source hashes and non-user-visible event JSON remain in HF Raw. Never treat
an omitted hash as proof of verification.
"""
from __future__ import annotations
import hashlib
import json
import sqlite3
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from fractions import Fraction
from pathlib import Path

import msgpack
import zstandard as zstd

KINDS = ("NONE", "SAME_SKU_BXGY", "DIRECT_DISCOUNT")
OMITTED = {"store_uuid", "state_hash", "menu_quote_history", "price_evidence_source_hash"}
STORE_HASH_FIELDS = ("name", "address", "city", "locality", "latitude", "longitude",
                     "rating", "review_count", "order_url", "is_open")
PRODUCT_HASH_FIELDS = ("product_name", "category", "description", "price", "quantity",
                       "promo_type", "effective_price", "order_url", "is_open")


def binary_uuid(value):
    try:
        u = uuid.UUID(value)
        return u.bytes if str(u) == value else value
    except (ValueError, TypeError, AttributeError):
        return value


def text_uuid(value):
    return str(uuid.UUID(bytes=value)) if isinstance(value, bytes) else value


def _pack(value):
    return zstd.ZstdCompressor(level=10, write_content_size=True).compress(
        msgpack.packb(value, use_bin_type=True))


def _unpack(value):
    return msgpack.unpackb(zstd.ZstdDecompressor().decompress(value), raw=False,
                           strict_map_key=False)


def create(source: str | Path, target: str | Path, *, store_ids=None) -> dict:
    source, target = Path(source), Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.unlink()
    db = sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    output = sqlite3.connect(target)
    output.executescript("""PRAGMA page_size=1024;
      PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
      CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
      CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
      CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
    """)
    dictionaries = defaultdict(dict)

    def token(kind, value):
        if value is None or not isinstance(value, str):
            return value
        values = dictionaries[kind]
        if value not in values:
            values[value] = len(values)
        return values[value]

    def encode(col, value):
        if col.endswith("_uuid"):
            return binary_uuid(value)
        if col == "catalog_web":
            details = json.loads(value or "{}")
            if not isinstance(details, dict):
                raise ValueError("Unexpected catalog_web")
            return [[token("catalog_key", key),
                     token("catalog:" + key, json.dumps(val, ensure_ascii=False, separators=(",", ":")))]
                    for key, val in sorted(details.items())]
        return token(col, value)

    scols = [r[1] for r in db.execute("PRAGMA table_info(stores)")
             if r[1] not in ("store_uuid", "state_hash")]
    pcols = [r[1] for r in db.execute("PRAGMA table_info(products)") if r[1] not in OMITTED]
    batches = [tuple(r) for r in db.execute(
        "SELECT DISTINCT batch_id,observed_at FROM product_price_observations ORDER BY observed_at,batch_id")]
    bidx = {b: i for i, b in enumerate(batches)}
    width = (len(batches) + 7) // 8
    counters = {"stores": 0, "products": 0, "observations": 0}
    identities=(sorted(store_ids) if store_ids is not None else
                [r[0] for r in db.execute("SELECT store_uuid FROM stores ORDER BY store_uuid")])
    for sid in identities:
        store = db.execute("SELECT * FROM stores WHERE store_uuid=?", (sid,)).fetchone()
        products = list(db.execute("SELECT * FROM products WHERE store_uuid=? ORDER BY product_uuid", (sid,)))
        ids = {p["product_uuid"]: i for i, p in enumerate(products)}
        state = [[encode(c, store[c]) for c in scols],
                 [[encode(c, p[c]) for c in pcols] for p in products]]
        groups = defaultdict(lambda: defaultdict(int))
        for row in db.execute("""SELECT product_uuid,batch_id,observed_at,
                menu_price_cents,promotion_type,buy_quantity,free_quantity,
                received_quantity,original_total_cents,payable_total_cents
                FROM product_price_observations WHERE store_uuid=?""", (sid,)):
            pid, batch, at, menu, kind, buy, free, received, original, payable = row
            key = (menu, KINDS.index(kind), buy, free, received, original, payable)
            groups[ids[pid]][key] |= (1 << bidx[(batch, at)])
            counters["observations"] += 1
        prices = [[pid, [[list(q), mask.to_bytes(width, "little")] for q, mask in values.items()]]
                  for pid, values in sorted(groups.items())]
        output.execute("INSERT INTO shop_state VALUES(?,?)", (binary_uuid(sid), _pack(state)))
        output.execute("INSERT INTO price_history VALUES(?,?)", (binary_uuid(sid), _pack(prices)))
        counters["stores"] += 1
        counters["products"] += len(products)
    for kind, values in dictionaries.items():
        output.execute("INSERT INTO dictionary VALUES(?,?)", (kind, _pack(list(values))))
    for key, value in {
        "__store_columns": scols, "__product_columns": pcols,
        "__batch_keys": batches,
        "__crawl_batches": [list(r) for r in db.execute("SELECT * FROM crawl_batches")],
        "__metadata": [list(r) for r in db.execute("SELECT * FROM metadata")],
    }.items():
        output.execute("INSERT INTO dictionary VALUES(?,?)", (key, _pack(value)))
    output.commit()
    expected = {"stores": db.execute("SELECT COUNT(*) FROM stores").fetchone()[0],
                "products": db.execute("SELECT COUNT(*) FROM products").fetchone()[0],
                "observations": db.execute("SELECT COUNT(*) FROM product_price_observations").fetchone()[0]}
    if store_ids is not None:
        if counters["stores"] != len(identities):
            raise ValueError("Sharded snapshot omitted stores")
        expected = counters
    if counters != expected or output.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("Compact checkpoint counts or integrity mismatch")
    size = target.stat().st_size
    output.close()
    db.close()
    return {**counters, "bytes": size, "format": "foodradar-compact-v1"}


def expand(source: str | Path, target: str | Path, *, append=False, finalize=True, lean=False) -> dict:
    """Recreate an ephemeral SQLite projection for legacy ETL and static export.

    Not an alternative authoritative Raw store. Original observation hashes can
    only be rechecked by retrieving their batch from immutable HF Raw.
    """
    from serving_state import ensure_schema_and_migrations, canonical_hash, refresh_product_search
    source, target = Path(source), Path(target)
    if target.exists() and not append:
        target.unlink()
    compact = sqlite3.connect(f"file:{source.resolve()}?mode=ro", uri=True)
    compact.execute("PRAGMA query_only=ON")
    dictionaries = {key: _unpack(blob) for key, blob in compact.execute("SELECT key,payload FROM dictionary")}
    from compact_templates import Templates
    templates = Templates(dictionaries)
    from compact_payload import PayloadCodec, KEY as PAYLOAD_KEY
    payload_codec=PayloadCodec(dictionaries.get(PAYLOAD_KEY))
    scols, pcols, batches = (dictionaries[key] for key in
                           ("__store_columns", "__product_columns", "__batch_keys"))

    def decode(col, value):
        if col.endswith("_uuid"):
            return text_uuid(value)
        if col == "catalog_web":
            return json.dumps({
                dictionaries["catalog_key"][key]:
                    json.loads(dictionaries["catalog:"+dictionaries["catalog_key"][key]][index])
                for key, index in value
            }, ensure_ascii=False)
        return (dictionaries[col][value] if col in dictionaries and isinstance(value, int)
                and not isinstance(value, bool) else value)

    db = sqlite3.connect(target)
    db.execute("PRAGMA journal_mode=OFF")
    db.execute("PRAGMA synchronous=OFF")
    if lean:
        from working_quotes import initialize, BulkQuoteWriter
        initialize(db)
        writer=BulkQuoteWriter(db)
    else:
        writer=None
    ensure_schema_and_migrations(db)
    batch_rows = dictionaries["__crawl_batches"]
    meta_rows = dictionaries["__metadata"]
    original_observations = 0
    stores = products = 0
    for blob_sid, state_blob in compact.execute("SELECT store,payload FROM shop_state"):
        sid = text_uuid(blob_sid)
        state, product_data = payload_codec.unpack(state_blob)
        s = dict(zip(scols, [decode(c, v) for c, v in zip(scols, state)]))
        s["store_uuid"] = sid
        s["state_hash"] = canonical_hash({k: s[k] for k in STORE_HASH_FIELDS})
        ins = list(s)
        db.execute(f"INSERT INTO stores({','.join(ins)}) VALUES({','.join('?' for _ in ins)})",
                   [s[k] for k in ins])
        stores += 1
        p_rows = []
        for record in templates.rows(product_data):
            p = dict(zip(pcols, [decode(c, v) for c, v in zip(pcols, record)]))
            p["store_uuid"] = sid
            p["state_hash"] = canonical_hash({k: p[k] for k in PRODUCT_HASH_FIELDS})
            p["menu_quote_history"] = "[]"
            p["price_evidence_source_hash"] = None
            p_rows.append(p)
        price_blob = compact.execute("SELECT payload FROM price_history WHERE store=?", (blob_sid,)).fetchone()
        if price_blob is None:
            raise ValueError("Missing store price history")
        raw_prices = payload_codec.unpack(price_blob[0])
        histories = defaultdict(list)
        for ordinal, variants in raw_prices:
            if ordinal >= len(p_rows):
                raise ValueError("Out-of-range SKU ordinal")
            pid = p_rows[ordinal]["product_uuid"]
            for fields, mask_bytes in variants:
                menu, kind, buy, free, received, original, payable = fields
                if not (menu > 0 and received > 0 and 0 <= kind < len(KINDS)):
                    raise ValueError("Invalid compact monetary fields")
                bits = int.from_bytes(mask_bytes, "little")
                while bits:
                    flag = bits & -bits
                    pos = flag.bit_length() - 1
                    if pos >= len(batches):
                        raise ValueError("Price mask exceeds verified batch keys")
                    batch, at = batches[pos]
                    observation=(batch,sid,pid,at,menu,KINDS[kind],buy,free,
                                 received,original,payable,"")
                    if writer:
                        writer.write(observation)
                    else:
                        db.execute("""INSERT INTO product_price_observations
                          (batch_id,store_uuid,product_uuid,observed_at,menu_price_cents,
                           promotion_type,buy_quantity,free_quantity,received_quantity,
                           original_total_cents,payable_total_cents,source_hash)
                          VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",observation)
                    label = "無" if kind == 0 else (
                        f"買{buy}送{free}" if kind == 1 else "直接折價")
                    if payable > 0:
                        histories[pid].append(
                            [at, round(float(Fraction(payable, 100*received)), 2),
                             menu/100.0, label, received])
                    original_observations += 1
                    bits ^= flag
        for p in p_rows:
            cutoff = (datetime.fromisoformat(p["last_seen"]) - timedelta(days=7)).isoformat()
            entries = [x for x in histories.get(p["product_uuid"], ())
                       if cutoff <= x[0] <= p["last_seen"]]
            entries.sort(key=lambda x: x[0])
            p["menu_quote_history"] = json.dumps(entries, ensure_ascii=False)
        if p_rows:
            columns = list(p_rows[0])
            db.executemany(
                f"INSERT INTO products({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                [[p[k] for k in columns] for p in p_rows])
            products += len(p_rows)
    if not append:
        if batch_rows:
            c = [r[1] for r in db.execute("PRAGMA table_info(crawl_batches)")]
            db.executemany(f"INSERT INTO crawl_batches({','.join(c)}) VALUES({','.join('?' for _ in c)})", batch_rows)
        db.executemany("INSERT INTO metadata(key,value) VALUES(?,?)", meta_rows)
    if finalize:
        refresh_product_search(db)
    db.commit()
    if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
        raise ValueError("Expanded checkpoint corrupted")
    db.close()
    compact.close()
    return {"stores": stores, "products": products, "observations": original_observations}


def create_shards(source: str | Path, folder: str | Path, *, shops_per_shard=500) -> list[dict]:
    """Nationwide-safe store partitions. Every file is individually under 20 MB."""
    folder=Path(folder)
    folder.mkdir(parents=True,exist_ok=True)
    with sqlite3.connect(source) as db:
        # Group nearby stores together; inside one large city automatically
        # split into bounded shop ranges. All identities are stable UUIDs.
        shops=[r[0] for r in db.execute("""SELECT store_uuid FROM stores
            ORDER BY CAST(COALESCE(latitude,0)*10 AS INTEGER),
                     CAST(COALESCE(longitude,0)*10 AS INTEGER),store_uuid""")]
    if not shops:
        raise ValueError("No shops in compact checkpoint")
    partitions=[shops[i:i+shops_per_shard] for i in range(0,len(shops),shops_per_shard)]
    result=[]
    for part,ids in enumerate(partitions):
        file=f"part-{part:05d}.db"
        path=folder/file
        stats=create(source,path,store_ids=ids)
        if stats["bytes"]>=20_000_000:
            raise ValueError("Oversized geographical shard; lower shops_per_shard")
        import hashlib
        with path.open("rb") as handle:
            sha=hashlib.file_digest(handle,"sha256").hexdigest()
        result.append({"path":"shards/"+file,"sha256":sha,**stats})
    return result


def expand_shards(folder: str | Path, parts: list[dict], target: str | Path, *, lean=False) -> dict:
    """Verify every part before reconstructing the ephemeral SQL projection."""
    import hashlib
    folder=Path(folder)
    if not parts:
        raise ValueError("Missing compact shard inventory")
    results={"stores":0,"products":0,"observations":0}
    for n,part in enumerate(parts):
        relative=Path(part["path"])
        if (relative.is_absolute() or ".." in relative.parts
                or relative.parts[0] != "shards" or len(relative.parts)!=2):
            raise ValueError("Untrusted compact shard path")
        path=folder/relative
        with path.open("rb") as handle:
            if hashlib.file_digest(handle,"sha256").hexdigest()!=part["sha256"]:
                raise ValueError("Corrupted compact shard")
        stats=expand(path,target,append=(n!=0),finalize=(n==len(parts)-1),lean=lean)
        if any(stats[key]!=part[key] for key in results):
            raise ValueError("Compact shard count mismatch")
        for key in results:results[key]+=stats[key]
    return results
