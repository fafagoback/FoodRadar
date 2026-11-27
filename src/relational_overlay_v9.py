"""FR-only relational SQLite overlay preserving the existing ultra-compact source.

Do not expand immutable compressed histories into SQL rows. The original
dictionary, shop_state and price_history tables are copied *unchanged*, then
lightweight SQL relational/search indexes are built over shared token IDs.
All serving rows refer to the original single copy of product text, through
the existing global dictionary. No crawl or HF write occurs here.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import json
import hashlib
import re
import sqlite3
import msgpack
import zstandard as zstd
import unicodedata

from compact_checkpoint import binary_uuid
from relational_compact import _current

VERSION=9
BUCKETS=512
SCHEMA="""
CREATE TABLE relational_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE relational_stores(
  id INTEGER PRIMARY KEY,
  uuid BLOB NOT NULL UNIQUE,
  name TEXT,
  city TEXT,
  locality TEXT,
  lat REAL,
  lon REAL,
  rating REAL,
  review_count INTEGER,
  active INTEGER NOT NULL
);
CREATE TABLE relational_products(
  id INTEGER PRIMARY KEY,
  name_token INTEGER NOT NULL,
  category_token INTEGER,
  description_token INTEGER,
  CHECK(name_token>=-1)
);
CREATE INDEX relational_product_names ON relational_products(name_token);
CREATE TABLE relational_prices(
  id INTEGER PRIMARY KEY,
  original_cents INTEGER NOT NULL,
  effective_cents INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  promotion TEXT NOT NULL,
  UNIQUE(original_cents,effective_cents,quantity,promotion)
);
CREATE TABLE relational_store_products(
  store_id INTEGER NOT NULL REFERENCES relational_stores(id),
  sku BLOB NOT NULL,
  product_id INTEGER NOT NULL REFERENCES relational_products(id),
  price_id INTEGER NOT NULL REFERENCES relational_prices(id),
  active INTEGER NOT NULL,
  deal INTEGER NOT NULL,
  promotion INTEGER NOT NULL,
  discount_pct REAL,
  PRIMARY KEY(store_id,sku)
) WITHOUT ROWID;
CREATE INDEX relational_product_shops ON relational_store_products(product_id,store_id);
CREATE INDEX relational_deals ON relational_store_products(discount_pct DESC,store_id) WHERE deal=1;
CREATE INDEX relational_store_rating ON relational_stores(rating DESC,id ASC);
CREATE TABLE relational_frontend_index(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;
CREATE VIRTUAL TABLE relational_geo USING rtree(id,min_lat,max_lat,min_lon,max_lon);

"""

def normalize_text(value):
    return unicodedata.normalize("NFKC",str(value or "")).casefold()

def search_terms(text):
    value=re.sub(r"\s+","",normalize_text(text))
    if not value:return set()
    tokens={"b:"+value[i:i+2] for i in range(len(value)-1)}
    tokens.update("t:"+value[i:i+3] for i in range(len(value)-2))
    return tokens

def bucket(term):
    return int.from_bytes(hashlib.sha256(term.encode("utf-8")).digest()[:2],"big")%BUCKETS

def build(collection, target, *, source_bytes=None):
    """FR single-shard checkpoint; byte-exact source copies, no decoded quote rewrite."""
    if len(collection.parts)!=1:
        raise ValueError("Only a single verified FR checkpoint shard is supported")
    dest=Path(target)
    temp=dest.with_suffix(dest.suffix+".building")
    if dest.exists() or temp.exists():
        raise FileExistsError("Refusing to overwrite a checkpoint or candidate")
    dest.parent.mkdir(parents=True,exist_ok=True)
    source=collection.parts[0]
    source_path=source.path
    original_size=source_path.stat().st_size
    counts=Counter()
    # Copy the original 18MB compact state and keep its exact original rows.
    try:
        with sqlite3.connect(f"file:{source_path.resolve()}?mode=ro",uri=True) as old:
            db=sqlite3.connect(temp)
            try:
                old.backup(db)
                db.execute("PRAGMA foreign_keys=ON")
                db.executescript(SCHEMA)
                tokens={}
                for field in ("product_name","category","description"):
                    terms=source.d.get(field,[])
                    tokens[field]={str(x):i for i,x in enumerate(terms)}
                defs={}
                prices={}
                # A numeric read index contains no duplicate product names or
                # full price rows. Names come from the existing dictionary;
                # prices from the SQL price_terms table.
                fast_shops=[]
                compressor=zstd.ZstdCompressor(level=12,write_content_size=True)
                # Stable, local integer IDs yield compact foreign keys.
                for store,products,quotes in collection.shops():
                    sid=store["store_uuid"]
                    db.execute("""INSERT INTO relational_stores
                        (uuid,name,city,locality,lat,lon,rating,review_count,active)
                        VALUES(?,?,?,?,?,?,?,?,?)""",
                        (binary_uuid(sid),store.get("name"),store.get("city"),
                         store.get("locality"),store.get("latitude"),store.get("longitude"),
                         store.get("rating"),store.get("review_count"),
                         int(store.get("status")=="active")))
                    store_id=db.execute("SELECT last_insert_rowid()").fetchone()[0]
                    if store.get("latitude") is not None and store.get("longitude") is not None:
                        lat=float(store["latitude"]);lon=float(store["longitude"])
                        if not (-90<=lat<=90 and -180<=lon<=180):
                            raise ValueError("Bad store coordinates")
                        db.execute("INSERT INTO relational_geo VALUES(?,?,?,?,?)",
                                   (store_id,lat,lat,lon,lon))
                    quick_store=[store_id,store.get("name"),store.get("city"),
                        store.get("locality"),store.get("latitude"),
                        store.get("longitude"),store.get("rating"),
                        store.get("review_count"),
                        int(store.get("status")=="active"),
                        store.get("first_seen"),store.get("last_seen"),[]]
                    fast_shops.append(quick_store)
                    seen=set()
                    for ordinal,item in enumerate(products):
                        sku=item.get("product_uuid")
                        if not sku or sku in seen:raise ValueError("Duplicate store SKU")
                        seen.add(sku)
                        key=tuple(tokens[field].get(str(item.get(field) or ""),-1)
                                  for field in ("product_name","category","description"))
                        # If a present value has no global dictionary token, bail;
                        # silently treating it as -1 would merge different goods.
                        for field,k in zip(("product_name","category","description"),key):
                            if item.get(field) not in ("",None) and k<0:
                                raise ValueError("Missing checkpoint product token: "+field)
                        pid=defs.get(key)
                        if pid is None:
                            db.execute("""INSERT INTO relational_products
                                (name_token,category_token,description_token)
                                VALUES(?,?,?)""",key)
                            pid=db.execute("SELECT last_insert_rowid()").fetchone()[0]
                            defs[key]=pid
                            # Product names are referenced by existing token IDs only.
                        term=_current(item)
                        # Current price terms have kind CURRENT and zero observation fields.
                        price=(term[1],term[2],term[3],term[4])
                        price_id=prices.get(price)
                        if price_id is None:
                            db.execute("""INSERT INTO relational_prices
                                (original_cents,effective_cents,quantity,promotion)
                                VALUES(?,?,?,?)""",price)
                            price_id=db.execute("SELECT last_insert_rowid()").fetchone()[0]
                            prices[price]=price_id
                        db.execute("""INSERT INTO relational_store_products
                            (store_id,sku,product_id,price_id,active,deal,promotion,discount_pct)
                            VALUES(?,?,?,?,?,?,?,?)""",(
                              store_id,binary_uuid(sku),pid,price_id,
                              int(item.get("status")=="active"),
                              int(bool(item.get("seven_day_is_price_deal"))),
                              int(bool(item.get("promo_type") not in (None,"","NONE","無"))),
                              item.get("seven_day_discount_pct") or 0,
                        ))
                        flags=(int(item.get("status")=="active") |
                            (int(bool(item.get("seven_day_is_price_deal")))<<1) |
                            (int(bool(item.get("is_price_drop")))<<2) |
                            (int(bool(item.get("delivery_available")))<<3))
                        quick_store[11].append([ordinal,key[0],key[1],price_id,
                            flags,item.get("seven_day_discount_pct") or 0,
                            item.get("seven_day_discount_amount") or 0,
                            str(item.get("first_seen") or "")[:10]])
                        counts["links"]+=1
                    # Check only. Price evidence stays in the original compressed
                    # price_history table unchanged, not expanded per observation.
                    counts["stores"]+=1
                    counts["observations"]+=len(quotes)
                    if counts["stores"]%200==0:db.commit()
                counts["definitions"]=len(defs)
                counts["prices"]=len(prices)
                # Small compressed name blocks avoid fetching the entire
                # 180k-name global dictionary during a browser search.
                # Group products by store, already referenced by integer IDs;
                # browser can scan once in memory rather than issuing thousands
                # of remote SQLite page requests for a multi-way join.
                fast_shops.sort(key=lambda s:(
                    -(float(s[6]) if s[6] is not None else 0),
                    s[0]))
                browse=compressor.compress(msgpack.packb(fast_shops,use_bin_type=True))
                db.execute("INSERT INTO relational_frontend_index VALUES(?,?)",
                           ("shops",browse))
                counts["frontend_index_bytes"]=len(browse)
                del fast_shops
                for k,v in {
                    "format":"foodradar-relational-overlay-v9",
                    "counts":dict(counts),
                    "batch":collection.meta["latest_batch"],
                    "original_bytes":original_size,
                    "source_codec":"compact-original-v3"
                }.items():
                    db.execute("INSERT INTO relational_meta VALUES(?,?)",
                               (k,json.dumps(v,ensure_ascii=False,sort_keys=True)))
                db.commit()
                if db.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
                    raise ValueError("SQLite integrity check failed")
                if list(db.execute("PRAGMA foreign_key_check")):
                    raise ValueError("Orphan relational references")
                src_counts=collection.stats()
                if (src_counts["stores"]!=counts["stores"]
                    or src_counts["products"]!=counts["links"]
                    or src_counts["observations"]!=counts["observations"]):
                    raise ValueError("Source data counts differ")
                db.execute("ANALYZE")
                db.commit()
                # Change the output only; HF source checkpoint stays byte-exact.
                # 4 KiB pages sharply reduce remote HTTP Range round trips.
                db.execute("PRAGMA journal_mode=DELETE")
                db.execute("PRAGMA page_size=4096")
                db.execute("VACUUM")
                if db.execute("PRAGMA page_size").fetchone()[0]!=4096:
                    raise ValueError("Serving DB page size conversion failed")
            finally:
                db.close()
        temp.rename(dest)
        return {**counts,"original_bytes":original_size,
                "candidate_bytes":dest.stat().st_size,
                "schema_version":VERSION}
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
