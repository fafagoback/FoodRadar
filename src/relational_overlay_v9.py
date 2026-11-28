"""FR-only relational SQLite overlay preserving the existing ultra-compact source.

Do not expand immutable compressed histories into SQL rows. The original
dictionary, shop_state and price_history tables are copied *unchanged*, then
lightweight SQL relational/search indexes are built over shared token IDs.
All serving rows refer to the original single copy of product text, through
the existing global dictionary. No crawl or HF write occurs here.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime,timedelta
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
CREATE TABLE relational_prices(
  id INTEGER PRIMARY KEY,
  original_cents INTEGER NOT NULL,
  effective_cents INTEGER NOT NULL,
  quantity INTEGER NOT NULL,
  promotion TEXT NOT NULL,
  UNIQUE(original_cents,effective_cents,quantity,promotion)
);
-- The shared product and price definitions are relational; an integer
-- position links each store to its canonical SKU in compressed shop_state.
-- Storing the same 16-byte SKU and search flags here duplicates that source.
CREATE TABLE relational_store_products(
  store_id INTEGER NOT NULL REFERENCES relational_stores(id),
  ordinal INTEGER NOT NULL CHECK(ordinal>=0),
  product_id INTEGER NOT NULL REFERENCES relational_products(id),
  price_id INTEGER NOT NULL REFERENCES relational_prices(id),
  PRIMARY KEY(store_id,ordinal)
) WITHOUT ROWID;
CREATE TABLE relational_frontend_index(key TEXT PRIMARY KEY,payload BLOB NOT NULL) WITHOUT ROWID;

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
                rows=(collection.batch_rows() if hasattr(collection,"batch_rows") else
                    [(collection.meta["latest_batch"],datetime.strptime(
                        collection.meta["latest_batch"],"%Y%m%d%H%M%S").isoformat())])
                times=[str(row[1]) for row in rows if row[1]]
                if not times:raise ValueError("Verified source has no batch times")
                earliest,latest=min(times),max(times)
                week_cutoff=(datetime.fromisoformat(latest)-timedelta(days=7)).isoformat()
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
                    quick_store=[store_id,store.get("name"),store.get("city"),
                        store.get("locality"),store.get("latitude"),
                        store.get("longitude"),store.get("rating"),
                        store.get("review_count"),
                        int(store.get("status")=="active"),
                        store.get("first_seen"),store.get("last_seen"),[],
                        store.get("address"),store.get("order_url")]
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
                        # The ordinal is the exact store-local SKU position
                        # in the preserved, immutable shop_state payload.
                        # Current availability, discount, and promotion flags
                        # already live in the compressed frontend index.
                        db.execute("""INSERT INTO relational_store_products
                            (store_id,ordinal,product_id,price_id)
                            VALUES(?,?,?,?)""",(store_id,ordinal,pid,price_id))
                        native=item.get("price_evidence_status")=="VERIFIED_MENU"
                        stock=json.loads(item.get("catalog_web") or "{}")
                        if not isinstance(stock,dict):stock={}
                        open_menu=(store.get("last_seen")==latest and item.get("last_seen")==latest
                            and int(store.get("is_open") or 0)==1
                            and int(item.get("is_open") or 0)==1 and native)
                        verified=(open_menu and int(item.get("delivery_available") or 0)==1
                            and stock.get("is_sold_out") is not True
                            and stock.get("is_available") is not False)
                        raw_price=float(item.get("menu_quote_price") or item.get("price") or 0)
                        verified_price=float(item.get("menu_quote_effective_price") or 0) if native else 0
                        effective=verified_price if verified_price>0 else raw_price
                        price_normal=float(item.get("price") or 0)
                        ref=float(item.get("seven_day_reference_price") or 0)
                        percent=float(item.get("seven_day_discount_pct") or 0) if open_menu else 0
                        drop=bool(open_menu and percent>0 and ref>effective)
                        price_current=bool(verified and price_normal>0)
                        ptype=item.get("menu_quote_promo_type") or item.get("promo_type")
                        platform=bool(native and float(item.get("verified_discount_pct") or 0)>0
                            and (re.fullmatch(r"買[0-9]+送[0-9]+",str(ptype or ""))
                                 or ptype=="直接折價"))
                        listed=str(item.get("listing_started_at") or item.get("first_seen") or "")
                        new_item=bool(listed[:10]>earliest[:10]
                            and listed[:10]>str(store.get("first_seen") or "")[:10]
                            and listed>=week_cutoff)
                        searchable=(store.get("status")=="active"
                            and item.get("status")=="active" and effective>0
                            and int(store.get("is_open") or 0)==1
                            and int(item.get("is_open") or 0)==1)
                        flags=(int(searchable) | (int(drop and price_current and bool(item.get("seven_day_is_price_deal")))<<1)
                            | (int(drop and price_current)<<2) | (int(price_current)<<3))
                        quick_store[11].append([ordinal,key[0],key[1],price_id,
                            flags,percent if drop else 0,
                            float(item.get("seven_day_discount_amount") or 0) if drop else 0,
                            str(item.get("first_seen") or "")[:10],
                            int(new_item),int(platform and price_current),
                            int((platform or drop) and price_current),str(ptype or "")])
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
