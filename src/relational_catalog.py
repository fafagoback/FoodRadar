"""Normalized, read-only SQLite candidate built from verified compact checkpoints.

Product definitions with identical immutable description fields are stored once.
That does NOT assert cross-store SKU identity: each store retains its source SKU,
own pricing, listing status, and exact verified observation history.
This publisher is deliberately opt-in until HF browser Range/CORS and speed
are benchmarked. Never overwrite a verified compact checkpoint.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sqlite3

SCHEMA_VERSION = 1
TW = timezone(timedelta(hours=8))
TERM_COLS = (
    "kind", "price_cents", "effective_cents", "quantity", "promotion",
    "buy_quantity", "free_quantity", "received_quantity",
    "original_total_cents", "payable_total_cents",
)
STORE_COLS = (
    "name", "address", "city", "locality", "latitude", "longitude",
    "rating", "review_count", "order_url", "is_open", "status",
    "first_seen", "last_seen",
)
LISTING_COLS = (
    "is_open", "status", "order_url", "first_seen", "last_seen",
    "delivery_available", "is_price_deal", "discount_pct",
    "is_price_drop", "seven_day_is_price_deal",
)
SCHEMA = """
PRAGMA page_size=4096;
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE stores(
  id INTEGER PRIMARY KEY, uuid TEXT NOT NULL UNIQUE, name TEXT, address TEXT,
  city TEXT, locality TEXT, latitude REAL, longitude REAL, rating REAL,
  review_count INTEGER, order_url TEXT, is_open INTEGER, status TEXT,
  first_seen TEXT, last_seen TEXT, extra_json TEXT
);
CREATE TABLE products(
  id INTEGER PRIMARY KEY, signature BLOB NOT NULL UNIQUE,
  product_name TEXT NOT NULL, category TEXT NOT NULL, description TEXT NOT NULL
);
CREATE TABLE price_terms(
  id INTEGER PRIMARY KEY, kind TEXT NOT NULL, price_cents INTEGER NOT NULL,
  effective_cents INTEGER NOT NULL, quantity INTEGER NOT NULL,
  promotion TEXT NOT NULL, buy_quantity INTEGER NOT NULL,
  free_quantity INTEGER NOT NULL, received_quantity INTEGER NOT NULL,
  original_total_cents INTEGER NOT NULL, payable_total_cents INTEGER NOT NULL,
  UNIQUE(kind,price_cents,effective_cents,quantity,promotion,buy_quantity,
         free_quantity,received_quantity,original_total_cents,payable_total_cents)
);
CREATE TABLE store_products(
  store_id INTEGER NOT NULL REFERENCES stores(id),
  sku TEXT NOT NULL, product_id INTEGER NOT NULL REFERENCES products(id),
  price_term_id INTEGER NOT NULL REFERENCES price_terms(id),
  is_open INTEGER, status TEXT, order_url TEXT, first_seen TEXT, last_seen TEXT,
  delivery_available INTEGER, is_price_deal INTEGER, discount_pct REAL,
  is_price_drop INTEGER, seven_day_is_price_deal INTEGER, extra_json TEXT,
  PRIMARY KEY(store_id,sku)
) WITHOUT ROWID;
CREATE TABLE price_observations(
  store_id INTEGER NOT NULL, sku TEXT NOT NULL, batch_id TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  price_term_id INTEGER NOT NULL REFERENCES price_terms(id),
  PRIMARY KEY(store_id,sku,batch_id),
  FOREIGN KEY(store_id,sku) REFERENCES store_products(store_id,sku)
) WITHOUT ROWID;
CREATE INDEX idx_stores_city ON stores(city,locality);
CREATE INDEX idx_store_products_product ON store_products(product_id,store_id);
CREATE INDEX idx_store_products_price ON store_products(price_term_id);
CREATE INDEX idx_store_products_deals ON store_products(is_price_deal,discount_pct DESC);
CREATE INDEX idx_observation_time ON price_observations(store_id,sku,observed_at DESC);
CREATE VIRTUAL TABLE product_fts USING fts5(
  product_name,category,description,content='products',content_rowid='id',
  tokenize='unicode61'
);
CREATE VIRTUAL TABLE store_geo USING rtree(id,min_lat,max_lat,min_lon,max_lon);
"""

def _json(data):
    return json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(",",":"),default=str)

def _cents(value):
    return 0 if value is None else int(
        (Decimal(str(value))*100).quantize(Decimal("1"),rounding=ROUND_HALF_UP))

def product_key(product):
    """Content dedup is safe even without chain metadata; SKU identity stays local."""
    parts=tuple(str(product.get(k) or "") for k in
                ("product_name","category","description"))
    return hashlib.sha256(_json(parts).encode("utf-8")).digest()[:16],parts

def current_terms(product):
    return ("CURRENT",_cents(product.get("price")),
            _cents(product.get("effective_price")),
            int(product.get("quantity") or 1),
            str(product.get("promo_type") or ""),0,0,0,0,0)

def observation_terms(quote):
    # batch,store_uuid,sku,observed_at,menu_cents,promo_type,buy,free,
    # received,original_cents,payable_cents,source_hash
    return ("OBSERVATION",int(quote[4]),0,0,str(quote[5]),
            int(quote[6]),int(quote[7]),int(quote[8]),int(quote[9]),
            int(quote[10]))

def _batch_time(batch):
    if len(batch)!=14 or not batch.isdigit():
        raise ValueError("Invalid verified 14-digit batch")
    return datetime.strptime(batch,"%Y%m%d%H%M%S").replace(tzinfo=TW)

def build(shops,target,batch,*,retention_days=60):
    """Materialize streamed (store,products,quotes) with strict identity checks."""
    output=Path(target)
    if output.exists():
        raise FileExistsError("Refusing to overwrite existing SQLite")
    output.parent.mkdir(parents=True,exist_ok=True)
    temporary=output.with_suffix(output.suffix+".building")
    if temporary.exists():
        raise FileExistsError("Unfinished SQLite build already exists")
    now=_batch_time(batch).isoformat()
    cutoff=(_batch_time(batch)-timedelta(days=retention_days)).isoformat()
    counts=Counter()
    try:
        with sqlite3.connect(temporary) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.executescript(SCHEMA)
            @lru_cache(maxsize=16384)
            def product_id(digest,values):
                db.execute("""INSERT OR IGNORE INTO products
                    (signature,product_name,category,description) VALUES(?,?,?,?)""",
                    (digest,*values))
                row=db.execute("""SELECT id,product_name,category,description
                    FROM products WHERE signature=?""",(digest,)).fetchone()
                if row is None or tuple(row[1:])!=values:
                    raise ValueError("Shared product digest collision")
                return row[0]
            @lru_cache(maxsize=16384)
            def price_id(term):
                params=",".join("?" for _ in TERM_COLS)
                db.execute("INSERT OR IGNORE INTO price_terms("+
                           ",".join(TERM_COLS)+") VALUES("+params+")",term)
                where=" AND ".join(k+"=?" for k in TERM_COLS)
                row=db.execute("SELECT id FROM price_terms WHERE "+where,term).fetchone()
                if not row:raise ValueError("Missing shared price term")
                return row[0]
            for store,products,quotes in shops:
                sid=store.get("store_uuid")
                if not sid:raise ValueError("Missing store UUID")
                extra={k:v for k,v in store.items() if k not in STORE_COLS
                       and k!="store_uuid"}
                values=[store.get(k) for k in STORE_COLS]
                db.execute("INSERT INTO stores(uuid,"+",".join(STORE_COLS)+
                           ",extra_json) VALUES("+",".join("?" for _ in
                           range(len(STORE_COLS)+2))+")",
                           (sid,*values,_json(extra)))
                store_id=db.execute("SELECT id FROM stores WHERE uuid=?",
                                    (sid,)).fetchone()[0]
                if store.get("latitude") is not None and store.get("longitude") is not None:
                    lat,lon=float(store["latitude"]),float(store["longitude"])
                    if not (-90<=lat<=90 and -180<=lon<=180):
                        raise ValueError("Invalid store coordinates")
                    db.execute("INSERT INTO store_geo VALUES(?,?,?,?,?)",
                               (store_id,lat,lat,lon,lon))
                skus=set()
                for product in products:
                    sku=product.get("product_uuid")
                    if not sku or sku in skus:
                        raise ValueError("Missing/duplicate SKU in store "+sid)
                    skus.add(sku)
                    definition=product_id(*product_key(product))
                    term=price_id(current_terms(product))
                    listing=[product.get(k) for k in LISTING_COLS]
                    excluded={"store_uuid","product_uuid","product_name",
                              "category","description","price","quantity",
                              "effective_price","promo_type","state_hash",
                              "menu_quote_history",*LISTING_COLS}
                    extra={k:v for k,v in product.items() if k not in excluded}
                    db.execute("INSERT INTO store_products VALUES("+
                               ",".join("?" for _ in
                               range(5+len(LISTING_COLS)))+")",
                               (store_id,sku,definition,term,*listing,_json(extra)))
                    counts["store_products"]+=1
                for quote in quotes:
                    if len(quote)<11 or quote[1]!=sid or quote[2] not in skus:
                        raise ValueError("Orphan/cross-shop observation")
                    if quote[3]<cutoff:
                        counts["expired_observations_ignored"]+=1
                        continue
                    if quote[3]>now:
                        raise ValueError("Observation newer than batch")
                    db.execute("INSERT INTO price_observations VALUES(?,?,?,?,?)",
                               (store_id,quote[2],quote[0],quote[3],
                                price_id(observation_terms(quote))))
                    counts["observations"]+=1
                counts["stores"]+=1
                if counts["stores"]%500==0:db.commit()
            db.execute("INSERT INTO product_fts(product_fts) VALUES('rebuild')")
            counts["products"]=db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            counts["price_terms"]=db.execute("SELECT COUNT(*) FROM price_terms").fetchone()[0]
            for key,value in {"schema_version":SCHEMA_VERSION,"batch":batch,
                              "retention_days":retention_days,
                              "counts":dict(counts)}.items():
                db.execute("INSERT INTO metadata VALUES(?,?)",(key,_json(value)))
            db.commit()
            if db.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
                raise RuntimeError("SQLite integrity check failed")
            if list(db.execute("PRAGMA foreign_key_check")):
                raise RuntimeError("SQLite orphan foreign keys")
            for tab,count in (("stores",counts["stores"]),
                              ("store_products",counts["store_products"]),
                              ("price_observations",counts["observations"])):
                if db.execute("SELECT COUNT(*) FROM "+tab).fetchone()[0]!=count:
                    raise RuntimeError("Lost verified rows: "+tab)
            db.execute("ANALYZE")
            db.execute("VACUUM")
        temporary.replace(output)
        return {**counts,"bytes":output.stat().st_size,
                "batch":batch,"schema_version":SCHEMA_VERSION}
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
