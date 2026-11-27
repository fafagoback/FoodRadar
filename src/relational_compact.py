"""Optimized SQL-serving candidate: shared product definitions, shared price terms,
one store/SKU relation each, and compressed per-store auxiliary and history data.

The original verified compact SQLite stays untouched. This schema is the
read-only serving candidate, NOT a rewrite of verified historical evidence.
"""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timedelta,timezone
from decimal import Decimal,ROUND_HALF_UP
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import sqlite3
import msgpack
import zstandard as zstd

TW=timezone(timedelta(hours=8))
SCHEMA_VERSION=2
SCHEMA="""
PRAGMA page_size=4096;
CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE stores(
 id INTEGER PRIMARY KEY,uuid TEXT NOT NULL UNIQUE,name TEXT,city TEXT,
 locality TEXT,address TEXT,latitude REAL,longitude REAL,rating REAL,
 review_count INTEGER,status TEXT,is_open INTEGER,first_seen TEXT,last_seen TEXT
);
CREATE TABLE products(
 id INTEGER PRIMARY KEY,signature BLOB NOT NULL UNIQUE,
 product_name TEXT NOT NULL,category TEXT NOT NULL,description TEXT NOT NULL
);
CREATE TABLE price_terms(
 id INTEGER PRIMARY KEY,kind TEXT NOT NULL,menu INTEGER NOT NULL,
 effective INTEGER NOT NULL,quantity INTEGER NOT NULL,
 promo TEXT NOT NULL,buy INTEGER NOT NULL,free INTEGER NOT NULL,
 received INTEGER NOT NULL,original INTEGER NOT NULL,payable INTEGER NOT NULL,
 UNIQUE(kind,menu,effective,quantity,promo,buy,free,received,original,payable)
);
CREATE TABLE store_products(
 store_id INTEGER NOT NULL,sku TEXT NOT NULL,product_id INTEGER NOT NULL,
 price_id INTEGER NOT NULL,active INTEGER NOT NULL,delivery INTEGER,
 discount_pct REAL,seven_day_deal INTEGER,first_seen TEXT,last_seen TEXT,
 PRIMARY KEY(store_id,sku),
 FOREIGN KEY(store_id) REFERENCES stores(id),
 FOREIGN KEY(product_id) REFERENCES products(id),
 FOREIGN KEY(price_id) REFERENCES price_terms(id)
) WITHOUT ROWID;
CREATE TABLE store_aux(
 store_id INTEGER PRIMARY KEY REFERENCES stores(id),
 product_state BLOB NOT NULL, history BLOB NOT NULL
);
CREATE INDEX idx_stores_city ON stores(city,locality);
CREATE INDEX idx_store_product_id ON store_products(product_id,store_id);
CREATE INDEX idx_store_deals ON store_products(seven_day_deal,discount_pct DESC);
CREATE VIRTUAL TABLE product_fts USING fts5(
 product_name,category,description,content='products',content_rowid='id'
);
CREATE VIRTUAL TABLE store_geo USING rtree(id,min_lat,max_lat,min_lon,max_lon);
"""

def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(",",":"),default=str)
def _cents(value):
    return 0 if value is None else int(
        (Decimal(str(value))*100).quantize(Decimal("1"),rounding=ROUND_HALF_UP))
def _content_key(product):
    fields=tuple(str(product.get(k) or "") for k in
                 ("product_name","category","description"))
    return hashlib.sha256(_json(fields).encode("utf-8")).digest()[:16],fields
def _current(product):
    return ("CURRENT",_cents(product.get("price")),
            _cents(product.get("effective_price")),
            int(product.get("quantity") or 1),str(product.get("promo_type") or ""),
            0,0,0,0,0)
def _observation(quote):
    return ("OBSERVATION",int(quote[4]),0,0,str(quote[5]),
            int(quote[6]),int(quote[7]),int(quote[8]),
            int(quote[9]),int(quote[10]))
def _compress(value,encoder):
    return encoder.compress(msgpack.packb(value,use_bin_type=True))
def decompress(value):
    return msgpack.unpackb(zstd.ZstdDecompressor().decompress(value),raw=False)

def build(shops,target,batch,*,retention_days=60):
    if len(batch)!=14 or not batch.isdigit():raise ValueError("Invalid batch")
    ref=datetime.strptime(batch,"%Y%m%d%H%M%S").replace(tzinfo=TW)
    cutoff=(ref-timedelta(days=retention_days)).isoformat()
    latest=ref.isoformat()
    path=Path(target)
    if path.exists() or path.with_suffix(path.suffix+".building").exists():
        raise FileExistsError("Candidate path already exists")
    path.parent.mkdir(parents=True,exist_ok=True)
    temp=path.with_suffix(path.suffix+".building")
    counts=Counter()
    encoder=zstd.ZstdCompressor(level=15,write_content_size=True)
    try:
        with sqlite3.connect(temp) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA journal_mode=MEMORY")
            db.execute("PRAGMA temp_store=MEMORY")
            db.executescript(SCHEMA)
            @lru_cache(maxsize=8192)
            def shared_product(key,fields):
                db.execute("INSERT OR IGNORE INTO products(signature,product_name,category,description) VALUES(?,?,?,?)",
                           (key,*fields))
                row=db.execute("SELECT id,product_name,category,description FROM products WHERE signature=?",
                               (key,)).fetchone()
                if row is None or tuple(row[1:])!=fields:
                    raise ValueError("Product signature collision")
                return row[0]
            @lru_cache(maxsize=16384)
            def shared_price(term):
                db.execute("INSERT OR IGNORE INTO price_terms(kind,menu,effective,quantity,promo,buy,free,received,original,payable) VALUES(?,?,?,?,?,?,?,?,?,?)",term)
                row=db.execute("SELECT id FROM price_terms WHERE "+
                    " AND ".join(k+"=?" for k in
                    ("kind","menu","effective","quantity","promo","buy","free","received","original","payable")),term).fetchone()
                if row is None:raise ValueError("Missing shared price term")
                return row[0]
            for store,products,quotes in shops:
                sid=store.get("store_uuid")
                if not sid:raise ValueError("Missing store identifier")
                columns=("name","city","locality","address","latitude","longitude",
                         "rating","review_count","status","is_open",
                         "first_seen","last_seen")
                db.execute("INSERT INTO stores(uuid,"+",".join(columns)+") VALUES("+
                    ",".join("?" for _ in range(len(columns)+1))+")",
                    (sid,*(store.get(k) for k in columns)))
                store_id=db.execute("SELECT id FROM stores WHERE uuid=?",(sid,)).fetchone()[0]
                if store.get("latitude") is not None and store.get("longitude") is not None:
                    lat,lon=float(store["latitude"]),float(store["longitude"])
                    if not(-90<=lat<=90 and -180<=lon<=180):
                        raise ValueError("Invalid store location")
                    db.execute("INSERT INTO store_geo VALUES(?,?,?,?,?)",
                               (store_id,lat,lat,lon,lon))
                # Store auxiliary fields once; repeated JSON property names compress
                # across the whole store instead of 277k independent SQL rows.
                minimal=("store_uuid",*columns)
                extras={k:v for k,v in store.items() if k not in minimal}
                auxiliary=[]
                sku_index={}
                for product in products:
                    sku=product.get("product_uuid")
                    if not sku or sku in sku_index:
                        raise ValueError("Duplicate or missing store SKU")
                    sku_index[sku]=len(auxiliary)
                    ref_id=shared_product(*_content_key(product))
                    price_id=shared_price(_current(product))
                    active=int(product.get("status")=="active")
                    db.execute("""INSERT INTO store_products
                        (store_id,sku,product_id,price_id,active,delivery,
                         discount_pct,seven_day_deal,first_seen,last_seen)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (store_id,sku,ref_id,price_id,active,
                         product.get("delivery_available"),
                         product.get("seven_day_discount_pct"),
                         product.get("seven_day_is_price_deal"),
                         product.get("first_seen"),product.get("last_seen")))
                    excluded={"product_uuid","store_uuid","product_name",
                              "category","description","price","effective_price",
                              "quantity","promo_type"}
                    auxiliary.append([sku,{k:v for k,v in product.items() if k not in excluded}])
                    counts["store_products"]+=1
                histories=defaultdict(list)
                for quote in quotes:
                    if len(quote)<11 or quote[1]!=sid or quote[2] not in sku_index:
                        raise ValueError("Orphan or cross-shop observation")
                    at=quote[3]
                    if at<cutoff:
                        counts["expired_observations_ignored"]+=1
                        continue
                    if at>latest:raise ValueError("Future observation")
                    histories[sku_index[quote[2]]].append(
                        [quote[0],at,shared_price(_observation(quote))])
                    counts["observations"]+=1
                packed_history=[[i,entries] for i,entries in sorted(histories.items())]
                db.execute("INSERT INTO store_aux VALUES(?,?,?)",(
                    store_id,_compress([extras,auxiliary],encoder),
                    _compress(packed_history,encoder)))
                counts["stores"]+=1
                if counts["stores"]%200==0:db.commit()
            db.execute("INSERT INTO product_fts(product_fts) VALUES('rebuild')")
            counts["products"]=db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
            counts["price_terms"]=db.execute("SELECT COUNT(*) FROM price_terms").fetchone()[0]
            for key,value in {
                "schema_version":SCHEMA_VERSION,"batch":batch,
                "retention_days":retention_days,"counts":dict(counts)
            }.items():
                db.execute("INSERT INTO metadata VALUES(?,?)",(key,_json(value)))
            db.commit()
            if db.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
                raise ValueError("SQLite integrity check failed")
            if list(db.execute("PRAGMA foreign_key_check")):
                raise ValueError("SQLite foreign key failure")
            for table,count in (("stores",counts["stores"]),
                                ("store_products",counts["store_products"])):
                if db.execute("SELECT COUNT(*) FROM "+table).fetchone()[0]!=count:
                    raise ValueError("Relational row loss")
            if db.execute("SELECT COUNT(*) FROM store_aux").fetchone()[0]!=counts["stores"]:
                raise ValueError("Compressed store inventory mismatch")
            db.execute("ANALYZE")
            db.execute("VACUUM")
        temp.replace(path)
        return {**counts,"bytes":path.stat().st_size,"schema_version":SCHEMA_VERSION,
                "batch":batch}
    except BaseException:
        temp.unlink(missing_ok=True)
        raise
