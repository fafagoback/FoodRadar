# -*- coding: utf-8 -*-
"""Build the normalized CURRENT/EVENTS serving database from one raw crawl.

Identity is taken from Uber's ``store_uuid`` and menu item's ``identifier``.
Fallback identities are deliberately namespaced and counted in the batch report.
"""
from __future__ import annotations

try:
    from .city_normalization import canonical_city
    from .json_to_db import extract_promo_info, calculate_effective_price
    from .price_change import PriceChange
except ImportError:
    from city_normalization import canonical_city
    from json_to_db import extract_promo_info, calculate_effective_price
    from price_change import PriceChange

import argparse
import hashlib
import html
import json
import math
import os
import re
import sqlite3
import tarfile
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from .price_observation import price_observation_valid, menu_catalog_valid, menu_price_observation_valid
except ImportError:
    from price_observation import price_observation_valid, menu_catalog_valid, menu_price_observation_valid

try:
    import orjson

    def json_loads(data: bytes | str) -> Any:
        return orjson.loads(data)
except ImportError:
    def json_loads(data: bytes | str) -> Any:
        return json.loads(data)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


TW = timezone(timedelta(hours=8))
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
DEFAULT_MISSING_THRESHOLD = int(os.getenv("MISSING_STREAK_THRESHOLD", "3"))
EVENT_RETENTION_DAYS = int(os.getenv("EVENT_RETENTION_DAYS", "0"))


def canonical_hash(value: dict[str, Any]) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def fallback_id(kind: str, value: str) -> str:
    return f"fallback:{kind}:{hashlib.sha256(value.encode()).hexdigest()[:32]}"


def store_identity(doc: dict[str, Any]) -> tuple[str, bool]:
    value = str(doc.get("store_uuid") or "").strip().lower()
    if UUID_RE.match(value):
        return value, False
    url = str(doc.get("@id") or doc.get("potentialAction", {}).get("target", {}).get("urlTemplate") or "")
    # Only legacy HTML fallback documents should reach this branch.
    return fallback_id("store-url", url), True


def product_identity(item: dict[str, Any], store_uuid: str) -> tuple[str, bool]:
    value = str(item.get("identifier") or "").strip().lower()
    if UUID_RE.match(value):
        return value, False
    # Uber HTML JSON-LD does not always expose an item UUID. Keep the fallback
    # stable within a store and visibly namespaced; it is never confused with UUID.
    name = html.unescape(str(item.get("name") or "")).strip()
    return fallback_id("item-name", f"{store_uuid}\x1f{name}"), True


SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS crawl_batches(
  batch_id TEXT PRIMARY KEY, processed_at TEXT NOT NULL, stores_seen INTEGER NOT NULL,
  products_seen INTEGER NOT NULL, fallback_stores INTEGER NOT NULL, fallback_products INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS stores(
  store_uuid TEXT PRIMARY KEY, name TEXT NOT NULL, address TEXT, city TEXT, locality TEXT,
  latitude REAL, longitude REAL, rating REAL, review_count INTEGER, order_url TEXT, first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL, status TEXT NOT NULL, missing_streak INTEGER NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL, is_open INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS products(
  store_uuid TEXT NOT NULL, product_uuid TEXT NOT NULL, product_name TEXT NOT NULL,
  category TEXT, description TEXT, price REAL NOT NULL, quantity INTEGER NOT NULL DEFAULT 1,
  promo_type TEXT, effective_price REAL NOT NULL, order_url TEXT, first_seen TEXT NOT NULL,
  last_seen TEXT NOT NULL, status TEXT NOT NULL, missing_streak INTEGER NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL, recent_prices TEXT NOT NULL DEFAULT '[]',
  price_novel_vs_previous_3 INTEGER NOT NULL DEFAULT 0,
  reference_price REAL, discount_amount REAL NOT NULL DEFAULT 0,
  discount_pct REAL NOT NULL DEFAULT 0, is_price_deal INTEGER NOT NULL DEFAULT 0,
  price_direction TEXT NOT NULL DEFAULT 'unknown',
  is_price_drop INTEGER NOT NULL DEFAULT 0,
  price_change_amount REAL NOT NULL DEFAULT 0,
  price_change_percent REAL NOT NULL DEFAULT 0,
  is_open INTEGER NOT NULL DEFAULT 1,
  seven_day_reference_price REAL,
  seven_day_discount_pct REAL NOT NULL DEFAULT 0,
  seven_day_discount_amount REAL NOT NULL DEFAULT 0,
  seven_day_is_price_deal INTEGER NOT NULL DEFAULT 0,
  archive_reference_price REAL,
  archive_discount_pct REAL NOT NULL DEFAULT 0,
  archive_discount_amount REAL NOT NULL DEFAULT 0,
  archive_evidence_count INTEGER NOT NULL DEFAULT 0,
  archive_is_price_deal INTEGER NOT NULL DEFAULT 0,
  menu_quote_price REAL,
  menu_quote_history TEXT NOT NULL DEFAULT '[]',
  menu_quote_effective_price REAL,
  menu_quote_promo_type TEXT,
  menu_quote_quantity INTEGER,
  delivery_available INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY(store_uuid, product_uuid),
  FOREIGN KEY(store_uuid) REFERENCES stores(store_uuid)
);
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT, event_time TEXT NOT NULL, store_uuid TEXT NOT NULL,
  product_uuid TEXT, event_type TEXT NOT NULL, old_state TEXT, new_state TEXT
);
CREATE TABLE IF NOT EXISTS pending_store_changes(store_uuid TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS pending_product_changes(
  store_uuid TEXT NOT NULL, product_uuid TEXT NOT NULL,
  PRIMARY KEY(store_uuid, product_uuid)
);
CREATE INDEX IF NOT EXISTS idx_stores_first_seen ON stores(first_seen);
CREATE INDEX IF NOT EXISTS idx_stores_city ON stores(city);
CREATE INDEX IF NOT EXISTS idx_stores_name ON stores(name);
CREATE INDEX IF NOT EXISTS idx_products_first_seen ON products(first_seen);
CREATE INDEX IF NOT EXISTS idx_products_price ON products(effective_price, price);
CREATE INDEX IF NOT EXISTS idx_products_promo ON products(promo_type);
CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name);
CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_events_product_time ON events(store_uuid, product_uuid, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(event_time);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, event_time DESC);
CREATE VIRTUAL TABLE IF NOT EXISTS product_search USING fts5(
  store_uuid UNINDEXED, product_uuid UNINDEXED, store_name, product_name, category,
  tokenize='unicode61'
);
"""


def iter_documents(source: str | Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    if not isinstance(source, (str, Path)):
        yield from source
        return
    p = Path(source)
    if p.is_dir():
        for f in p.rglob("*.json"):
            try:
                doc = json_loads(f.read_bytes())
                if isinstance(doc, dict) and (doc.get("store_uuid") or doc.get("hasMenu")):
                    yield doc
            except (OSError, ValueError, TypeError):
                continue
    elif tarfile.is_tarfile(p):
        with tarfile.open(p, "r:*") as tf:
            for member in tf:
                if member.isfile() and member.name.endswith(".json") and not member.name.startswith("ApiRaw/"):
                    fh = tf.extractfile(member)
                    if fh:
                        try:
                            doc = json_loads(fh.read())
                            if isinstance(doc, dict) and (doc.get("store_uuid") or doc.get("hasMenu")):
                                yield doc
                        except (OSError, ValueError, TypeError):
                            continue
    else:
        raise ValueError(f"unsupported raw source: {source}")


def _num(value: Any, typ=float, default=0):
    try:
        converted = typ(value)
        return default if isinstance(converted, float) and not math.isfinite(converted) else converted
    except (TypeError, ValueError, OverflowError):
        return default


def _event(conn: sqlite3.Connection, now: str, sid: str, pid: str | None, kind: str, old, new):
    conn.execute(
        "INSERT INTO events(event_time,store_uuid,product_uuid,event_type,old_state,new_state) VALUES(?,?,?,?,?,?)",
        (now, sid, pid, kind, json_dumps(old) if old is not None else None,
         json_dumps(new) if new is not None else None),
    )


def refresh_product_search(conn: sqlite3.Connection, latest_observation_at: str | None = None) -> None:
    """Index latest-known active menu products, even when shops close or delivery stops."""
    if latest_observation_at is None:
        row = conn.execute("SELECT MAX(processed_at) FROM crawl_batches").fetchone()
        latest_observation_at = row[0] if row else None
    conn.execute("DELETE FROM product_search")
    if latest_observation_at:
        conn.execute(
            """INSERT INTO product_search
               SELECT p.store_uuid,p.product_uuid,s.name,p.product_name,p.category
               FROM products p JOIN stores s USING(store_uuid)
               WHERE p.status='active' AND s.status='active'
                 AND COALESCE(p.menu_quote_price,p.price)>0"""
        )


def ensure_schema_and_migrations(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    store_columns = {row[1] for row in conn.execute("PRAGMA table_info(stores)")}
    for name in ("latitude", "longitude"):
        if name not in store_columns:
            conn.execute(f"ALTER TABLE stores ADD COLUMN {name} REAL")
    if "is_open" not in store_columns:
        conn.execute("ALTER TABLE stores ADD COLUMN is_open INTEGER NOT NULL DEFAULT 1")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_coordinates ON stores(latitude, longitude)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stores_is_open ON stores(is_open)")
    product_columns = {row[1] for row in conn.execute("PRAGMA table_info(products)")}
    if "recent_prices" not in product_columns:
        conn.execute("ALTER TABLE products ADD COLUMN recent_prices TEXT NOT NULL DEFAULT '[]'")
    if "price_novel_vs_previous_3" not in product_columns:
        conn.execute("ALTER TABLE products ADD COLUMN price_novel_vs_previous_3 INTEGER NOT NULL DEFAULT 0")
    for name, definition in (
        ("reference_price", "REAL"),
        ("discount_amount", "REAL NOT NULL DEFAULT 0"),
        ("discount_pct", "REAL NOT NULL DEFAULT 0"),
        ("is_price_deal", "INTEGER NOT NULL DEFAULT 0"),
        ("price_direction", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("is_price_drop", "INTEGER NOT NULL DEFAULT 0"),
        ("price_change_amount", "REAL NOT NULL DEFAULT 0"),
        ("price_change_percent", "REAL NOT NULL DEFAULT 0"),
        ("is_open", "INTEGER NOT NULL DEFAULT 1"),
        ("seven_day_reference_price", "REAL"),
        ("seven_day_discount_pct", "REAL NOT NULL DEFAULT 0"),
        ("seven_day_discount_amount", "REAL NOT NULL DEFAULT 0"),
        ("seven_day_is_price_deal", "INTEGER NOT NULL DEFAULT 0"),
        ("archive_reference_price", "REAL"),
        ("archive_discount_pct", "REAL NOT NULL DEFAULT 0"),
        ("archive_discount_amount", "REAL NOT NULL DEFAULT 0"),
        ("archive_evidence_count", "INTEGER NOT NULL DEFAULT 0"),
        ("archive_is_price_deal", "INTEGER NOT NULL DEFAULT 0"),
        ("menu_quote_price", "REAL"),
        ("menu_quote_history", "TEXT NOT NULL DEFAULT '[]'"),
        ("menu_quote_effective_price", "REAL"),
        ("menu_quote_promo_type", "TEXT"),
        ("menu_quote_quantity", "INTEGER"),
        ("delivery_available", "INTEGER NOT NULL DEFAULT 1"),
    ):
        if name not in product_columns:
            conn.execute(f"ALTER TABLE products ADD COLUMN {name} {definition}")
    if "listing_started_at" not in product_columns:
        conn.execute("ALTER TABLE products ADD COLUMN listing_started_at TEXT")
        conn.execute("UPDATE products SET listing_started_at=first_seen")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_listing ON products(listing_started_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_deals ON products(is_price_deal, discount_pct DESC, discount_amount DESC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_is_open ON products(is_open)")


class ServingStateCache:
    """Bounded read-through cache for full rows.

    Active/seen identities intentionally live in SQLite, not Python sets: production
    databases contain millions of products and two identity sets can exhaust a
    GitHub-hosted runner before a snapshot starts applying.
    """

    def __init__(self, conn: sqlite3.Connection | None = None, max_rows: int = 20_000):
        self.conn = conn
        self.max_rows = max_rows
        self.stores: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.products: OrderedDict[tuple[str, str], dict[str, Any]] = OrderedDict()
        if conn is not None:
            ensure_schema_and_migrations(conn)
            self.load(conn)

    def load(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def _remember(self, cache: OrderedDict, key, value):
        cache[key] = value
        cache.move_to_end(key)
        if len(cache) > self.max_rows:
            cache.popitem(last=False)
        return value

    def get_store(self, sid: str) -> dict[str, Any] | None:
        cached = self.stores.get(sid)
        if cached is not None:
            self.stores.move_to_end(sid)
            return cached
        row = self.conn.execute("SELECT * FROM stores WHERE store_uuid=?", (sid,)).fetchone()
        return self._remember(self.stores, sid, dict(row)) if row else None

    def put_store(self, sid: str, value: dict[str, Any]) -> None:
        self._remember(self.stores, sid, value)

    def get_product(self, key: tuple[str, str]) -> dict[str, Any] | None:
        cached = self.products.get(key)
        if cached is not None:
            self.products.move_to_end(key)
            return cached
        row = self.conn.execute("SELECT * FROM products WHERE store_uuid=? AND product_uuid=?", key).fetchone()
        if not row:
            return None
        value = dict(row)
        try:
            value["recent_prices"] = [float(v) for v in json_loads(value.get("recent_prices") or "[]")]
        except Exception:
            value["recent_prices"] = []
        try:
            value["menu_quote_history"] = json_loads(value.get("menu_quote_history") or "[]")
        except (ValueError, TypeError):
            value["menu_quote_history"] = []
        return self._remember(self.products, key, value)

    def put_product(self, key: tuple[str, str], value: dict[str, Any]) -> None:
        self._remember(self.products, key, value)


def apply_snapshot(conn: sqlite3.Connection, docs: Iterable[dict[str, Any]], batch_id: str,
                   missing_threshold: int = DEFAULT_MISSING_THRESHOLD, baseline: bool | None = None,
                   event_retention_days: int | None = None,
                   refresh_search: bool = True,
                   state_cache: ServingStateCache | None = None) -> dict[str, int]:
    ensure_schema_and_migrations(conn)
    now = datetime.strptime(batch_id, "%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()
    latest = conn.execute("SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
    if latest and batch_id <= latest[0]:
        raise ValueError(f"snapshot batch {batch_id} is not newer than latest batch {latest[0]}")
    if baseline is None:
        baseline = conn.execute("SELECT COUNT(*)=0 FROM crawl_batches").fetchone()[0] == 1

    if state_cache is None:
        state_cache = ServingStateCache(conn)

    # TEMP tables are connection-local and backed by SQLite's temp_store policy.
    # They bound Python memory while retaining indexed missing-item lookups.
    conn.executescript("""
        CREATE TEMP TABLE IF NOT EXISTS snapshot_seen_stores(
            store_uuid TEXT PRIMARY KEY, is_open INTEGER NOT NULL
        ) WITHOUT ROWID;
        CREATE TEMP TABLE IF NOT EXISTS snapshot_seen_products(
            store_uuid TEXT NOT NULL, product_uuid TEXT NOT NULL,
            PRIMARY KEY(store_uuid, product_uuid)
        ) WITHOUT ROWID;
        DELETE FROM snapshot_seen_stores;
        DELETE FROM snapshot_seen_products;
    """)
    seen_stores: set[str] = set()
    counts = {k: 0 for k in ("stores", "products", "new", "changed", "reappeared", "removed", "unchanged", "store_new", "store_changed", "store_reappeared", "store_removed", "fallback_stores", "fallback_products", "skipped_closed_stores")}

    insert_stores_batch: list[tuple] = []
    update_stores_batch: list[tuple] = []
    insert_products_batch: list[tuple] = []
    update_products_batch: list[tuple] = []
    events_batch: list[tuple] = []
    changed_store_keys: set[str] = set()
    seen_product_rows: list[tuple[str, str]] = []
    changed_product_rows: list[tuple[str, str]] = []
    quote_history_batch: list[tuple[str, str, str]] = []

    def flush_snapshot_batches() -> None:
        if insert_stores_batch:
            conn.executemany("INSERT INTO stores(store_uuid,name,address,city,locality,latitude,longitude,rating,review_count,order_url,is_open,first_seen,last_seen,status,missing_streak,state_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", insert_stores_batch)
            insert_stores_batch.clear()
        if update_stores_batch:
            conn.executemany("UPDATE stores SET name=?,address=?,city=?,locality=?,latitude=?,longitude=?,rating=?,review_count=?,order_url=?,is_open=?,last_seen=?,status=?,missing_streak=0,state_hash=? WHERE store_uuid=?", update_stores_batch)
            update_stores_batch.clear()
        if insert_products_batch:
            conn.executemany("INSERT INTO products(store_uuid,product_uuid,product_name,category,description,price,quantity,promo_type,effective_price,order_url,is_open,first_seen,last_seen,status,missing_streak,state_hash,recent_prices,price_novel_vs_previous_3,reference_price,discount_amount,discount_pct,is_price_deal,price_direction,is_price_drop,price_change_amount,price_change_percent,listing_started_at,menu_quote_price,menu_quote_effective_price,menu_quote_promo_type,menu_quote_quantity,delivery_available) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", insert_products_batch)
            insert_products_batch.clear()
        if update_products_batch:
            conn.executemany("UPDATE products SET product_name=?,category=?,description=?,price=?,quantity=?,promo_type=?,effective_price=?,order_url=?,is_open=?,last_seen=?,status='active',missing_streak=0,state_hash=?,recent_prices=?,price_novel_vs_previous_3=?,reference_price=?,discount_amount=?,discount_pct=?,is_price_deal=?,price_direction=?,is_price_drop=?,price_change_amount=?,price_change_percent=?,listing_started_at=?,menu_quote_price=?,menu_quote_effective_price=?,menu_quote_promo_type=?,menu_quote_quantity=?,delivery_available=? WHERE store_uuid=? AND product_uuid=?", update_products_batch)
            update_products_batch.clear()
        if events_batch:
            conn.executemany("INSERT INTO events(event_time,store_uuid,product_uuid,event_type,old_state,new_state) VALUES(?,?,?,?,?,?)", events_batch)
            events_batch.clear()
        if seen_product_rows:
            conn.executemany("INSERT OR IGNORE INTO snapshot_seen_products VALUES(?,?)", seen_product_rows)
            seen_product_rows.clear()
        if quote_history_batch:
            conn.executemany("UPDATE products SET menu_quote_history=? WHERE store_uuid=? AND product_uuid=?", quote_history_batch)
            quote_history_batch.clear()
        if changed_product_rows:
            conn.executemany("INSERT OR IGNORE INTO pending_product_changes VALUES(?,?)", changed_product_rows)
            changed_product_rows.clear()

    for doc in docs:
        sid, sfallback = store_identity(doc)
        if sid in seen_stores:
            continue
        seen_stores.add(sid)
        # Delivery availability cannot erase an existing open-hours menu.
        catalog_seen = int(menu_catalog_valid(doc, batch_id))
        conn.execute("INSERT OR REPLACE INTO snapshot_seen_stores VALUES(?,?)", (sid, catalog_seen))
        if not catalog_seen:
            counts["skipped_closed_stores"] += 1
            continue

        # is_open means business hours, not whether a closed shop exposed a menu.
        is_open = int(menu_price_observation_valid(doc, batch_id))
        counts["fallback_stores"] += int(sfallback)
        counts["stores"] += 1
        if counts["stores"] > 1 and counts["stores"] % 100 == 1:
            flush_snapshot_batches()

        address = doc.get("address") or {}
        rating = doc.get("aggregateRating") or {}
        geo = doc.get("geo") or {}
        order_url = str(doc.get("potentialAction", {}).get("target", {}).get("urlTemplate") or doc.get("@id") or "")
        sstate = {
            "name": str(doc.get("name") or ""),
            "address": str(address.get("streetAddress") or ""),
            "city": canonical_city(address.get("addressRegion"), address.get("addressLocality"), address.get("streetAddress")),
            "locality": str(address.get("addressLocality") or ""),
            "latitude": _num(geo.get("latitude"), float, None),
            "longitude": _num(geo.get("longitude"), float, None),
            "rating": _num(rating.get("ratingValue"), float, None),
            "review_count": _num(rating.get("reviewCount"), int, None),
            "order_url": order_url,
            "is_open": is_open,
        }

        old_s = state_cache.get_store(sid)
        if old_s:
            for coordinate in ("latitude", "longitude"):
                if sstate[coordinate] is None:
                    sstate[coordinate] = old_s[coordinate]
        sh = canonical_hash(sstate)

        if old_s:
            store_needs_publish = old_s["status"] != "active" or old_s["state_hash"] != sh
            if old_s["status"] != "active":
                counts["store_reappeared"] += 1
                if not baseline:
                    events_batch.append((now, sid, None, "STORE_REAPPEARED", json_dumps({"status": old_s["status"]}), json_dumps({"status": "active"})))
            if old_s["state_hash"] != sh:
                counts["store_changed"] += 1
                if not baseline:
                    events_batch.append((now, sid, None, "STORE_CHANGED", json_dumps(dict(old_s)), json_dumps(sstate)))
            update_stores_batch.append((*sstate.values(), now, "active", sh, sid))
            old_s.update(sstate)
            old_s["last_seen"] = now
            old_s["status"] = "active"
            old_s["missing_streak"] = 0
            old_s["state_hash"] = sh
            if store_needs_publish:
                changed_store_keys.add(sid)
        else:
            insert_stores_batch.append((sid, *sstate.values(), now, now, "active", 0, sh))
            counts["store_new"] += 1
            if not baseline:
                events_batch.append((now, sid, None, "STORE_NEW", None, json_dumps(sstate)))
            state_cache.put_store(sid, {
                "store_uuid": sid, **sstate, "first_seen": now, "last_seen": now,
                "status": "active", "missing_streak": 0, "state_hash": sh
            })
            changed_store_keys.add(sid)

        # Keep payable-price verification independent of menu visibility.
        store_price_valid = price_observation_valid(doc, batch_id)
        quote_price_valid = menu_price_observation_valid(doc, batch_id)
        sections = (doc.get("hasMenu") or {}).get("hasMenuSection") or []
        # A SKU may appear both in a normal category and in the live BOGO
        # category. Selecting the first occurrence makes prices jump when
        # getStoreV1 changes section order. Gather all explicit promo-category
        # evidence first, then apply it consistently to each deduplicated SKU.
        preferred_promos = {}
        for candidate_section in sections:
            candidate_category = html.unescape(str(candidate_section.get("name") or "一般"))
            for candidate_item in candidate_section.get("hasMenuItem") or []:
                candidate_name = html.unescape(str(candidate_item.get("name") or "")).strip()
                if not candidate_name:
                    continue
                candidate_pid, _ = product_identity(candidate_item, sid)
                inferred, inferred_qty = extract_promo_info(
                    candidate_category, candidate_name,
                    str(candidate_item.get("description") or ""))
                candidate_promo = str(candidate_item.get("promo_type") or inferred)
                candidate_qty = max(1, _num(candidate_item.get("quantity"), int, inferred_qty))
                if candidate_promo == "無":
                    continue
                # Quantity-changing offers rank over descriptive sale labels.
                # Resolve ties by stable text, never by API category order.
                candidate_price = _num((candidate_item.get("offers") or {}).get("price"), float, 0)
                rank = (int(candidate_item.get("promotion_source") == "native_structured"),
                        int(candidate_qty > 1), int(candidate_price > 0), candidate_promo)
                prior = preferred_promos.get(candidate_pid)
                if prior is None or rank > prior[0]:
                    preferred_promos[candidate_pid] = (rank, candidate_promo, candidate_qty, candidate_price)
        store_product_keys: set[str] = set()
        for sec in sections:
            category = html.unescape(str(sec.get("name") or "一般"))
            for item in sec.get("hasMenuItem") or []:
                name = html.unescape(str(item.get("name") or "")).strip()
                if not name:
                    continue
                pid, pfallback = product_identity(item, sid)
                counts["fallback_products"] += int(pfallback)
                key = (sid, pid)
                if pid in store_product_keys:
                    continue
                store_product_keys.add(pid)
                seen_product_rows.append(key)
                counts["products"] += 1

                old = state_cache.get_product(key)
                observed_price = _num((item.get("offers") or {}).get("price"), float, 0)
                desc = str(item.get("description") or "")
                # Original Restaurant JSON describes offers in section/name/
                # description; it does not carry ETL-only promo_type fields.
                inferred_promo, inferred_qty = extract_promo_info(category, name, desc)
                observed_promo = str(item.get("promo_type") or inferred_promo)
                observed_qty = max(1, _num(item.get("quantity"), int, inferred_qty))
                preferred = preferred_promos.get(pid)
                if preferred is not None:
                    observed_promo, observed_qty = preferred[1], preferred[2]
                    # A SKU can have different list prices in its promotional
                    # section; never combine BOGO terms with the first category's price.
                    if preferred[3] > 0:
                        observed_price = preferred[3]
                price_valid = store_price_valid and observed_price > 0
                # Never replace the last verified price/promotion with an
                # unpurchasable closed-hours menu snapshot.
                price = observed_price if price_valid else (float(old["price"]) if old and old["recent_prices"] else 0.0)
                promo = observed_promo if price_valid else (str(old["promo_type"]) if old else "無")
                qty = observed_qty if price_valid else (int(old["quantity"]) if old else 1)
                effective = calculate_effective_price(price, promo, qty)
                quote_unit = calculate_effective_price(observed_price, observed_promo, observed_qty) if observed_price > 0 else 0
                quote = (observed_price if 0 < observed_price < 1_000_000 else None,
                         quote_unit if 0 < quote_unit < 1_000_000 else None,
                         observed_promo, observed_qty, int(bool(price_valid)))
                # Keep all menu quotes visible, but only observations made when
                # the store is ACTUALLY OPEN may become a historical price
                # baseline. Delivery unavailable never rejects an open-hours quote.
                cutoff = (datetime.fromisoformat(now) - timedelta(days=7)).isoformat()
                previous_quotes = (old or {}).get("menu_quote_history") or []
                if isinstance(previous_quotes, str):
                    try: previous_quotes = json_loads(previous_quotes)
                    except (TypeError, ValueError): previous_quotes = []
                # Retain RAW evidence with each comparison point:
                # [timestamp, effective_unit, original_list_price, promo, quantity].
                # Pre-existing two-field points remain readable but are NOT
                # falsely labelled as "no promotion" / inferred list prices.
                quote_history = [
                    point for point in previous_quotes
                    if isinstance(point, (list, tuple)) and len(point) >= 2
                    and isinstance(point[0], str) and cutoff <= point[0] < now
                    and isinstance(point[1], (int, float)) and 0 < point[1] < 1_000_000
                ]
                if quote_price_valid and quote[1] is not None:
                    quote_history.append([
                        now, round(quote[1], 2), round(quote[0], 2),
                        quote[2] or "無", quote[3]
                    ])
                quote_history_batch.append((json_dumps(quote_history), sid, pid))
                state = {
                    "product_name": name, "category": category, "description": desc,
                    "price": price, "quantity": qty, "promo_type": promo,
                    "effective_price": effective, "order_url": order_url,
                    "is_open": is_open,
                }
                # A flag on event JSON, not another SQL/Turso column.
                event_state = {**state, "price_valid": bool(price_valid),
                               "menu_quote_valid": bool(observed_price > 0),
                               "menu_price_comparable": bool(quote_price_valid and observed_price > 0),
                               "delivery_available": bool(store_price_valid)}
                ph = canonical_hash(state)

                if old is None:
                    init_recent = [price] if price_valid else []
                    insert_products_batch.append((sid, pid, *state.values(), now, now, "active", 0, ph, json_dumps(init_recent), 0, None, 0, 0, 0, 'unknown', 0, 0, 0, now, *quote))
                    counts["new"] += 1
                    # A first valid in-hours observation must survive even when
                    # this snapshot initializes the whole dataset. Otherwise a
                    # permanently stable-priced product has no chart history.
                    # Replay archived verified observations for this case.
                    if price_valid or not baseline:
                        events_batch.append((now, sid, pid, "NEW", None, json_dumps(event_state)))
                    state_cache.put_product(key, {
                        "store_uuid": sid, "product_uuid": pid, **state,
                        "first_seen": now, "listing_started_at": now, "last_seen": now, "status": "active",
                        "missing_streak": 0, "state_hash": ph, "recent_prices": init_recent,
                        "price_novel_vs_previous_3": 0, "reference_price": None,
                        "discount_amount": 0, "discount_pct": 0, "is_price_deal": 0,
                        "price_direction": "unknown", "is_price_drop": 0,
                        "price_change_amount": 0,
                        "price_change_percent": 0,
                        "menu_quote_price": quote[0],
                        "menu_quote_history": quote_history,
                        "menu_quote_effective_price": quote[1],
                        "menu_quote_promo_type": quote[2],
                        "menu_quote_quantity": quote[3],
                        "delivery_available": quote[4],
                    })
                    changed_product_rows.append(key)
                else:
                    old_state = dict(old)
                    old_state["recent_prices"] = json_dumps(old["recent_prices"])
                    old_state["price_valid"] = bool(old["recent_prices"])
                    # Keep the last verified raw payable price separately from its
                    # unit-equivalent price. An older buy-one-get-one at $89 costs
                    # $44.50 per unit, NOT $89. Do not overwrite this historical
                    # effective price with the raw price in event old_state.
                    if old["recent_prices"]:
                        old_state["price"] = old["recent_prices"][-1]
                    if price_valid:
                        # Price drops, price rises and promotion start/end MUST be
                        # compared on the same per-unit basis (not menu list price).
                        previous_unit = float(old["effective_price"] or 0) if old["recent_prices"] else 0
                        comparison = PriceChange.compare(previous_unit, effective)
                        price_novel = int(bool(comparison and comparison["change"] != 0))
                        change_fields = PriceChange.persisted(comparison)
                        reference_price = change_fields["reference_price"]
                        discount_amount = change_fields["discount_amount"]
                        discount_pct = change_fields["discount_pct"]
                        is_price_deal = change_fields["is_price_deal"]
                        price_direction = change_fields["price_direction"]
                        is_price_drop = change_fields["is_price_drop"]
                        price_change_amount = change_fields["price_change_amount"]
                        price_change_percent = change_fields["price_change_percent"]
                        # recent_prices stores last verified raw price for off-hours
                        # protection; reference_price stores prior effective unit cost.
                        next_recent_prices = [price]
                    else:
                        price_novel = 0
                        reference_price = old.get("reference_price")
                        discount_amount = old.get("discount_amount", 0)
                        discount_pct = old.get("discount_pct", 0)
                        is_price_deal = old.get("is_price_deal", 0)
                        price_direction = old.get("price_direction", "unknown")
                        is_price_drop = old.get("is_price_drop", 0)
                        price_change_amount = old.get("price_change_amount", 0)
                        price_change_percent = old.get("price_change_percent", 0)
                        next_recent_prices = old["recent_prices"]

                    listing_started_at = old.get("listing_started_at") or old["first_seen"]
                    seasonal_relist = (old["status"] == "inactive" and
                        datetime.fromisoformat(now) - datetime.fromisoformat(old["last_seen"]) >= timedelta(days=60))
                    if seasonal_relist:
                        listing_started_at = now
                    product_needs_publish = (
                        old["status"] != "active" or old["state_hash"] != ph
                        or old["recent_prices"] != next_recent_prices
                        or old.get("price_novel_vs_previous_3") != price_novel
                        or old.get("reference_price") != reference_price
                        or old.get("discount_amount") != discount_amount
                        or old.get("discount_pct") != discount_pct
                        or old.get("is_price_deal") != is_price_deal
                        or old.get("price_direction") != price_direction
                        or old.get("is_price_drop") != is_price_drop
                        or old.get("price_change_amount") != price_change_amount
                        or old.get("price_change_percent") != price_change_percent
                        or old.get("menu_quote_history") != quote_history
                        or (old.get("menu_quote_price"), old.get("menu_quote_effective_price"),
                            old.get("menu_quote_promo_type"), old.get("menu_quote_quantity"),
                            old.get("delivery_available", 1)) != quote
                    )

                    if old["status"] != "active":
                        counts["reappeared"] += 1
                        if not baseline:
                            events_batch.append((now, sid, pid, "SEASONAL_RELIST" if seasonal_relist else "REAPPEARED", json_dumps({"status": old["status"], "last_seen": old["last_seen"]}), json_dumps({"status": "active", "listing_started_at": listing_started_at})))
                    if old["state_hash"] != ph or price_novel:
                        kinds = []
                        if price_valid and not old["recent_prices"]:
                            kinds.append("PRICE_OBSERVED")
                        elif price_valid and price_novel:
                            kinds.append("PRICE_CHANGED")
                        if price_valid and (old["promo_type"] != promo or old["quantity"] != qty):
                            kinds.append("PROMOTION_CHANGED")
                        if any(old[k] != state[k] for k in ("product_name", "category", "description", "order_url")):
                            kinds.append("CONTENT_CHANGED")
                        for kind in kinds:
                            if not baseline:
                                events_batch.append((now, sid, pid, kind, json_dumps(old_state), json_dumps(event_state)))
                        counts["changed"] += 1
                    else:
                        counts["unchanged"] += 1

                    # A verified unchanged price is still a new point on the
                    # time series. Store a compact observation, not a copy of
                    # the entire product row, to keep packed history affordable.
                    # Changed prices and promotion changes already emit events.
                    if (price_valid and not baseline and old["recent_prices"]
                            and not price_novel
                            and old["promo_type"] == promo and old["quantity"] == qty):
                        events_batch.append((
                            now, sid, pid, "PRICE_OBSERVED", None,
                            json_dumps({
                                "price": price, "promo_type": promo,
                                "quantity": qty, "is_open": 1, "price_valid": True
                            })
                        ))

                    update_products_batch.append((*state.values(), now, ph, json_dumps(next_recent_prices), price_novel, reference_price, discount_amount, discount_pct, is_price_deal, price_direction, is_price_drop, price_change_amount, price_change_percent, listing_started_at, *quote, sid, pid))
                    old.update(state)
                    old["listing_started_at"] = listing_started_at
                    old["last_seen"] = now
                    old["status"] = "active"
                    old["missing_streak"] = 0
                    old["state_hash"] = ph
                    old["recent_prices"] = next_recent_prices
                    old["price_novel_vs_previous_3"] = price_novel
                    old["reference_price"] = reference_price
                    old["discount_amount"] = discount_amount
                    old["discount_pct"] = discount_pct
                    old["is_price_deal"] = is_price_deal
                    old["price_direction"] = price_direction
                    old["is_price_drop"] = is_price_drop
                    old["price_change_amount"] = price_change_amount
                    old["price_change_percent"] = price_change_percent
                    old["menu_quote_history"] = quote_history
                    old.update(zip(("menu_quote_price", "menu_quote_effective_price",
                                    "menu_quote_promo_type", "menu_quote_quantity",
                                    "delivery_available"), quote))
                    if product_needs_publish:
                        changed_product_rows.append(key)

    flush_snapshot_batches()

    missing_products_batch: list[tuple] = []
    missing_products = conn.execute("""
        SELECT p.store_uuid,p.product_uuid,p.missing_streak
        FROM products p
        LEFT JOIN snapshot_seen_stores ss ON ss.store_uuid=p.store_uuid
        LEFT JOIN stores s ON s.store_uuid=p.store_uuid
        WHERE p.status='active'
          AND NOT EXISTS (
              SELECT 1 FROM snapshot_seen_products sp
              WHERE sp.store_uuid=p.store_uuid AND sp.product_uuid=p.product_uuid
          )
          AND ss.is_open=1
    """)
    for store_id, product_id, old_streak in missing_products:
        key = (store_id, product_id)
        streak = old_streak + 1
        status = "inactive" if streak >= missing_threshold else "active"
        missing_products_batch.append((streak, status, store_id, product_id))
        changed_product_rows.append(key)
        if status == "inactive":
            counts["removed"] += 1
            if not baseline:
                events_batch.append((now, store_id, product_id, "REMOVED", json_dumps({"status": "active"}), json_dumps({"status": "inactive"})))
        if len(missing_products_batch) >= 10_000:
            conn.executemany("UPDATE products SET missing_streak=?,status=? WHERE store_uuid=? AND product_uuid=?", missing_products_batch)
            missing_products_batch.clear()
            flush_snapshot_batches()

    missing_stores_batch: list[tuple] = []
    missing_store_keys = conn.execute("""
        SELECT s.store_uuid FROM stores s
        WHERE s.status='active'
          AND NOT EXISTS (SELECT 1 FROM snapshot_seen_stores ss WHERE ss.store_uuid=s.store_uuid)
    """)
    for (sid,) in missing_store_keys:
        old = state_cache.get_store(sid)
        if old is None:
            continue
        streak = old["missing_streak"] + 1
        status = "inactive" if streak >= missing_threshold else "active"
        old["missing_streak"] = streak
        old["status"] = status
        missing_stores_batch.append((streak, status, sid))
        changed_store_keys.add(sid)
        if status == "inactive":
            counts["store_removed"] += 1
            if not baseline:
                events_batch.append((now, sid, None, "STORE_REMOVED", json_dumps({"status": "active"}), json_dumps({"status": "inactive"})))

    # Missing status updates invalidate cached rows before the next snapshot.
    state_cache.products.clear()

    # Execute batched queries
    if insert_stores_batch:
        conn.executemany(
            "INSERT INTO stores(store_uuid,name,address,city,locality,latitude,longitude,rating,review_count,order_url,is_open,first_seen,last_seen,status,missing_streak,state_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            insert_stores_batch,
        )
    if update_stores_batch:
        conn.executemany(
            "UPDATE stores SET name=?,address=?,city=?,locality=?,latitude=?,longitude=?,rating=?,review_count=?,order_url=?,is_open=?,last_seen=?,status=?,missing_streak=0,state_hash=? WHERE store_uuid=?",
            update_stores_batch,
        )
    if missing_stores_batch:
        conn.executemany("UPDATE stores SET missing_streak=?,status=? WHERE store_uuid=?", missing_stores_batch)

    if insert_products_batch:
        conn.executemany(
            "INSERT INTO products(store_uuid,product_uuid,product_name,category,description,price,quantity,promo_type,effective_price,order_url,is_open,first_seen,last_seen,status,missing_streak,state_hash,recent_prices,price_novel_vs_previous_3,reference_price,discount_amount,discount_pct,is_price_deal,price_direction,is_price_drop,price_change_amount,price_change_percent,listing_started_at,menu_quote_price,menu_quote_effective_price,menu_quote_promo_type,menu_quote_quantity,delivery_available) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            insert_products_batch,
        )
    if update_products_batch:
        conn.executemany(
            "UPDATE products SET product_name=?,category=?,description=?,price=?,quantity=?,promo_type=?,effective_price=?,order_url=?,is_open=?,last_seen=?,status='active',missing_streak=0,state_hash=?,recent_prices=?,price_novel_vs_previous_3=?,reference_price=?,discount_amount=?,discount_pct=?,is_price_deal=?,price_direction=?,is_price_drop=?,price_change_amount=?,price_change_percent=?,listing_started_at=?,menu_quote_price=?,menu_quote_effective_price=?,menu_quote_promo_type=?,menu_quote_quantity=?,delivery_available=? WHERE store_uuid=? AND product_uuid=?",
            update_products_batch,
        )
    if missing_products_batch:
        conn.executemany("UPDATE products SET missing_streak=?,status=? WHERE store_uuid=? AND product_uuid=?", missing_products_batch)

    if events_batch:
        conn.executemany(
            "INSERT INTO events(event_time,store_uuid,product_uuid,event_type,old_state,new_state) VALUES(?,?,?,?,?,?)",
            events_batch,
        )

    if changed_store_keys:
        conn.executemany("INSERT OR IGNORE INTO pending_store_changes(store_uuid) VALUES(?)", ((sid,) for sid in changed_store_keys))
    if changed_product_rows:
        conn.executemany("INSERT OR IGNORE INTO pending_product_changes VALUES(?,?)", changed_product_rows)

    if event_retention_days is not None:
        cutoff = (datetime.fromisoformat(now) - timedelta(days=event_retention_days)).isoformat()
        conn.execute("DELETE FROM events WHERE event_time < ?", (cutoff,))
    if refresh_search:
        refresh_product_search(conn, now)
    conn.execute("INSERT OR REPLACE INTO crawl_batches VALUES(?,?,?,?,?,?)", (batch_id, now, counts["stores"], counts["products"], counts["fallback_stores"], counts["fallback_products"]))
    conn.execute("INSERT OR REPLACE INTO metadata VALUES('latest_batch',?)", (batch_id,))
    conn.commit()
    return counts


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--database", required=True)
    p.add_argument("--batch-id", required=True)
    p.add_argument("--missing-threshold", type=int, default=DEFAULT_MISSING_THRESHOLD)
    p.add_argument("--baseline", action="store_true")
    p.add_argument("--event-retention-days", type=int, default=EVENT_RETENTION_DAYS,
                   help="0 keeps all derived events (default)")
    a = p.parse_args()
    conn = sqlite3.connect(a.database)
    conn.row_factory = sqlite3.Row
    retention = a.event_retention_days if a.event_retention_days > 0 else None
    counts = apply_snapshot(conn, iter_documents(a.source), a.batch_id, a.missing_threshold, True if a.baseline else None, retention)
    print(json.dumps(counts, ensure_ascii=False, indent=2))
    conn.close()


if __name__ == "__main__":
    main()
