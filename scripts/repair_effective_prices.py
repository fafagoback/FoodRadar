"""Repair derived unit prices in an existing FoodRadar serving SQLite DB without crawling."""
import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from json_to_db import calculate_effective_price
from price_change import PriceChange


def verified_unit(state_json):
    try:
        state = json.loads(state_json or '{}')
        if state.get('price_valid') is not True or state.get('is_open') not in (1, True):
            return None
        value = calculate_effective_price(
            float(state['price']), state.get('promo_type') or '無',
            max(1, int(state.get('quantity') or 1)))
        return round(value, 2) if value > 0 else None
    except (TypeError, ValueError, KeyError, json.JSONDecodeError):
        return None


def repair(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        columns = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
        required = {"store_uuid", "product_uuid", "price", "promo_type", "quantity", "effective_price"}
        if not required.issubset(columns):
            raise ValueError("Not a FoodRadar serving products table")
        updates = []
        count = 0
        for sid, pid, price, promo, qty, old in conn.execute(
            "SELECT store_uuid,product_uuid,price,promo_type,quantity,effective_price FROM products"
        ):
            count += 1
            new = calculate_effective_price(price, promo, qty)
            if abs(float(old) - new) > 0.001:
                updates.append((new, sid, pid))
        conn.executemany(
            "UPDATE products SET effective_price=? WHERE store_uuid=? AND product_uuid=?", updates
        )
        # Only the LAST verified observation can be used as a comparison.
        # Historical PRICE_CHANGED events omit identical-price crawls; do not
        # overwrite a later valid baseline with the previous change event.
        deal_columns = {'reference_price', 'discount_amount', 'discount_pct', 'is_price_deal'}
        events_exist = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone() is not None
        deal_updates = []
        if deal_columns.issubset(columns):
            for name, definition in (
                ("price_direction", "TEXT NOT NULL DEFAULT 'unknown'"),
                ("is_price_drop", "INTEGER NOT NULL DEFAULT 0"),
                ("price_change_amount", "REAL NOT NULL DEFAULT 0"),
                ("price_change_percent", "REAL NOT NULL DEFAULT 0"),
            ):
                if name not in columns:
                    conn.execute(f"ALTER TABLE products ADD COLUMN {name} {definition}")
            has_last_seen = 'last_seen' in columns
            last_seen_column = 'last_seen' if has_last_seen else 'NULL AS last_seen'
            candidates = conn.execute(
                f"""SELECT store_uuid,product_uuid,price,promo_type,quantity,
                          reference_price,{last_seen_column} FROM products"""
            ).fetchall()
            for sid, pid, raw, promo, qty, prior, last_seen in candidates:
                current = calculate_effective_price(raw, promo, qty)
                # Use event evidence only if it is the latest actual observation.
                # The no-last_seen fallback exists for limited legacy repair DBs.
                if events_exist:
                    event = conn.execute(
                        """SELECT event_time,old_state,new_state FROM events
                           WHERE store_uuid=? AND product_uuid=?
                             AND event_type IN ('PRICE_CHANGED','PROMOTION_CHANGED')
                           ORDER BY event_time DESC,id DESC LIMIT 1""",
                        (sid, pid)
                    ).fetchone()
                    if event and (not has_last_seen or str(event[0]) == str(last_seen)):
                        old_unit = verified_unit(event[1])
                        new_unit = verified_unit(event[2])
                        if old_unit and new_unit and abs(new_unit - current) <= 0.01:
                            prior = old_unit
                fields = PriceChange.persisted(PriceChange.compare(prior, current))
                deal_updates.append((
                    fields["reference_price"], fields["discount_amount"],
                    fields["discount_pct"], fields["is_price_deal"],
                    fields["price_direction"], fields["is_price_drop"],
                    fields["price_change_amount"],
                    fields["price_change_percent"], sid, pid
                ))
            conn.executemany(
                """UPDATE products SET reference_price=?,discount_amount=?,
                   discount_pct=?,is_price_deal=?,price_direction=?,is_price_drop=?,
                   price_change_amount=?,price_change_percent=?
                   WHERE store_uuid=? AND product_uuid=?""",
                deal_updates
            )
        # Main list: maximum verified effective unit price in the previous
        # seven days, excluding the current batch. Legacy last-change fields
        # remain intact for a supplementary "new drop" indication.
        for col, ddl in (
            ("seven_day_reference_price", "REAL"),
            ("seven_day_discount_pct", "REAL NOT NULL DEFAULT 0"),
            ("seven_day_discount_amount", "REAL NOT NULL DEFAULT 0"),
            ("seven_day_is_price_deal", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in columns:
                conn.execute(f"ALTER TABLE products ADD COLUMN {col} {ddl}")
        has_batches = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='crawl_batches'"
        ).fetchone() is not None
        last_batch = conn.execute("SELECT MAX(processed_at) FROM crawl_batches").fetchone()[0] if has_batches else None
        histories = defaultdict(list)
        if events_exist and last_batch:
            start = (datetime.fromisoformat(last_batch) - timedelta(days=7)).isoformat()
            for sid, pid, when, state in conn.execute(
                """SELECT store_uuid,product_uuid,event_time,new_state
                   FROM events WHERE event_time>=? AND event_time<?
                     AND event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED','PROMOTION_CHANGED')
                     AND product_uuid IS NOT NULL ORDER BY event_time,id""",
                (start, last_batch)
            ):
                unit = verified_unit(state)
                if unit is not None:
                    histories[(sid, pid)].append((when, unit))
        week_updates = []
        week_deals = 0
        seen_sql = "last_seen" if "last_seen" in columns else "NULL AS last_seen"
        for sid, pid, unit, last_seen in conn.execute(
            f"SELECT store_uuid,product_uuid,effective_price,{seen_sql} FROM products"
        ):
            seven = PriceChange.seven_day_high(
                histories.get((sid, pid), ()), last_batch, unit) if last_batch == last_seen else None
            # The published deal list uses the highest VERIFIED unit price
            # inside the prior seven days, not the previous-crawl movement.
            week_fields = PriceChange.persisted_seven_day(seven)
            week_deals += week_fields["seven_day_is_price_deal"]
            week_updates.append((
                week_fields["seven_day_reference_price"],
                week_fields["seven_day_discount_pct"],
                week_fields["seven_day_discount_amount"],
                week_fields["seven_day_is_price_deal"], sid, pid))
        conn.executemany(
            """UPDATE products SET seven_day_reference_price=?,
               seven_day_discount_pct=?,seven_day_discount_amount=?,
               seven_day_is_price_deal=? WHERE store_uuid=? AND product_uuid=?""",
            week_updates)
        print(f"SEVEN_DAY_PRICE_REPAIR current_deals={week_deals} histories={len(histories)}",flush=True)
        errors = conn.execute(
            "SELECT COUNT(*) FROM products WHERE price>0 AND effective_price<=0"
        ).fetchone()[0]
        if errors:
            raise ValueError(f"{errors} invalid derived prices remain")
        conn.commit()
        print(f"PRICE_REPAIR total={count} corrected={len(updates)} invalid={errors} "
              f"recomputed_price_flags={len(deal_updates)}", flush=True)
        return count, len(updates)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    repair(args.database)
