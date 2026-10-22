"""Repair derived unit prices in an existing FoodRadar serving SQLite DB without crawling."""
import argparse
import json
import sqlite3
import sys
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
                    fields["price_direction"], fields["price_change_amount"],
                    fields["price_change_percent"], sid, pid
                ))
            conn.executemany(
                """UPDATE products SET reference_price=?,discount_amount=?,
                   discount_pct=?,is_price_deal=?,price_direction=?,
                   price_change_amount=?,price_change_percent=?
                   WHERE store_uuid=? AND product_uuid=?""",
                deal_updates
            )
        errors = conn.execute(
            "SELECT COUNT(*) FROM products WHERE price>0 AND effective_price<=0"
        ).fetchone()[0]
        if errors:
            raise ValueError(f"{errors} invalid derived prices remain")
        conn.commit()
        print(f"PRICE_REPAIR total={count} corrected={len(updates)} invalid={errors} "
              f"verified_deals={len(deal_updates)}", flush=True)
        return count, len(updates)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    repair(args.database)
