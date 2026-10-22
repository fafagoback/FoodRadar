"""Repair derived unit prices in an existing FoodRadar serving SQLite DB without crawling."""
import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from json_to_db import calculate_effective_price


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
        # Older checkpoints compared raw sticker prices across buy-one-get-one
        # transitions. Recover the previous *effective unit price* from the
        # verified price-change event, never from a potentially stale baseline.
        deal_columns = {'reference_price', 'discount_amount', 'discount_pct', 'is_price_deal'}
        events_exist = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='events'"
        ).fetchone() is not None
        deal_updates = []
        if events_exist and deal_columns.issubset(columns):
            candidates = conn.execute(
                """SELECT store_uuid,product_uuid,price,promo_type,quantity
                   FROM products
                   WHERE is_price_deal=1 OR discount_pct>0 OR discount_amount>0"""
            ).fetchall()
            for sid, pid, raw, promo, qty in candidates:
                latest_unit = round(calculate_effective_price(raw, promo, qty), 2)
                events = conn.execute(
                    """SELECT old_state,new_state FROM events
                       WHERE store_uuid=? AND product_uuid=?
                         AND event_type IN ('PRICE_CHANGED','PROMOTION_CHANGED')
                       ORDER BY event_time DESC,id DESC LIMIT 8""",
                    (sid, pid)
                )
                for old_state, new_state in events:
                    previous_unit = verified_unit(old_state)
                    observed_unit = verified_unit(new_state)
                    if (previous_unit is None or observed_unit is None
                            or abs(observed_unit - latest_unit) > 0.01):
                        continue
                    savings = round(max(0, previous_unit - latest_unit), 2)
                    percent = round(savings / previous_unit * 100, 2)
                    deal_updates.append(
                        (previous_unit, savings, percent, int(percent >= 30), sid, pid))
                    break
            conn.executemany(
                """UPDATE products SET reference_price=?,discount_amount=?,
                   discount_pct=?,is_price_deal=? WHERE store_uuid=? AND product_uuid=?""",
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
