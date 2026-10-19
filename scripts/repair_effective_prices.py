"""Repair derived unit prices in an existing FoodRadar serving SQLite DB without crawling."""
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from json_to_db import calculate_effective_price


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
        conn.commit()
        errors = conn.execute(
            "SELECT COUNT(*) FROM products WHERE price>0 AND effective_price<=0"
        ).fetchone()[0]
        if errors:
            raise ValueError(f"{errors} invalid derived prices remain")
        print(f"PRICE_REPAIR total={count} corrected={len(updates)} invalid={errors}", flush=True)
        return count, len(updates)
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    repair(args.database)
