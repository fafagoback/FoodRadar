"""V2-only integrity repair, never reconstruct a discount from promo text.

Legacy SQLite price/quantity/promo_type columns do not constitute native
evidence. Refuse to recompute them or create false deals.
"""
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from verified_weekly import update_verified_weekly


def repair(db_path):
    with sqlite3.connect(str(db_path)) as conn:
        schema={row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "product_price_observations" not in schema:
            raise ValueError("V1 SQLite lacks certified native quotes: rebuild from HF ApiRaw")
        meta=conn.execute(
            "SELECT value FROM metadata WHERE key='latest_batch'").fetchone()
        if not meta:
            raise ValueError("Missing current batch: cannot recalculate V2")
        result=update_verified_weekly(conn,meta[0])
        return result["qualified_comparisons"],result["deals"]


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("database",type=Path)
    print(repair(parser.parse_args().database))
