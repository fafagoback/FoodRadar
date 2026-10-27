"""Build a current-price web projection while keeping Raw/history on HF/runner.

A product must retain its actual last verified price observation timestamp:
rewriting last_seen to first_seen breaks latest-batch membership and causes
old prices to appear as current deals, or current prices to disappear.
"""
import argparse
import sqlite3
from pathlib import Path
from prototype_pack_db import build, verify


def build_web_db(source, target):
    projection = Path(target).with_name('web-projection.db')
    projection.unlink(missing_ok=True)
    src = sqlite3.connect(source)
    dst = sqlite3.connect(projection)
    src.backup(dst)
    src.close()
    # Preserve exact last_seen for store and product independently, including
    # unchanged valid prices. Membership in the latest successful batch is
    # determined by these timestamps, not by any elapsed-time grace period.
    for table in ('stores', 'products'):
        dst.execute(f'UPDATE {table} SET missing_streak=0')
    dst.execute("UPDATE products SET recent_prices='[]',price_novel_vs_previous_3=0")
    # Keep the previous verified unit price for flat/rising and small drops too.
    dst.commit()
    dst.close()
    build(str(projection), str(target))
    verify(str(projection), str(target))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('target')
    args = parser.parse_args()
    build_web_db(args.source, args.target)
