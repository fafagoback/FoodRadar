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
    # PRICE_OBSERVED events are redundant only when the same verified timestamp
    # survives in the product's original-price/promotion history. Keep any
    # observation without matching quote evidence, and all real change events.
    # This projection is Turso-only: HF Raw and serving.db remain unchanged.
    before = dst.execute("SELECT COUNT(*) FROM events WHERE event_type='PRICE_OBSERVED'").fetchone()[0]
    dst.execute("""
        DELETE FROM events
        WHERE event_type='PRICE_OBSERVED'
          AND EXISTS (
              SELECT 1 FROM products AS p, json_each(p.menu_quote_history) AS quote
              WHERE p.store_uuid=events.store_uuid
                AND p.product_uuid=events.product_uuid
                AND json_extract(quote.value,'$[0]')=events.event_time
          )
    """)
    after = dst.execute("SELECT COUNT(*) FROM events WHERE event_type='PRICE_OBSERVED'").fetchone()[0]
    print("FOODRADAR_REDUNDANT_EVENT_COMPACTION=" + __import__('json').dumps({
        "redundant_observations_removed": before - after,
        "unmatched_observations_preserved": after,
    }), flush=True)
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
