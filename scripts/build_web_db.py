"""Build a stable web projection; Raw and normalized history remain on HF/runner.

Daily observation timestamps and internal price-observation bookkeeping are not
duplicated inside every web bundle. The web uses the release observation time.
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
    # first_seen is stable; last_seen is derived from release metadata for rows
    # observed this batch. Preserve the true timestamp for missing rows.
    for table in ('stores', 'products'):
        dst.execute(f'UPDATE {table} SET last_seen=first_seen WHERE missing_streak=0')
        dst.execute(f'UPDATE {table} SET missing_streak=0')
    dst.execute("UPDATE products SET recent_prices='[]',price_novel_vs_previous_3=0")
    dst.execute('UPDATE products SET reference_price=NULL WHERE is_price_deal=0')
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
