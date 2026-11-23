"""Read-only evidence: an excluded historical batch is absent from live Turso.

Run after the checksum-verified delta publication, not as an alternative to it.
"""
import argparse
import hashlib
import json
import re
from datetime import datetime, timedelta, timezone

import msgpack
import zstandard

from publish_turso_delta import Remote

TW = timezone(timedelta(hours=8))


def verify_target_bundles(rows, excluded_at):
    dec = zstandard.ZstdDecompressor()
    product_cols = event_cols = None
    products = source_points = bad_points = bad_events = 0
    for row in rows:
        payload = row["payload"]
        if not isinstance(payload, bytes):
            raise ValueError("Turso did not return an intact bundle BLOB")
        raw = dec.decompress(payload)
        if hashlib.sha256(raw).hexdigest() != row["checksum"]:
            raise ValueError("Turso bundle has invalid SHA256")
        chunk = msgpack.unpackb(raw, raw=False)
        product_cols = chunk.get("product_columns") or product_cols
        event_cols = chunk.get("event_columns") or event_cols
        for _, array in chunk.get("products") or []:
            if not product_cols:
                raise ValueError("Missing product columns in Turso bundle")
            item = dict(zip(product_cols, array))
            name = re.sub(r"\s+", "", str(item.get("product_name") or "")).lower()
            if all(x in name for x in ("ariel", "抗菌洗衣精", "800g", "除蟎")):
                products += 1
                try:
                    history = json.loads(item.get("menu_quote_history") or "[]")
                except (TypeError, ValueError) as error:
                    raise ValueError("Invalid product quote history") from error
                for point in history:
                    if isinstance(point, list) and point:
                        source_points += 1
                        if point[0] == excluded_at:
                            bad_points += 1
        for array in chunk.get("events") or []:
            if not event_cols:
                raise ValueError("Missing event columns in Turso bundle")
            event = dict(zip(event_cols, array))
            if event.get("event_time") == excluded_at:
                bad_events += 1
    if not rows:
        raise ValueError("Target store was not found in Turso")
    if bad_points or bad_events:
        raise ValueError(f"Removed Raw batch still appears in live Turso: "
                         f"quote_points={bad_points}, events={bad_events}")
    return {"bundle_chunks": len(rows), "matching_products": products,
            "matching_product_history_points": source_points,
            "excluded_quote_points": bad_points, "excluded_events": bad_events}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"\d{14}", args.batch):
        raise ValueError("Expected a 14-digit Raw batch ID")
    stamp = datetime.strptime(args.batch, "%Y%m%d%H%M%S").replace(tzinfo=TW).isoformat()
    import os
    remote = Remote(os.environ["TURSO_DATABASE_URL"], os.environ["TURSO_READONLY_TOKEN"])
    try:
        stores = remote.query(
            "SELECT store_id FROM store_directory WHERE instr(name,?)>0 LIMIT 3",
            ("Uber Eats 優市 北投店",))
        if len(stores) != 1:
            raise ValueError(f"Expected exactly one audited store, got {len(stores)}")
        rows = remote.query(
            "SELECT payload,checksum FROM store_bundles WHERE store_id=? ORDER BY chunk_no",
            (stores[0]["store_id"],))
        report = verify_target_bundles(rows, stamp)
        print("FOODRADAR_LIVE_REMOVED_BATCH_AUDIT=" + json.dumps({
            "batch": args.batch, **report
        }, ensure_ascii=False), flush=True)
    finally:
        remote.close()


if __name__ == "__main__":
    main()
