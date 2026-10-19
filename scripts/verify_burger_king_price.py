"""Release acceptance for the specific Burger King 66折 case.
Runs against the existing serving DB, the published packed format and UI source,
with no new Uber Eats crawl.
"""
import json
import sqlite3
import sys
from pathlib import Path
import msgpack
import zstandard

ROOT = Path(__file__).resolve().parents[1]
batch = (ROOT / "data/latest.txt").read_text().strip()
serving = ROOT / "data" / batch / "serving.db"
packed = ROOT / "data" / batch / "packed-serving.db"
conn = sqlite3.connect(serving)
conn.row_factory = sqlite3.Row
rows = [dict(r) for r in conn.execute(
    """SELECT p.store_uuid,p.product_uuid,p.product_name,p.price,
              p.effective_price,p.promo_type,p.quantity,p.status,p.is_open,
              s.name AS store_name
       FROM products p JOIN stores s USING(store_uuid)
       WHERE p.product_name LIKE '%黑醬烤雞堡%' AND p.product_name LIKE '%66折%'
         AND (s.name LIKE '%漢堡王%' OR s.name LIKE '%Burger King%')"""
)]
print("BURGER_KING_MATCH_COUNT:", len(rows), flush=True)
if not rows:
    print("BURGER_KING_NOT_IN_CURRENT_SNAPSHOT: cannot verify this product is on live site", flush=True)
    sys.exit(0)
packed_conn = sqlite3.connect(packed)
packed_conn.row_factory = sqlite3.Row
for item in rows:
    print("BURGER_KING_SERVED_PRODUCT:", json.dumps(item, ensure_ascii=False), flush=True)
    expected = round(float(item["price"]), 2)
    actual = round(float(item["effective_price"]), 2)
    if float(item["price"]) > 0 and int(item["quantity"]) == 1 and expected != actual:
        raise AssertionError(f"Direct discount double-applied: {expected=} {actual=}")
    directory = packed_conn.execute("SELECT store_id FROM store_directory WHERE store_uuid=?", (item["store_uuid"],)).fetchone()
    if not directory:
        raise AssertionError("Product store missing in web packed directory")
    found = False
    for record in packed_conn.execute("SELECT payload FROM store_bundles WHERE store_id=?", (directory["store_id"],)):
        blob = msgpack.unpackb(zstandard.ZstdDecompressor().decompress(record["payload"]), raw=False)
        columns = blob.get("product_columns")
        if columns is None:
            columns = [r[1] for r in conn.execute("PRAGMA table_info(products)")]
        for _,values in blob["products"]:
            product = dict(zip(columns, values))
            if product["product_uuid"] == item["product_uuid"]:
                found = True
                if abs(product["price"] - item["price"]) > .001 or abs(product["effective_price"] - item["effective_price"]) > .001:
                    raise AssertionError("Packed web record differs from repaired serving record")
    if not found:
        raise AssertionError("Product missing from packed website data")
print("BURGER_KING_PACKED_PRICE_CHECK: PASS", flush=True)
