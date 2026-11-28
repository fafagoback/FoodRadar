"""Make compact shop/history payloads universally readable by browser fzstd.

Converts *only the new serving candidate*, not the canonical HF compact state.
Drops redundant SQL indexes: the compressed shared product index already serves
keyword, category and store lookup. Checks source decoded bytes before/after.
"""
from __future__ import annotations
import argparse
import json
import sqlite3
import zstandard as zstd
import msgpack
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from compact_payload import PayloadCodec, KEY, MAGIC

def slim(path):
    file=Path(path)
    with sqlite3.connect(file) as db:
        db.execute("PRAGMA foreign_keys=ON")
        row=db.execute("SELECT payload FROM dictionary WHERE key=?",(KEY,)).fetchone()
        trained=msgpack.unpackb(zstd.ZstdDecompressor().decompress(row[0]),raw=False) if row else None
        codec=PayloadCodec(trained)
        encoder=zstd.ZstdCompressor(level=19,write_content_size=True)
        decoder=zstd.ZstdDecompressor()
        converted=0
        byte_savings=0
        # Store, price-history and token dictionary are the actual canonical
        # data. Recompress only when smaller; compare the EXACT decoded bytes.
        # The browser reads standard Zstd frames directly (no dictionary).
        for table,key in (("shop_state","store"),("price_history","store"),
                          ("dictionary","key")):
            for sid,blob in db.execute("SELECT "+key+",payload FROM "+table):
                if not isinstance(blob,bytes):
                    raise ValueError("Invalid source compressed payload")
                raw=codec.raw(blob) if blob.startswith(MAGIC) else decoder.decompress(blob)
                candidate=encoder.compress(raw)
                if decoder.decompress(candidate)!=raw:
                    raise ValueError("Browser codec changed source semantics")
                if blob.startswith(MAGIC) or len(candidate)<len(blob):
                    db.execute("UPDATE "+table+" SET payload=? WHERE "+key+"=?",(candidate,sid))
                    converted+=1
                    byte_savings+=len(blob)-len(candidate)
        # All search, geo and promotion indices used by the website are
        # already represented by the single compressed frontend index.
        # Do not keep duplicate SQLite B-trees for the same facts.
        for index in ("relational_product_shops","relational_product_names",
                      "relational_deals","relational_store_rating"):
            db.execute("DROP INDEX IF EXISTS "+index)
        db.commit()
        db.execute("VACUUM")
        if db.execute("PRAGMA integrity_check").fetchone()[0]!="ok":
            raise ValueError("Slim SQLite integrity mismatch")
        if list(db.execute("PRAGMA foreign_key_check")):
            raise ValueError("Relational links changed in slimming")
        product_count=db.execute("SELECT COUNT(*) FROM relational_store_products").fetchone()[0]
        counts=json.loads(db.execute("SELECT value FROM relational_meta WHERE key='counts'").fetchone()[0])
        if product_count!=counts["links"]:
            raise ValueError("SQL product links changed")
        duplicate=db.execute("""SELECT COUNT(*) FROM (
            SELECT store_id,ordinal,COUNT(*) n FROM relational_store_products
            GROUP BY store_id,ordinal HAVING n>1)""").fetchone()[0]
        if duplicate:
            raise ValueError("Duplicate store ordinal after compaction")
        try:
            pages=dict(db.execute("SELECT name,SUM(pgsize) FROM dbstat GROUP BY name"))
        except sqlite3.DatabaseError:
            pages={}
    result={"converted_payloads":converted,"saved_payload_bytes":byte_savings,
            "bytes":file.stat().st_size,"product_links":product_count,
            "sqlite_page_breakdown":pages}
    print("FR_SINGLE_DB_COMPACTION="+json.dumps(result),flush=True)
    return result

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--database",required=True)
    a=p.parse_args()
    slim(a.database)
