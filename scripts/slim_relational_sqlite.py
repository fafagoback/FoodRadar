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
        encoder=zstd.ZstdCompressor(level=12,write_content_size=True)
        converted=0
        for table in ("shop_state","price_history"):
            for sid,blob in db.execute("SELECT store,payload FROM "+table):
                if not blob.startswith(MAGIC):
                    continue
                raw=codec.raw(blob)
                candidate=encoder.compress(raw)
                if zstd.ZstdDecompressor().decompress(candidate)!=raw:
                    raise ValueError("Browser codec changed source semantics")
                db.execute("UPDATE "+table+" SET payload=? WHERE store=?",(candidate,sid))
                converted+=1
        db.execute("DROP INDEX IF EXISTS relational_product_shops")
        db.execute("DROP INDEX IF EXISTS relational_product_names")
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
    result={"converted_payloads":converted,"bytes":file.stat().st_size,
            "product_links":product_count}
    print("FR_SINGLE_DB_COMPACTION="+json.dumps(result),flush=True)
    return result

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--database",required=True)
    a=p.parse_args()
    slim(a.database)
