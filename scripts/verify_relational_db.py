"""Audit new normalized SQLite against the existing verified compact checkpoint.

Only READS both databases; no UE, HF upload or website mutation.
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"scripts"))
from build_relational_db import load_checkpoint
from relational_catalog import _cents

def validate(state,dbfile,samples=1000):
    counts=state.stats()
    path=Path(dbfile).resolve()
    with sqlite3.connect(path.as_uri()+"?mode=ro",uri=True) as db:
        db.row_factory=sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        tables={t:db.execute("SELECT COUNT(*) FROM "+t).fetchone()[0]
                for t in ("stores","products","store_products",
                          "price_terms","price_observations")}
        if (tables["stores"]!=counts["stores"]
            or tables["store_products"]!=counts["products"]):
            raise ValueError("Store/product row counts changed")
        metadata={row["key"]:json.loads(row["value"])
                  for row in db.execute("SELECT * FROM metadata")}
        report=metadata["counts"]
        if (report["observations"]+
            report.get("expired_observations_ignored",0)!=counts["observations"]):
            raise ValueError("Verified observation counts differ")
        sampled=0
        for sid in state.ids()[:samples]:
            store,products,quotes=state.shop(sid)
            rows=db.execute("""SELECT sp.sku,p.product_name,p.category,p.description,
                        t.price_cents,t.effective_cents
                FROM store_products sp JOIN stores s ON s.id=sp.store_id
                JOIN products p ON p.id=sp.product_id
                JOIN price_terms t ON t.id=sp.price_term_id
                WHERE s.uuid=?""",(sid,)).fetchall()
            found={row["sku"]:row for row in rows}
            if len(found)!=len(products):
                raise ValueError("Product count corrupted: "+sid)
            for product in products:
                row=found.get(product["product_uuid"])
                if (row is None or
                    (row["product_name"],row["category"],row["description"])!=
                    tuple(str(product.get(k) or "") for k in
                          ("product_name","category","description")) or
                    row["price_cents"]!=_cents(product.get("price")) or
                    row["effective_cents"]!=_cents(product.get("effective_price"))):
                    raise ValueError("Product or branch price changed: "+sid)
            row=db.execute("""SELECT count(*) FROM price_observations q
                JOIN stores s ON q.store_id=s.id WHERE s.uuid=?""",
                (sid,)).fetchone()[0]
            if row>len(quotes):
                raise ValueError("Added phantom price observation: "+sid)
            sampled+=1
        compact=sum(part.path.stat().st_size for part in state.parts)
        return {"matched_stores":tables["stores"],
                "matched_listings":tables["store_products"],
                "retained_observations":tables["price_observations"],
                "shared_definitions":tables["products"],
                "shared_price_terms":tables["price_terms"],
                "stores_sampled":sampled,
                "database_bytes":path.stat().st_size,
                "source_compact_bytes":compact,
                "candidate_smaller_than_compact":path.stat().st_size<compact}

if __name__=="__main__":
    p=argparse.ArgumentParser()
    p.add_argument("--checkpoint",default="data/state-checkpoint")
    p.add_argument("--file",required=True)
    p.add_argument("--samples",type=int,default=1000)
    p.add_argument("--report")
    args=p.parse_args()
    state,_=load_checkpoint(Path(args.checkpoint))
    try: result=validate(state,args.file,args.samples)
    finally:state.close()
    print("RELATIONAL_PARITY_REPORT="+json.dumps(result,ensure_ascii=False))
    if args.report:
        Path(args.report).write_text(
            json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
