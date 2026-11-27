"""Explain and time candidate FR SQLite queries using already verified live data."""
from __future__ import annotations
import argparse,sqlite3,time,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"scripts"))
from build_relational_db import load_checkpoint
def main():
    p=argparse.ArgumentParser()
    p.add_argument("--file",required=True)
    p.add_argument("--checkpoint",default="data/state-checkpoint")
    a=p.parse_args()
    state,_=load_checkpoint(Path(a.checkpoint))
    try:
        dictionary=state.parts[0].d["product_name"]
        tokens=[i for i,n in enumerate(dictionary) if "雞排" in str(n)]
        print("FR_REAL_INDEX_CANDIDATES=",len(tokens),flush=True)
    finally:state.close()
    with sqlite3.connect(a.file) as db:
        db.execute("pragma cache_size = -100000")
        indexes=[x for x in db.execute("pragma index_list(relational_store_products)")]
        print("FR_REL_INDEXES=",indexes,flush=True)
        names=",".join(str(int(t)) for t in tokens)
        selected="SELECT rp.name_token,rs.name,rs.rating,pr.original_cents FROM "
        variant1=selected+"""relational_products rp INDEXED BY relational_product_names
            JOIN relational_store_products sp INDEXED BY relational_product_shops
              ON sp.product_id=rp.id
            JOIN relational_stores rs ON rs.id=sp.store_id
            JOIN relational_prices pr ON pr.id=sp.price_id
            WHERE sp.active=1 AND rs.active=1 AND rp.name_token IN ("""+names+""")
            ORDER BY rs.rating DESC,rs.id ASC,sp.sku ASC LIMIT 51"""
        variant2=selected+"""relational_stores rs INDEXED BY relational_store_rating
            CROSS JOIN relational_store_products sp ON sp.store_id=rs.id
            JOIN relational_products rp ON rp.id=sp.product_id
            JOIN relational_prices pr ON pr.id=sp.price_id
            WHERE sp.active=1 AND rs.active=1 AND rp.name_token IN ("""+names+""")
            ORDER BY rs.rating DESC,rs.id ASC,sp.sku ASC LIMIT 51"""
        for name,sql in [("product_first",variant1),("store_first",variant2)]:
            explain=list(db.execute("EXPLAIN QUERY PLAN "+sql))
            start=time.monotonic()
            result=list(db.execute(sql))
            secs=round(time.monotonic()-start,4)
            print("FR_REAL_INDEX_PROFILE=",{
                "name":name,"result_count":len(result),
                "seconds_local":secs,"plan":explain[:15]},flush=True)
if __name__=="__main__":main()
