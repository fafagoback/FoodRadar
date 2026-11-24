"""Replay verified immutable Raw into the one compact DB and publish static data.

No whole-store relational database or second packed SQLite is constructed.
Only the single shop being changed has a bounded memory-only SQL scratch.
"""
import json,os,sys
from collections import Counter
from pathlib import Path
from time import perf_counter
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from pipeline import restore_history
from state_checkpoint import save_direct
from prototype_pack_db import build as project
from build_static_catalog import build as publish,verify
from audit_store_identity import audit_stores
from audit_deal_pipeline import report as audit_deals

def main():
    started=perf_counter()
    state=restore_history(json.loads((ROOT/"config.json").read_text(encoding="utf-8")),
        rebuild_all=os.environ.get("FOODRADAR_REBUILD_VALID_PRICE_HISTORY")=="1")
    try:
        batch=state.meta["latest_batch"]
        dest=ROOT/"data"/batch
        kinds=Counter()
        for quotes in state.quote_shops():
            kinds.update(q[5] for q in quotes)
        if set(kinds)!={"NONE","SAME_SKU_BXGY","DIRECT_DISCOUNT"} or any(n<=0 for n in kinds.values()):
            raise ValueError("Missing native price evidence kinds")
        audit=audit_deals(direct=state)
        print("V2_PREPUBLICATION="+json.dumps({"batch":batch,"verified_types":dict(kinds),
            "invalid_deals":len(audit["errors"])},ensure_ascii=False),flush=True)
        identity=audit_stores(direct=state)
        (dest/"store-identity-report.json").write_text(json.dumps(identity,ensure_ascii=False,indent=2))
        print("FOODRADAR_STORE_HISTORY_AUDIT="+json.dumps(identity,ensure_ascii=False),flush=True)
        projection=project(None,None,static_only=True,direct=state)
        packed=projection.validate()
        (ROOT/"packed-report.json").write_text(json.dumps(packed,ensure_ascii=False,indent=2))
        print("FOODRADAR_DIRECT_WEB_PROJECTION="+json.dumps({
            "bytes":packed["bytes"],"cards":packed["web_products"],"checksum":packed["checksum_round_trip"]}),flush=True)
        manifest=publish(None,None,dest/"static-public",direct=state,projection=projection)
        verify(dest/"static-public")
        print("FOODRADAR_STATIC_BUILD="+json.dumps({"batch":batch,"counts":manifest["counts"],
            "bytes":manifest["bytes"],"revision":manifest["data_revision"]},ensure_ascii=False),flush=True)
        for forbidden in (ROOT/"data/raw-only-reconstructed.db",dest/"serving.db",dest/"packed-serving.db"):
            if forbidden.exists():raise ValueError("Forbidden full database produced: "+str(forbidden))
        save_direct(state,dest/"raw-manifest-index.json")
        metrics={**state.metrics(),"static_bytes":manifest["bytes"],
                 "projection_compressed_bytes":packed["bytes"],"seconds":round(perf_counter()-started,2),
                 "verified_product_cards":manifest["counts"]["products"]}
        (dest/"direct-db-report.json").write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
        print("FOODRADAR_DIRECT_RELEASE="+json.dumps(metrics,ensure_ascii=False),flush=True)
    finally:state.close()

if __name__=="__main__":main()
