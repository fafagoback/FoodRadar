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
from build_static_catalog import build as publish
from audit_store_identity import audit_stores
from audit_deal_pipeline import report as audit_deals

def main():
    started=perf_counter()
    state=restore_history(json.loads((ROOT/"config.json").read_text(encoding="utf-8")),
        rebuild_all=os.environ.get("FOODRADAR_REBUILD_VALID_PRICE_HISTORY")=="1")
    projection=None
    marks={}
    def mark(name,t):marks[name]=round(perf_counter()-t,3)
    mark("raw_to_db_seconds",started)
    try:
        batch=state.meta["latest_batch"]
        dest=ROOT/"data"/batch
        if os.environ.get("FOODRADAR_PERSIST_REPLAY_PROGRESS") == "1":
            # Store verified database independently of website build/hosting.
            from huggingface_hub import HfApi
            from hf_retention import persist
            t=perf_counter()
            save_direct(state,dest/"raw-manifest-index.json")
            persist(HfApi(token=os.environ.get("HF_TOKEN")))
            mark("checkpoint_before_website_seconds",t)
        t=perf_counter();kinds=Counter()
        if state.weekly_batch==batch and state.weekly_quote_kinds is not None:
            kinds.update(state.weekly_quote_kinds)
        else:
            for quotes in state.quote_shops():kinds.update(q[5] for q in quotes)
        # A legitimate batch need not contain every promotion kind.
        if not kinds or set(kinds)-{"NONE","SAME_SKU_BXGY","DIRECT_DISCOUNT"} or any(n<=0 for n in kinds.values()):
            raise ValueError("Missing or invalid native price evidence")
        mark("native_kind_audit_seconds",t);t=perf_counter()
        audit=audit_deals(direct=state,reuse_weekly_evidence=True)
        mark("deal_audit_seconds",t);t=perf_counter()
        print("V2_PREPUBLICATION="+json.dumps({"batch":batch,"verified_types":dict(kinds),
            "invalid_deals":len(audit["errors"])},ensure_ascii=False),flush=True)
        identity=audit_stores(direct=state)
        (dest/"store-identity-report.json").write_text(json.dumps(identity,ensure_ascii=False,indent=2))
        print("FOODRADAR_STORE_HISTORY_AUDIT="+json.dumps(identity,ensure_ascii=False),flush=True)
        mark("store_identity_audit_seconds",t);t=perf_counter()
        projection=project(None,None,static_only=True,direct=state)
        packed=projection.validation_report
        mark("card_projection_seconds",t);t=perf_counter()
        (ROOT/"packed-report.json").write_text(json.dumps(packed,ensure_ascii=False,indent=2))
        print("FOODRADAR_DIRECT_WEB_PROJECTION="+json.dumps({
            "bytes":packed["bytes"],"cards":packed["web_products"],"checksum":packed["checksum_round_trip"]}),flush=True)
        manifest=publish(None,None,dest/"static-public",direct=state,projection=projection,
                         cache_dir=ROOT/"data/static-build-cache")
        mark("static_build_seconds",t);t=perf_counter()
        print("FOODRADAR_STATIC_BUILD="+json.dumps({"batch":batch,"counts":manifest["counts"],
            "bytes":manifest["bytes"],"revision":manifest["data_revision"]},ensure_ascii=False),flush=True)
        for forbidden in (ROOT/"data/raw-only-reconstructed.db",dest/"serving.db",dest/"packed-serving.db"):
            if forbidden.exists():raise ValueError("Forbidden full database produced: "+str(forbidden))
        if os.environ.get("FOODRADAR_PERSIST_REPLAY_PROGRESS") != "1":
            save_direct(state,dest/"raw-manifest-index.json")
        mark("checkpoint_save_seconds",t)
        print("FOODRADAR_RELEASE_PHASE_TIMING="+json.dumps(marks),flush=True)
        summary=os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary,"a",encoding="utf-8") as out:
                out.write("\n### 第16台發布細部耗時\n| 階段 | 秒 |\n|---|---:|\n")
                for key,value in marks.items():out.write(f"| {key} | {value} |\n")
        metrics={**state.metrics(),"static_bytes":manifest["bytes"],
                 "projection_spool_bytes":packed["bytes"],"phase_seconds":marks,"static_timing":projection.static_timing,"seconds":round(perf_counter()-started,2),
                 "verified_product_cards":manifest["counts"]["products"]}
        (dest/"direct-db-report.json").write_text(json.dumps(metrics,ensure_ascii=False,indent=2))
        print("FOODRADAR_DIRECT_RELEASE="+json.dumps(metrics,ensure_ascii=False),flush=True)
    finally:
        if projection is not None:projection.close()
        state.close()

if __name__=="__main__":main()
